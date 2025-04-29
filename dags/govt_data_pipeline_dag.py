# dags/rss_document_processing_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
import logging
import json
import os
import asyncio
from typing import List, Dict, Any, Optional

# Import custom utility classes and hooks
from utils.document import Document
from utils.rss_source import RSSSource, RSSFeedManager
from hooks.supabase_hook import SupabaseHook
from hooks.langchain_hook import LangChainHook
from hooks.pinecone_hook import PineconeHook
from hooks.neo4j_hook import Neo4jHook

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

@task
def load_and_process_rss_feeds(
    config_path: str = 'plugins/config/rss_config.json',
    supabase_conn_id: str = 'supabase_default',
    limit: int = 10
) -> List[str]:
    """Task to load RSS feeds and process documents."""
    logger.info(f"Loading RSS feeds from config: {config_path}")
    
    # Check if config file exists
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        return []
    
    # Load config
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        
        sources = config_data.get('sources', [])
        logger.info(f"Loaded {len(sources)} sources from config")
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        return []
    
    # Initialize Supabase hook
    try:
        supabase_hook = SupabaseHook(conn_id=supabase_conn_id)
    except Exception as e:
        logger.error(f"Error initializing Supabase hook: {str(e)}")
        return []
    
    # Initialize RSS Feed Manager
    feed_manager = RSSFeedManager()
    
    # Register sources with the feed manager
    for source in sources:
        name = source.get('name', '')
        url = source.get('url', '')
        source_name = source.get('source_name', name)
        subsource_name = source.get('subsource', '')
        
        if not url:
            logger.warning(f"Missing URL for source: {name}")
            continue
            
        feed_manager.add_source(name, url, source_name, subsource_name)
    
    # Custom document handler to ensure all fields are correctly populated
    async def custom_document_handler(rss_source, entry, content):
        """Create a document with all required fields properly set."""
        now = datetime.now()
        
        # Extract basic metadata
        title = entry.get('title', '')
        url = entry.get('link', '').strip()
        description = entry.get('description', '')
        category = entry.get('category', '') if hasattr(entry, 'category') else ''
        
        # Parse publication date
        pub_date = None
        if hasattr(entry, 'published_parsed'):
            pub_date = datetime(*entry.published_parsed[:6])
        elif hasattr(entry, 'pubDate'):
            try:
                from email.utils import parsedate_to_datetime
                pub_date = parsedate_to_datetime(entry.pubDate)
            except:
                logger.warning(f"Could not parse date: {entry.pubDate}")
        
        # Calculate content hash
        import hashlib
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        
        # Create document with all required fields
        doc = Document(
            url=url,
            title=title,
            content=content,
            source_name=rss_source.source_name,
            subsource_name=rss_source.subsource_name,
            content_hash=content_hash,
            summary=None,  # Will be populated in processing step
            embedding_id=None,  # Will be populated in processing step
            status="scraped",
            scrape_time=now,
            process_time=None,  # Will be populated in processing step
            last_checked=now,
            created_at=now.isoformat(),
            updated_at=now.isoformat()
        )
        
        # Add any additional metadata
        if pub_date:
            doc.pub_date = pub_date
        if category:
            doc.category = category
        
        return doc
    
    # Process all feeds and get documents
    try:
        # Set the custom document handler in the feed manager
        feed_manager.document_handler = custom_document_handler
        
        all_docs = asyncio.run(feed_manager.process_all_feeds(
            limit=limit, 
            supabase_hook=supabase_hook
        ))
        
        # Extract document IDs
        doc_ids = [doc.id for doc in all_docs if doc.id]
        logger.info(f"Processed {len(doc_ids)} documents")
        return doc_ids
    except Exception as e:
        logger.error(f"Error processing feeds: {str(e)}")
        return []

@task
def process_documents(
    doc_ids: List[str],
    supabase_conn_id: str = 'supabase_default',
    anthropic_conn_id: str = 'anthropic_default',
    pinecone_conn_id: str = 'pinecone_default',
    openai_conn_id: str = 'openai_default',
    neo4j_conn_id: str = 'neo4j_default',
    enable_kg: bool = True,
    batch_size: int = 5
) -> List[str]:
    """Task to process documents using Document class methods."""
    if not doc_ids:
        logger.info("No documents to process")
        return []
        
    logger.info(f"Processing {len(doc_ids)} documents")
    
    # Initialize hooks
    try:
        supabase_hook = SupabaseHook(conn_id=supabase_conn_id)
        langchain_hook = LangChainHook(anthropic_conn_id=anthropic_conn_id, openai_conn_id=openai_conn_id)
        pinecone_hook = PineconeHook(conn_id=pinecone_conn_id)
        neo4j_hook = Neo4jHook(conn_id=neo4j_conn_id) if enable_kg else None
    except Exception as e:
        logger.error(f"Error initializing hooks: {str(e)}")
        return []
    
    processed_doc_ids = []
    
    # Process in batches to avoid memory issues
    for i in range(0, len(doc_ids), batch_size):
        batch = doc_ids[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(doc_ids)-1)//batch_size + 1}, size: {len(batch)}")
        
        for doc_id in batch:
            try:
                # Find document by ID
                doc = Document.find_by_id(doc_id, supabase_hook)
                
                if not doc:
                    logger.error(f"Document not found: {doc_id}")
                    continue
                
                # Skip if already processed
                if doc.embedding_id and doc.status == "processed":
                    logger.info(f"Document already processed: {doc.url}")
                    processed_doc_ids.append(doc_id)
                    continue
                
                # Generate summary if needed
                if not doc.summary:
                    doc.generate_summary(langchain_hook)
                
                # Process document using Document.process method
                # This handles embedding creation and knowledge graph processing
                doc.process(
                    langchain_hook=langchain_hook,
                    pinecone_hook=pinecone_hook,
                    neo4j_hook=neo4j_hook,
                    supabase_hook=supabase_hook,
                    enable_kg=enable_kg
                )
                
                # Update timestamps
                now = datetime.now()
                doc.process_time = now
                doc.updated_at = now.isoformat()
                doc.last_checked = now
                
                # Save to database
                doc.save(supabase_hook)
                
                processed_doc_ids.append(doc_id)
                logger.info(f"Successfully processed document: {doc.url}")
                
            except Exception as e:
                logger.error(f"Error processing document {doc_id}: {str(e)}")
                
                # Update document status to error
                try:
                    doc = Document.find_by_id(doc_id, supabase_hook)
                    if doc:
                        doc.status = "error_processing"
                        doc.updated_at = datetime.now().isoformat()
                        doc.save_to_db(supabase_hook)
                except Exception as update_err:
                    logger.error(f"Error updating document status: {update_err}")
    
    # Close Neo4j connection if opened
    if neo4j_hook:
        try:
            neo4j_hook.close()
        except Exception as e:
            logger.error(f"Error closing Neo4j connection: {str(e)}")
    
    logger.info(f"Successfully processed {len(processed_doc_ids)} documents")
    return processed_doc_ids

# Create DAG
with DAG(
    'rss_document_processing',
    default_args=default_args,
    description='Process documents from RSS feeds',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['rss', 'documents', 'processing'],
) as dag:
    
    # Start task
    start = DummyOperator(task_id='start')
    
    # Load and process RSS feeds
    fetch_rss = load_and_process_rss_feeds(
        config_path='plugins/config/rss_config.json',
        supabase_conn_id='supabase_default',
        limit=10
    )
    
    # Process documents
    process_docs = process_documents(
        doc_ids=fetch_rss,
        supabase_conn_id='supabase_default',
        anthropic_conn_id='anthropic_default',
        openai_conn_id='openai_default',
        pinecone_conn_id='pinecone_default',
        neo4j_conn_id='neo4j_default',
        enable_kg=True,
        batch_size=5
    )
    
    # End task
    end = DummyOperator(task_id='end')
    
    # Define task dependencies
    start >> fetch_rss >> process_docs >> end