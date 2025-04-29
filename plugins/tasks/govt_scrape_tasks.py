

from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from airflow.decorators import task
from utils.document import Document, DocumentService

logger = logging.getLogger(__name__)


@task
def verify_supabase_tables(supabase_conn_id: str = 'supabase_default') -> bool:
    """Verifies the existence of required tables in Supabase."""
    from airflow.exceptions import AirflowException
    from hooks.supabase_hook import SupabaseHook
    
    logger.info("Verifying Supabase tables exist")
    hook = SupabaseHook()
    
    if hook.setup_tables():
        logger.info("All required Supabase tables exist")
        return True
    else:
        error_msg = "Required tables don't exist in Supabase. Please run setup_supabase.py."
        logger.error(error_msg)
        raise AirflowException(error_msg)


@task
def load_sources(
    config_path: str,
    supabase_conn_id: str = 'supabase_default'
) -> List[Dict[str, Any]]:
    """Task to load sources from configuration."""
    from hooks.supabase_hook import SupabaseHook
    from plugins.utils.rss_source import SourceService
    
    logger.info(f"Loading sources from config: {config_path}")
    
    supabase_hook = SupabaseHook(supabase_conn_id)
    service = SourceService(supabase_hook=supabase_hook)
    
    sources = service.load_sources_from_config(config_path)
    
    # Save sources to database
    source_dicts = []
    for source in sources:
        source_id = service.save_source(source)
        if source_id:
            # Save subsources
            for subsource in source.get_subsources():
                subsource.source_id = source_id
                service.save_subsource(subsource)
            
            source_dicts.append(source.to_dict())
    
    logger.info(f"Loaded and saved {len(source_dicts)} sources")
    return source_dicts

@task
def scrape_source_documents(
    source_dict: str,  # This will be a string from XCom due to templating
    supabase_conn_id: str = 'supabase_default',
    headless: bool = True,
    limit: int = 2
) -> List[str]:
    """Task to scrape documents from sources."""
    from hooks.supabase_hook import SupabaseHook
    from plugins.utils.rss_source import Source, SourceService
    from utils.document import DocumentService
    from utils.nodriver_scraper import WebScraper
    import asyncio
    import json
    
    # Parse the JSON string if it's a string
    if isinstance(source_dict, str):
        try:
            source_dict = json.loads(source_dict)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse source_dict as JSON: {source_dict}")
            return []
    
    logger.info(f"Scraping documents from {len(source_dict)} sources")
    
    # Initialize hooks and services
    supabase_hook = SupabaseHook(supabase_conn_id)
    document_service = DocumentService(supabase_hook=supabase_hook)
    
    # Process each source
    all_doc_ids = []
    
    for source_data in source_dict:
        # Initialize source
        source = Source.from_dict(source_data)
        
        # Fetch subsources from database
        supabase = supabase_hook.get_conn()
        subsource_result = supabase.table("govt_subsources").select("*").eq("source_id", source.source_id).execute()
        
        if not subsource_result.data:
            logger.info(f"No subsources found for source: {source.name}")
            continue
        
        # Initialize subsources
        from plugins.utils.rss_source import Subsource
        subsources = [Subsource.from_dict(data) for data in subsource_result.data]
        
        # Initialize scraper and source service
        async def scrape_all_subsources():
            # Initialize scraper
            scraper = WebScraper(
                headless=headless,
                user_data_dir='./profiles',
                profile_name='document_scraper',
                use_virtual_display=False
            )
            
            source_service = SourceService(
                supabase_hook=supabase_hook,
                scraper=scraper
            )
            
            try:
                # Start scraper
                await scraper.start()
                
                # Scrape documents from each subsource
                subsource_doc_ids = []
                for subsource in subsources:
                    doc_ids = await source_service.scrape_documents_from_subsource(
                        subsource=subsource,
                        document_service=document_service,
                        limit=limit
                    )
                    subsource_doc_ids.extend(doc_ids)
                
                return subsource_doc_ids
            finally:
                # Stop scraper
                await scraper.stop()
        
        # Run async function
        doc_ids = asyncio.run(scrape_all_subsources())
        logger.info(f"Scraped {len(doc_ids)} documents from source: {source.name}")
        all_doc_ids.extend(doc_ids)
    
    logger.info(f"Successfully scraped {len(all_doc_ids)} documents total")
    return all_doc_ids

@task
def coordinate_scraping(
    config_path: str,
    supabase_conn_id: str = 'supabase_default',
    headless: bool = True,
    limit: int = 2
) -> List[str]:
    """Coordinator task for document scraping workflow."""
    # Step 1: Load sources
    sources = load_sources(
        config_path=config_path,
        supabase_conn_id=supabase_conn_id
    )
    
    if not sources:
        logger.info("No sources to scrape")
        return []
    
    logger.info(f"Scraping from {len(sources)} sources")
    all_doc_ids = []
    
    # Step 2: Scrape documents from each source
    for source in sources:
        doc_ids = scrape_source_documents(
            source_dict=source,
            supabase_conn_id=supabase_conn_id,
            headless=headless,
            limit=limit
        )
        all_doc_ids.extend(doc_ids)
    
    logger.info(f"Successfully scraped {len(all_doc_ids)} documents total")
    return all_doc_ids


@task
def fetch_documents(
    doc_ids: Optional[List[str]] = None,
    supabase_conn_id: str = 'supabase_default',
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Task to fetch documents for processing."""
    from hooks.supabase_hook import SupabaseHook

    supabase_hook = SupabaseHook()
    service = DocumentService(supabase_hook=supabase_hook)

    documents = service.fetch_documents(doc_ids=doc_ids, limit=limit)
    return [doc.to_dict() for doc in documents]


@task
def process_document(
    doc_dict: Dict[str, Any],
    supabase_conn_id: str = 'supabase_default',
    langchain_conn_id: str = 'anthropic_default',
    pinecone_conn_id: str = 'pinecone_default',
    neo4j_conn_id: str = 'neo4j_default',
    enable_kg: bool = True
) -> Dict[str, Any]:
    """Task to process a single document."""
    from hooks.supabase_hook import SupabaseHook
    from hooks.langchain_hook import LangChainHook
    from hooks.pinecone_hook import PineconeHook
    from hooks.neo4j_hook import Neo4jHook

    # Initialize hooks
    supabase_hook = SupabaseHook()
    langchain_hook = LangChainHook()
    pinecone_hook = PineconeHook()

    # Initialize Neo4j hook if needed
    neo4j_hook = Neo4jHook(neo4j_conn_id) if enable_kg else None

    # Create service
    service = DocumentService(
        supabase_hook=supabase_hook,
        langchain_hook=langchain_hook,
        pinecone_hook=pinecone_hook,
        neo4j_hook=neo4j_hook
    )

    # Process document
    doc = Document.from_dict(doc_dict)
    processed_doc = service.process_document(doc, enable_kg=enable_kg)

    # Close Neo4j connection if opened
    if neo4j_hook:
        neo4j_hook.close()

    return processed_doc.to_dict()


@task
def coordinate_document_processing(
    doc_ids: Optional[str] = None,  # This could be a string from XCom
    supabase_conn_id: str = 'supabase_default',
    langchain_conn_id: str = 'anthropic_default',
    pinecone_conn_id: str = 'pinecone_default',
    neo4j_conn_id: str = 'neo4j_default',
    limit: int = 20,
    enable_kg: bool = True
) -> List[str]:
    """Coordinator task for document processing workflow."""
    import json
    
    # Parse doc_ids from JSON string if needed
    if doc_ids and isinstance(doc_ids, str):
        try:
            doc_ids = json.loads(doc_ids)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse doc_ids as JSON: {doc_ids}")
            doc_ids = None
    
    # Step 1: Fetch documents
    documents = fetch_documents(
        doc_ids=doc_ids,
        supabase_conn_id=supabase_conn_id,
        limit=limit
    )
    
    if not documents:
        logger.info("No documents to process")
        return []
    
    logger.info(f"Processing {len(documents)} documents")
    processed_doc_ids = []
    
    # Step 2: Process each document
    for doc_dict in documents:
        processed_doc = process_document(
            doc_dict=doc_dict,
            supabase_conn_id=supabase_conn_id,
            langchain_conn_id=langchain_conn_id,
            pinecone_conn_id=pinecone_conn_id,
            neo4j_conn_id=neo4j_conn_id,
            enable_kg=enable_kg
        )
        
        # Add to processed IDs
        doc_id = processed_doc.get('id') or processed_doc.get('doc_id')
        if doc_id:
            processed_doc_ids.append(doc_id)
    
    logger.info(f"Successfully processed {len(processed_doc_ids)} documents")
    return processed_doc_ids