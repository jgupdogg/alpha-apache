from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import hashlib
import asyncio
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class Document:
    """Represents a document with its metadata, content, and processing capabilities."""
    
    def __init__(
        self,
        doc_id: str = None,
        url: str = None,
        title: str = None,
        content: str = None,
        summary: str = None,
        source_name: str = None,
        subsource_name: str = None,
        status: str = "new",
        embedding_id: str = None,
        scrape_time: datetime = None,
        process_time: datetime = None,
        content_hash: str = None,
        last_checked: datetime = None,
        **kwargs
    ):
        self.id = doc_id
        self.url = url
        self.title = title
        self.content = content
        self.summary = summary
        self.source_name = source_name
        self.subsource_name = subsource_name
        self.status = status
        self.embedding_id = embedding_id
        self.scrape_time = scrape_time
        self.process_time = process_time
        self.content_hash = content_hash
        self.last_checked = last_checked
        
        # Store any additional fields
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Document':
        """Create a Document from a dictionary."""
        # Convert string dates to datetime objects if needed
        for date_field in ['scrape_time', 'process_time', 'last_checked']:
            if date_field in data and isinstance(data[date_field], str):
                try:
                    data[date_field] = datetime.fromisoformat(data[date_field].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    data[date_field] = None
        
        # Use id as doc_id if present
        if 'id' in data and 'doc_id' not in data:
            data['doc_id'] = data.pop('id')
            
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Document to a dictionary."""
        result = {}
        # Get all attributes
        for key, value in self.__dict__.items():
            # Convert datetime objects to ISO format strings
            if isinstance(value, datetime):
                value = value.isoformat()
            result[key] = value
        
        # If doc_id exists, also set it as 'id' for compatibility
        if 'doc_id' in result and result['doc_id']:
            result['id'] = result['doc_id']
            
        return result
    
    # --- Scraping methods ---
    
    async def fetch_content(self, scraper) -> bool:
        """Fetch document content using the web scraper."""
        logger.info(f"Fetching content from URL: {self.url}")
        
        try:
            # Navigate to the URL
            tab = await scraper.navigate(self.url)
            if not tab:
                logger.error(f"Failed to navigate to {self.url}")
                self.status = "error_navigation"
                return False
            
            # Get page content
            html = await scraper.get_page_content()
            
            if not html:
                logger.error(f"No HTML content retrieved from {self.url}")
                self.status = "error_no_html"
                return False
            
            # Extract content using BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try to find main content with common selectors
            main_content = soup.find('main') or soup.find('article') or soup.find('div', {'id': 'content'})
            
            if main_content:
                self.content = main_content.get_text(' ', strip=True)
            else:
                # Use body as fallback
                body = soup.find('body')
                self.content = body.get_text(' ', strip=True) if body else ''
            
            # Check if content was extracted
            if not self.content or len(self.content.strip()) == 0:
                logger.warning(f"No content extracted from {self.url}")
                self.status = "error_no_content"
                return False
            
            # Update document metadata
            self.scrape_time = datetime.now()
            self.last_checked = datetime.now()
            self.content_hash = hashlib.md5(self.content.encode('utf-8')).hexdigest()
            self.status = "scraped"
            
            logger.info(f"Successfully fetched content from {self.url}")
            return True
            
        except Exception as e:
            logger.error(f"Error fetching content for {self.url}: {str(e)}", exc_info=True)
            self.status = "error_exception"
            return False
    
    async def scrape(self, scraper):
        """Scrape document content using the WebScraper."""
        try:
            success = await self.fetch_content(scraper)
            return success
        except Exception as e:
            logger.error(f"Error during document scraping: {str(e)}", exc_info=True)
            self.status = "error_exception"
            return False
    
    # --- Processing methods ---
    
    def generate_summary(self, langchain_hook):
        """Generate summary for the document."""
        if not self.summary:
            self.summary = langchain_hook.generate_summary(self)
            logger.info(f"Generated summary for document: {self.url}")
        return self
    
    def create_embedding(self, pinecone_hook):
        """Create embedding for the document."""
        self.embedding_id = pinecone_hook.store_embedding(self)
        self.status = "embeddings_created"
        logger.info(f"Created and stored embedding for document: {self.url}")
        return self
    
    def process_knowledge_graph(self, neo4j_hook, langchain_hook):
        """Process knowledge graph for the document."""
        from utils.entity_extractor import EntityExtractor
        
        logger.info(f"Processing knowledge graph for document: {self.url}")
        
        try:
            # Setup Neo4j constraints
            neo4j_hook.setup_constraints()
            
            # Initialize entity extractor and extract entities
            entity_extractor = EntityExtractor(langchain_hook.get_llm())
            extraction_result = entity_extractor.extract_entities(self)
            
            if extraction_result.get("entities") or extraction_result.get("relationships"):
                # Add document to Neo4j
                self._add_to_graph(neo4j_hook, extraction_result)
                logger.info(f"Added document and entities to knowledge graph: {self.url}")
            
            # Update document status
            self.status = "processed"
            return self
            
        except Exception as e:
            logger.error(f"Error processing knowledge graph for {self.url}: {e}", exc_info=True)
            self.status = "error_processing"
            return self
    
    def _add_to_graph(self, neo4j_hook, extraction_result):
        """Add document, entities and relationships to graph database."""
        # Add document node
        neo4j_hook.run_query("""
        MERGE (d:Document {url: $url})
        ON CREATE SET 
            d.title = $title,
            d.source_name = $source_name,
            d.subsource_name = $subsource_name,
            d.created_at = $created_at,
            d.doc_id = $doc_id
        ON MATCH SET
            d.title = $title,
            d.updated_at = $updated_at
        """, {
            "url": self.url,
            "title": self.title,
            "source_name": self.source_name,
            "subsource_name": self.subsource_name,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "doc_id": str(self.id)
        })
        
        # Add entities
        for entity in extraction_result.get("entities", []):
            if not entity.get("canonical_name") or not entity.get("entity_type"):
                continue
                
            # Create entity node
            neo4j_hook.run_query("""
            MERGE (e:Entity {canonical_name: $canonical_name})
            ON CREATE SET 
                e.type = $entity_type,
                e.created_at = $created_at
            ON MATCH SET
                e.type = $entity_type,
                e.updated_at = $updated_at
            """, {
                "canonical_name": entity["canonical_name"],
                "entity_type": entity["entity_type"],
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            })
            
            # Create mention and relationships
            mention_text = entity.get("mention", entity["canonical_name"])
            neo4j_hook.run_query("""
            MATCH (e:Entity {canonical_name: $canonical_name})
            MATCH (d:Document {url: $doc_url})
            MERGE (m:Mention {
                text: $mention,
                document_url: $doc_url
            })
            MERGE (m)-[:REFERS_TO]->(e)
            MERGE (m)-[:APPEARS_IN]->(d)
            """, {
                "canonical_name": entity["canonical_name"],
                "mention": mention_text,
                "doc_url": self.url
            })
        
        # Add relationships
        for relationship in extraction_result.get("relationships", []):
            if not relationship.get("source_canonical") or not relationship.get("target_canonical"):
                continue
                
            relation = relationship.get("relation", "RELATED_TO")
            neo4j_hook.run_query(
                f"""
                MATCH (source:Entity {{canonical_name: $source_canonical}})
                MATCH (target:Entity {{canonical_name: $target_canonical}})
                MERGE (source)-[r:{relation}]->(target)
                SET r.updated_at = $updated_at,
                    r.document_url = $doc_url,
                    r.document_id = $doc_id
                """, {
                    "source_canonical": relationship["source_canonical"],
                    "target_canonical": relationship["target_canonical"],
                    "doc_url": self.url,
                    "doc_id": str(self.id) if self.id else None,
                    "updated_at": datetime.now().isoformat()
                }
            )
    
    
    def process(self, langchain_hook, pinecone_hook, neo4j_hook=None, supabase_hook=None, enable_kg=True) -> 'Document':
        """Process document through summarization, embeddings, and knowledge graph."""
        if not langchain_hook or not pinecone_hook:
            raise ValueError("Required hooks not provided")
            
        logger.info(f"Processing document: {self.url}")
        
        try:
            # Skip if already processed
            if self.embedding_id and self.status == "processed":
                logger.info(f"Document already processed with embedding_id: {self.embedding_id}")
                return self
            
            # Generate summary
            self.generate_summary(langchain_hook)
            
            # Create embedding
            self.create_embedding(pinecone_hook)
            
            # Process knowledge graph if enabled
            if enable_kg and neo4j_hook:
                self.process_knowledge_graph(neo4j_hook, langchain_hook)
            
            # Update processing time
            self.process_time = datetime.now()
            
            # Update document in database if supabase_hook provided
            if supabase_hook:
                self.save(supabase_hook)
            
            return self
            
        except Exception as e:
            logger.error(f"Error processing document {self.url}: {e}", exc_info=True)
            
            # Update document status to error
            self.status = "error_processing"
            if supabase_hook:
                try:
                    self.save(supabase_hook)
                except Exception as update_err:
                    logger.error(f"Error updating document status: {update_err}")
                
            return self
    # --- Database interaction methods ---
    
    def save(self, supabase_hook):
        """Insert or update document in the database."""
        doc_dict = self.to_dict()
        now = datetime.now().isoformat()
        doc_dict["updated_at"] = now
        
        supabase = supabase_hook.get_conn()
        
        if self.id:
            # Update existing document
            result = supabase.table("govt_documents").update(doc_dict).eq("id", self.id).execute()
            logger.info(f"Updated document in database: {self.url}")
        else:
            # Insert new document
            doc_dict["created_at"] = now
            # Remove ID for insertion
            doc_dict.pop("id", None)
            
            result = supabase.table("govt_documents").insert(doc_dict).execute()
            
            if result.data and len(result.data) > 0:
                self.id = result.data[0]["id"]
                logger.info(f"Document stored with ID: {self.id}")
            else:
                logger.error(f"Failed to store document in database: {self.url}")
                
        return self.id
    

    
    @classmethod
    def find_by_url(cls, url: str, supabase_hook) -> Optional['Document']:
        """Find a document by URL in the database."""
        supabase = supabase_hook.get_conn()
        result = supabase.table("govt_documents").select("*").eq("url", url).execute()
        
        logger.info(f"Document search result: {result}")
        
        if result.data and len(result.data) > 0:
            return cls.from_dict(result.data[0])
        return None
    
    @classmethod
    def find_by_id(cls, doc_id: str, supabase_hook) -> Optional['Document']:
        """Find a document by ID in the database."""
        supabase = supabase_hook.get_conn()
        result = supabase.table("govt_documents").select("*").eq("id", doc_id).execute()
        
        if result.data and len(result.data) > 0:
            return cls.from_dict(result.data[0])
        return None
    
    @classmethod
    def find_unprocessed(cls, limit: int, supabase_hook) -> List['Document']:
        """Find unprocessed documents in the database."""
        supabase = supabase_hook.get_conn()
        result = supabase.table("govt_documents").select("*").eq(
            "status", "scraped"
        ).order("scrape_time").limit(limit).execute()
        
        if result.data:
            return [cls.from_dict(doc_data) for doc_data in result.data]
        return []
    
    
