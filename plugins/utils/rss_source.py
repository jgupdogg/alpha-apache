import feedparser
import asyncio
import logging
from datetime import datetime
from typing import List, Optional
import hashlib
from bs4 import BeautifulSoup
import aiohttp
import os
import sys

# Add plugins directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
plugins_dir = os.path.abspath(os.path.join(current_dir, '../../plugins'))
if plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)

from utils.document import Document

logger = logging.getLogger("rss_processor")

class RSSSource:
    """Handles fetching and processing RSS feeds."""
    
    def __init__(self, name: str, url: str, source_name: str = None, subsource_name: str = None):
        self.name = name
        self.url = url
        self.source_name = source_name or name
        self.subsource_name = subsource_name
        
    async def fetch_feed(self) -> dict:
        """Fetch the RSS feed."""
        logger.info(f"Fetching RSS feed: {self.url}")
        feed = feedparser.parse(self.url)
        return feed
        
    async def fetch_content(self, url: str) -> str:
        """Fetch full content from article URL."""
        logger.info(f"Fetching content: {url}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Parse content with BeautifulSoup
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Try to find main content with common selectors
                        main_content = soup.find('main') or soup.find('article') or soup.find('div', {'id': 'content'})
                        
                        if main_content:
                            content = main_content.get_text(' ', strip=True)
                        else:
                            # Use body as fallback
                            body = soup.find('body')
                            content = body.get_text(' ', strip=True) if body else ''
                        
                        if not content or len(content.strip()) < 100:
                            logger.warning(f"Minimal content extracted: {url}")
                        
                        return content
                    else:
                        logger.error(f"HTTP error: {response.status}")
                        
        except Exception as e:
            logger.error(f"Error fetching content: {str(e)}")
        
        return ""
    
    async def process_feed(self, limit: int = None, supabase_hook=None) -> List[Document]:
        """
        Process the RSS feed and create Document objects for new items.
        
        Args:
            limit: Optional maximum number of entries to process
            supabase_hook: Optional database hook
            
        Returns:
            List of Document objects
        """
        feed = await self.fetch_feed()
        new_documents = []
        
        # Apply limit if specified
        entries = feed.entries
        if limit and limit > 0 and len(entries) > limit:
            entries = entries[:limit]
        
        logger.info(f"Processing {len(entries)} items from feed: {self.name}")
        
        for entry in entries:
            # Extract data from feed entry
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
                    # RSS feed dates often follow RFC 822 format
                    from email.utils import parsedate_to_datetime
                    pub_date = parsedate_to_datetime(entry.pubDate)
                except:
                    logger.warning(f"Could not parse date: {entry.pubDate}")
            
            # Check if document already exists
            if supabase_hook:
                existing_doc = Document.find_by_url(url, supabase_hook)
                if existing_doc:
                    logger.info(f"Document already exists: {url}")
                    continue
            
            # Fetch full content
            content = await self.fetch_content(url)
            
            if not content:
                logger.warning(f"No content found: {url}")
                continue
            
            # Calculate content hash
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
            
            # Create document
            doc = Document(
                url=url,
                title=title,
                content=content,
                source_name=self.source_name,
                subsource_name=self.subsource_name,
                scrape_time=datetime.now(),
                content_hash=content_hash,
                status="scraped",
                category=category,
                pub_date=pub_date
            )
            
            # Save to database if hook provided
            if supabase_hook:
                doc.save(supabase_hook)
            
            new_documents.append(doc)
            logger.info(f"Created document: {url}")
        
        return new_documents


class RSSFeedManager:
    """Manages multiple RSS sources and coordinates processing."""
    
    def __init__(self):
        self.sources = {}
    
    def add_source(self, name: str, url: str, source_name: str = None, subsource_name: str = None):
        """Add an RSS source to the manager."""
        self.sources[name] = RSSSource(name, url, source_name, subsource_name)
        
    async def process_source(self, source_name: str, limit: int = None, supabase_hook=None) -> List[Document]:
        """
        Process a specific source.
        
        Args:
            source_name: Name of the source to process
            limit: Optional maximum number of entries to process
            supabase_hook: Optional database hook
            
        Returns:
            List of Document objects
        """
        if source_name not in self.sources:
            logger.error(f"Source not found: {source_name}")
            return []
            
        source = self.sources[source_name]
        try:
            documents = await source.process_feed(limit=limit, supabase_hook=supabase_hook)
            return documents
        except Exception as e:
            logger.error(f"Error processing feed {source_name}: {str(e)}")
            return []
        
    async def process_all_feeds(self, limit: int = None, supabase_hook=None) -> List[Document]:
        """
        Process all registered feeds.
        
        Args:
            limit: Optional maximum number of entries to process per source
            supabase_hook: Optional database hook
            
        Returns:
            List of Document objects
        """
        all_documents = []
        
        for source_name in self.sources:
            documents = await self.process_source(source_name, limit, supabase_hook)
            all_documents.extend(documents)
        
        return all_documents