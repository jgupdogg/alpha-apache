# dags/tasks/oi_tasks.py

import os
import logging
import time
import json
import asyncio
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime

from utils.nodriver_scraper import WebScraper, scrape_api_data_sync
from utils.scrapers.scraping_utils import extract_getTheBars_response, build_aggregated_dataframe
from db_manager import DBManager

logger = logging.getLogger(__name__)

def _fetch_coinalyze_data(url: str, category: str) -> Dict[str, Any]:
    """
    Use nodriver scraper to capture the 'getTheBars' data from Coinalyze.
    
    Args:
        url: URL to scrape
        category: Category of data being scraped (for logging)
        
    Returns:
        Dictionary containing the response data
    """
    logger.info(f"Fetching {category} data from {url}")
    
    # Pattern to match the 'getTheBars' API endpoint
    url_pattern = "getTheBars"
    
    # Use the synchronous wrapper for scrape_api_data
    response_data, _ = scrape_api_data_sync(
        url=url,
        url_pattern=url_pattern,
        headless=True,
        timeout=30
    )
    
    if not response_data or 'body' not in response_data:
        logger.warning(f"Failed to capture {category} API response")
        return {}
    
    try:
        # Parse the response body as JSON
        response_json = json.loads(response_data['body'])
        return response_json
    except Exception as e:
        logger.error(f"Error parsing API response for {category}: {e}")
        return {}

def async_extract_getTheBars(url: str) -> List[Dict[str, Any]]:
    """
    Use nodriver to capture getTheBars network responses.
    """
    # Create an async function to run in an event loop
    async def _extract():
        scraper = None
        try:
            scraper = WebScraper(headless=True)
            await scraper.start()
            
            # Navigate to URL and wait for data to load
            tab = await scraper.navigate(url)
            await scraper.wait(3)  # Wait 3 seconds for data to load
            
            # Set up monitoring for network requests
            tab.add_listener("Network.responseReceived", lambda msg: print(f"Response received: {msg.get('response', {}).get('url', '')}"))
            
            # Extract data from network logs
            logs = await tab.get_log("performance")
            getthebars_data = []
            
            for entry in logs:
                try:
                    log_msg = json.loads(entry["message"])["message"]
                except json.JSONDecodeError:
                    continue
                
                if log_msg.get("method") == "Network.responseReceived":
                    response_url = log_msg["params"]["response"]["url"]
                    if "getTheBars" in response_url:
                        request_id = log_msg["params"]["requestId"]
                        try:
                            body_data = await tab.send("Network.getResponseBody", {"requestId": request_id})
                            body = body_data.get("body", "")
                            if body_data.get("base64Encoded"):
                                body = base64.b64decode(body).decode("utf-8")
                            json_data = json.loads(body)
                            getthebars_data.append(json_data)
                        except Exception as e:
                            logger.error(f"Error extracting response: {e}")
            
            return getthebars_data
        except Exception as e:
            logger.error(f"Error in browser automation: {e}")
            return []
        finally:
            if scraper:
                await scraper.stop()
    
    # Run the async function in a new event loop
    return asyncio.run(_extract())

def extract_coinalyze_data(target_urls: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Extract data from Coinalyze for multiple categories.
    
    Args:
        target_urls: List of dictionaries with 'url' and 'category' keys
        
    Returns:
        Dictionary with data for each category
    """
    results = {}
    
    for item in target_urls:
        url = item['url']
        category = item['category']
        
        # Use the scraper to get data
        data = _fetch_coinalyze_data(url, category)
        
        if not data:
            logger.warning(f"No data found for {category}")
            results[category] = {}
            continue
            
        # Extract bar data from the response
        if 'barData' in data:
            bar_data = data.get('barData', [])
            results[category] = bar_data
        else:
            logger.warning(f"No 'barData' found in response for {category}")
            results[category] = {}
    
    # For price, we might need a separate approach
    if 'price' not in results:
        # Assuming price comes from a similar endpoint
        price_url = "https://coinalyze.net/bitcoin/price/"
        price_data = _fetch_coinalyze_data(price_url, "price")
        if price_data and 'barData' in price_data:
            results['price'] = price_data.get('barData', [])
        else:
            results['price'] = []
    
    return results