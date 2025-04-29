# dags/fng_tasks.py

import os
import logging
import json
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

from utils.nodriver_scraper import WebScraper, scrape_api_data_sync
from db_manager import DBManager

logger = logging.getLogger(__name__)

def _fetch_fear_and_greed_data(url: str = "https://alternative.me/crypto/fear-and-greed-index/"):
    """
    Use nodriver scraper to capture the Fear & Greed Index history API response.
    
    Returns the nested 'data' dict if found, otherwise an empty dict.
    """
    logger.info(f"Fetching Fear & Greed data from {url}")
    
    # Pattern to match the history API endpoint
    url_pattern = "/api/crypto/fear-and-greed-index/history"
    
    # Use the synchronous wrapper for scrape_api_data
    response_data, _ = scrape_api_data_sync(
        url=url,
        url_pattern=url_pattern,
        headless=True,
        timeout=30
    )
    
    if not response_data or 'body' not in response_data:
        logger.warning("Failed to capture Fear & Greed history API response")
        return {}
    
    try:
        # Parse the response body as JSON
        response_json = json.loads(response_data['body'])
        if response_json.get("success") == 1:
            extracted_data = response_json.get("data", {})
            logger.info("Successfully extracted Fear & Greed history data.")
            return extracted_data
        else:
            logger.warning("API response did not indicate success")
            return {}
    except Exception as e:
        logger.error(f"Error parsing API response: {e}")
        return {}

def _parse_fear_and_greed_history(fear_and_greed_data: dict) -> List[Dict[str, Any]]:
    """
    Convert the raw fear_and_greed_data dict into a list of dicts.
    Each dict has { 'timestamp': <str>, 'value': <int> }, where
    'timestamp' is in the format: YYYY-MM-DD HH:MM:SS±hh:mm
    """
    labels = fear_and_greed_data.get("labels", [])
    datasets = fear_and_greed_data.get("datasets", [])

    if not datasets:
        return []

    # Typically there's a single dataset under "datasets[0]"
    values = datasets[0].get("data", [])
    if not labels or not values:
        return []

    parsed_rows = []
    for label_str, val in zip(labels, values):
        # label_str is something like "9 Jan, 2025"
        dt_obj = datetime.strptime(label_str, "%d %b, %Y")

        # Convert dt_obj into a string with a timezone
        dt_with_tz = dt_obj.astimezone()  # local timezone
        iso_str = dt_with_tz.isoformat(timespec='seconds')  
        timestamp_str = iso_str.replace('T', ' ')

        row = {
            "timestamp": timestamp_str,
            "value": val
        }
        parsed_rows.append(row)

    return parsed_rows