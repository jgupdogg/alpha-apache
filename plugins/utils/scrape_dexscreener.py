#!/usr/bin/env python3

import asyncio
import csv
import logging
import os
import argparse
import pandas as pd
import re
from datetime import datetime
from utils.nodriver_scraper import WebScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DexScreenerScraper")

from bs4 import BeautifulSoup

# Utility functions for data transformations
def extract_token_address_from_image(row):
    """Extract token address from token image URL"""
    try:
        # Look for images in the row
        img_elements = row.select('img')
        
        # Check each image to find token images
        for img in img_elements:
            src = img.get('src', '')
            
            # Check if it's a token image URL
            if '/ds-data/tokens/' in src:
                # Parse the URL to extract the token address
                # Format: https://dd.dexscreener.com/ds-data/tokens/solana/F9TgEJLLRUKDRF16HgjUCdJfJ5BK6ucyiW8uJxVPpump.png?key=966d41
                
                # Split by '/' and get the last part
                path_parts = src.split('/')
                if len(path_parts) > 0:
                    # Get the filename (last part of path)
                    filename = path_parts[-1]
                    
                    # Remove .png and any query parameters
                    address = filename.split('.png')[0]
                    if '?' in address:
                        address = address.split('?')[0]
                    
                    return address.lower()  # Return as lowercase for SQL query compatibility
        
        # If no matching image found, return unknown
        return "UNKNOWN"
    except Exception as e:
        # Log the error but don't crash
        logger.error(f"Error extracting token address from image: {e}")
        return "UNKNOWN"

def convert_currency_to_float(text):
    """Convert currency text to float"""
    if text == "N/A" or not text or text == "-":
        return None
    return float(text.replace('$', '').replace(',', ''))

def convert_to_int(text):
    """Convert text with commas to integer"""
    if text == "N/A" or not text or text == "-":
        return None
    return int(text.replace(',', ''))

def convert_suffixed_number(text):
    """Convert number with suffix (K, M, B) to numeric value"""
    if text == "N/A" or not text or text == "-":
        return None
    
    # Remove $ and commas
    text = text.replace('$', '').replace(',', '')
    
    # Define multipliers for suffixes
    multipliers = {'k': 1_000, 'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000, 'T': 1_000_000_000_000}
    
    # Check for suffixes
    for suffix, multiplier in multipliers.items():
        if text.endswith(suffix):
            return float(text[:-1]) * multiplier
    
    # No suffix, just convert to float
    return float(text)

def convert_percentage_to_float(text):
    """Convert percentage text to float"""
    if text == "N/A" or not text or text == "-":
        return None
    return float(text.replace('%', ''))

def convert_age_to_days(age_text):
    """Convert age text (like '1y', '3mo', '5d') to days"""
    if age_text == "N/A" or not age_text or age_text == "-":
        return None
    
    # Extract number and unit
    match = re.match(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', age_text)
    if not match:
        return None
    
    value, unit = float(match.group(1)), match.group(2).lower()
    
    # Convert to days based on unit
    if unit in ('y', 'yr', 'yrs', 'year', 'years'):
        return value * 365
    elif unit in ('mo', 'month', 'months'):
        return value * 30
    elif unit in ('w', 'wk', 'wks', 'week', 'weeks'):
        return value * 7
    elif unit in ('d', 'day', 'days'):
        return value
    elif unit in ('h', 'hr', 'hrs', 'hour', 'hours'):
        return value / 24
    else:
        return None

async def setup_scraper(profile_dir="./profiles", profile_name="dexscreener_auth"):
    """Set up a WebScraper with previously saved cookies"""
    # Create directory if it doesn't exist
    os.makedirs(profile_dir, exist_ok=True)
    
    # Create scraper with dedicated profile
    scraper = WebScraper(
        headless=False,
        user_data_dir=profile_dir,
        profile_name=profile_name
    )
    
    try:
        # Start the browser
        await scraper.start()
        
        # Load cookies if available
        await scraper.load_cookies()
        
        logger.info("Browser started and authenticated session loaded")
        return scraper
        
    except Exception as e:
        logger.error(f"Failed to set up scraper: {e}")
        if scraper:
            await scraper.stop()
        raise

async def scrape_dexscreener_watchlist(
    scraper, 
    watchlist_url='https://dexscreener.com/watchlist/PZ5oeMyixsBce5Tshsqu', 
    max_scrolls=5
    ):
    """
    Scrape token data from the DexScreener watchlist
    
    Args:
        scraper: Configured WebScraper instance
        watchlist_url: URL of the watchlist to scrape
        max_scrolls: Maximum number of scrolls to perform to load more data
        
    Returns:
        list: List of dictionaries containing token data
    """
    try:
         # Navigate to the watchlist
        logger.info(f"Navigating to watchlist: {watchlist_url}")
        tab = await scraper.navigate(watchlist_url)
        
        # Wait for content to load
        await tab.wait(5)
        
        # Check if we're still on a Cloudflare page after navigation
        try:
            page_content = await tab.get_content()
            
            # Check if we're still on a Cloudflare page
            if any(indicator in page_content.lower() for indicator in ["cloudflare", "verify you are human", "challenges.cloudflare.com"]):
                logger.warning("Still on Cloudflare verification page. Retrying bypass...")
                # Try the verify_cf method again
                await tab.verify_cf(flash=True)
                # Wait longer after second attempt
                await tab.wait(8)
        except Exception as cf_retry_error:
            logger.warning(f"Second Cloudflare bypass attempt failed: {cf_retry_error}")
        
        
        # Track tokens to avoid duplicates
        token_data = []
        processed_pairs = set()
        
        # Scroll to load more content if needed
        scroll_count = 0
        no_new_tokens_count = 0
        
        while scroll_count < max_scrolls:
            # Get the HTML content
            html_content = await scraper.get_page_content()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all token rows
            token_rows = soup.select('a.ds-dex-table-row')
            current_count = len(token_rows)
            logger.info(f"Found {current_count} token rows on page")
            
            # Check if we found new tokens
            initial_token_count = len(token_data)
            
            # Parse each row
            for row in token_rows:
                try:
                    # Extract contract URL to use as unique identifier
                    contract_url = row.get('href', 'Unknown')
                    
                    # Skip if we've already processed this pair
                    if contract_url in processed_pairs:
                        continue
                    
                    processed_pairs.add(contract_url)
                    
                    # Get pair age text for conversion
                    pair_age_text = row.select_one('.ds-dex-table-row-col-pair-age span').text.strip() if row.select_one('.ds-dex-table-row-col-pair-age span') else "N/A"
                    
                    # Extract data using more efficient selectors
                    data = {
                        'token_address': extract_token_address_from_image(row),
                        'rank': convert_to_int(row.select_one('.ds-dex-table-row-badge-pair-no').text.replace('#', '')) if row.select_one('.ds-dex-table-row-badge-pair-no') else None,
                        'chain': row.select_one('.ds-dex-table-row-chain-icon').get('title', 'Unknown') if row.select_one('.ds-dex-table-row-chain-icon') else "Unknown",
                        'symbol': row.select_one('.ds-dex-table-row-base-token-symbol').text.strip() if row.select_one('.ds-dex-table-row-base-token-symbol') else "Unknown",
                        'quote_symbol': row.select_one('.ds-dex-table-row-quote-token-symbol').text.strip() if row.select_one('.ds-dex-table-row-quote-token-symbol') else "Unknown",
                        'token_name': row.select_one('.ds-dex-table-row-base-token-name-text').text.strip() if row.select_one('.ds-dex-table-row-base-token-name-text') else "Unknown",
                        'price': convert_currency_to_float(row.select_one('.ds-dex-table-row-col-price').text.strip()) if row.select_one('.ds-dex-table-row-col-price') else None,
                        'age_days': convert_age_to_days(pair_age_text),
                        'txns': convert_to_int(row.select_one('.ds-dex-table-row-col-txns').text.strip()) if row.select_one('.ds-dex-table-row-col-txns') else None,
                        'volume': convert_suffixed_number(row.select_one('.ds-dex-table-row-col-volume').text.strip()) if row.select_one('.ds-dex-table-row-col-volume') else None,
                        'makers': convert_to_int(row.select_one('.ds-dex-table-row-col-makers').text.strip()) if row.select_one('.ds-dex-table-row-col-makers') else None,
                        'liquidity': convert_suffixed_number(row.select_one('.ds-dex-table-row-col-liquidity').text.strip()) if row.select_one('.ds-dex-table-row-col-liquidity') else None,
                        'market_cap': convert_suffixed_number(row.select_one('.ds-dex-table-row-col-market-cap').text.strip()) if row.select_one('.ds-dex-table-row-col-market-cap') else None,
                    }
                    
                    # Extract price changes and convert to floats
                    for period, selector in {
                        'm5_change': '.ds-dex-table-row-col-price-change-m5 .ds-change-perc',
                        'h1_change': '.ds-dex-table-row-col-price-change-h1 .ds-change-perc',
                        'h6_change': '.ds-dex-table-row-col-price-change-h6 .ds-change-perc',
                        'h24_change': '.ds-dex-table-row-col-price-change-h24 .ds-change-perc'
                    }.items():
                        change_elem = row.select_one(selector)
                        data[period] = convert_percentage_to_float(change_elem.text.strip()) if change_elem else None
                    
                    # Add combined pair
                    data['pair'] = f"{data['symbol']}/{data['quote_symbol']}"
                    
                    # Add to our dataset
                    token_data.append(data)
                    logger.info(f"Parsed token: {data['pair']} (Rank: {data['rank']})")
                    
                except Exception as e:
                    logger.error(f"Error parsing row: {e}")
            
            # Check if we found new tokens in this iteration
            new_token_count = len(token_data) - initial_token_count
            logger.info(f"Found {new_token_count} new tokens in this scroll")
            
            if new_token_count == 0:
                no_new_tokens_count += 1
                logger.info(f"No new tokens found. Attempt {no_new_tokens_count}/3")
                if no_new_tokens_count >= 3:
                    logger.info("No new tokens found after multiple attempts. Stopping.")
                    break
            else:
                no_new_tokens_count = 0
            
            # Scroll down to load more rows
            logger.info(f"Scrolling down to load more rows (scroll {scroll_count + 1}/{max_scrolls})")
            await scraper.scroll_down(800)  # Scroll down by 800 pixels
            await scraper.wait(3)  # Wait for content to load
            scroll_count += 1
        
        return token_data
        
    except Exception as e:
        logger.error(f"Error scraping DexScreener watchlist: {e}")
        return []

