#!/usr/bin/env python3

import logging
import pandas as pd
from datetime import datetime
import os
import asyncio
from typing import List, Dict, Any, Optional
from airflow.decorators import task
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils import timezone
from airflow.exceptions import AirflowSkipException
from bs4 import BeautifulSoup
from utils.nodriver_scraper import WebScraper
from db_manager import DBManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("airflow.task.dexscreener")


@task
def export_to_database(token_data: List[Dict[str, Any]]) -> bool:
    """
    Export token data to database
    
    Args:
        token_data: Token data from scraper
        
    Returns:
        Success status
    """
    try:
        if not token_data:
            logger.warning("No token data to export")
            return False
            
        from db_manager import DBManager
        db_manager = DBManager(db_name='solana_insiders_bronze')
        
        # Convert list of dictionaries to pandas DataFrame
        df = pd.DataFrame(token_data)
        
        # Add timestamp for when the data was scraped
        df['scraped_at'] = datetime.now()
        
        # Insert data into the 'watchlist' table
        db_manager.insert_raw_dataframe('watchlist', df)
        db_manager.quit()
        
        logger.info(f"Successfully exported {len(token_data)} tokens to database")
        return True
            
    except Exception as e:
        logger.error(f"Error exporting to database: {e}")
        return False
    
    
@task
def scrape_dexscreener_data(
    watchlist_url='https://dexscreener.com/watchlist/PZ5oeMyixsBce5Tshsqu',
    max_scrolls=5,
    **kwargs
):
    """Airflow task to scrape DexScreener watchlist data with auth issue detection"""
    
    # Utility functions for data transformation
    def extract_token_address_from_image(row):
        try:
            img_elements = row.select('img')
            for img in img_elements:
                src = img.get('src', '')
                if '/ds-data/tokens/' in src:
                    # Parse the URL path to extract the filename
                    path_parts = src.split('/')
                    if len(path_parts) > 0:
                        filename = path_parts[-1]
                        # Extract the address by removing the .png extension and any URL parameters
                        address = filename.split('.png')[0]
                        if '?' in address:
                            address = address.split('?')[0]
                        return address.lower()
            # Only return UNKNOWN after checking all images
            return "UNKNOWN"
        except Exception as e:
            logger.error(f"Error extracting token address: {e}")
            return "UNKNOWN"

    def convert_currency_to_float(text):
        if text == "N/A" or not text or text == "-":
            return None
        return float(text.replace('$', '').replace(',', ''))

    def convert_to_int(text):
        if text == "N/A" or not text or text == "-":
            return None
        return int(text.replace(',', ''))

    def convert_suffixed_number(text):
        if text == "N/A" or not text or text == "-":
            return None
        text = text.replace('$', '').replace(',', '')
        multipliers = {'k': 1_000, 'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000, 'T': 1_000_000_000_000}
        for suffix, multiplier in multipliers.items():
            if text.endswith(suffix):
                return float(text[:-1]) * multiplier
        return float(text)

    def convert_percentage_to_float(text):
        if text == "N/A" or not text or text == "-":
            return None
        return float(text.replace('%', ''))

    def convert_age_to_days(age_text):
        import re
        if age_text == "N/A" or not age_text or age_text == "-":
            return None
        match = re.match(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', age_text)
        if not match:
            return None
        value, unit = float(match.group(1)), match.group(2).lower()
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

    # Run the async scraper
    def run_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def _scrape():
            profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
            os.makedirs(profile_dir, exist_ok=True)
            
            # Initialize scraper with virtual display
            scraper = WebScraper(
                headless=False,
                user_data_dir=profile_dir,
                use_virtual_display=True,
                display_visible=False,
                profile_name="dexscreener_auth"
            )
            
            try:
                logger.info("Starting browser")
                await scraper.start()
                await scraper.load_cookies()
                
                logger.info(f"Navigating to watchlist: {watchlist_url}")
                tab = await scraper.navigate(watchlist_url)
                await tab.wait(5)
                
                # Take a screenshot for debugging
                await scraper.take_screenshot(f"/tmp/dexscreener_watchlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                
                # Take a screenshot for debugging
                await scraper.take_screenshot(f"/tmp/dexscreener_watchlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                
                # Continue with normal scraping if no auth issues
                token_data = []
                processed_pairs = set()
                scroll_count = 0
                no_new_tokens_count = 0
                
                while scroll_count < max_scrolls:
                    html_content = await scraper.get_page_content()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    token_rows = soup.select('a.ds-dex-table-row')
                    logger.info(f"Found {len(token_rows)} token rows on page")
                    
                    initial_token_count = len(token_data)
                    
                    for row in token_rows:
                        try:
                            contract_url = row.get('href', 'Unknown')
                            if contract_url in processed_pairs:
                                continue
                            
                            processed_pairs.add(contract_url)
                            pair_age_text = row.select_one('.ds-dex-table-row-col-pair-age span').text.strip() if row.select_one('.ds-dex-table-row-col-pair-age span') else "N/A"
                            
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
                            
                            # Extract price changes
                            for period, selector in {
                                'm5_change': '.ds-dex-table-row-col-price-change-m5 .ds-change-perc',
                                'h1_change': '.ds-dex-table-row-col-price-change-h1 .ds-change-perc',
                                'h6_change': '.ds-dex-table-row-col-price-change-h6 .ds-change-perc',
                                'h24_change': '.ds-dex-table-row-col-price-change-h24 .ds-change-perc'
                            }.items():
                                change_elem = row.select_one(selector)
                                data[period] = convert_percentage_to_float(change_elem.text.strip()) if change_elem else None
                            
                            data['pair'] = f"{data['symbol']}/{data['quote_symbol']}"
                            token_data.append(data)
                            logger.info(f"Parsed token: {data['pair']} (Rank: {data['rank']})")
                        except Exception as e:
                            logger.error(f"Error parsing row: {e}")
                    
                    new_token_count = len(token_data) - initial_token_count
                    logger.info(f"Found {new_token_count} new tokens in this scroll")
                    
                    if new_token_count == 0:
                        no_new_tokens_count += 1
                        if no_new_tokens_count >= 3:
                            logger.info("No new tokens found after multiple attempts. Stopping.")
                            break
                    else:
                        no_new_tokens_count = 0
                    
                    await scraper.scroll_down(800)
                    await scraper.wait(3)
                    scroll_count += 1
                
                # Export to database
                if token_data:
                    db_manager = DBManager(db_name='solana_insiders_bronze')
                    df = pd.DataFrame(token_data)
                    df['scraped_at'] = datetime.now()
                    db_manager.insert_raw_dataframe('watchlist', df)
                    db_manager.quit()
                    logger.info(f"Successfully exported {len(token_data)} tokens to database")
                    return {"status": "success", "tokens_found": len(token_data), "scrolls_performed": scroll_count}
                else:
                    # No data found - likely due to auth/access issues
                    logger.warning("No token data found, triggering auth setup DAG")
                    run_id = f"manual__{timezone.utcnow().isoformat()}"
                    trigger_dag(dag_id="dexscreener_auth_setup", run_id=run_id)
                    return {"status": "no_data", "message": "No token data found, triggered auth setup DAG"}
            
            finally:
                # Always clean up resources
                await scraper.stop()
                logger.info("Browser closed")
        
        try:
            result = loop.run_until_complete(_scrape())
            if result.get("status") == "no_data":
                # Skip the current task execution
                raise AirflowSkipException("No data found, triggered auth setup DAG")
            return result
        finally:
            loop.close()
    
    # Run the scraper and return the results
    return run_scraper()