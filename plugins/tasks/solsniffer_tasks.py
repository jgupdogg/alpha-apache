from airflow.decorators import task
from utils.nodriver_scraper import scrape_api_data_sync, WebScraper
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import re
import asyncio

# Assuming logger is defined elsewhere, add this if needed
# logger = logging.getLogger(__name__)

def normalize_data_types(data_dict):
    """
    Convert ALL risk indicator fields to strings to avoid any type conflicts.
    This ensures database inserts won't fail due to boolean/integer mismatches.
    """
    normalized_data = {}
    
    for key, value in data_dict.items():
        # Skip None values
        if value is None:
            normalized_data[key] = value
            continue
            
        # Convert ALL risk indicator fields to strings, except count fields
        if key.startswith('risk_') and not key.endswith('_count') and not key.endswith('_details_raw'):
            # Convert any value to string representation (true, false, 0, 1, etc.)
            normalized_data[key] = str(value)
        else:
            # Keep other fields as-is
            normalized_data[key] = value
    
    return normalized_data

@task(pool='scraping_pool', task_id='scrape_solsniffer_tokens')
def task_scrape_solsniffer_tokens(token_ids: list = ["A8JBhB6t8ktie5FcX2wqieQfQFJ34JyPZYvGVE3Ypump"]):
    """Solsniffer token data scraper task with database freshness check for multiple tokens"""
    # Initialize DB connection
    from db_manager import DBManager
    db_manager = DBManager(db_name='solana_insiders_silver')
    
    results = {}
    
    # log the number of tokens to scrape
    logging.info(f"Number of tokens to scrape: {len(token_ids)}")
    
    for token_id in token_ids:
        try:
            logging.info(f"Processing token ID: {token_id}")
            
            # Check if we have fresh data already (less than 10 days old)
            latest_scrape = None
            
            try:
                query = f"""
                SELECT MAX(scraped_at) as latest_scrape
                FROM solsniffer_token_data 
                WHERE token_id = '{token_id}'
                """
                
                result = db_manager.execute_query(query)
                
                if isinstance(result, pd.DataFrame):
                    if not result.empty and result.iloc[0]['latest_scrape'] is not None:
                        latest_scrape = result.iloc[0]['latest_scrape']
                else:
                    if result and len(result) > 0 and result[0]['latest_scrape'] is not None:
                        latest_scrape = result[0]['latest_scrape']
                
                if latest_scrape:
                    cutoff_date = datetime.now() - timedelta(days=10)
                    
                    # If data is fresh (less than 10 days old), skip this token
                    if latest_scrape > cutoff_date:
                        logging.info(f"Recent data exists for {token_id} (scraped on {latest_scrape}), skipping scrape")
                        results[token_id] = {
                            "success": True, 
                            "message": f"Recent data exists (scraped on {latest_scrape}), skipping scrape",
                            "fresh_data": True
                        }
                        continue
            except Exception as e:
                # Log the error but continue execution
                logging.warning(f"Error checking for existing data for {token_id} (table might not exist yet): {e}")
                # Continue with scraping regardless of database state
            
            # Proceed with API scraping
            token_url = f"https://solsniffer.com/scanner/{token_id}"
            api_url_pattern = f"solsniffer.com/api/v1/sniffer/token/{token_id}"
            
            logging.info(f"Scraping data for token {token_id}")
            response_data, _ = scrape_api_data_sync(
                url=token_url,
                url_pattern=api_url_pattern,
                timeout=20
            )
            
            # Get HTML content separately using WebScraper
            html_content = ""
            snifscore = None
            
            try:
                # This function runs the WebScraper async operations
                async def get_html_content():
                    scraper = WebScraper(headless=True)
                    try:
                        await scraper.start()
                        await scraper.navigate(token_url)
                        # Wait a bit for content to load
                        await scraper.wait(3)
                        html = await scraper.get_page_content()
                        return html
                    finally:
                        await scraper.stop()
                
                # Run the async function
                html_content = asyncio.run(get_html_content())
                
                # Extract Snifscore from HTML content
                if html_content:
                    # Define a regex pattern to extract the Snifscore value
                    pattern = r'<div class="">Snifscore:<\/div>.*?<div[^>]*?>[^>]*?<div[^>]*?>(\d+)\/100<\/div>'
                    snifscore_match = re.search(pattern, html_content, re.DOTALL)
                    
                    if snifscore_match:
                        snifscore = snifscore_match.group(1)
                        logging.info(f"Found Snifscore: {snifscore}/100 for token {token_id}")
                    else:
                        # Try an alternative pattern in case the HTML structure is slightly different
                        alt_pattern = r'Snifscore:.*?(\d+)\/100'
                        alt_match = re.search(alt_pattern, html_content, re.DOTALL)
                        if alt_match:
                            snifscore = alt_match.group(1)
                            logging.info(f"Found Snifscore (alt pattern): {snifscore}/100 for token {token_id}")
                        else:
                            logging.warning(f"Could not find Snifscore in HTML for token {token_id}")
            except Exception as e:
                logging.error(f"Error extracting Snifscore from HTML for {token_id}: {str(e)}")
            
            if response_data and 'body' in response_data:
                try:
                    # Parse the JSON response
                    data = json.loads(response_data['body'])
                    
                    # Extract relevant data
                    token_data = data.get('tokenData', {})
                    
                    # Create a flattened dictionary for the main DataFrame
                    current_time = datetime.now()
                    flat_data = {
                        'scraped_at': current_time,
                        'token_id': token_id,
                        'token_name': token_data.get('tokenName', ''),
                        'deploy_time': token_data.get('deployTime', '')
                    }
                    
                    # Add the Snifscore to the data
                    if snifscore is not None:
                        flat_data['snifscore'] = int(snifscore)
                    
                    # Add token overview data
                    overview = token_data.get('tokenOverview', {})
                    for key, value in overview.items():
                        flat_data[f'overview_{key}'] = value
                    
                    # Process indicator data
                    indicators = token_data.get('indicatorData', {})
                    if indicators:
                        # Add count fields for each risk level
                        for level in ['high', 'moderate', 'low', 'specific']:
                            if level in indicators:
                                flat_data[f'indicator_{level}_count'] = indicators[level].get('count', 0)
                        
                        # Unpack the details for each risk level
                        for level in ['high', 'moderate', 'low', 'specific']:
                            if level in indicators and 'details' in indicators[level]:
                                try:
                                    details = json.loads(indicators[level]['details'])
                                    # Convert each risk indicator to a column
                                    for indicator, value in details.items():
                                        # Create a safe column name
                                        column_name = f"risk_{level}_{indicator.replace(' ', '_').lower()}"
                                        column_name = column_name.replace('-', '_').replace('(', '').replace(')', '')
                                        flat_data[column_name] = value
                                except json.JSONDecodeError:
                                    flat_data[f'risk_{level}_details_raw'] = indicators[level]['details']
                    
                    # Store externals if present
                    externals_str = token_data.get('externals', '{}')
                    try:
                        externals = json.loads(externals_str)
                        for key, value in externals.items():
                            flat_data[f'external_{key}'] = value
                    except:
                        flat_data['externals_raw'] = externals_str
                    
                    # Normalize data types to ensure consistency by converting ALL risk fields to strings
                    normalized_data = normalize_data_types(flat_data)
                    
                    # Create the main DataFrame
                    main_df = pd.DataFrame([normalized_data])
                    
                    # Log the data types for debugging
                    logging.info(f"DataFrame dtypes after normalization: {main_df.dtypes}")
                    
                    # First try direct insertion
                    try:
                        db_manager.insert_raw_dataframe("solsniffer_token_data", main_df)
                        
                        results[token_id] = {
                            "success": True, 
                            "rows": len(main_df),
                            "fresh_data": False,
                            "message": f"Successfully scraped new data for {token_id}"
                        }
                    except Exception as e:
                        logging.error(f"Error saving to database for {token_id}: {str(e)}")
                        
                        # If direct insertion fails, try using a temp table and upsert approach
                        try:
                            logging.info("Attempting alternative insertion method (temp table)")
                            
                            # Create unique temp table name
                            temp_table = f"temp_solsniffer_{token_id.replace('-', '_')}"
                            
                            # Create a SQL insert statement with all columns as TEXT
                            columns = ", ".join([f'"{col}"' for col in main_df.columns])
                            placeholders = ", ".join(["%s" for _ in main_df.columns])
                            
                            # Convert all risk fields values to strings
                            values = []
                            for _, row in main_df.iterrows():
                                row_values = []
                                for col, val in row.items():
                                    row_values.append(str(val) if col.startswith('risk_') and val is not None else val)
                                values.append(tuple(row_values))
                            
                            # Execute raw SQL insert
                            insert_sql = f'INSERT INTO "solsniffer_token_data" ({columns}) VALUES ({placeholders})'
                            db_manager.execute_query(insert_sql, values[0], return_df=False)
                            
                            results[token_id] = {
                                "success": True,
                                "rows": len(main_df),
                                "fresh_data": False,
                                "message": f"Successfully scraped and inserted data for {token_id} using manual SQL"
                            }
                        except Exception as inner_e:
                            logging.error(f"Alternative insertion also failed: {inner_e}")
                            results[token_id] = {
                                "success": True,  # Return true since we got the data
                                "rows": len(main_df),
                                "fresh_data": False,
                                "db_error": f"{str(e)} | Alt method: {str(inner_e)}",
                                "message": f"Successfully scraped data for {token_id} but couldn't save to database"
                            }
                    
                except Exception as e:
                    logging.error(f"Error processing data for {token_id}: {str(e)}")
                    results[token_id] = {
                        "success": False, 
                        "reason": f"Error processing data: {str(e)}"
                    }
            else:
                logging.error(f"No valid data captured for {token_id}")
                results[token_id] = {
                    "success": False, 
                    "reason": "No valid data captured"
                }
                
        except Exception as e:
            logging.error(f"Unexpected error processing token {token_id}: {str(e)}")
            results[token_id] = {
                "success": False,
                "reason": f"Unexpected error: {str(e)}"
            }
    
    # Clean up resources
    
    if 'db_manager' in locals():
        db_manager.quit()
    
    # Return combined results for all tokens
    return results