from datetime import datetime, timedelta, UTC
import logging
import asyncio
import pandas as pd
import re
from bs4 import BeautifulSoup

from airflow.decorators import task
from utils.nodriver_scraper import WebScraper
from db_manager import DBManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TokenProcessor")


@task
def scrape_dexscreener(token_addresses):
    """
    Scrape DexScreener for token metadata.
    Takes a list of token addresses and processes them sequentially.
    Skips tokens that have metadata scraped within the past 12 hours.
    Returns a dictionary mapping token addresses to X handles.
    """
    async def _scrape():
        results = {}
        profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
        db_manager = DBManager(db_name='solana_insiders_silver')
        
        # log the number of tokens to scrape
        logger.info(f"Scraping {len(token_addresses)} tokens from DexScreener.")
        
        try:
            # Format the list for SQL IN clause
            token_addresses_str = "'" + "','".join(token_addresses) + "'"

            # Query to find which tokens have been recently scraped
            recently_scraped_query = f"""
                SELECT token_address 
                FROM token_metadata 
                WHERE token_address IN ({token_addresses_str})
                AND scraped_at > NOW() - INTERVAL '72 hours'
                GROUP BY token_address
            """
            recently_scraped_df = db_manager.execute_query(recently_scraped_query, df=True)

            # Extract token addresses from DataFrame
            if not recently_scraped_df.empty:
                recently_scraped_tokens = recently_scraped_df['token_address'].tolist()
            else:
                recently_scraped_tokens = []

            # Log the number of recently scraped tokens
            logger.info(f"Recently scraped tokens: {len(recently_scraped_tokens)}")

            # Filter out recently scraped tokens
            tokens_to_scrape = [addr for addr in token_addresses if addr not in recently_scraped_tokens]

            # Log the number of tokens to scrape
            logger.info(f"Tokens to scrape: {len(tokens_to_scrape)}")

            # Only proceed if there are tokens to scrape
            if not tokens_to_scrape:
                logger.info("No tokens need to be scraped at this time")
                return []
                
            # Initialize browser only if we have tokens to scrape
            logger.info(f"Scraping {len(tokens_to_scrape)} tokens")
            dex_scraper = WebScraper(
                headless=True, 
                user_data_dir=profile_dir, 
                profile_name='dexscreener_auth'
            )
            
            await dex_scraper.start()
            await dex_scraper.load_cookies()
            
            # Process each token
            for token_address in tokens_to_scrape:
                try:
                    # Navigate to token page
                    logger.info(f"Scraping token: {token_address}")
                    await dex_scraper.navigate(f"https://dexscreener.com/solana/{token_address}")
                    await dex_scraper.wait(5)
                    
                    # Parse the page
                    html = await dex_scraper.get_page_content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Initialize metadata with defaults
                    metadata = {
                        'token_address': token_address,
                        'chain': 'solana',
                        'source': 'dexscreener',
                        'token_symbol': None,
                        'summary': None,
                        'x_handle': None,
                        'scraped_at': datetime.now(UTC)
                    }
                    
                    # Extract token symbol
                    symbol_elem = soup.select_one('span.chakra-text.custom-s0pol')
                    if symbol_elem:
                        metadata['token_symbol'] = symbol_elem.text.strip()
                    
                    # Extract summary
                    summary_elem = soup.select_one('p.chakra-text.custom-nn0azp')
                    if summary_elem:
                        metadata['summary'] = summary_elem.text.strip()
                    
                    # Extract X handle
                    social_container = soup.select_one('div.chakra-stack.custom-1qmkv3p, div.chakra-stack.custom-1aau1at')
                    if social_container:
                        for link in social_container.find_all('a'):
                            href = link.get('href', '')
                            if ('twitter.com' in href or 'x.com' in href) and 'dexscreener' not in href:
                                match = re.search(r'(?:twitter\.com|x\.com)/([^/?#]+)', href)
                                if match:
                                    handle = match.group(1)
                                    if handle.lower() not in ['share', 'intent', 'home']:
                                        metadata['x_handle'] = handle
                                        results[token_address] = handle
                    
                    # Save to database
                    db_manager.insert_raw_dataframe('token_metadata', pd.DataFrame([metadata]))
                    logger.info(f"Saved metadata for {token_address}")
                    
                except Exception as e:
                    logger.error(f"Error processing token {token_address}: {e}")
            
            # Close scraper at the end
            await dex_scraper.stop()
        
        except Exception as e:
            logger.error(f"Error in scrape_dexscreener: {e}")
            
            # Make sure to close scraper if it was initialized
            if 'dex_scraper' in locals():
                await dex_scraper.stop()
        
        finally:
            db_manager.quit()
        
        return results
    
    return asyncio.run(_scrape())

@task
def scrape_twitter(token_addresses):
    """
    Scrape Twitter followers for a list of tokens.
    Only scrapes tokens that:
    1. Have an X handle in token_metadata
    2. Don't have follower data scraped within the last 3 days
    Returns a dictionary of token addresses and their scraping success status.
    """
    
    # log the number of tokens to scrape
    logger.info(f"Scraping {len(token_addresses)} tokens from x.")
    
    # Define the asynchronous function to scrape Twitte
    async def _scrape():
        profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
        results = {}
        db_manager = DBManager(db_name='solana_insiders_silver')
        
        try:
            # Format token addresses for SQL
            token_addresses_str = "'" + "','".join(token_addresses) + "'"
            
            # Step 1: Check which tokens already have recent follower data (within 3 days)
            recent_followers_query = f"""
                SELECT DISTINCT token_address 
                FROM x_followers 
                WHERE token_address IN ({token_addresses_str})
                AND scraped_at > NOW() - INTERVAL '3 days'
            """
            
            recent_followers = db_manager.execute_query(recent_followers_query, df=True)
            tokens_with_recent_data = set()
            
            if isinstance(recent_followers, pd.DataFrame):
                tokens_with_recent_data = set(recent_followers['token_address'].tolist())
            
            # Mark tokens with recent data as success (no need to scrape)
            for token in tokens_with_recent_data:
                logger.info(f"Token {token} has recent follower data. Skipping.")
                results[token] = True
            
            # Get list of tokens that need scraping
            tokens_to_process = [token for token in token_addresses if token not in tokens_with_recent_data]
            
            if not tokens_to_process:
                logger.info("No tokens need follower scraping")
                return results
            
            logger.info(f"Need to check X handles for {len(tokens_to_process)} tokens")
            
            # Step 2: Get handles from token_metadata for tokens that need scraping
            handles_query = f"""
    SELECT DISTINCT ON (token_address) token_address, x_handle, scraped_at 
    FROM token_metadata 
    WHERE token_address IN ({', '.join([f"'{t}'" for t in tokens_to_process])})
    AND NOT x_handle = 'None'
    ORDER BY token_address, scraped_at DESC
            """
            
            handles_result = db_manager.execute_query(handles_query, df=True)
            token_handles = {}
            
            # log the number of tokens with handles
            logger.info(f"Found {len(handles_result)} tokens with X handles")
            
            # Process the results to get the most recent handle for each token
            if handles_result is not None and not handles_result.empty:
                for _, row in handles_result.iterrows():
                    token = row['token_address']
                    handle = row['x_handle']
                    token_handles[token] = handle
                    logger.info(f"Token {token} has X handle: {handle}")
                               
            # log the number of tokens with handles
            logger.info(f"Tokens with handles: {len(token_handles)}")
            
            # Step 3: Scrape Twitter for tokens with handles
            tokens_for_twitter = [(token, handle) for token, handle in token_handles.items()]
            
            # log the number of tokens with handles
            logger.info(f"Tokens with handles to scrape: {len(tokens_for_twitter)}")
            
            if not tokens_for_twitter:
                logger.info("No tokens with X handles to scrape")
                return results
            
            # Initialize Twitter scraper
            logger.info(f"Scraping X followers for {len(tokens_for_twitter)} tokens")
            twitter_scraper = WebScraper(
                headless=True,
                user_data_dir=profile_dir,
                profile_name='x_auth'
            )
            
            await twitter_scraper.start()
            await twitter_scraper.load_cookies()
            
            # Verify login
            await twitter_scraper.navigate("https://x.com/home")
            await twitter_scraper.wait(3)
            
            html = await twitter_scraper.get_page_content()
            soup = BeautifulSoup(html, 'html.parser')
            
            is_logged_in = bool(
                soup.select_one('a[data-testid="AppTabBar_Profile_Link"]') or 
                soup.select_one('div[data-testid="primaryColumn"]')
            )
            
            if not is_logged_in:
                logger.warning("X session not authenticated. Run x_auth_setup first.")
                await twitter_scraper.stop()
                
                # Mark all remaining tokens as failed
                for token, _ in tokens_for_twitter:
                    if token not in results:
                        results[token] = False
                
                return results
            
            # Process each token with a handle
            for token, handle in tokens_for_twitter:
                try:
                    logger.info(f"Scraping X followers for {token} (@{handle})")
                    
                    # Navigate to followers page
                    await twitter_scraper.navigate(f"https://x.com/{handle}/followers_you_follow")
                    await twitter_scraper.wait(5)
                    
                    # Collect followers
                    followers = set()
                    no_new_count = 0
                    
                    for scroll in range(5):
                        html = await twitter_scraper.get_page_content()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        initial_count = len(followers)
                        
                        # Extract handles from username links
                        for link in soup.select('a[role="link"][tabindex="-1"]'):
                            for handle_text in re.findall(r'@([a-zA-Z0-9_]+)', str(link)):
                                if handle_text and 1 < len(handle_text) < 16:
                                    followers.add(handle_text)
                        
                        # Stop if no new followers found in 3 consecutive scrolls
                        if len(followers) > initial_count:
                            no_new_count = 0
                        else:
                            no_new_count += 1
                            if no_new_count >= 3:
                                break
                        
                        await twitter_scraper.scroll_down(800)
                        await asyncio.sleep(3)
                    
                    # Save followers to database
                    if followers:
                        follower_data = [{
                            'token_address': token,
                            'x_handle': handle,
                            'follower_handle': follower,
                            'scraped_at': datetime.now(UTC)
                        } for follower in followers]
                        
                        db_manager.insert_raw_dataframe('x_followers', pd.DataFrame(follower_data))
                        logger.info(f"Saved {len(followers)} followers for {token} to database")
                        results[token] = True
                    else:
                        logger.info(f"No followers found for {token} (@{handle})")
                        results[token] = True  # Still mark as success
                
                except Exception as e:
                    logger.error(f"Error processing token {token}: {e}")
                    results[token] = False
            
            # Close Twitter scraper
            await twitter_scraper.stop()
            
        except Exception as e:
            logger.error(f"Error in scrape_twitter: {e}")
            
            # Close Twitter scraper if it was initialized
            if 'twitter_scraper' in locals() and twitter_scraper:
                await twitter_scraper.stop()
        
        finally:
            db_manager.quit()
        
        return results
    
    return asyncio.run(_scrape())