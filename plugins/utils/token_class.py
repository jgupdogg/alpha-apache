# plugins/utils/crypto/token.py

import logging
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timezone
import re
from utils.nodriver_scraper import WebScraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, UTC
from hooks.dune_hook import DuneHook
from utils.crypto.birdseye_sdk import BirdEyeSDK

# Configure logging
logger = logging.getLogger("TokenWhaleAnalyzer")

class Token:
    """
    A class to manage and analyze token whale information.
    Integrates with DBManager and BirdEyeSDK to fetch and store whale data.
    """
    
    def __init__(self, address: str, blockchain: str = "solana", 
                 db_name: str = "solana", api_key: Optional[str] = None,
                 db_manager=None, birdeye_sdk=None, scrapers=None):
        """
        Initialize a Token object.
        
        Args:
            address (str): The token address
            blockchain (str): The blockchain name (default: "solana")
            db_name (str): The database name to use (default: "solana_whales_bronze")
            api_key (str, optional): BirdEye API key. If None, will attempt to get from environment
            db_manager: Optional existing DBManager instance 
            birdeye_sdk: Optional existing BirdEyeSDK instance
            scrapers: Optional dictionary of WebScraper instances for web scraping
        """
        
        # Initialize logger
        import logging
        self.logger = logging.getLogger(__name__)
        
        self.address = address
        self.blockchain = blockchain
        self.db_name = db_name
        self.scrapers = scrapers or {}  # Store the scrapers dict or initialize empty
        logger.info(f"Token {address} initialized with scrapers: {list(self.scrapers.keys())}")

        
        # Use provided instances or create new ones
        if db_manager:
            self.db = db_manager
        else:
            from db_manager import DBManager
            self.db = DBManager(db_name=self.db_name)
        
        from utils.crypto.birdseye_sdk import BirdEyeSDK
        self.birdeye = BirdEyeSDK(chain=self.blockchain)

    # Add this utility method to your Token class
    def _needs_refresh(self, table_name: str, age_threshold: timedelta = timedelta(hours=1), force_refresh: bool = False, custom_query: str = None) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Determine if data needs to be refreshed.
        """
        if force_refresh:
            return True, None
        
        # Query for most recent entry
        if custom_query:
            query = custom_query
        else:
            query = f"""
            SELECT * FROM {table_name}
            WHERE token_address = '{self.address}'
            ORDER BY scraped_at DESC
            LIMIT 1
            """
        
        try:
            result = self.db.execute_query(query, df=True)
            
            if result.empty:
                return True, None
            
            # Get the most recent data
            latest_data = result.iloc[0].to_dict()
            scraped_at = latest_data.get('scraped_at')
            
            # Current time with timezone
            current_time = datetime.now(timezone.utc)
            
            # Ensure scraped_at has timezone info
            if scraped_at and (current_time - scraped_at) <= age_threshold:
                self.logger.info(f"Using cached data for {self.address} from {table_name}, last updated at {scraped_at}")
                return False, latest_data
            
            # Data is too old or missing timestamp
            return True, None
        except Exception as e:
            self.logger.error(f"Error checking refresh status for {self.address} in {table_name}: {e}")
            return True, None



    def get_token_metadata(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Get basic token metadata from BirdEye API.
        
        Args:
            force_refresh (bool): Force refresh from API even if data exists in DB
            
        Returns:
            Dict[str, Any]: Basic token information
        """
        # Use the utility function to check if refresh is needed (72 hour threshold)
        needs_refresh, cached_data = self._needs_refresh(
            'token_metadata', 
            age_threshold=timedelta(hours=72),
            force_refresh=force_refresh
        )
        
        # If data is fresh enough, return the cached data
        if not needs_refresh and cached_data:
            return cached_data
        
        # If we need to refresh, call the BirdEye API
        try:
            response = self.birdeye.token.get_token_metadata(self.address)
            
            # log the response for debugging
            logger.info(f"BirdEye API response for {self.address}: {response}")
            
            if response and 'data' in response and response.get('success', False):
                # Extract relevant metadata from the response
                api_data = response['data']
                
                # Format metadata for our database
                extensions = api_data.get('extensions') or {}
                
                # Extract twitter handle from URL if necessary
                twitter_url = extensions.get('twitter')
                twitter_handle = None
                if twitter_url:
                    # Check if it's a URL (contains twitter.com or x.com)
                    if 'twitter.com/' in twitter_url:
                        twitter_handle = twitter_url.split('twitter.com/')[-1]
                    elif 'x.com/' in twitter_url:
                        twitter_handle = twitter_url.split('x.com/')[-1]
                    else:
                        # Assume it's already just the handle
                        twitter_handle = twitter_url
                        
                    # Remove any trailing slashes, query parameters, etc.
                    twitter_handle = twitter_handle.split('/')[0].split('?')[0]
                
                    metadata = {
                        'token_address': self.address,
                        'blockchain': self.blockchain,
                        'token_symbol': api_data.get('symbol'),
                        'token_name': api_data.get('name'),
                        'decimals': None if api_data.get('decimals') is None else int(api_data.get('decimals')),  # Explicitly convert to int or None
                        'coingecko_id': extensions.get('coingecko_id'),
                        'website': extensions.get('website'),
                        'twitter': twitter_handle,
                        'discord': extensions.get('discord'),
                        'medium': extensions.get('medium'),
                        'logo_uri': api_data.get('logo_uri'),
                        'source': 'birdeye',
                        'scraped_at': datetime.now(timezone.utc)
                    }
                
                    # Before inserting, convert string 'None' values to actual None
                    for key, value in metadata.items():
                        if value == 'None':
                            metadata[key] = None
                    
                    # When inserting into the database
                    df = pd.DataFrame([metadata])
                    
                    # Ensure pandas doesn't convert None to 'None'
                    self.db.insert_raw_dataframe('token_metadata', df)
                logger.info(f"Retrieved and stored token metadata for {self.address}")
                
                return metadata
            else:
                logger.warning(f"Failed to get metadata from BirdEye API for {self.address}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting token metadata for {self.address}: {e}")
            return {}
    
    
    def get_whales(self, min_holdings: float = 10.0, limit: int = 100, num_whales: int = 100, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get token whale holders from BirdEye API.
        
        Args:
            min_holdings (float): Minimum token holdings to be considered a whale
            limit (int): Maximum number of holders per API call (max 100)
            num_whales (int): Total number of whales to retrieve (may require multiple API calls)
            force_refresh (bool): Force refresh from API even if data exists in DB
            
        Returns:
            pd.DataFrame: Whale holders data
        """
        # Use the utility function to check if refresh is needed (24 hour threshold)
        needs_refresh, _ = self._needs_refresh(
            'token_whales', 
            age_threshold=timedelta(hours=48),
            force_refresh=force_refresh,
            custom_query=f"""
                SELECT token_address, MAX(scraped_at) as scraped_at
                FROM token_whales 
                WHERE token_address = '{self.address}'
                GROUP BY token_address
            """
        )
        
        # If no refresh needed, get all whales from the database
        if not needs_refresh:
            query = f"""
            SELECT * 
            FROM token_whales 
            WHERE token_address = '{self.address}'
            ORDER BY ui_amount DESC
            """
            
            try:
                whales_df = self.db.execute_query(query, df=True)
                if not whales_df.empty:
                    logger.info(f"Using cached whale data for {self.address}, found {len(whales_df)} whales")
                    return whales_df
            except Exception as e:
                logger.error(f"Error retrieving whales from database: {e}")
                # Continue to fetching if retrieval fails
        
        # If we need fresh data, fetch whales from API
        try:
            # Get metadata to get decimals
            metadata = self.get_token_metadata()
            token_decimals = metadata.get('decimals', 9)  # Default to 9 for Solana tokens
            
            # Ensure limit doesn't exceed 100 (API's max)
            api_limit = min(limit, 100)
            remaining_whales = num_whales
            offset = 0
            all_holders = []
            
            # Fetch total supply for ownership percentage calculation
            total_supply = 0
            try:
                supply_response = self.birdeye.token.get_token_overview(self.address)
                if supply_response and 'data' in supply_response:
                    total_supply = int(supply_response['data'].get('totalSupply', 0))
            except Exception as e:
                logger.warning(f"Could not get total supply for {self.address}: {e}")
            
            # Make multiple API calls with pagination
            while remaining_whales > 0:
                logger.info(f"Fetching whale holders for {self.address} (offset: {offset}, limit: {api_limit})")
                
                response = self.birdeye.token.get_token_top_holders(
                    address=self.address,
                    offset=offset,
                    limit=api_limit
                )
                
                if not (response and 'data' in response and 'items' in response['data'] and response.get('success', False)):
                    logger.warning(f"Failed to get holder data from BirdEye API for {self.address}")
                    break
                
                # Extract holder data items
                holders_data = response['data']['items']
                
                # If no more holders returned, we've reached the end
                if not holders_data:
                    break
                
                # Process holders from this batch
                for item in holders_data:
                    amount = int(item.get('amount', 0))
                    ui_amount = item.get('ui_amount', 0)
                    
                    # Calculate ownership percentage if total supply is available
                    ownership_percent = 0
                    if total_supply > 0:
                        ownership_percent = (amount / total_supply) * 100
                    
                    # Skip if UI amount is less than minimum holdings
                    if ui_amount < min_holdings:
                        continue
                    
                    record = {
                        'token_address': self.address,
                        'blockchain': self.blockchain,
                        'holder_address': item.get('owner'),
                        'token_account': item.get('token_account'),
                        'amount': amount,
                        'ui_amount': ui_amount,
                        'decimals': item.get('decimals', token_decimals),
                        'ownership_percent': ownership_percent,
                        'is_whale': True,
                    }
                    all_holders.append(record)
                
                # Update counters for next iteration
                fetched_count = len(holders_data)
                offset += fetched_count
                remaining_whales -= fetched_count
                
                # If we got fewer results than requested, we've reached the end
                if fetched_count < api_limit:
                    break
                
                # Sleep briefly to avoid rate limiting
                import time
                time.sleep(0.5)
            
            # Check if we found any whales
            if not all_holders:
                logger.info(f"No whales found for {self.address} with min holdings {min_holdings}")
                return pd.DataFrame()
            
            # Create DataFrame and store in database
            whale_df = pd.DataFrame(all_holders)
            whale_df['scraped_at'] = datetime.now(timezone.utc)
            
            self.db.insert_raw_dataframe('token_whales', whale_df)
            logger.info(f"Retrieved and stored {len(all_holders)} whale records for {self.address}")
            
            return whale_df
                
        except Exception as e:
            logger.error(f"Error getting token whales for {self.address}: {e}")
            return pd.DataFrame()
        
            
    def get_x_handle(self, force_refresh: bool = False):
        """
        Get X handle for this token from the database or by scraping DexScreener.
        
        Args:
            force_refresh (bool): Force refresh by scraping even if recent data exists
            
        Returns:
            Optional[str]: X handle if found, None otherwise
        """
        # Use the utility function to check if refresh is needed
        # Increased cache time to 14 days as requested
        needs_refresh, cached_data = self._needs_refresh(
            'token_metadata', 
            age_threshold=timedelta(days=14),  # Increased from 48 hours to 14 days
            force_refresh=force_refresh
        )
        
        # If data is fresh enough and has a twitter handle, return it
        if not needs_refresh and cached_data and cached_data.get('twitter'):
            twitter_value = cached_data['twitter']
            
            # Check if it's a URL and extract handle if needed
            if isinstance(twitter_value, str):
                # Skip empty values
                if twitter_value.strip() == "" or twitter_value.lower() == "none":
                    logger.info(f"Found empty or 'None' X handle for {self.address}, treating as no handle")
                    return None
                    
                if 'twitter.com/' in twitter_value:
                    handle = twitter_value.split('twitter.com/')[-1]
                    # Remove any trailing slashes, query parameters, etc.
                    handle = handle.split('/')[0].split('?')[0]
                    # Validate handle format (only return if it looks legitimate)
                    if handle and handle.strip() and handle.lower() not in ['share', 'intent', 'home', 'none', '']:
                        return handle
                    return None
                elif 'x.com/' in twitter_value:
                    handle = twitter_value.split('x.com/')[-1]
                    # Remove any trailing slashes, query parameters, etc.
                    handle = handle.split('/')[0].split('?')[0]
                    # Validate handle format (only return if it looks legitimate)
                    if handle and handle.strip() and handle.lower() not in ['share', 'intent', 'home', 'none', '']:
                        return handle
                    return None
                
                # If it's not a URL, validate that it's a proper handle
                if twitter_value and twitter_value.strip() and twitter_value.lower() not in ['none', '']:
                    return twitter_value
                return None
            return None
        
        # If we need to refresh, check if we have the required scraper
        if 'dexscreener' not in self.scrapers:
            logger.warning(f"Cannot scrape X handle for {self.address}: Required 'dexscreener' scraper not provided")
            return None
        
        logger.info(f"Scraping DexScreener for X handle for token {self.address}")
        
        try:
            # Import the needed modules
            import nest_asyncio
            import asyncio
            from bs4 import BeautifulSoup
            import re
            
            # Apply nest_asyncio
            nest_asyncio.apply()
            
            # Define the async function
            async def scrape_dexscreener():
                # Use the shared scraper - NEVER create a new one
                dex_scraper = self.scrapers['dexscreener']
                logger.info(f"Using provided DexScreener scraper for {self.address}")
                
                # Navigate to token page
                await dex_scraper.start()
                await dex_scraper.navigate(f"https://dexscreener.com/solana/{self.address}")
                await asyncio.sleep(2)  # Add short delay before waiting
                await dex_scraper.wait(5)
                
                # Get the page source
                html = await dex_scraper.get_page_content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract X handle
                x_handle = None
                
                # First try social containers
                social_selectors = [
                    'div.chakra-stack.custom-1qmkv3p', 
                    'div.chakra-stack.custom-1aau1at',
                    'div.chakra-stack[role="group"]'
                ]
                
                for selector in social_selectors:
                    social_container = soup.select_one(selector)
                    if social_container:
                        for link in social_container.find_all('a'):
                            href = link.get('href', '')
                            if ('twitter.com' in href or 'x.com' in href) and 'dexscreener' not in href:
                                match = re.search(r'(?:twitter\.com|x\.com)/([^/?#]+)', href)
                                if match:
                                    handle = match.group(1)
                                    if handle and handle.strip() and handle.lower() not in ['share', 'intent', 'home', 'none', '']:
                                        x_handle = handle
                                        break
                    if x_handle:
                        break
                
                # If still no handle, try all links
                if not x_handle:
                    for link in soup.find_all('a'):
                        href = link.get('href', '')
                        if ('twitter.com' in href or 'x.com' in href) and 'dexscreener' not in href:
                            match = re.search(r'(?:twitter\.com|x\.com)/([^/?#]+)', href)
                            if match:
                                handle = match.group(1)
                                if handle and handle.strip() and handle.lower() not in ['share', 'intent', 'home', 'none', '']:
                                    x_handle = handle
                                    break
                
                return x_handle
            
            # Run the async function
            loop = asyncio.get_event_loop()
            x_handle = loop.run_until_complete(scrape_dexscreener())
            
            # Update database if handle found
            if x_handle:
                logger.info(f"Found X handle: {x_handle} for token {self.address}")
                
                # Create or update the token metadata with the X handle
                metadata = {
                    'token_address': self.address,
                    'blockchain': self.blockchain,
                    'twitter': x_handle,
                    'source': 'dexscreener',
                    'scraped_at': datetime.now(timezone.utc)
                }
                
                self.db.insert_raw_dataframe('token_metadata', pd.DataFrame([metadata]))
                logger.info(f"Updated token_metadata with X handle {x_handle} for {self.address}")
            else:
                # Store None in the database to avoid repeated scraping attempts
                logger.warning(f"No X handle found for {self.address}, storing NULL value in database")
                metadata = {
                    'token_address': self.address,
                    'blockchain': self.blockchain,
                    'twitter': None,  # Explicitly store NULL
                    'source': 'dexscreener',
                    'scraped_at': datetime.now(timezone.utc)
                }
                self.db.insert_raw_dataframe('token_metadata', pd.DataFrame([metadata]))
            
            return x_handle
        
        except Exception as e:
            logger.error(f"Error scraping DexScreener for {self.address}: {e}")
            return None

    def get_x_followers(self, force_refresh: bool = False):
        """
        Get X followers for this token.
        
        Args:
            force_refresh (bool): Force refresh from X even if recent data exists
                    
        Returns:
            Dict[str, Any]: Dictionary with token address, handle, followers, and status
        """
        # First make sure we have a handle
        x_handle = self.get_x_handle(force_refresh=force_refresh)
        
        # Extra validation to ensure handle is valid
        if not x_handle or not isinstance(x_handle, str) or x_handle.lower() == 'none' or x_handle.strip() == '':
            logger.warning(f"No valid X handle for {self.address}, skipping follower scraping")
            return {
                'token_address': self.address,
                'x_handle': None,
                'followers': [],
                'follower_count': 0,
                'status': 'no_handle'
            }
        
        # Log the handle for debugging
        logger.info(f"Valid X handle found for {self.address}: {x_handle}")
        
        # Use _needs_refresh to check if we need fresh data
        needs_refresh, cached_result = self._needs_refresh(
            'x_followers', 
            age_threshold=timedelta(days=1),
            force_refresh=force_refresh,
            custom_query=f"""
                SELECT token_address, x_handle, MAX(scraped_at) as scraped_at
                FROM x_followers 
                WHERE token_address = '{self.address}'
                AND x_handle = '{x_handle}'
                GROUP BY token_address, x_handle
            """
        )
        
        # If no refresh needed, get all followers from the database
        if not needs_refresh and cached_result:
            followers_query = f"""
            SELECT follower_handle, scraped_at
            FROM x_followers 
            WHERE token_address = '{self.address}' 
            AND x_handle = '{x_handle}'
            ORDER BY scraped_at DESC
            """
            
            try:
                followers_result = self.db.execute_query(followers_query, df=True)
                if not followers_result.empty:
                    cached_followers = followers_result['follower_handle'].tolist()
                    scraped_at = followers_result.iloc[0]['scraped_at']
                    
                    logger.info(f"Using cached followers for {self.address}, found {len(cached_followers)} followers")
                    return {
                        'token_address': self.address,
                        'x_handle': x_handle,
                        'followers': cached_followers,
                        'scraped_at': scraped_at,
                        'follower_count': len(cached_followers)
                    }
            except Exception as e:
                logger.error(f"Error retrieving followers from database: {e}")
                # Continue to scraping if retrieval fails
        
        # Check if X scraper is available - if not, return early
        if 'x' not in self.scrapers:
            logger.warning(f"Cannot scrape X followers for {self.address}: Required 'x' scraper not provided")
            return {
                'token_address': self.address,
                'x_handle': x_handle,
                'followers': [],
                'follower_count': 0,
                'status': 'no_scraper'
            }
        
        # If we need to refresh, scrape X followers
        logger.info(f"Scraping X followers for {self.address} with handle @{x_handle}")
        
        try:
            # Import the needed modules
            import nest_asyncio
            import asyncio
            from bs4 import BeautifulSoup
            import re
            
            # Apply nest_asyncio to make asyncio work
            nest_asyncio.apply()
            
            # Define the async function we'll run
            async def scrape_x_followers():
                # Use the provided scraper - NEVER create a new one
                twitter_scraper = self.scrapers['x']
                logger.info(f"Using provided X scraper for {self.address}")
                
                # Verify login
                await twitter_scraper.start()
                await twitter_scraper.navigate("https://x.com/home")
                await asyncio.sleep(2)  # Add short delay before waiting
                await twitter_scraper.wait(3)
                
                html = await twitter_scraper.get_page_content()
                soup = BeautifulSoup(html, 'html.parser')
                
                is_logged_in = bool(
                    soup.select_one('a[data-testid="AppTabBar_Profile_Link"]') or 
                    soup.select_one('div[data-testid="primaryColumn"]')
                )
                
                if not is_logged_in:
                    logger.warning("X session not authenticated. Run x_auth_setup first.")
                    return {
                        'token_address': self.address,
                        'x_handle': x_handle,
                        'followers': [],
                        'follower_count': 0,
                        'status': 'auth_failed'
                    }
                
                # Double-check handle format before navigating
                if not x_handle or not isinstance(x_handle, str) or x_handle.lower() == 'none' or x_handle.strip() == '':
                    logger.warning(f"Invalid X handle: '{x_handle}', skipping followers")
                    return {
                        'token_address': self.address,
                        'x_handle': None,
                        'followers': [],
                        'follower_count': 0,
                        'status': 'invalid_handle'
                    }
                
                # Navigate to followers page
                followers_url = f"https://x.com/{x_handle}/followers_you_follow"
                logger.info(f"Navigating to {followers_url}")
                await twitter_scraper.navigate(followers_url)
                await asyncio.sleep(3)  # Add short delay before waiting
                await twitter_scraper.wait(5)
                
                # Check if we hit a "User not found" or similar error
                html = await twitter_scraper.get_page_content()
                if "This account doesn't exist" in html or "User not found" in html:
                    logger.warning(f"X account @{x_handle} doesn't exist or was suspended")
                    # Store this as a valid result with no followers to prevent repeated scraping
                    return {
                        'token_address': self.address,
                        'x_handle': x_handle,
                        'followers': [],
                        'follower_count': 0,
                        'status': 'account_not_found'
                    }
                
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
                    
                    # Scroll down
                    await twitter_scraper.scroll_down(800)
                    await asyncio.sleep(3)
                
                return {
                    'token_address': self.address,
                    'x_handle': x_handle,
                    'followers': list(followers),
                    'follower_count': len(followers),
                    'status': 'success' if followers else 'no_followers',
                }
            
            # Run the async function
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(scrape_x_followers())
            
            # Save followers to database if any were found
            if result['status'] in ['success', 'no_followers', 'account_not_found']:
                followers = result.get('followers', [])
                current_time = datetime.now(timezone.utc)
                
                # Even if no followers were found, save at least one record to mark this as processed
                if not followers:
                    logger.info(f"No followers found for {self.address} (@{x_handle}), storing empty result")
                    df = pd.DataFrame([{
                        'token_address': self.address,
                        'x_handle': x_handle,
                        'follower_handle': 'NO_FOLLOWERS',  # Special marker
                        'scraped_at': current_time
                    }])
                    self.db.insert_raw_dataframe('x_followers', df)
                else:
                    # Create a DataFrame with one row per follower
                    follower_data = []
                    for follower in followers:
                        follower_data.append({
                            'token_address': self.address,
                            'x_handle': x_handle,
                            'follower_handle': follower,
                            'scraped_at': current_time
                        })
                    
                    df = pd.DataFrame(follower_data)
                    self.db.insert_raw_dataframe('x_followers', df)
                    logger.info(f"Saved {len(followers)} followers for {self.address} to database")
            
            return result
        
        except Exception as e:
            logger.error(f"Error scraping X for {self.address}: {e}")
            return {
                'token_address': self.address,
                'x_handle': x_handle,
                'followers': [],
                'follower_count': 0,
                'status': 'error',
                'error': str(e)
            }
            
    def get_sniff_score(self, force_refresh: bool = False):
        """
        Get Sniff Score for this token from the database or by scraping SolSniffer.
        
        Args:
            force_refresh (bool): Force refresh by scraping even if recent data exists
            
        Returns:
            Optional[int]: Sniff Score if found, None otherwise
        """
        # Use the utility function to check if refresh is needed
        needs_refresh, cached_data = self._needs_refresh(
            'solsniffer_token_data', 
            age_threshold=timedelta(hours=24),
            force_refresh=force_refresh
        )
        
        # If data is fresh enough, return the cached score
        if not needs_refresh and cached_data and 'snifscore' in cached_data:
            return cached_data['snifscore']
        
        # Check if SolSniffer scraper is available - if not, return early
        if 'solsniffer' not in self.scrapers:
            logger.warning(f"Cannot scrape SolSniffer for {self.address}: Required 'solsniffer' scraper not provided")
            return None
        
        # If we got here, we need to scrape SolSniffer
        logger.info(f"No recent Sniff Score found in database for {self.address}, scraping SolSniffer")
        token_url = f"https://solsniffer.com/scanner/{self.address}"
        
        try:
            # Import the needed modules
            import nest_asyncio
            import asyncio
            import re
            
            # Apply nest_asyncio to make asyncio work
            nest_asyncio.apply()
            
            # Define the async function we'll run
            async def scrape_solsniffer():
                # Use the provided scraper - NEVER create a new one
                sol_scraper = self.scrapers['solsniffer']
                logger.info(f"Using provided SolSniffer scraper for {self.address}")
                
                logger.info(f"Navigating to SolSniffer page for {self.address}")
                await sol_scraper.start()
                await sol_scraper.navigate(token_url)
                await sol_scraper.wait(5)  # Wait for page to load
                
                html_content = await sol_scraper.get_page_content()
                
                # Extract Snifscore from HTML content
                snifscore = None
                if html_content:
                    # Try primary pattern
                    pattern = r'<div class="">Snifscore:<\/div>.*?<div[^>]*?>[^>]*?<div[^>]*?>(\d+)\/100<\/div>'
                    snifscore_match = re.search(pattern, html_content, re.DOTALL)
                    
                    if snifscore_match:
                        snifscore = int(snifscore_match.group(1))
                        logger.info(f"Found Snifscore: {snifscore}/100 for token {self.address}")
                    else:
                        # Try alternative pattern
                        alt_pattern = r'Snifscore:.*?(\d+)\/100'
                        alt_match = re.search(alt_pattern, html_content, re.DOTALL)
                        if alt_match:
                            snifscore = int(alt_match.group(1))
                            logger.info(f"Found Snifscore (alt pattern): {snifscore}/100 for token {self.address}")
                        else:
                            logger.warning(f"Could not find Snifscore in HTML for token {self.address}")
                
                return snifscore
            
            # Run the async function
            loop = asyncio.get_event_loop()
            sniff_score = loop.run_until_complete(scrape_solsniffer())
            
            # Update database if sniff_score found
            if sniff_score is not None:
                logger.info(f"Found Sniff Score: {sniff_score}, updating database")
                
                # Insert new record with current timestamp
                metadata = {
                    'token_address': self.address,
                    'snifscore': sniff_score,
                    'scraped_at': datetime.now(timezone.utc)
                }
                
                try:
                    # This will create the table if needed and insert the data
                    self.db.insert_raw_dataframe('solsniffer_token_data', pd.DataFrame([metadata]))
                    logger.info(f"Stored Sniff Score {sniff_score} for token {self.address}")
                except Exception as e:
                    logger.error(f"Failed to store Sniff Score: {e}")
            
            return sniff_score
        except Exception as e:
            logger.error(f"Error scraping SolSniffer for {self.address}: {e}")
            return None
    
    def get_token_creation_info(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Get token creation info from BirdEye API.
        
        Args:
            force_refresh (bool): Force refresh from API even if data exists in DB
            
        Returns:
            Dict[str, Any]: Token creation information
        """
        # Use the utility function to check if refresh is needed (token creation info never changes)
        needs_refresh, cached_data = self._needs_refresh(
            'token_creation_info', 
            age_threshold=timedelta(days=10000),  # Long threshold since creation info doesn't change
            force_refresh=force_refresh
        )
        
        # If data is fresh enough, return the cached data
        if not needs_refresh and cached_data:
            return cached_data
        
        # If we need to refresh, call the BirdEye API
        try:
            response = self.birdeye.token.get_token_creation_info(self.address)
            
            if response and 'data' in response and response.get('success', False):
                # Extract relevant data from the response
                api_data = response['data']
                
                # Format data for our database
                creation_data = {
                    'token_address': self.address,
                    'tx_hash': api_data.get('txHash'),
                    'slot': api_data.get('slot'),
                    'decimals': api_data.get('decimals'),
                    'owner': api_data.get('owner'),
                    'block_unix_time': api_data.get('blockUnixTime'),
                    'block_human_time': api_data.get('blockHumanTime'),
                    'scraped_at': datetime.now(timezone.utc)
                }
                
                # Store in database (insert_raw_dataframe will create the table if needed)
                self.db.insert_raw_dataframe('token_creation_info', pd.DataFrame([creation_data]))
                logger.info(f"Retrieved and stored token creation info for {self.address}")
                
                return creation_data
            else:
                logger.warning(f"Failed to get creation info from BirdEye API for {self.address}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting token creation info for {self.address}: {e}")
            return {}
    
    
    def get_token_market_data(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Get token market data from BirdEye API.
        
        Args:
            force_refresh (bool): Force refresh from API even if recent data exists
            
        Returns:
            Dict[str, Any]: Token market data
        """
        # Use the utility function to check if refresh is needed (12 hour threshold for market data)
        needs_refresh, cached_data = self._needs_refresh(
            'token_market_data', 
            age_threshold=timedelta(hours=12),
            force_refresh=force_refresh
        )
        
        # If data is fresh enough, return the cached data
        if not needs_refresh and cached_data:
            return cached_data
        
        # If we need to refresh, call the BirdEye API
        try:
            response = self.birdeye.token.get_token_market_data(self.address)
            
            if response and 'data' in response and response.get('success', False):
                # Extract relevant data from the response
                api_data = response['data']
                
                # Format data for our database
                market_data = {
                    'token_address': self.address,
                    'liquidity': api_data.get('liquidity'),
                    'price': api_data.get('price'),
                    'total_supply': api_data.get('total_supply'),
                    'circulating_supply': api_data.get('circulating_supply'),
                    'fdv': api_data.get('fdv'),
                    'market_cap': api_data.get('market_cap'),
                    'scraped_at': datetime.now(timezone.utc)
                }
                
                # Store in database (insert_raw_dataframe will create the table if needed)
                self.db.insert_raw_dataframe('token_market_data', pd.DataFrame([market_data]))
                logger.info(f"Retrieved and stored market data for {self.address}")
                
                return market_data
            else:
                logger.warning(f"Failed to get market data from BirdEye API for {self.address}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting market data for {self.address}: {e}")
            return {}
        
    def get_token_trade_data(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Get detailed token trade data from BirdEye API."""
        # Check freshness using _needs_refresh
        needs_refresh, cached_data = self._needs_refresh(
            'token_trade_data', 
            age_threshold=timedelta(hours=6),
            force_refresh=force_refresh
        )
        
        if not needs_refresh and cached_data:
            return cached_data
        
        try:
            response = self.birdeye.token.get_token_trade_data(self.address)
            
            if response and 'data' in response and response.get('success', False):
                api_data = response['data']
                
                # Add token_address and scraped_at
                record = {
                    'token_address': self.address,
                    'scraped_at': datetime.now(timezone.utc)
                }
                
                # Process all values to handle 'None' strings
                for key, value in api_data.items():
                    # Handle 'None' values - convert to actual None
                    if isinstance(value, str) and value.lower() == 'none':
                        record[key] = None
                    else:
                        record[key] = value
                
                try:
                    # Check if table exists
                    check_result = self.db.execute_query("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'token_trade_data');")
                    table_exists = check_result[0]['exists'] if isinstance(check_result, list) else check_result.iloc[0][0]
                    
                    if not table_exists:
                        # Create table with proper column types
                        columns = ["token_address TEXT NOT NULL", "scraped_at TIMESTAMP WITH TIME ZONE NOT NULL"]
                        for key, value in api_data.items():
                            if isinstance(value, (int, float)) or (isinstance(value, str) and value.lower() == 'none'):
                                columns.append(f"{key} NUMERIC")
                            elif isinstance(value, str):
                                columns.append(f"{key} TEXT")
                            else:
                                columns.append(f"{key} TEXT")
                        
                        create_table_query = f"""
                        CREATE TABLE token_trade_data (
                            {', '.join(columns)},
                            PRIMARY KEY (token_address, scraped_at)
                        );
                        """
                        
                        self.db.execute_query(create_table_query)
                    
                    # Create DataFrame and explicitly convert all 'None' values to None before saving
                    df = pd.DataFrame([record])
                    
                    # Apply conversion to all string columns
                    for col in df.columns:
                        df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.lower() == 'none' else x)
                    
                    # Store in database
                    self.db.insert_raw_dataframe('token_trade_data', df)
                    self.logger.info(f"Saved trade data for {self.address} to database")
                    
                except Exception as db_error:
                    self.logger.error(f"Database operation failed: {db_error}")
                
                return record
            else:
                self.logger.warning(f"Failed to get trade data from BirdEye API for {self.address}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting trade data for {self.address}: {e}")
            return {}
            
    def get_historical_price(self, interval: str = "15m", time_from: Optional[int] = None, 
                            time_to: Optional[int] = None, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get historical price data for token from BirdEye API.
        
        Args:
            interval (str): Time interval ('5m', '15m', '1h', '4h', '1d')
            time_from (int, optional): Start time in unix timestamp
            time_to (int, optional): End time in unix timestamp
            force_refresh (bool): Force refresh from API even if data exists in DB
            
        Returns:
            pd.DataFrame: Historical price data
        """
        # Check if we can use cached data
        if not force_refresh and time_from is not None and time_to is not None:
            query = f"""
            SELECT * FROM token_historical_prices 
            WHERE token_address = '{self.address}'
            AND unixtime >= {time_from} AND unixtime <= {time_to}
            AND interval_type = '{interval}'
            ORDER BY unixtime ASC
            """
            
            try:
                result = self.db.execute_query(query, df=True)
                
                if not result.empty:
                    logger.info(f"Retrieved {len(result)} historical prices for {self.address} from database")
                    return result
            except Exception as e:
                # Table might not exist yet
                logger.info(f"No existing historical data found: {e}")
        
        try:
            # Ensure table exists
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS token_historical_prices (
                token_address TEXT NOT NULL,
                unixtime BIGINT NOT NULL,
                price NUMERIC NOT NULL,
                interval_type TEXT NOT NULL,
                scraped_at TIMESTAMP NOT NULL,
                PRIMARY KEY (token_address, unixtime, interval_type)
            )
            """)
            
            # Call API
            response = self.birdeye.defi.get_historical_price(
                address=self.address,
                address_type="token",
                type=interval
            )
            
            if response and 'data' in response and 'items' in response['data'] and response.get('success', False):
                
                # log the response for debugging
                logger.info(f"Received historical price data for {self.address}: {response}")
                
                # Extract price points from the response
                price_points = response['data']['items']
                
                if not price_points:
                    logger.warning(f"No historical price data returned for {self.address}")
                    return pd.DataFrame()
                
                # Prepare data for database
                current_time = datetime.now(UTC)
                records = []
                
                for point in price_points:
                    records.append({
                        'token_address': self.address,
                        'unixtime': point.get('unixTime'),
                        'price': point.get('value'),
                        'interval_type': interval,
                        'scraped_at': current_time
                    })
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                                
                # Alternatively, use the insert_raw_dataframe method if it supports ON CONFLICT
                try:
                    # Insert with ON CONFLICT handling
                    # This is a simplified approach, you may need to adjust based on your DB manager's capabilities
                    self.db.insert_raw_dataframe('token_historical_prices', df)
                    logger.info(f"Saved {len(records)} historical price points for {self.address}")
                except Exception as e:
                    logger.warning(f"Error saving historical data, attempting individual inserts: {e}")
                    # Fall back to individual inserts if batch insert fails
                    for record in records:
                        try:
                            self.db.insert_raw_dataframe('token_historical_prices', pd.DataFrame([record]))
                        except Exception:
                            pass
                
                return df
            else:
                logger.warning(f"Failed to get historical price data from BirdEye API for {self.address}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting historical price data for {self.address}: {e}")
            return pd.DataFrame()

    def get_historical_price_unix(self, unixtime: Optional[int] = None) -> Dict[str, Any]:
        """
        Get token price at a specific unix timestamp.
        
        Args:
            unixtime (int, optional): Unix timestamp to get price for. Defaults to latest.
            
        Returns:
            Dict[str, Any]: Price data at the specified time
        """
        try:
            # Call API
            response = self.birdeye.defi.get_historical_price_unix(
                address=self.address,
                unixtime=unixtime
            )
            
            if response and 'data' in response and response.get('success', False):
                # Return the data directly without DB storage
                price_data = {
                    'token_address': self.address,
                    'price': response['data'].get('value'),
                    'update_time': response['data'].get('updateUnixTime'),
                    'price_change_24h': response['data'].get('priceChange24h')
                }
                
                return price_data
            else:
                logger.warning(f"Failed to get historical price at unix time from BirdEye API for {self.address}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting historical price at unix time for {self.address}: {e}")
            return {}
        
    def get_inception_holder_stats(self, store_in_db: bool = True) -> pd.DataFrame:
        """
        Get holder statistics from token inception using Dune Analytics.
        
        Executes Dune query 4993581 'token_inception_holder_stats' to analyze
        holder patterns since token creation.
        
        Args:
            store_in_db (bool): Whether to store results in database
            
        Returns:
            pd.DataFrame: Holder statistics from token inception
        """
        logger.info(f"Fetching inception holder stats for token {self.address}")
        
        try:
            # Initialize Dune hook
            from hooks.dune_hook import DuneHook
            dune_hook = DuneHook()
            
            # Execute the query for this token using our generic execute_query method
            df = dune_hook.execute_query(
                query_id=4990363,  # 'token_inception_holder_stats' query ID
                parameters={
                    'token_address': self.address  # Ensure lowercase for Dune
                },
                timeout=120  # 2-minute timeout
            )
            
            # Check if we have results
            if df.empty:
                logger.info(f"No inception holder stats found for token {self.address}")
                return pd.DataFrame()
            
            logger.info(f"Found inception holder stats for token {self.address} with {len(df)} rows")
            
            # Store in database if requested
            if store_in_db:                
                # Add metadata columns
                df['token_address'] = self.address
                df['scraped_at'] = datetime.now(UTC)
                
                # Convert timestamps from Unix time to datetime objects if needed
                if 'first_acquisition_time' in df.columns and pd.api.types.is_numeric_dtype(df['first_acquisition_time']):
                    logger.info("Converting first_acquisition_time from Unix timestamp to datetime")
                    # Convert Unix timestamp to datetime (handle zeros and nulls)
                    df['first_acquisition_time'] = df['first_acquisition_time'].apply(
                        lambda x: datetime.fromtimestamp(x, UTC) if x > 0 else None
                    )
                
                # Convert boolean fields if needed
                if 'active_recently' in df.columns and not pd.api.types.is_bool_dtype(df['active_recently']):
                    logger.info("Converting active_recently to boolean")
                    df['active_recently'] = df['active_recently'].astype(bool)
                
                # Save to database
                try:
                    self.db.insert_raw_dataframe('token_inception_holder_stats', df)
                    logger.info(f"Successfully saved inception holder stats for token {self.address}")
                
                except Exception as fallback_error:
                    logger.error(f"Insertion failed: {fallback_error}")
                
            return df
                
        except Exception as e:
            logger.error(f"Error fetching inception holder stats for {self.address}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()  
    
    def close(self):
        """
        Close database connections and clean up resources.
        """
        if hasattr(self, 'db') and self.db:
            self.db.quit()
        logger.info(f"Closed Token resources for {self.address}")