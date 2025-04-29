from utils.token_class import Token
from db_manager import DBManager
from utils.nodriver_scraper import WebScraper
import logging
import asyncio
from airflow.decorators import task
from datetime import timedelta
import random
import time
import os
import signal
import psutil
import subprocess

# Function to force-kill all Chrome processes before starting task
def kill_all_chrome_processes():
    """Kill all Chrome processes that might interfere with our scrapers."""
    logger = logging.getLogger(__name__)
    logger.info("Killing all existing Chrome processes")
    
    killed_count = 0
    try:
        # First try a more gentle approach
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                # Look for Chrome, Chromium, or Google Chrome processes
                if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()):
                    logger.info(f"Terminating Chrome process with PID: {proc.info['pid']}")
                    os.kill(proc.info['pid'], signal.SIGTERM)
                    killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
            except Exception as e:
                logger.error(f"Error terminating Chrome process: {e}")
        
        # Give processes time to terminate gracefully
        if killed_count > 0:
            logger.info(f"Sent SIGTERM to {killed_count} Chrome processes, waiting for them to exit...")
            time.sleep(2)
        
        # Check if any are still running and use SIGKILL
        force_killed = 0
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()):
                    logger.info(f"Force killing Chrome process with PID: {proc.info['pid']}")
                    os.kill(proc.info['pid'], signal.SIGKILL)
                    force_killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
            except Exception as e:
                logger.error(f"Error force-killing Chrome process: {e}")
                
        if force_killed > 0:
            logger.info(f"Sent SIGKILL to {force_killed} Chrome processes")
            time.sleep(1)  # Short delay to ensure processes are gone
            
        # As a last resort, use system commands
        try:
            if os.name == 'posix':  # Linux/Mac
                subprocess.run(["pkill", "-9", "chrome"], check=False)
                subprocess.run(["pkill", "-9", "chromium"], check=False)
                subprocess.run(["pkill", "-9", "google-chrome"], check=False)
            elif os.name == 'nt':  # Windows
                subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"], check=False)
                subprocess.run(["taskkill", "/F", "/IM", "chromium.exe"], check=False)
        except Exception as e:
            logger.error(f"Error using system commands to kill Chrome: {e}")
        
        # Verify all Chrome processes are gone
        chrome_count = sum(1 for proc in psutil.process_iter(['name']) 
                          if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()))
        
        if chrome_count > 0:
            logger.warning(f"There are still {chrome_count} Chrome processes running after cleanup")
        else:
            logger.info("All Chrome processes have been terminated")
            
    except Exception as e:
        logger.error(f"Error during Chrome process cleanup: {e}")

@task(task_id="task_fetch_token_data", retries=2, retry_delay=timedelta(minutes=1))
def task_fetch_token_data(token_list: list) -> list:
    """
    Task to fetch all token data - processing by data type to minimize browser initializations.
    
    Args:
        token_list: List of token addresses to update
        
    Returns:
        List of token addresses that were updated
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Processing data for {len(token_list)} tokens")
    
    # Skip if no tokens
    if not token_list or len(token_list) == 0:
        logger.info("No tokens to process")
        return []
    
    # Create database manager
    db = DBManager(db_name="solana")
    updated_tokens = []
    
    # Filter out sol and stables
    token_list = [t for t in token_list if t not in [
        'So11111111111111111111111111111111111111112', 
        'he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A', 
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    ]]
    
    if not token_list:
        logger.info("No tokens to process after filtering")
        db.quit()
        return []
    
    # Dictionary to track successful processing
    successful_tokens = {token: {} for token in token_list}
    profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
    
    # *** IMPORTANT: First kill any existing Chrome processes ***
    kill_all_chrome_processes()
    
    try:
        # STEP 1: Process all metadata (no browser needed)
        logger.info("STEP 1: Processing metadata for all tokens")
        for token_address in token_list:
            try:
                token = Token(token_address, db_manager=db)
                metadata = token.get_token_metadata()
                if metadata:
                    logger.info(f"Processed metadata for {token_address}: {metadata.get('token_name', 'Unknown')}")
                    successful_tokens[token_address]['metadata'] = True
                
                # Add small delay between API calls to avoid rate limiting
                time.sleep(random.uniform(0.1, 0.2))
            except Exception as e:
                logger.error(f"Error processing metadata for {token_address}: {str(e)}")
        
        # STEP 2: Process all DexScreener data with a single browser
        logger.info("STEP 2: Processing DexScreener data for all tokens")

        dex_scraper = None
        dex_success = False
        try:
            # Initialize DexScreener scraper with no_sandbox
            dex_scraper = WebScraper(
                headless=False,
                user_data_dir=profile_dir,
                profile_name='dexscreener_auth',
                no_sandbox=True,
                use_virtual_display=True,
                display_visible=False,      
            )
            
            # Start scraper synchronously with proper cleanup
            dex_success = dex_scraper.start()
            
            if not dex_success:
                logger.error("Failed to start DexScreener scraper. Skipping DexScreener processing.")
                # Ensure cleanup happens
                if dex_scraper:
                    dex_scraper.stop()
                    dex_scraper = None
            else:
                logger.info("DexScreener scraper initialized successfully")
                
                # Load cookies (if available)
                dex_scraper.load_cookies()
                
                # Create scrapers dictionary with only dexscreener
                scrapers = {'dexscreener': dex_scraper}
                
                # Process all tokens with this scraper
                for i, token_address in enumerate(token_list):
                    try:
                        # Log progress
                        logger.info(f"Processing DexScreener data for token {i+1}/{len(token_list)}: {token_address}")
                        
                        # Create token with only the dexscreener scraper
                        token = Token(token_address, db_manager=db, scrapers=scrapers)
                        
                        # Get X handle (uses DexScreener)
                        x_handle = token.get_x_handle()
                        if x_handle:
                            logger.info(f"Processed X handle for {token_address}: {x_handle}")
                            successful_tokens[token_address]['x_handle'] = True
                        
                        # Add delay between tokens to avoid overwhelming the browser
                        if i < len(token_list) - 1:  # Don't delay after the last token
                            delay = random.uniform(1.0, 2.0)
                            logger.info(f"Waiting {delay:.2f}s before processing next token with DexScreener")
                            time.sleep(delay)
                    
                    except Exception as e:
                        logger.error(f"Error processing DexScreener data for {token_address}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Error initializing DexScreener scraper: {str(e)}")
        finally:
            # Close DexScreener scraper
            if dex_scraper:
                try:
                    dex_scraper.stop()
                    logger.info("DexScreener scraper closed")
                except Exception as e:
                    logger.error(f"Error closing DexScreener scraper: {str(e)}")
            
            # Ensure there are no lingering Chrome processes
            kill_all_chrome_processes()
        
        # STEP 3: Process all X/Twitter data with a single browser
        logger.info("STEP 3: Processing X/Twitter data for all tokens")

        x_scraper = None
        x_success = False
        try:
            # Initialize X/Twitter scraper with no_sandbox
            x_scraper = WebScraper(
                headless=False,
                user_data_dir=profile_dir,
                profile_name='x_auth',
                no_sandbox=True,
                use_virtual_display=True,
                display_visible=False,
            )
            
            # Start scraper synchronously with proper cleanup
            x_success = x_scraper.start()
            
            if not x_success:
                logger.error("Failed to start X/Twitter scraper. Skipping X/Twitter processing.")
                # Ensure cleanup happens
                if x_scraper:
                    x_scraper.stop()
                    x_scraper = None
            else:
                logger.info("X/Twitter scraper initialized successfully")
                
                # Load cookies (if available)
                x_scraper.load_cookies()
                
                # Create scrapers dictionary with only x
                scrapers = {'x': x_scraper}
                
                # Process all tokens with this scraper
                for i, token_address in enumerate(token_list):
                    try:
                        # Create token with only the x scraper
                        token = Token(token_address, db_manager=db, scrapers=scrapers)
                        
                        # Get X handle from database (already scraped in step 2)
                        x_handle = token.get_x_handle(force_refresh=False)
                        
                        # Only process if the token has an X handle
                        if x_handle:
                            logger.info(f"Processing X followers for token {i+1}/{len(token_list)}: {token_address} (@{x_handle})")
                            x_followers = token.get_x_followers()
                            if x_followers:
                                follower_count = x_followers.get('follower_count', 0)
                                logger.info(f"Processed {follower_count} X followers for {token_address}")
                                successful_tokens[token_address]['x_followers'] = True
                            
                            # Add delay between tokens to avoid rate limiting
                            if i < len(token_list) - 1:  # Don't delay after the last token
                                delay = random.uniform(2.0, 4.0)
                                logger.info(f"Waiting {delay:.2f}s before processing next token with X")
                                time.sleep(delay)
                    except Exception as e:
                        logger.error(f"Error processing X/Twitter data for {token_address}: {str(e)}")
        except Exception as e:
            logger.error(f"Error initializing X/Twitter scraper: {str(e)}")
        finally:
            # Close X/Twitter scraper
            if x_scraper:
                try:
                    x_scraper.stop()
                    logger.info("X/Twitter scraper closed")
                except Exception as e:
                    logger.error(f"Error closing X/Twitter scraper: {str(e)}")
            
            # Ensure there are no lingering Chrome processes
            kill_all_chrome_processes()
        
        # STEP 4: Process all SolSniffer data with a single browser
        logger.info("STEP 4: Processing SolSniffer data for all tokens")

        sol_scraper = None
        sol_success = False
        try:
            # Initialize SolSniffer scraper with no_sandbox
            sol_scraper = WebScraper(
                headless=False, 
                no_sandbox=True,
                use_virtual_display=True,
                display_visible=False,
            )
            
            # Start scraper synchronously with proper cleanup
            sol_success = sol_scraper.start()
            
            if not sol_success:
                logger.error("Failed to start SolSniffer scraper. Skipping SolSniffer processing.")
                # Ensure cleanup happens
                if sol_scraper:
                    sol_scraper.stop()
                    sol_scraper = None
            else:
                logger.info("SolSniffer scraper initialized successfully")
                
                # Create scrapers dictionary with only solsniffer
                scrapers = {'solsniffer': sol_scraper}
                
                # Process all tokens with this scraper - with longer delays for SolSniffer
                for i, token_address in enumerate(token_list):
                    try:
                        # Log progress
                        logger.info(f"Processing SolSniffer data for token {i+1}/{len(token_list)}: {token_address}")
                        
                        # Create token with only the solsniffer scraper
                        token = Token(token_address, db_manager=db, scrapers=scrapers)
                        
                        # Get SolSniffer score - token class now has internal rate limiting
                        sniff_score = token.get_sniff_score()
                        if sniff_score is not None:
                            logger.info(f"Processed SolSniffer score for {token_address}: {sniff_score}")
                            successful_tokens[token_address]['sniff_score'] = True
                        
                        # Add longer delay between tokens for SolSniffer (known rate limiting)
                        if i < len(token_list) - 1:  # Don't delay after the last token
                            delay = random.uniform(3.0, 5.0)
                            logger.info(f"Waiting {delay:.2f}s before processing next token with SolSniffer")
                            time.sleep(delay)
                    except Exception as e:
                        logger.error(f"Error processing SolSniffer data for {token_address}: {str(e)}")
        except Exception as e:
            logger.error(f"Error initializing SolSniffer scraper: {str(e)}")
        finally:
            # Close SolSniffer scraper
            if sol_scraper:
                try:
                    sol_scraper.stop()
                    logger.info("SolSniffer scraper closed")
                except Exception as e:
                    logger.error(f"Error closing SolSniffer scraper: {str(e)}")
            
            # Ensure there are no lingering Chrome processes
            kill_all_chrome_processes()
        
        # STEP 5: Process creation info (no browser needed)
        logger.info("STEP 5: Processing creation info for all tokens")
        for i, token_address in enumerate(token_list):
            try:
                token = Token(token_address, db_manager=db)
                creation_info = token.get_token_creation_info()
                if creation_info:
                    logger.info(f"Processed creation info for {token_address}")
                    successful_tokens[token_address]['creation_info'] = True
                
                # Add small delay between API calls
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.error(f"Error processing creation info for {token_address}: {str(e)}")
        
        # Determine which tokens were successfully processed
        for token_address, status in successful_tokens.items():
            if any(status.values()):
                updated_tokens.append(token_address)
                
    except Exception as e:
        logger.error(f"Error in token processing: {str(e)}")
    
    finally:
        # Close the database connection
        db.quit()
    
    logger.info(f"Successfully processed {len(updated_tokens)} out of {len(token_list)} tokens")
    return updated_tokens