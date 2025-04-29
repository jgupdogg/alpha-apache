# File: dags/dexscreener_auth_dag.py
from datetime import datetime, timedelta
import logging
import os
import asyncio
import nest_asyncio

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DexScreenerAuth")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def setup_dexscreener_profile(wait_time_minutes: int = 2):
    """
    Task to set up DexScreener authentication profile using nodriver.
    Simplified to match the working test approach.
    """
    from utils.nodriver_scraper import WebScraper
    
    # Define profile directory
    profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
    os.makedirs(profile_dir, exist_ok=True)
    
    # Run the async authentication process
    def run_auth():
        # Apply nest_asyncio for running async code in synchronous context
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        
        async def _auth():
            scraper = None
            try:
                logger.info("Initializing browser for manual authentication...")
                
                # Create scraper with authentication profile - simplified based on working test
                scraper = WebScraper(
                    headless=False,                # Visible for manual login
                    user_data_dir=profile_dir,     # Use the shared profile directory
                    profile_name="dexscreener_auth", # Specific profile name
                    log_level=logging.INFO
                )
                
                # Start the browser
                await scraper.start()
                logger.info("Browser started successfully")
                
                # Navigate to login page
                login_url = 'https://dexscreener.com/watchlist'
                logger.info(f"Navigating to {login_url}")
                tab = await scraper.navigate(login_url)
                
                # Optional: Display login reminder
                await tab.evaluate("""
                    (() => {
                        const div = document.createElement('div');
                        div.id = 'login-reminder';
                        div.style = 'position: fixed; top: 20px; left: 20px; background: red; color: white; padding: 20px; z-index: 9999; font-size: 24px; border-radius: 5px;';
                        div.textContent = 'Please log in to DexScreener now. This window will automatically close after you login.';
                        document.body.appendChild(div);
                    })()
                """)
                
                # Wait for manual login
                wait_seconds = wait_time_minutes * 60
                logger.info(f"Waiting {wait_time_minutes} minutes for manual login...")
                await scraper.wait(wait_seconds)
                
                # Save cookies after login
                await scraper.save_cookies()
                logger.info(f"Saved cookies to session file")
                
                # Verify authentication by checking watchlist access
                watch_url = 'https://dexscreener.com/watchlist/PZ5oeMyixsBce5Tshsqu'
                logger.info(f"Verifying authentication by accessing {watch_url}")
                
                tab = await scraper.navigate(watch_url)
                await scraper.wait(5)  # Wait a bit for page to load
                
                html_content = await scraper.get_page_content()
                
                if "ds-dex-table-row" in html_content:
                    logger.info("✅ Successfully authenticated! Watchlist is accessible.")
                    # Save profile info for reference
                    scraper.save_profile_info({
                        "authenticated": True,
                        "timestamp": datetime.now().isoformat(),
                        "notes": "Login successful - profile ready for scraping"
                    })
                    return True
                else:
                    logger.warning("⚠️ Authentication may not be complete. Could not verify watchlist access.")
                    return False
                
            except Exception as e:
                logger.error(f"Error in authentication process: {e}")
                raise
            finally:
                if scraper:
                    logger.info("Closing browser...")
                    await scraper.stop()
        
        try:
            return loop.run_until_complete(_auth())
        finally:
            loop.close()
    
    # Run the authentication process
    return run_auth()

# Create the DAG
with DAG(
    'auth_dexscreener_setup',
    default_args=default_args,
    description='DAG to set up DexScreener authentication profile using nodriver',
    schedule=None,  # Only manually triggered
    start_date=days_ago(1),
    catchup=False,
    tags=['auth', 'setup', 'profile', 'nodriver'],
) as dag:
    
    # Run the authentication task
    auth_task = setup_dexscreener_profile(wait_time_minutes=2)  # Increased to 2 minutes