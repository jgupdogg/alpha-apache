from datetime import datetime
import logging
import time
import os
import asyncio
import nest_asyncio
import nodriver as uc
from nodriver import Config

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("TestXAuth")

async def test_x_auth():
    """
    Test script to try accessing X without virtual display
    """
    # Define profile directory
    profile_dir = os.path.expanduser("~/test_profiles")
    os.makedirs(profile_dir, exist_ok=True)
    logger.info(f"Using profile directory: {profile_dir}")
    
    # Define specific profile for X auth
    x_profile_dir = os.path.join(profile_dir, "x_test")
    os.makedirs(x_profile_dir, exist_ok=True)
    
    browser = None
    try:
        logger.info("Initializing browser for X login test...")
        
        # Simple browser arguments
        browser_args = [
            f'--user-data-dir={x_profile_dir}',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--window-size=1920,1080'
        ]
        
        # Create config
        config = Config(
            headless=False,
            user_data_dir=x_profile_dir,
            browser_args=browser_args
        )
        
        # Start browser directly
        logger.info("Starting browser...")
        browser = await uc.start(config=config)
        logger.info("Browser started successfully!")
        
        # Navigate to X
        logger.info("Navigating to X login page...")
        tab = await browser.get('https://x.com/login')
        
        # Add a visual indicator
        await tab.evaluate("""
            (() => {
                // Check if we're on a page with a body
                if (document.body) {
                    const div = document.createElement('div');
                    div.id = 'login-reminder';
                    div.style = 'position: fixed; top: 20px; left: 20px; background: red; color: white; padding: 20px; z-index: 9999; font-size: 24px; border-radius: 5px;';
                    div.textContent = 'TEST MODE: Browser is visible! This is the X login page.';
                    document.body.appendChild(div);
                    console.log('Added visual indicator');
                } else {
                    console.log('No document body found');
                }
            })()
        """)
        
        # Wait for manual verification
        logger.info("Waiting 30 seconds for you to confirm browser visibility...")
        await asyncio.sleep(30)
        
        # Get page title
        title = await tab.get_title()
        logger.info(f"Page title: {title}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in X auth test: {e}")
        return False
    finally:
        if browser:
            logger.info("Closing browser...")
            await browser.stop()

# Run the test
if __name__ == "__main__":
    logger.info("Setting up X auth test")
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(test_x_auth())
    logger.info(f"Test result: {'Success' if result else 'Failed'}")