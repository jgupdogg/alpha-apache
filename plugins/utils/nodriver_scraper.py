import asyncio
import os
import logging
from typing import List, Dict, Union, Optional, Any
import json
import re
from datetime import datetime
import base64
import tempfile
import subprocess
import nodriver as uc
from nodriver import Config, ContraDict
import nodriver.cdp.network as network
import random
from pyvirtualdisplay import Display
from urllib.parse import urlparse, quote
import pandas as pd
from pathlib import Path 
import time
from typing import Tuple
import shutil
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nodriver_scraper")

class SimpleNetworkCapture:
    """Simple class to capture network requests and responses."""
    
    def __init__(self, tab, url_pattern):
        self.tab = tab
        self.url_pattern = url_pattern
        self.requests = []
        self.responses = []
    
    async def start_monitoring(self):
        await self.tab.send(network.enable())
        self.tab.add_handler(network.RequestWillBeSent, self._on_request)
        self.tab.add_handler(network.ResponseReceived, self._on_response)
    
    async def stop_monitoring(self):
        await self.tab.send(network.disable())
    
    def _on_request(self, event):
        request = event.request
        if re.search(self.url_pattern, request.url):
            self.requests.append({
                'requestId': event.request_id,
                'url': request.url,
                'method': request.method,
                'headers': request.headers
            })
    
    def _on_response(self, event):
        request_match = next((req for req in self.requests if req['requestId'] == event.request_id), None)
        if request_match:
            self.responses.append({
                'requestId': event.request_id,
                'url': event.response.url,
                'status': event.response.status,
                'headers': event.response.headers,
                'mimeType': event.response.mime_type
            })
    
    async def wait_for_response(self, timeout=30):
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if self.responses:
                return self.responses[-1]  # Return most recent response
            await asyncio.sleep(0.5)
        return None



class WebScraper:
    """
    Simplified webscraper class built on nodriver to perform basic operations.
    Modified to follow the working test approach but retains the option for virtual display.
    """
    
    def __init__(self, 
                headless: bool = False,
                user_data_dir: str = None, 
                profile_name: str = None,
                no_sandbox: bool = True,
                cookies_file: str = None,
                log_level: int = logging.INFO,
                use_virtual_display: bool = True,
                display_visible: bool = False,
                display_size: tuple = (1920, 1080)):
        """
        Initialize the WebScraper with simplified configuration options.
        
        Args:
            headless: Whether to run the browser in headless mode
            user_data_dir: Directory to store user data
            profile_name: Name of profile to use
            no_sandbox: Whether to run Chrome without sandbox
            cookies_file: File to save/load cookies from
            log_level: Logging level
            use_virtual_display: Whether to use a virtual display (requires pyvirtualdisplay)
            display_visible: Whether the virtual display should be visible
            display_size: Size of the virtual display
        """
        # Setup logging
        logging.basicConfig(level=log_level)
        self.browser_id = f"browser_{id(self)}"
        self.logger = logging.getLogger("WebScraper")
        self.logger.info(f"Creating browser instance {self.browser_id}")

        # Virtual display settings
        self.use_virtual_display = use_virtual_display
        self.display_visible = display_visible
        self.display_size = display_size
        self.display = None
        
        # Check if virtual display is available - do this inside __init__ to avoid global variable issues
        self.has_display = False
        if self.use_virtual_display:
            try:
                # Try to import Display only if virtual display is requested
                from pyvirtualdisplay import Display
                self.Display = Display  # Store the class for later use
                self.has_display = True
                self.logger.info("Virtual display support is available")
            except ImportError:
                self.logger.warning("Virtual display requested but pyvirtualdisplay not installed. Run 'pip install pyvirtualdisplay'")
                self.use_virtual_display = False
        
        # Always initialize profile_name attribute
        self.profile_name = profile_name
        
        # Setup custom profile directory if needed
        if self.profile_name and user_data_dir:
            # Create a profile-specific subdirectory
            profile_dir = os.path.join(user_data_dir, profile_name)
            os.makedirs(profile_dir, exist_ok=True)
            self.user_data_dir = profile_dir
        else:
            # Create a temp dir if none provided
            timestamp = int(time.time())
            self.user_data_dir = user_data_dir or os.path.join(tempfile.gettempdir(), f"nodriver_{timestamp}")
            os.makedirs(self.user_data_dir, exist_ok=True)

        # Set cookies file path
        if cookies_file is None and self.profile_name:
            self.cookies_file = os.path.join(self.user_data_dir, '.session.dat')
        else:
            self.cookies_file = cookies_file or '.session.dat'
            
        # Profile info file path
        self.profile_info_file = os.path.join(self.user_data_dir, 'profile_info.json')

        # Keep browser arguments minimal like in the test script
        self.browser_arguments = [
            f'--user-data-dir={self.user_data_dir}',
            '--window-size=1920,1080'
        ]
        
        if no_sandbox:
            self.browser_arguments.extend([
                '--no-sandbox',
                '--disable-setuid-sandbox'
            ])
        
        # Create config with these arguments
        self.config = Config(
            headless=headless,
            user_data_dir=self.user_data_dir,
            browser_args=self.browser_arguments
        )
        
        # Additional settings based on working test
        self.config.connection_timeout = 30  # seconds
        
        self.browser = None
        self.main_tab = None
        self.is_logged_in = False

    async def start(self):
        """Start browser with retry logic."""
        self.logger.info(f"Starting browser (profile: {self.user_data_dir})")
        
        # Start virtual display if needed and available
        if self.use_virtual_display and self.has_display:
            try:
                self.display = self.Display(visible=1 if self.display_visible else 0, size=self.display_size)
                self.display.start()
                self.logger.info(f"Started virtual display (visible: {self.display_visible}, size: {self.display_size})")
            except Exception as e:
                self.logger.warning(f"Failed to start virtual display: {e}")
                self.display = None
        
       # Retry logic with exponential backoff
        max_retries = 3
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    # Calculate backoff with randomization
                    backoff = (2 ** attempt) + random.uniform(1, 5)
                    self.logger.info(f"Retrying browser start (attempt {attempt+1}/{max_retries+1}) after {backoff:.2f}s delay")
                    # Wait longer between retries with exponential backoff
                    await asyncio.sleep(backoff)
                
                # Start browser the same way as in the test script
                self.browser = await uc.start(config=self.config)
                self.main_tab = await self.browser.get('about:blank')
                
                self.logger.info("Browser started successfully")
                return
                
            except Exception as e:
                error_msg = str(e)
                self.logger.warning(f"Browser start failed: {error_msg}")
                
                # More helpful error message for sandbox issues
                if "sandbox" in error_msg.lower():
                    self.logger.error("This appears to be a sandbox issue. Ensure no_sandbox=True is set.")
                
                # Clean up resources thoroughly
                if self.browser:
                    try:
                        await self.browser.stop()
                    except Exception as cleanup_error:
                        self.logger.warning(f"Error during browser cleanup: {cleanup_error}")
                    self.browser = None
                    self.main_tab = None
                
                # Handle last attempt failure
                if attempt == max_retries:
                    self.logger.error(f"Failed to start browser after {max_retries+1} attempts")
                    raise
                    
    async def save_cookies(self, cookie_file: str = None) -> bool:
        """
        Save cookies to file.
        
        Args:
            cookie_file: Optional alternative cookie file path
            
        Returns:
            bool: True if cookies were saved successfully, False otherwise
        """
        if not self.browser:
            self.logger.error("Browser not started. Call start() first.")
            return False
        
        file_to_save = cookie_file or self.cookies_file
        
        # If profile name is set, use profile-specific cookie file
        if self.profile_name and not cookie_file:
            file_name, ext = os.path.splitext(file_to_save)
            file_to_save = f"{file_name}_{self.profile_name}{ext}"
        
        try:
            await self.browser.cookies.save(file_to_save)
            self.logger.info(f"Cookies saved to {file_to_save}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}")
            return False
    
    async def load_cookies(self, cookie_file: str = None) -> bool:
        """
        Load cookies from the saved file.
        
        Args:
            cookie_file: Optional alternative cookie file path
            
        Returns:
            bool: True if cookies were loaded successfully, False otherwise
        """
        if not self.browser:
            self.logger.error("Browser not started. Call start() first.")
            return False
        
        file_to_load = cookie_file or self.cookies_file
        
        try:
            if os.path.exists(file_to_load):
                await self.browser.cookies.load(file_to_load)
                self.logger.info(f"Cookies loaded from {file_to_load}")
                return True
            else:
                self.logger.warning(f"Cookie file {file_to_load} not found")
                return False
        except Exception as e:
            self.logger.error(f"Error loading cookies: {e}")
            return False
    

    
    async def stop(self):
        """Stop the browser and cleanup resources."""
        self.logger.info(f"Stopping browser instance {self.browser_id}")
        if self.browser:
            try:
                await self.browser.stop()
                self.logger.info("Browser stopped successfully")
            except Exception as e:
                self.logger.warning(f"Error stopping browser: {e}")
            finally:
                self.browser = None
                self.main_tab = None
        
        # Stop the display if we created one
        if self.display:
            try:
                self.display.stop()
                self.logger.info("Virtual display stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping virtual display: {e}")
            self.display = None
        
        # Only clean temp directories we created
        if self.user_data_dir and os.path.exists(self.user_data_dir) and not self.profile_name:
            if os.path.basename(self.user_data_dir).startswith("nodriver_"):
                try:
                    shutil.rmtree(self.user_data_dir, ignore_errors=True)
                    self.logger.info(f"Removed temporary user data directory: {self.user_data_dir}")
                except Exception as e:
                    self.logger.warning(f"Error removing user data directory: {e}")
        


    
    async def get_cookies_for_requests(self) -> List:
        """
        Get cookies in a format suitable for the requests library.
        
        Returns:
            List: Cookies formatted for requests
        """
        if not self.browser:
            self.logger.error("Browser not started. Call start() first.")
            return []
        
        return await self.browser.cookies.get_all(requests_cookie_format=True)
    
        
    async def check_login_status(self, url: str, logged_in_selector: str) -> bool:
        """
        Check if the user is currently logged in.
        
        Args:
            url: URL to navigate to for checking login status
            logged_in_selector: CSS selector that should exist only if logged in
            
        Returns:
            bool: True if logged in, False otherwise
        """
        if not self.browser:
            self.logger.error("Browser not started. Call start() first.")
            return False
        
        try:
            tab = await self.navigate(url)
            element = await self.find_element(logged_in_selector)
            
            self.is_logged_in = element is not None
            self.logger.info(f"Login status check: {'Logged in' if self.is_logged_in else 'Not logged in'}")
            
            return self.is_logged_in
        except Exception as e:
            self.logger.error(f"Error checking login status: {e}")
            return False
    

    async def navigate(self, url: str, new_tab: bool = False, 
                    new_window: bool = False, 
                    check_redirect: bool = False) -> Union[None, Any]:
        """
        Navigate to a URL and ensure proper URL formatting.

        Args:
            url: URL to navigate to
            new_tab: Whether to open in a new tab
            new_window: Whether to open in a new window
            check_redirect: Whether to check for redirects (currently disabled)

        Returns:
            Tab object or None if navigation failed
        """
        if not self.browser:
            self.logger.error("Browser not started. Call start() first.")
            return None

        try:
            # Ensure URL is properly formatted
            # Parse the URL to extract components
            parsed = urlparse(url)

            # Reconstruct with proper encoding if needed
            if not parsed.scheme:
                # Add https:// if missing
                url = f"https://{url}"
                parsed = urlparse(url)

            # Ensure the path is properly encoded
            path = quote(parsed.path)

            # Reconstruct the URL with proper encoding
            clean_url = f"{parsed.scheme}://{parsed.netloc}{path}"
            if parsed.query:
                clean_url += f"?{parsed.query}"
            if parsed.fragment:
                clean_url += f"#{parsed.fragment}"

            self.logger.info(f"Navigating to: {clean_url}")

            # Use the clean URL for navigation
            tab = await self.browser.get(clean_url, new_tab=new_tab, new_window=new_window)
            self.logger.info(f"Navigated to {clean_url}")

            if new_tab or new_window:
                return tab
            else:
                self.main_tab = tab
                return self.main_tab
        except Exception as e:
            self.logger.error(f"Error navigating to {url}: {e}")
            raise

    async def get_page_content(self, tab=None) -> str:
        """
        Get the HTML content of the current page.
        
        Args:
            tab: Optional tab object to get content from (uses main_tab if None)
            
        Returns:
            str: HTML content of the page
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return ""
        
        try:
            return await tab.get_content()
        except Exception as e:
            self.logger.error(f"Error getting page content: {e}")
            return ""
    
    async def find_elements(self, selector: str, tab=None) -> List:
        """
        Find elements matching a CSS selector.
        
        Args:
            selector: CSS selector
            tab: Optional tab object (uses main_tab if None)
            
        Returns:
            List: Found elements
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return []
        
        try:
            return await tab.select_all(selector)
        except Exception as e:
            self.logger.error(f"Error finding elements with selector '{selector}': {e}")
            return []
    
    async def find_element(self, selector: str, tab=None) -> Optional[Any]:
        """
        Find a single element matching a CSS selector.
        
        Args:
            selector: CSS selector
            tab: Optional tab object (uses main_tab if None)
            
        Returns:
            Element object or None if not found
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return None
        
        try:
            return await tab.select(selector)
        except Exception as e:
            self.logger.error(f"Error finding element with selector '{selector}': {e}")
            return None
    
    async def find_by_text(self, text: str, best_match: bool = True, tab=None) -> Optional[Any]:
        """
        Find an element containing specific text.
        
        Args:
            text: Text to search for
            best_match: Whether to return the best match or all matches
            tab: Optional tab object (uses main_tab if None)
            
        Returns:
            Element object or None if not found
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return None
        
        try:
            return await tab.find(text, best_match=best_match)
        except Exception as e:
            self.logger.error(f"Error finding element with text '{text}': {e}")
            return None
    
    async def click_element(self, element) -> bool:
        """
        Click on an element.
        
        Args:
            element: Element to click
            
        Returns:
            bool: True if click was successful, False otherwise
        """
        if not element:
            self.logger.error("No element provided to click")
            return False
        
        try:
            await element.click()
            return True
        except Exception as e:
            self.logger.error(f"Error clicking element: {e}")
            return False
    
    
    async def take_screenshot(self, filename: str = "screenshot.jpg", tab=None) -> bool:
        """
        Take a screenshot of the current page.
        
        Args:
            filename: Path to save the screenshot
            tab: Optional tab object (uses main_tab if None)
            
        Returns:
            bool: True if screenshot was taken successfully, False otherwise
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return False
        
        try:
            await tab.save_screenshot(filename=filename)
            self.logger.info(f"Screenshot saved to {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Error taking screenshot: {e}")
            return False
    
    async def scroll_down(self, pixels: int = 300, tab=None) -> bool:
        """
        Scroll down the page.
        
        Args:
            pixels: Number of pixels to scroll
            tab: Optional tab object (uses main_tab if None)
            
        Returns:
            bool: True if scroll was successful, False otherwise
        """
        tab = tab or self.main_tab
        if not tab:
            self.logger.error("No active tab. Navigate to a page first.")
            return False
        
        try:
            await tab.scroll_down(pixels)
            return True
        except Exception as e:
            self.logger.error(f"Error scrolling down: {e}")
            return False
    
    async def wait(self, seconds: float = 1.0) -> None:
        """
        Wait for a specified number of seconds.
        
        Args:
            seconds: Number of seconds to wait
        """
        if self.browser:
            await self.browser.wait(seconds)
        else:
            await asyncio.sleep(seconds)

    
    async def _simulate_human_scrolling(self, tab):
        """Simulate human-like scrolling behavior."""
        try:
            # Get page height
            page_height = await tab.evaluate("document.body.scrollHeight")
            
            # Number of scroll steps
            steps = 3
            
            # Scroll down in steps with random pauses
            for i in range(1, steps + 1):
                scroll_amount = (page_height / steps) * i
                await tab.evaluate(f"window.scrollTo(0, {scroll_amount})")
                await tab.sleep(random.uniform(0.3, 0.7))
            
            # Scroll back to top with random pause
            await tab.sleep(random.uniform(0.5, 1.0))
            await tab.evaluate("window.scrollTo(0, 0)")
            await tab.sleep(random.uniform(0.3, 0.7))
        except Exception as e:
            self.logger.warning(f"Error during scroll simulation: {e}")

    async def _simulate_human_mouse_movement(self, tab, target_element):
        """Simulate human-like mouse movement before clicking an element."""
        try:
            # Get element position
            element_rect = await tab.evaluate(f"""
                (() => {{
                    const elem = {target_element};
                    if (!elem) return null;
                    const rect = elem.getBoundingClientRect();
                    return {{
                        left: rect.left,
                        top: rect.top,
                        width: rect.width,
                        height: rect.height
                    }};
                }})()
            """)
            
            if not element_rect:
                return
            
            # Calculate target coordinates (center of element)
            target_x = element_rect['left'] + (element_rect['width'] / 2)
            target_y = element_rect['top'] + (element_rect['height'] / 2)
            
            # Start from a random position on the screen
            viewport_size = await tab.evaluate("({width: window.innerWidth, height: window.innerHeight})")
            start_x = random.uniform(0, viewport_size['width'])
            start_y = random.uniform(0, viewport_size['height'])
            
            # Number of points in the mouse path
            num_points = random.randint(5, 10)
            
            # Generate a slightly curved path to the target
            for i in range(1, num_points + 1):
                # Add some randomness to the path
                progress = i / num_points
                x = start_x + (target_x - start_x) * progress + random.uniform(-10, 10)
                y = start_y + (target_y - start_y) * progress + random.uniform(-10, 10)
                
                # Move mouse to this point
                await tab.mouse_move(x, y)
                
                # Random delay between moves
                await tab.sleep(random.uniform(0.01, 0.05))
            
            # Final move to the exact target
            await tab.mouse_move(target_x, target_y)
            await tab.sleep(random.uniform(0.2, 0.5))
        except Exception as e:
            self.logger.warning(f"Error during mouse movement simulation: {e}")

    
    
    def save_profile_info(self, profile_info: Dict, filename: str = None) -> bool:
        """
        Save additional profile information (e.g., login credentials) to a JSON file.
        
        Args:
            profile_info: Dictionary containing profile information
            filename: Optional file path for saving profile info
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not filename and self.profile_name:
            # Use profile name for the info file
            filename = f"profile_{self.profile_name}_info.json"
        elif not filename:
            filename = "profile_info.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(profile_info, f)
            self.logger.info(f"Profile info saved to {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving profile info: {e}")
            return False
    
    def load_profile_info(self, filename: str = None) -> Optional[Dict]:
        """
        Load profile information from a JSON file.
        
        Args:
            filename: Optional file path for loading profile info
            
        Returns:
            Dict: Profile information or None if loading failed
        """
        if not filename and self.profile_name:
            # Use profile name for the info file
            filename = f"profile_{self.profile_name}_info.json"
        elif not filename:
            filename = "profile_info.json"
        
        try:
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    info = json.load(f)
                self.logger.info(f"Profile info loaded from {filename}")
                return info
            else:
                self.logger.warning(f"Profile info file {filename} not found")
                return None
        except Exception as e:
            self.logger.error(f"Error loading profile info: {e}")
            return None
            

    
    
    
def parse_response_to_dataframe(body_content: str) -> Optional[pd.DataFrame]:
    """
    Parse API response body content to a pandas DataFrame.
    
    Args:
        body_content: JSON response body as string
        
    Returns:
        pandas.DataFrame or None if parsing fails
    """
    if not body_content:
        return None
    
    # Ensure body is a string
    if not isinstance(body_content, str):
        body_content = str(body_content)
    
    # Clean up the body
    body_content = body_content.strip()
    
    # Ensure valid JSON start
    if not body_content.startswith('{') and not body_content.startswith('['):
        json_start = max(body_content.find('{'), body_content.find('['))
        if json_start >= 0:
            body_content = body_content[json_start:]
        else:
            logger.error("Could not find valid JSON in response")
            return None
    
    # Parse JSON
    try:
        data = json.loads(body_content)
        
        # Handle different data structures
        if 'data' in data and 'rank' in data.get('data', {}) and isinstance(data['data']['rank'], list):
            # GMGN structure
            df = pd.DataFrame(data['data']['rank'])
        elif 'data' in data and isinstance(data.get('data'), list):
            # DEX3 structure
            df = pd.DataFrame(data['data'])
        elif isinstance(data, list):
            # Direct array
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Look for list values in the dict
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    df = pd.DataFrame(value)
                    break
            else:
                # No list found, use the dict itself
                df = pd.DataFrame([data])
        else:
            # Fallback
            df = pd.DataFrame([data])
        
        return df
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing response: {e}")
        return None

async def scrape_api_data(url: str, url_pattern: str, headless: bool = True, timeout: int = 30) -> Tuple[Optional[Dict[str, Any]], Optional[pd.DataFrame]]:
    """
    Scrape API data from a website.
    
    Args:
        url: URL to navigate to
        url_pattern: Regex pattern to match API requests
        headless: Whether to run browser in headless mode
        timeout: Maximum time to wait for API response in seconds
        
    Returns:
        Tuple of (raw_response_data, dataframe) or (None, None) if capture fails
    """
    # Initialize browser
    config = Config(headless=headless)
    browser = None
    
    try:
        # Start browser
        browser = await uc.start(config=config)
        tab = await browser.get('about:blank')
        
        # Set up network monitoring
        net_capture = SimpleNetworkCapture(tab, url_pattern)
        await net_capture.start_monitoring()
        
        # Navigate to URL
        tab = await browser.get(url)
        await asyncio.sleep(5)  # Wait for page to load
        
        # Wait for matching response
        response_metadata = await net_capture.wait_for_response(timeout=timeout)
        
        if not response_metadata:
            logger.warning(f"No API response captured for pattern: {url_pattern}")
            return None, None
        
        # Get response body
        request_id = response_metadata.get('requestId')
        if not request_id:
            logger.error("Response metadata missing 'requestId'")
            return response_metadata, None
        
        try:
            # Get response body
            body_data = await tab.send(
                network.get_response_body(request_id=request_id)
            )
            
            # Extract body content
            if isinstance(body_data, tuple):
                body_content = body_data[0]
            elif hasattr(body_data, 'body'):
                body_content = body_data.body
            else:
                body_content = str(body_data)
            
            # Add body to response metadata
            response_metadata['body'] = body_content
            
            # Parse response to DataFrame
            df = parse_response_to_dataframe(body_content)
            
            # Add metadata columns
            if df is not None:
                df['capture_time'] = pd.Timestamp.now()
                df['api_url'] = response_metadata.get('url', '')
            
            return response_metadata, df
            
        except Exception as e:
            logger.error(f"Error getting response body: {e}")
            response_metadata['body_error'] = str(e)
            return response_metadata, None
            
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return None, None
        
    finally:
        # Cleanup
        if 'net_capture' in locals():
            await net_capture.stop_monitoring()
        if browser:
            browser.stop()

def scrape_api_data_sync(url: str, url_pattern: str, headless: bool = True, timeout: int = 30) -> Tuple[Optional[Dict[str, Any]], Optional[pd.DataFrame]]:
    """
    Synchronous wrapper for scrape_api_data.
    
    Args:
        url: URL to navigate to
        url_pattern: Regex pattern to match API requests
        headless: Whether to run browser in headless mode
        timeout: Maximum time to wait for API response in seconds
        
    Returns:
        Tuple of (raw_response_data, dataframe) or (None, None) if capture fails
    """
    return asyncio.run(scrape_api_data(url, url_pattern, headless, timeout))
