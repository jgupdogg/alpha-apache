# plugins/utils/twitter_utils.py

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Set, Dict, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException
)

from utils.scrapers.custom_driver import CustomDriver  # Ensure this path is correct

logger = logging.getLogger(__name__)

class TwitterSession:
    def __init__(self, driver: CustomDriver = None):
        """
        Initialize a TwitterSession with a provided CustomDriver.
        """
        if driver:
            self.browser = driver
            logger.info("TwitterSession initialized with external driver.")
        else:
            self.browser = CustomDriver(headless=False, local_profile=True)
            logger.info("TwitterSession initialized with internal driver.")
    
    def login(self):
        """
        Perform login to Twitter (or X) if necessary.
        Implement this method based on your authentication mechanism.
        """
        try:
            login_url = "https://x.com/login"
            self.browser.navigate(login_url)
            # Add your login steps here (e.g., entering username and password)
            logger.info("Navigated to login page.")
            # Example:
            # self.browser.driver.find_element(By.NAME, "username").send_keys("your_username")
            # self.browser.driver.find_element(By.NAME, "password").send_keys("your_password")
            # self.browser.driver.find_element(By.XPATH, "//button[@type='submit']").click()
            # Add appropriate waits and error handling
            # For demonstration, we'll wait for 30 seconds to manually complete login
            time.sleep(30)
            logger.info("Assuming login is completed.")
        except Exception as e:
            logger.error(f"Error during login: {e}", exc_info=True)
            raise
    
    # --------------------------------------------------------------------------
    # 3. Navigation and basic scraping
    # --------------------------------------------------------------------------
    def open_profile(self, handle: str) -> None:
        """Navigate to the main profile URL."""
        try:
            profile_url = f"https://x.com/{handle}"
            self.browser.navigate(profile_url)
            logger.info(f"Navigated to {profile_url}")
        except WebDriverException as e:
            logger.error(f"Failed to open profile URL: {profile_url}", exc_info=True)
            raise
        self.browser.random_wait()
        
    
    def scrape_profile_data(self, wait_timeout: int = 10) -> dict:
        """
        Scrape follower count and joined date from the profile.
        Returns a dict:
        {
            "followers": <str or None>,
            "joined_date": <str or None>
        }
        """
        try:
            followers_xpath = (
                "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div/div/div/div/div/div[5]/div[2]/a/span[1]/span"
            )
            joined_date_xpath = (
                "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div/div/div/div/div/div[4]/div/span/span"
            )

            # Use the injected driver for waiting
            wait = WebDriverWait(self.browser.driver, wait_timeout)

            followers_element = wait.until(
                EC.visibility_of_element_located((By.XPATH, followers_xpath))
            )
            joined_date_element = wait.until(
                EC.visibility_of_element_located((By.XPATH, joined_date_xpath))
            )

            data = {
                "followers": followers_element.text.strip(),
                "joined_date": joined_date_element.text.strip(),
            }
            logger.info(f"Scraped profile data: {data}")
            return data

        except Exception as e:
            logger.error("Error scraping profile data", exc_info=True)
            return {"followers": None, "joined_date": None}
        
    # --------------------------------------------------------------------------
    # 4. Scraping Followers You Follow
    # --------------------------------------------------------------------------
    def scrape_followers_you_follow(self, handle: str, wait_timeout: int = 10) -> Set[str]:
        """
        Scrape 'followers_you_follow' for the given Twitter handle.
        Returns a set of follower account names.
        """
        try:
            self.navigate_to_followers_you_follow(handle)
            fyf_accounts = self.scroll_and_collect_followers(wait_timeout=wait_timeout)
            return fyf_accounts
        except Exception as e:
            logger.error(f"Error scraping 'followers_you_follow' for @{handle}: {e}", exc_info=True)
            return set()
    
    # --------------------------------------------------------------------------
    # 5. Navigation to followers_you_follow pages
    # --------------------------------------------------------------------------
    def navigate_to_followers_you_follow(self, handle: str) -> None:
        """
        Append '/followers_you_follow' to the main URL and navigate there.
        """
        full_url = f"https://x.com/{handle}/followers_you_follow"
        try:
            self.browser.navigate(full_url)
            logger.info(f"Navigated to {full_url}")
        except WebDriverException as e:
            logger.error(f"Failed to navigate to {full_url}", exc_info=True)
            raise
        self.browser.random_wait()

    # --------------------------------------------------------------------------
    # 6. Scroll-based follower collection
    # --------------------------------------------------------------------------
    def scroll_and_collect_followers(self, wait_timeout: int = 10) -> Set[str]:
        """
        Scroll through the dynamically loaded follower list and collect unique account names.
        Uses a sentinel approach: stop when no new followers appear between scrolls.

        This method can be used after navigating to *either* '/followers' or '/followers_you_follow',
        because both pages have a similar structure for listing accounts.
        """
        all_accounts = set()
        last_count = 0

        container_xpath = "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/section/div/div"
        account_xpath_relative = ".//div/button/div/div[2]/div[1]/div[1]/div/div[2]/div/a/div/div/span"

        while True:
            try:
                container_element = WebDriverWait(self.browser.driver, wait_timeout).until(
                    EC.presence_of_element_located((By.XPATH, container_xpath))
                )
                follower_items = container_element.find_elements(By.XPATH, "./div")
                logger.info(f"Found {len(follower_items)} follower items on the page.")

                for item in follower_items:
                    try:
                        account_element = item.find_element(By.XPATH, account_xpath_relative)
                        account_name = account_element.text.strip()
                        if account_name:
                            all_accounts.add(account_name)
                    except Exception:
                        # Possibly an ad or a weird item
                        pass

                if len(all_accounts) == last_count:
                    # No new accounts found => end collection
                    logger.info("No new follower accounts found in this scroll. Ending collection.")
                    break
                else:
                    last_count = len(all_accounts)
                    logger.info(f"Collected {last_count} unique follower accounts so far.")

                self.browser._scroll_down(2000)
                self.browser.random_wait()

            except TimeoutException:
                logger.error("Timeout while waiting for follower container. Ending collection.", exc_info=True)
                break
            except WebDriverException as e:
                logger.error("WebDriver exception during follower collection.", exc_info=True)
                break
            except Exception as e:
                logger.error("Unexpected error during scrolling and collecting followers.", exc_info=True)
                break

        return all_accounts


    def close(self):
        """
        Close the browser session.
        """
        try:
            self.browser.quit()
            logger.info("Browser session closed successfully.")
        except Exception as e:
            logger.error(f"Error closing browser session: {e}", exc_info=True)
