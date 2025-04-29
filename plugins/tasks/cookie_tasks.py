# tasks/cookie_tasks.py

import os
import logging
import time
import random
import re
import json
from typing import List, Dict, Any
from datetime import datetime, timezone
import pandas as pd
from bs4 import BeautifulSoup

from db_manager import DBManager
from utils.scrapers.custom_driver import CustomDriver  # Ensure CustomDriver is correctly implemented
from utils.scrapers.scraping_utils import extract_json_data, transform_json_to_df, aggregate_profile_jsons_to_df

logger = logging.getLogger(__name__)

def task_scrape_and_store_cookie_data(chrome_path: str, chromedriver_path: str, db_url: str, **kwargs) -> None:
    """
    Combines scraping of hyperlinks and storing extracted data into a single task to use one driver instance.
    """
    logger.info("Starting task_scrape_and_store_cookie_data")

    db_manager = DBManager(db_url=db_url)
    logger.info("DBManager instantiated successfully.")

    # Initialize the custom Selenium driver
    driver_wrapper = CustomDriver(
        chrome_path=chrome_path,
        chromedriver_path=chromedriver_path,
        headless=False,
        enable_logging=False,
        incognito=True,
        local_profile=False
    )
    time.sleep(random.uniform(2, 4))
    logger.info("CustomDriver initialized successfully for cookie scraping and data extraction.")

    try:
        # Step 1: Scrape hyperlinks
        all_hyperlinks = scrape_cookie_hyperlinks(driver_wrapper, max_range=10)
        logger.info(f"Total hyperlinks collected: {len(all_hyperlinks)}")

        # Create DataFrame and add timestamp
        hyperlink_df = pd.DataFrame(all_hyperlinks, columns=['hyperlink'])
        hyperlink_df['timestamp'] = datetime.now(timezone.utc).isoformat()

        # Insert into 'cookie_hyperlinks' table
        db_manager.insert_raw_dataframe('cookie_hyperlinks', hyperlink_df)
        logger.info(f"Inserted {len(hyperlink_df)} hyperlinks into 'cookie_hyperlinks' table.")

        # Step 2: Process each hyperlink
        for link in all_hyperlinks:
            max_attempts = 2
            attempts = 0
            data = None

            while attempts < max_attempts and data is None:
                attempts += 1
                logger.info(f"Attempt {attempts} for link: {link}")

                try:
                    # Navigate to the link
                    driver_wrapper.navigate(link)
                    driver_wrapper.random_wait(2, 4)

                    # Scroll to the bottom to load dynamic content
                    driver_wrapper.scroll_to_bottom()

                    # Get the page source
                    page_source = driver_wrapper.driver.execute_script("return document.documentElement.outerHTML;")

                    # Extract relevant script containing 'investorsBalance'
                    interest = scrape_filter_scripts(page_source, 'investorsBalance')
                    if not interest:
                        logger.warning(f"No scripts matched 'investorsBalance' on {link}")
                        continue

                    script_content = interest[0]['content'].get_text()

                    # Extract JSON data from the script
                    data = extract_json_data(script_content)
                    if data:
                        logger.info("JSON extraction succeeded.")
                    else:
                        logger.warning(f"Data extraction failed on attempt {attempts} for link: {link}")

                except Exception as e:
                    logger.error(f"Error processing link {link} on attempt {attempts}: {e}", exc_info=True)
                    data = None

            if data is None:
                logger.warning(f"Skipping link after {max_attempts} attempts: {link}")
                continue

            # Process the extracted data
            try:
                series_keys = ['onChainStats', 'twitterStats', 'marketCapGraph']
                series = {key: data['children'][0][3]['profile']['projectSummary'][key] for key in series_keys}
                summ = {
                    k: data['children'][0][3]['profile']['projectSummary'][k]
                    for k in data['children'][0][3]['profile']['projectSummary'].keys()
                    if k not in series_keys
                }

                final_tables = {}
                final_tables['cookie_profile'] = aggregate_profile_jsons_to_df(summ)
                final_tables['cookie_onchain'] = transform_json_to_df(series['onChainStats']['dataPoints'])
                final_tables['cookie_twitter'] = transform_json_to_df(series['twitterStats']['dataPoints'])
                final_tables['cookie_mktcap'] = pd.DataFrame(series['marketCapGraph'])

                # Clean up DataFrames
                unwanted_columns = [
                    'hhindex_value',
                    'hhindex_valueForSimilarProjects',
                    'theilindex_value',
                    'theilindex_valueForSimilarProjects'
                ]
                final_tables['cookie_onchain'].drop(columns=unwanted_columns, inplace=True, errors='ignore')
                final_tables['cookie_onchain'].rename(
                    columns={'contractaddress': 'contractAddress'},
                    inplace=True,
                    errors='ignore'
                )

                # We'll assume 'contractAddress' is present for this logic to work
                address = final_tables['cookie_onchain']['contractAddress'].iloc[0]

                # -------------- NEW CODE: Flatten dictionaries / convert lists -------------
                from utils.scrapers.scraping_utils import flatten_dictionary_columns

                # Add timestamp and contractAddress to each DataFrame and insert into DB
                for key, df in final_tables.items():
                    # Flatten dict columns and convert list columns in-place
                    df = flatten_dictionary_columns(df)

                    df['timestamp'] = datetime.now(timezone.utc).isoformat()
                    df['contractAddress'] = address

                    db_manager.insert_raw_dataframe(key, df)
                    logger.info(f"Inserted data into '{key}' table.")

            except Exception as e:
                logger.error(f"Error finalizing data for link {link}: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.error(f"Error in task_scrape_and_store_cookie_data: {e}", exc_info=True)
        raise

    finally:
        driver_wrapper.quit()
        logger.info("CustomDriver closed successfully.")


def scrape_cookie_hyperlinks(web_driver: CustomDriver, max_range: int = 10) -> List[str]:
    """
    Scrapes hyperlinks from the cookie.fun website by interacting with the web driver.
    """
    logger.info("Starting scrape_cookie_hyperlinks function.")

    web_driver.navigate("https://www.cookie.fun/")
    web_driver.random_wait(5, 10)

    # Define XPaths
    click_header_xpath = "/html/body/div[2]/div[1]/div/div[2]/div/div[3]/div[2]/div/div/table/thead/tr/th[7]/div/div"
    anchor_xpath = "/html/body/div[2]/div[1]/div/div[2]/div/div[3]/div[2]/div/div/table/tbody/tr/td[1]/div/a"

    all_hyperlinks = []

    for i in range(max_range):
        try:
            # Click the header to sort or load data
            # web_driver.click_element_by_xpath(click_header_xpath)
            # web_driver.random_wait(8, 10)

            # Extract hyperlinks
            items_after_header = web_driver.extract_data_by_xpath(anchor_xpath, attribute="href")
            new_items = [link for link in items_after_header if link not in all_hyperlinks]
            all_hyperlinks.extend(new_items)
            logger.info(f"Iteration {i+1} - after clicking header, new items: {new_items}")

            # Click the 'Next' button to load more data
            next_button_xpath = "/html/body/div[2]/div[1]/div/div[2]/div/div[4]/div/div/button[2]"
            web_driver.click_element_by_xpath(next_button_xpath)
            web_driver.random_wait(2, 6)

            # Extract hyperlinks again after clicking 'Next'
            items_after_next = web_driver.extract_data_by_xpath(anchor_xpath, attribute="href")
            new_items = [link for link in items_after_next if link not in all_hyperlinks]
            all_hyperlinks.extend(new_items)
            logger.info(f"Iteration {i+1} - after clicking next button, new items: {new_items}")

        except Exception as e:
            logger.error(f"Error during scrape_cookie_hyperlinks iteration {i+1}: {e}", exc_info=True)
            continue

    logger.info(f"Total hyperlinks collected: {len(all_hyperlinks)}")
    return all_hyperlinks

def scrape_filter_scripts(page_source: str, keyword: str) -> List[Dict[str, Any]]:
    """
    Filters and extracts script tags containing a specific keyword.
    """
    logger.info(f"Filtering scripts containing keyword: {keyword}")

    soup = BeautifulSoup(page_source, 'html.parser')
    script_tags = soup.find_all('script')

    scripts_info = [
        {
            'index': idx,
            'interest': keyword in tag.get_text(),
            'content': tag
        }
        for idx, tag in enumerate(script_tags)
    ]

    interest = [script for script in scripts_info if script['interest']]

    logger.info(f"Found {len(interest)} scripts containing the keyword '{keyword}'.")
    return interest
