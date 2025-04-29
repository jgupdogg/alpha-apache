# plugins/tasks/twitter_tasks.py

import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowSkipException

from utils.twitter_utils import TwitterSession  # Ensure this path is correct
from db_manager import DBManager  # Ensure this path is correct

# Configure logging
logger = logging.getLogger(__name__)

# Pre-fetch database connection to avoid doing it inside each task
def get_db_credentials():
    db_conn = BaseHook.get_connection("pipeline_db")
    return db_conn.get_uri()

DB_URL = get_db_credentials()

@task(pool='birdeye_pool', task_id='task_get_twitter_handles')
def task_get_twitter_handles(db_url: str, **kwargs) -> List[Dict[str, Any]]:
    """
    Fetches Twitter URLs from the database, normalizes them, and pushes to XCom.
    
    :param db_url: Database connection URL.
    :return: List of dictionaries containing 'blockchain', 'address', and 'twitter_url'.
    """
    db_manager = DBManager(db_url)
    try:
        query = """
        SELECT DISTINCT blockchain, address, "extensions.twitter" AS twitter_url
        FROM token_metadata_raw
        WHERE "extensions.twitter" IS NOT NULL AND "extensions.twitter" <> ''
        """
        records = db_manager.execute_query(query)
        logger.info(f"Fetched {len(records)} twitter handle records from DBManager.")

        # Normalize Twitter URLs
        for record in records:
            twitter_url = record.get("twitter_url", "")
            if twitter_url:
                # Replace 'twitter.com' with 'x.com' and ensure it starts with 'https://'
                twitter_url = twitter_url.replace("twitter.com", "x.com").replace("www.twitter.com", "x.com")
                if not twitter_url.startswith("https://"):
                    twitter_url = f"https://{twitter_url.lstrip('/')}"
                record["twitter_url"] = twitter_url
                logger.debug(f"Normalized Twitter URL: {twitter_url}")

        # Push updated records to XCom
        kwargs['ti'].xcom_push(key='twitter_handles', value=records)
        return records
    except Exception as e:
        logger.error(f"Error in task_get_twitter_handles: {e}", exc_info=True)
        kwargs['ti'].xcom_push(key='twitter_handles', value=[])
        return []
    finally:
        db_manager.close()

@task(pool='birdeye_pool', task_id='task_fetch_twitter_common_followers')
def task_fetch_twitter_common_followers(
    records: Optional[List[Dict[str, Any]]] = None,
    table_name: str = "twitter_common_followers",
) -> None:
    """
    Fetches Twitter followers and basic profile information for a list of tokens and stores the data in PostgreSQL.
    
    :param records: List of dictionaries containing 'blockchain', 'address', and 'twitter_handle'.
    :param table_name: Target table name in PostgreSQL for followers data.
    """
    
    if not records:
        logger.warning("No records provided for Twitter security info fetching. Skipping task.")
        return
    
    # Initialize DBManager
    db_manager = DBManager(DB_URL)
    
    # Initialize TwitterSession
    try:
        logger.info("Initializing TwitterSession for scraping.")
        twitter_session = TwitterSession()  # Ensure headless mode for automation
        twitter_session.login()  # Ensure this method logs into Twitter successfully
        logger.info("TwitterSession initialized and logged in successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize TwitterSession: {e}", exc_info=True)
        return
    
    # Prepare containers for DB inserts
    entities_rows: List[Dict[str, Any]] = []
    followers_rows: List[Dict[str, Any]] = []
    
    # Iterate over each record
    for idx, rec in enumerate(records, start=1):
        blockchain = rec.get("blockchain")
        address = rec.get("address")
        twitter_handle = rec.get("twitter_handle")
        
        if not twitter_handle:
            logger.warning(f"Record {idx}: No 'twitter_handle' found; skipping.")
            continue
        
        logger.info(f"Record {idx}: Scraping data for '{twitter_handle}' associated with Blockchain: '{blockchain}', Address: '{address}'")
        
        try:
            # Scrape Twitter data
            twitter_session.open_profile(twitter_handle)
            profile_data = twitter_session.scrape_profile_data()
            
            if profile_data:
                entities_rows.append({
                    "scraped_at":      datetime.utcnow(),
                    "blockchain":      blockchain,
                    "address":         address,
                    "twitter_handle":  twitter_handle,
                    "joined_date":     profile_data.get("joined_date"),
                    "followers":       profile_data.get("followers"),
                })
                logger.info(f"Record {idx}: Profile data scraped successfully.")
            else:
                logger.warning(f"Record {idx}: No profile data scraped for '{twitter_handle}'.")
                    
            
            # Scrape Followers You Follow
            fyf_accounts = twitter_session.scrape_followers_you_follow(twitter_handle)
            logger.info(f"Record {idx}: {len(fyf_accounts)} 'followers_you_follow' scraped.")
            
            for follower in fyf_accounts:
                followers_rows.append({
                    "scraped_at":        datetime.utcnow(),
                    "blockchain":        blockchain,
                    "address":           address,
                    "twitter_handle":    twitter_handle,
                    "follower_account":  follower,
                    "follower_type":     "followers_you_follow"
                })
            
        except Exception as e:
            logger.error(f"Record {idx}: Error scraping data for '{twitter_handle}': {e}", exc_info=True)
            continue  # Continue with the next record
    
    # Close TwitterSession
    try:
        twitter_session.close()
        logger.info("TwitterSession closed successfully.")
    except Exception as e:
        logger.warning(f"Error closing TwitterSession: {e}", exc_info=True)
    
    # Insert Entities
    if entities_rows:
        try:
            df_entities = pd.DataFrame(entities_rows)
            db_manager.insert_raw_dataframe("twitter_metadata", df_entities)
            logger.info(f"Inserted {len(df_entities)} rows into 'twitter_entities'.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting into 'twitter_entities': {e}", exc_info=True)
       
    # Insert Followers
    if followers_rows:
        try:
            df_followers = pd.DataFrame(followers_rows)
            db_manager.insert_raw_dataframe("twitter_common_followers", df_followers)
            logger.info(f"Inserted {len(df_followers)} rows into '{table_name}'.")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting into '{table_name}': {e}", exc_info=True)
    
    logger.info("Completed Twitter followers fetching and data storage.")

@task(pool='birdeye_pool', task_id='task_get_new_handles_from_followers')


def task_get_new_handles_from_followers(db_url: str, **kwargs) -> List[str]:
    """
    Extracts new Twitter handles from existing followers that are not already in the account_name field.
    
    :param db_url: Database connection URL.
    :return: List of new Twitter handles.
    """
    logger.info("Starting task_get_new_handles_from_followers...")
    ti = kwargs['ti']

    db_manager = DBManager(db_url)

    try:
        # 1) Get all distinct follower_accounts
        distinct_followers_sql = """
            SELECT DISTINCT follower_account
            FROM twitter_followers
            WHERE follower_account IS NOT NULL AND follower_account <> ''
        """
        results = db_manager.execute_query(distinct_followers_sql)
        candidate_handles = {r[0].lower() for r in results}  # set of lowercased strings

        logger.info(f"Found {len(candidate_handles)} unique follower_account entries.")

        # 2) Get all existing account_names
        existing_accounts_sql = """
            SELECT DISTINCT LOWER(account_name) AS account_name_lower
            FROM twitter_followers
            WHERE account_name IS NOT NULL AND account_name <> ''
        """
        existing_results = db_manager.execute_query(existing_accounts_sql)
        existing_accounts = {r[0] for r in existing_results}

        # 3) Determine new handles
        new_handles = [handle for handle in candidate_handles if handle not in existing_accounts]

        logger.info(f"{len(new_handles)} new handles remain after filtering out existing accounts.")

        # 4) Push to XCom
        ti.xcom_push(key='new_handles', value=new_handles)
        return new_handles

    except Exception as e:
        logger.error(f"Error in task_get_new_handles_from_followers: {e}", exc_info=True)
        ti.xcom_push(key='new_handles', value=[])
        return []
    finally:
        db_manager.close()



@task(pool='birdeye_pool', task_id='task_scrape_minimal_for_handles')
def task_scrape_minimal_for_handles(
    addresses: Optional[List[Dict[str, str]]] = None,
    table_name_entities: str = "twitter_entities",
    table_name_followers: str = "twitter_followers",
    freshness_hours: int = 24,
    **kwargs
) -> None:
    """
    Scrapes minimal Twitter data (profile and followers_you_follow) for new handles,
    checks freshness, and stores in PostgreSQL.

    :param addresses: List of dicts with "blockchain", "address", and "twitter_handle" keys.
    :param chrome_path: Path to Chrome executable.
    :param chromedriver_path: Path to ChromeDriver executable.
    :param table_name_entities: DB table for storing entity data (profile snapshots).
    :param table_name_followers: DB table for storing 'followers_you_follow'.
    :param freshness_hours: Number of hours after which data is considered stale.
    :param db_url: Database connection URL.
    """
    logger.info("Starting task_scrape_minimal_for_handles.")
    
    # Validate addresses
    if not addresses:
        logger.warning("No addresses provided. Skipping task.")
        raise AirflowSkipException("No addresses provided to scrape_minimal_for_handles.")
    
    # Extract Twitter handles from addresses
    all_handles = []
    for item in addresses:
        handle = item.get("twitter_handle")
        if handle:
            handle_str = handle.lstrip("@")  # Remove "@" if present
            all_handles.append(handle_str)
        else:
            logger.warning(f"No 'twitter_handle' found in item {item}, skipping.")
    
    if not all_handles:
        logger.warning("No valid twitter_handle found in addresses. Skipping.")
        raise AirflowSkipException("No valid handles to scrape.")
    
    # Initialize DBManager
    db_manager = DBManager(db_url=DB_URL)
    
    try:
        # Filter for stale or missing handles using existing filter_for_freshness
        stale_records = db_manager.filter_for_freshness(
            table_name=table_name_entities,
            blockchain="twitter",
            addresses=all_handles,
            freshness_hours=freshness_hours,
            timestamp_column="scraped_at",
            block_column="blockchain",
            address_column="twitter_handle"
        )
        
        if not stale_records:
            logger.info("All handles are fresh. Skipping scrape.")
            raise AirflowSkipException("All handles are fresh; skipping task_scrape_minimal_for_handles.")
        
        stale_handles = [record["address"] for record in stale_records]
        logger.info(f"{len(stale_handles)} handles need scraping: {stale_handles[:5]}...")
        
        # Initialize TwitterSession
        # Ensure TwitterSession is correctly implemented to accept chrome_path and chromedriver_path
        twitter_session = None
        try:
            logger.info("Initializing TwitterSession...")
            twitter_session = TwitterSession()
            twitter_session.login()
            logger.info("TwitterSession logged in successfully.")
        except Exception as e:
            logger.error(f"Error initializing TwitterSession: {e}", exc_info=True)
            raise
        
        # Initialize lists to hold scraped data
        entities_rows = []
        fyf_rows = []
        scraped_at = datetime.utcnow()
        
        # Iterate over each stale handle
        for idx, handle in enumerate(stale_handles, start=1):
            logger.info(f"[{idx}/{len(stale_handles)}] Scraping @{handle}")
            try:
                # Scrape Twitter profile data
                twitter_session.open_profile(handle)
                profile_data = twitter_session.scrape_profile_data()
                if not profile_data:
                    logger.warning(f"No profile data scraped for @{handle}. Skipping.")
                    continue
                
                # Append entity data
                entities_rows.append({
                    "blockchain":      "twitter",          # Set as "twitter" to denote the source
                    "address":         None,               # Optional: can be set if relevant
                    "twitter_handle":  handle,
                    "scraped_at":      scraped_at,
                    "joined_date":     profile_data.get("joined_date"),
                    "followers":       profile_data.get("followers"),
                    # Add other fields as necessary
                })
                
                # Scrape "followers_you_follow"
                fyf_accounts = twitter_session.scrape_followers_you_follow(handle)
                logger.info(f"Collected {len(fyf_accounts)} 'followers_you_follow' for @{handle}.")
                
                for acct in fyf_accounts:
                    fyf_rows.append({
                        "blockchain":        "twitter",
                        "address":           None,               # Optional: can be set if relevant
                        "twitter_handle":    handle,
                        "follower_account":  acct,
                        "follower_is_verif": "unknown",
                        "follower_type":     "followers_you_follow",
                        "scraped_at":        scraped_at
                    })
                    
            except Exception as e:
                logger.error(f"Error scraping data for @{handle}: {e}", exc_info=True)
                continue  # Proceed with the next handle
        
        # Close TwitterSession
        if twitter_session:
            try:
                twitter_session.close()
                logger.info("TwitterSession closed successfully.")
            except Exception as e:
                logger.warning(f"Error closing TwitterSession: {e}", exc_info=True)
        
        # Insert scraped data into twitter_entities
        if entities_rows:
            try:
                df_entities = pd.DataFrame(entities_rows)
                db_manager.insert_raw_dataframe(table_name_entities, df_entities)
                logger.info(f"Inserted {len(df_entities)} rows into '{table_name_entities}'.")
            except SQLAlchemyError as e:
                logger.error(f"Error inserting into '{table_name_entities}': {e}", exc_info=True)
        
        # Insert scraped data into twitter_followers
        if fyf_rows:
            try:
                df_fyf = pd.DataFrame(fyf_rows)
                db_manager.insert_raw_dataframe(table_name_followers, df_fyf)
                logger.info(f"Inserted {len(df_fyf)} rows into '{table_name_followers}'.")
            except SQLAlchemyError as e:
                logger.error(f"Error inserting into '{table_name_followers}': {e}", exc_info=True)
        
        logger.info("Completed minimal scrape for stale handles.")
    
    except AirflowSkipException as skip_e:
        logger.info(str(skip_e))
        raise
    except Exception as e:
        logger.error("Unexpected error in task_scrape_minimal_for_handles", exc_info=True)
        raise
    finally:
        # Ensure DB session is closed
        try:
            db_manager.quit()
            logger.info("Database session closed successfully.")
        except Exception as db_close_e:
            logger.warning(f"Error closing DB session: {db_close_e}", exc_info=True)