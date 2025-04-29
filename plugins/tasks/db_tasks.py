# plugins/custom/tasks/db_tasks.py

import logging
from typing import Optional, Union, List, Dict, Any
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.hooks.base import BaseHook
import pandas as pd

# Import your DBManager
from db_manager import DBManager

from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule

logger = logging.getLogger(__name__)

# Pre-fetch connections to avoid fetching them inside each task
def get_db_credentials():
    """
    Fetches the database connection URI from Airflow Connections.
    
    :return: The database URI as a string.
    """
    try:
        db_conn = BaseHook.get_connection("pipeline_db")
        db_url = db_conn.get_uri()
        logger.info("Successfully fetched 'pipeline_db' connection.")
        return db_url
    except Exception as e:
        logger.error("Error fetching 'pipeline_db' connection.", exc_info=True)
        raise

# Pre-fetch the database URL
DB_URL = get_db_credentials()

@task(task_id='task_execute_sql_query', trigger_rule=TriggerRule.ONE_SUCCESS)
def task_execute_sql_query(
    query: str,
    table_name: Optional[str] = None, 
    return_list: bool = False
) -> Optional[List[Dict[str, Any]]]:
    """
    Executes a SQL query against the PostgreSQL database and returns the result.
    
    :param query: The SQL query string to execute.
    :param table_name: (Optional) Name of the table related to the query, used for logging purposes.
    :param return_list: If True, returns the result as a list of dictionaries.
    :return: Either a pandas DataFrame or a list of dictionaries.
    """
    logger.info(f"Executing SQL query on table '{table_name}': {query}")
    
    try:
        # Initialize the DBManager with the fetched DB_URL
        db_manager = DBManager(db_url=DB_URL)
        
        # Execute the query and retrieve the DataFrame
        df = db_manager.execute_query(query, df=True)
        
        logger.info(f"Successfully executed query on table '{table_name}'. Retrieved {len(df)} records.")
        
        if return_list:
            logger.info("Converting DataFrame to list of dictionaries.")
            result = df.to_dict(orient='records')
            logger.info(f"Converted DataFrame to list with {len(result)} records.")
            return result
        
        return None  # Or return df if needed elsewhere
    except Exception as e:
        logger.error(f"Failed to execute query on table '{table_name}'. Error: {e}", exc_info=True)
        raise


@task(task_id='is_data_stale')
def is_data_stale(table_name: str, threshold_hours: int) -> bool:
    """
    Checks if the latest data in the specified table is older than the threshold.
    Returns False if the table does not exist.

    :param table_name: Name of the table to check.
    :param threshold_hours: The threshold in hours to determine data freshness.
    :return: True if data is stale, False otherwise.
    """
    logger.info(f"Checking if data in '{table_name}' is stale based on {threshold_hours} hours threshold.")

    # Instantiate DBManager with the db_url
    try:
        db_manager = DBManager(db_url=DB_URL)
    except Exception as e:
        logger.error(f"Failed to instantiate DBManager with db_url '{DB_URL}': {e}", exc_info=True)
        raise

    # Define the SQL query to check table existence
    table_exists_query = f"""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        ) AS table_exists;
    """

    logger.info(f"Executing SQL query to check table existence: {table_exists_query}")

    try:
        # Execute the table existence query
        result = db_manager.execute_query(table_exists_query, df=True)
        
        if isinstance(result, pd.DataFrame):
            table_exists = result.iloc[0]['table_exists']
            logger.info(f"Table '{table_name}' exists: {table_exists}")
        else:
            logger.error(f"Unexpected result type: {type(result)}")
            raise ValueError("Unexpected result type from execute_query")
        
        if not table_exists:
            logger.info(f"Table '{table_name}' does not exist. Returning False.")
            return True  # Table does not exist, treat as not stale (or handle as per your logic)
        
    except Exception as e:
        logger.error(f"Error checking table existence for '{table_name}': {e}", exc_info=True)
        raise

    # Define the SQL query to get the latest timestamp
    latest_timestamp_query = f"""
        SELECT MAX(timestamp) as latest_timestamp
        FROM {table_name};
    """

    logger.info(f"Executing SQL query to get latest timestamp: {latest_timestamp_query}")

    try:
        # Execute the query
        result = db_manager.execute_query(latest_timestamp_query, df=True)
        if isinstance(result, pd.DataFrame):
            latest_timestamp = result.iloc[0]['latest_timestamp']
            logger.info(f"Latest data timestamp in table '{table_name}': {latest_timestamp}")
        else:
            logger.error(f"Unexpected result type: {type(result)}")
            raise ValueError("Unexpected result type from execute_query")

        if latest_timestamp is None:
            logger.info(f"No existing data found in table '{table_name}'. Need to fetch new data.")
            return True  # No data exists, need to fetch

        # Current time in UTC
        current_time = datetime.utcnow()

        # Calculate the difference
        time_diff = current_time - latest_timestamp

        logger.info(f"Time since last update in table '{table_name}': {time_diff}")

        if time_diff.total_seconds() > threshold_hours * 3600:
            logger.info(f"Data in table '{table_name}' is stale. Need to fetch new data.")
            return True  # Data is stale
        else:
            logger.info(f"Data in table '{table_name}' is fresh. No need to fetch new data.")
            return False  # Data is fresh

    except Exception as e:
        logger.error(f"Error checking data freshness for table '{table_name}': {e}", exc_info=True)
        raise