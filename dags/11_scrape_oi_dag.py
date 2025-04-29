# dags/fetch_coinalyze.py

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import logging
from typing import List, Dict, Any
import pandas as pd

from tasks.oi_tasks import extract_coinalyze_data, build_aggregated_dataframe
from db_manager import DBManager

# Set up logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='scrape_btc_oi',
    default_args=default_args,
    schedule_interval='14 */2 * * *',  # every 2 hours
    catchup=False,
    tags=['coinalyze', 'scraping'],
) as dag:
    
    @task(pool='scraping_pool')
    def scrape_coinalyze() -> List[Dict[str, Any]]:
        """
        Task to scrape Coinalyze data using nodriver.
        """
        # Define target URLs
        target_urls = [
            {"url": "https://coinalyze.net/bitcoin/open-interest/", "category": "open_interest"},
            {"url": "https://coinalyze.net/bitcoin/funding-rate/",  "category": "funding_rate"},
            {"url": "https://coinalyze.net/bitcoin/liquidations/",  "category": "liquidations"},
            {"url": "https://coinalyze.net/bitcoin/long-short-ratio/", "category": "long_short_ratio"}
        ]
        
        try:
            # Extract data from all targets
            results = extract_coinalyze_data(target_urls)
            
            # Build the aggregated DataFrame
            df_aggregated = build_aggregated_dataframe(results)
            
            # Convert timestamp to string for XCom serialization
            df_aggregated['timestamp'] = df_aggregated['timestamp'].astype(str)
            logger.info(f"DataFrame info: {df_aggregated.info()}")
            
            # Convert DataFrame to dict for XCom
            return df_aggregated.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error scraping Coinalyze data: {e}")
            return []
    
    @task
    def store_coinalyze_data(records: List[Dict[str, Any]], db_url: str = None) -> None:
        """
        Task to store the Coinalyze data in the database.
        """
        db_url = db_url or os.getenv("AIRFLOW_CONN_PIPELINE_DB")
        if not db_url:
            raise ValueError("Database URL (AIRFLOW_CONN_PIPELINE_DB) not provided.")
        
        if not records:
            logger.warning("No data records found; nothing to insert.")
            return
        
        # Convert records back to DataFrame
        df_aggregated = pd.DataFrame(records)
        
        # Convert timestamp strings back to datetime
        df_aggregated['timestamp'] = pd.to_datetime(df_aggregated['timestamp'], utc=True)
        
        # Connect to database
        db_manager = DBManager(db_name='pipeline_db')
        
        # Table name for Coinalyze data
        table_name = 'btc_oi'
        
        # Check if table exists
        inspector = db_manager.engine.dialect.get_table_names(db_manager.engine.connect())
        if table_name not in inspector:
            # Table does not exist -> create it with entire DataFrame
            db_manager.insert_raw_dataframe(table_name, df_aggregated)
            logger.info(f"Created '{table_name}' with {len(df_aggregated)} rows.")
            return
        
        # Table exists -> fetch the max timestamp
        query = f"SELECT MAX(timestamp) as max_ts FROM {table_name}"
        df_max_ts = pd.DataFrame(db_manager.execute_query(query))
        
        if df_max_ts.empty or pd.isnull(df_max_ts['max_ts'].iloc[0]):
            # Table is empty or no valid timestamp -> insert everything
            df_new = df_aggregated
            logger.info(f"No existing rows in '{table_name}' or no max timestamp found.")
        else:
            max_ts_value = pd.to_datetime(df_max_ts['max_ts'].iloc[0], utc=True)
            logger.info(f"Latest timestamp in '{table_name}' = {max_ts_value}")
            
            # Filter new data to rows strictly newer than max_ts_value
            df_new = df_aggregated[df_aggregated['timestamp'] > max_ts_value]
        
        if not df_new.empty:
            # Insert only the new rows
            db_manager.insert_raw_dataframe(table_name, df_new)
            logger.info(f"Inserted {len(df_new)} new rows into '{table_name}'.")
        else:
            logger.info(f"No new rows to insert. '{table_name}' is already up-to-date.")
    
    # Define the task dependencies using TaskFlow API
    db_url = os.getenv('AIRFLOW_CONN_PIPELINE_DB')
    store_coinalyze_data(scrape_coinalyze(), db_url)