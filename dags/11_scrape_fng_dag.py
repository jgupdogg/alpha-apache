# dags/scrape_and_store_btc_fng.py

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import logging
from typing import List, Dict, Any

from tasks.fng_tasks import _fetch_fear_and_greed_data, _parse_fear_and_greed_history
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
    dag_id='scrape_and_store_btc_fng',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # Set to run daily at 8:00 AM
    catchup=False,
    tags=['scraping', 'btc_fng'],
) as dag:
    
    @task(pool='scraping_pool')
    def scrape_btc_fng() -> List[Dict[str, Any]]:
        """
        Airflow task to scrape the Fear & Greed data using nodriver scraper
        and parse it into a list of dictionaries.
        """
        try:
            # Scrape the data with the nodriver-based implementation
            fng_data = _fetch_fear_and_greed_data()
            rows = _parse_fear_and_greed_history(fng_data)
            logger.info(f"Scraped {len(rows)} Fear & Greed records.")
            return rows
        except Exception as e:
            logger.error(f"Error scraping Fear & Greed data: {e}")
            return []
    
    @task
    def store_btc_fng(rows: List[Dict[str, Any]], db_url: str = None) -> None:
        """
        Airflow task to filter out rows older than the max timestamp in the 'btc_fng' table,
        and insert the new data into the table.
        """
        db_url = db_url or os.getenv("AIRFLOW_CONN_PIPELINE_DB")
        if not db_url:
            raise ValueError("Database URL (AIRFLOW_CONN_PIPELINE_DB) not provided.")
            
        if not rows:
            logger.warning("No Fear & Greed data found. Nothing to store.")
            return
            
        db_manager = DBManager(db_name='pipeline_db')
        
        # 1) Query the most recent timestamp if table exists / has data
        query = "SELECT MAX(timestamp) as last_ts FROM btc_fng"
        result = db_manager.execute_query(query)
        
        last_ts = None
        if result and result[0] and result[0]["last_ts"]:
            last_ts = result[0]["last_ts"]
            logger.info(f"Most recent timestamp in btc_fng: {last_ts}")
            
        # 2) Filter out any rows that are <= last_ts
        filtered_rows = []
        if last_ts:
            dt_last = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S%z")
            for r in rows:
                dt_new = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S%z")
                if dt_new > dt_last:
                    filtered_rows.append(r)
        else:
            filtered_rows = rows
            
        logger.info(f"Inserting {len(filtered_rows)} new rows into btc_fng.")
        
        # 3) Insert the new rows
        if filtered_rows:
            import pandas as pd
            df = pd.DataFrame(filtered_rows)
            db_manager.insert_raw_dataframe("btc_fng", df)
            logger.info("Data insert complete.")
    
    # Define the task dependencies
    db_url = os.getenv('AIRFLOW_CONN_PIPELINE_DB')
    store_btc_fng(scrape_btc_fng(), db_url)