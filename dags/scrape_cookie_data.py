# dags/scrape_cookie_data.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging

from tasks.cookie_tasks import task_scrape_and_store_cookie_data

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='scrape_cookie_data',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # Set to run daily at 1:00 AM
    catchup=False,
    tags=['cookie', 'scraping', ''],
) as dag:
    
    pool_name='scraping_pool'

    scrape_and_store = PythonOperator(
        task_id='scrape_and_store_cookie_data',
        python_callable=task_scrape_and_store_cookie_data,
        op_kwargs={
            'chrome_path': os.getenv('CHROME_PATH'),
            'chromedriver_path': os.getenv('CHROMEDRIVER_PATH'),
            'db_url': os.getenv('AIRFLOW_CONN_PIPELINE_DB')
        },
        provide_context=True,
        pool=pool_name
    )

    scrape_and_store
