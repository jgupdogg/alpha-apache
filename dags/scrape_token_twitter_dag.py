# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import os
# import logging

# from tasks.twitter_tasks import (
#     task_get_twitter_handles,
#     task_do_all_scraping_for_handles
# )

# logger = logging.getLogger(__name__)

# default_args = {
#     'owner': 'admin',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='scrape_token_twitter',
#     default_args=default_args,
#     schedule_interval='0 8 * * *',  # Set to run daily at 3:00 AM
#     catchup=False,
#     tags=['twitter', 'scraping'],
# ) as dag:
    
#     pool_name = 'scraping_pool'  

#     get_twitter_handles = PythonOperator(
#         task_id='get_twitter_handles',
#         python_callable=task_get_twitter_handles,
#         op_kwargs={
#             'db_url': os.getenv('AIRFLOW_CONN_PIPELINE_DB')
#         },
#         provide_context=True,
#     )

#     do_all_scraping_for_handles = PythonOperator(
#         task_id='do_all_scraping_for_handles',
#         python_callable=task_do_all_scraping_for_handles,
#         op_kwargs={
#             'chrome_path': os.getenv('CHROME_PATH'),
#             'chromedriver_path': os.getenv('CHROMEDRIVER_PATH'),
#             'db_url': os.getenv('AIRFLOW_CONN_PIPELINE_DB')
#         },
#         provide_context=True,
#         pool=pool_name  # Assign the pool to this task
#     )

#     # Chain them so we only run scraping if Chrome can be started and handles are fetched
#     get_twitter_handles >> do_all_scraping_for_handles
