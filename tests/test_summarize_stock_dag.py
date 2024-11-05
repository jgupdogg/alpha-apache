# tests/test_govt_scraper_dag.py

import pytest
from airflow.models import DagBag
from unittest.mock import patch
from dags.summarize_stock_news_dag import run_summarize_stock_news_task

# Define the path to your DAG file
DAG_FILE_PATH = "dags.summarize_stock_news_dag"


def test_run_summarize_stock_news_task():
    import os
    from dotenv import load_dotenv

    # Construct the path to the .env file
    dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
    load_dotenv(dotenv_path)

    # Now run the function
    try:
        run_summarize_stock_news_task(test=False)
    except Exception as e:
        pytest.fail(f"run_summarize_stock_news_task() raised an exception: {e}")

