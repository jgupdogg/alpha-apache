# tests/test_govt_scraper_dag.py

import pytest
from airflow.models import DagBag
from unittest.mock import patch
from dags.news_ingestion_dag import run_stock_news

# Define the path to your DAG file
DAG_FILE_PATH = "dags.news_ingestion_dag.py"


def test_run_news_ingestion_task():
    """
    Test the execution of the 'run_news_ingestion' task with mocked dependencies.
    """
    run_stock_news()
