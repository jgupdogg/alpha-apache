# tests/test_govt_scraper_dag.py

import pytest
from airflow.models import DagBag
from unittest.mock import patch
from dags.ticker_outlook_dag import run_ticker_outlook

# Define the path to your DAG file
DAG_FILE_PATH = "dags.ticker_outlook_dag"


def test_run_ticker_outlook_task():
    """
    Test the execution of the 'run_govt_scraper' task with mocked dependencies.
    """
    run_ticker_outlook()
