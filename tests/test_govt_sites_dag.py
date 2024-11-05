# tests/test_govt_scraper_dag.py

import pytest
from airflow.models import DagBag
from unittest.mock import patch
from dags.govt_scraper_dag import run_govt_scraper

# Define the path to your DAG file
DAG_FILE_PATH = "dags.govt_scraper_dag"


def test_run_govt_scraper_task():
    """
    Test the execution of the 'run_govt_scraper' task with mocked dependencies.
    """
    run_govt_scraper()
