import os
import logging
import pandas as pd

from typing import Dict, Any
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone

# Adjust import paths to your project structure
from db_manager import DBManager
import networkx as nx

# Load environment variables
dotenv_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '.env')
)
load_dotenv(dotenv_path)

logger = logging.getLogger(__name__)

DB_URL = os.getenv("AIRFLOW_CONN_PIPELINE_DB")

############################################################
# Task 1: Fetch Twitter Followers
############################################################
def task_fetch_twitter_followers(**context) -> str:
    """
    Fetches distinct follower/following relationships from
    the 'twitter_followers' table in the DB and returns
    a serialized DataFrame (JSON) to XCom.
    """
    logger.info("Starting task_fetch_twitter_followers...")
    db_url = context['params'].get('db_url', DB_URL)

    sql_query = """
    SELECT DISTINCT ON (account_name, follower_account)
        account_name,
        follower_account,
        follower_is_verif,
        follower_type,
        scraped_at
    FROM twitter_followers
    ORDER BY account_name, follower_account, scraped_at DESC;
    """

    try:
        db_manager = DBManager(db_url)
        df = db_manager.fetch_data(sql_query)
        logger.info(f"Fetched {len(df)} rows from twitter_followers.")
        
        # Serialize DataFrame as JSON for XCom
        return df.to_json(orient='split')
    
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemyError encountered: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise



############################################################
# Task 2: Compute Twitter Graph Metrics
############################################################
def task_compute_twitter_graph_metrics(**context) -> str:
    """
    Builds a directed graph from the twitter_followers DataFrame,
    calculates centrality metrics, and returns a metrics DataFrame
    as a serialized JSON.
    """
    logger.info("Starting task_compute_twitter_graph_metrics...")

    # Retrieve JSON from XCom and rebuild DataFrame
    df_json = context['ti'].xcom_pull(
        task_ids='fetch_twitter_followers'
    )
    if not df_json:
        raise ValueError("No data received from task_fetch_twitter_followers")

    df = pd.read_json(df_json, orient='split')

    # Build directed graph
    G = nx.from_pandas_edgelist(
        df,
        source="follower_account",
        target="account_name",
        create_using=nx.DiGraph()
    )

    # Compute metrics
    logger.info("Calculating PageRank...")
    pagerank = nx.pagerank(G)

    logger.info("Calculating Betweenness Centrality...")
    betweenness = nx.betweenness_centrality(G, k=100, normalized=True, seed=42)

    logger.info("Calculating In-Degree Centrality...")
    in_degree = nx.in_degree_centrality(G)

    logger.info("Calculating Out-Degree Centrality...")
    out_degree = nx.out_degree_centrality(G)

    # Create single DataFrame for metrics
    metrics_df = pd.DataFrame({
        'account_name': list(pagerank.keys()),
        'pagerank': list(pagerank.values()),
        'betweenness': list(betweenness.values()),
        'in_degree': list(in_degree.values()),
        'out_degree': list(out_degree.values())
    })

    # Add 'timestamp' column with current UTC time in specified format
    current_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat(sep=' ')
    metrics_df['timestamp'] = current_timestamp

    logger.info(f"Metrics DataFrame prepared with {len(metrics_df)} records.")

    # Return serialized DataFrame
    return metrics_df.to_json(orient='split')



############################################################
# Task 3: Store Twitter Graph Metrics
############################################################
def task_store_twitter_graph_metrics(**context) -> None:
    """
    Reads the metrics DataFrame from XCom and inserts into
    the 'twitter_node_metrics' table in the DB.
    """
    logger.info("Starting task_store_twitter_graph_metrics...")

    db_url = context['params'].get('db_url', DB_URL)
    target_table = 'twitter_node_metrics'

    # Retrieve JSON from XCom and rebuild DataFrame
    metrics_df_json = context['ti'].xcom_pull(
        task_ids='compute_twitter_graph_metrics'
    )
    if not metrics_df_json:
        raise ValueError("No metrics data received from compute_twitter_graph_metrics")

    metrics_df = pd.read_json(metrics_df_json, orient='split')

    try:
        db_manager = DBManager(db_url)
        db_manager.insert_dataframe(table_name=target_table, df=metrics_df)
        logger.info(f"Inserted {len(metrics_df)} records into {target_table}.")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemyError encountered: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
