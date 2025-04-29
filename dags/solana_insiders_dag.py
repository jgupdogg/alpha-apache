#!/usr/bin/env python3

from datetime import datetime, timedelta
from airflow.decorators import dag

# Import tasks from both DAGs
from tasks.dexscreener_tasks import scrape_dexscreener_data
from tasks.dune_tasks import process_trader_pnl, process_trending_tokens
from tasks.webhook_tasks import register_or_update_webhook
from tasks.scrape_traders import task_scrape_gmgn, task_scrape_dex3

# Combined default arguments taking the more comprehensive settings from both DAGs
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 31),  # Using the later start date
}

@dag(
    dag_id='solana_traders_hook_update',
    default_args=default_args,
    description='Combined pipeline for Solana data analysis including top traders',
    schedule_interval='0 */12 * * *',  # Keeping 12-hour interval from first DAG
    catchup=False,
    tags=['solana', 'dexscreener', 'pnl', 'crypto', 'webhook', 'top_traders'],
    max_active_runs=1  # Prevent overlapping runs
)
def solana_comprehensive_dag():
    """
    Combined DAG for Solana data pipeline:
    1. Scrape DexScreener data and store in database
    2. Process trending tokens
    3. Process trader PNL
    4. Scrape top trader addresses from GMGN and Dex3
    5. Register/update webhook (after trader PNL and scraped traders complete)
    """
    # Task 1: Scrape DexScreener and export
    token_data = scrape_dexscreener_data()
    
    # Task 2: Process trending tokens
    trending_tokens_result = process_trending_tokens(
        min_usd=1000,
        num_traders=50,
        num_trader_txns=20,
        timeout=180
    )
    
    # Task 3: Process trader PNL
    trader_pnl_result = process_trader_pnl(
        num_days=30,
        timeout=180
    )
    
    # Task 4-5: Scrape top traders from both sources (can run in parallel)
    gmgn_results = task_scrape_gmgn()
    dex3_results = task_scrape_dex3()
    
    # Task 6: Register/update webhook
    webhook_result = register_or_update_webhook()
    
    # Define task dependencies
    # Original flow for token data
    token_data >> trending_tokens_result >> trader_pnl_result
    
    # Webhook needs to wait for trader PNL AND both scraped trader results
    [trader_pnl_result, gmgn_results, dex3_results] >> webhook_result

# Create the DAG instance
solana_comprehensive_pipeline = solana_comprehensive_dag()