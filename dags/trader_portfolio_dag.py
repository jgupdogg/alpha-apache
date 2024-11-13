# dags/trader_portfolio_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
import sys

from dotenv import load_dotenv
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure Airflow can find the utils package:
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils', 'workflows', 'crypto'))

# Import the BirdEyeSDK and helper functions
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
from utils.workflows.crypto.trader_processing import get_portfolio_balances

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configure detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path)

default_args = {
    'owner': 'justin',  # Replace with your name
    'start_date': datetime(2024, 11, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Global variables
API_KEY = os.getenv('BIRDSEYE_API_KEY')

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = f"{os.getenv('SNOWFLAKE_ACCOUNT')}.{os.getenv('SNOWFLAKE_REGION')}"
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = 'CRYPTO'  # Set database to 'CRYPTO'
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

logger.info(f'SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}')
logger.info(f'SNOWFLAKE_USER: {SNOWFLAKE_USER}')
logger.info(f'SNOWFLAKE_DATABASE: {SNOWFLAKE_DATABASE}')
logger.info(f'SNOWFLAKE_SCHEMA: {SNOWFLAKE_SCHEMA}')

def run_trader_portfolio():
    asyncio.run(main_async())

async def main_async():
    """
    Main asynchronous function to fetch and aggregate portfolio balances of traders and insert into Snowflake.
    """
    # Initialize the BirdEye SDK
    sdk = BirdEyeSDK(API_KEY)
    logger.info("Initialized BirdEyeSDK.")

    # Create a Snowflake connector connection
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        logger.info("Connected to Snowflake.")
    except Exception as e:
        logger.exception(f"Failed to connect to Snowflake: {e}")
        raise  # Re-raise exception to notify Airflow of the failure

    try:
        # Fetch the unique trader addresses and their categories from the TRADERS table
        df_traders = pd.read_sql("SELECT ADDRESS, CATEGORY FROM TRADERS", conn)
        trader_addresses = df_traders['ADDRESS'].dropna().unique().tolist()
        logger.info(f"Fetched {len(trader_addresses)} unique trader addresses from TRADERS table.")

        if not trader_addresses:
            logger.warning("No trader addresses found in TRADERS table.")
            raise Exception("No trader addresses found in TRADERS table.")

    except Exception as e:
        logger.exception(f"Failed to fetch trader addresses from Snowflake: {e}")
        conn.close()
        raise  # Re-raise exception to notify Airflow of the failure

    # Fetch portfolio balances for the traders
    logger.info("Fetching portfolio balances for traders...")
    try:
        df_portfolio_balances = await asyncio.to_thread(
            get_portfolio_balances,
            api_sdk=sdk,
            trader_addresses=trader_addresses
        )
        logger.info(f"Fetched portfolio balances with {len(df_portfolio_balances)} records.")
    except Exception as e:
        logger.exception(f"Failed to fetch portfolio balances: {e}")
        conn.close()
        raise  # Re-raise exception to notify Airflow of the failure

    if df_portfolio_balances.empty:
        logger.warning("No portfolio balances data fetched.")
        conn.close()
        raise Exception("No portfolio balances data fetched.")

    # Drop balances with VALUE_USD < $100
    initial_count = len(df_portfolio_balances)
    df_portfolio_balances = df_portfolio_balances[df_portfolio_balances['VALUE_USD'] >= 100]
    filtered_count = len(df_portfolio_balances)
    logger.info(f"Dropped {initial_count - filtered_count} portfolio balances with VALUE_USD < $100.")

    if df_portfolio_balances.empty:
        logger.warning("All portfolio balances were below $100 after filtering.")
        conn.close()
        raise Exception("All portfolio balances were below $100 after filtering.")

    # Add FETCH_DATE column
    fetch_date = datetime.utcnow()
    df_portfolio_balances['FETCH_DATE'] = fetch_date
    logger.info("Added FETCH_DATE to portfolio balances DataFrame.")

    # Merge portfolio balances with trader categories
    logger.info("Merging portfolio balances with trader categories...")
    try:
        df_portfolio_balances = pd.merge(
            df_portfolio_balances,
            df_traders,
            left_on='TRADER_ADDRESS',
            right_on='ADDRESS',
            how='left',
            suffixes=('', '_TRADER')
        )
        logger.info(f"Merged portfolio balances with trader categories. DataFrame now has {len(df_portfolio_balances)} records.")
    except Exception as e:
        logger.exception(f"Failed to merge portfolio balances with trader categories: {e}")
        conn.close()
        raise  # Re-raise exception to notify Airflow of the failure

    # Check for any traders without a category
    missing_categories = df_portfolio_balances[df_portfolio_balances['CATEGORY'].isna()]['TRADER_ADDRESS'].unique().tolist()
    if missing_categories:
        logger.warning(f"The following trader addresses do not have a category mapping: {missing_categories}")
        # Optionally, handle unmapped traders here (e.g., assign a default category or exclude)
        # For now, we'll drop them
        df_portfolio_balances = df_portfolio_balances.dropna(subset=['CATEGORY'])
        logger.info(f"Dropped {len(missing_categories)} portfolio balances without category mapping. Remaining records: {len(df_portfolio_balances)}")

    if df_portfolio_balances.empty:
        logger.warning("No portfolio balances with valid category mappings remain after merging.")
        conn.close()
        raise Exception("No portfolio balances with valid category mappings remain after merging.")

    # Aggregate the balances: sum VALUE_USD and BALANCE per TOKEN_ADDRESS, TOKEN_SYMBOL, and CATEGORY, count unique TRADER_ADDRESS
    logger.info("Aggregating portfolio balances...")
    try:
        # Ensure required columns exist
        required_columns = {'TOKEN_SYMBOL', 'TOKEN_ADDRESS', 'VALUE_USD', 'TRADER_ADDRESS', 'BALANCE', 'CATEGORY'}
        if not required_columns.issubset(df_portfolio_balances.columns):
            missing = required_columns - set(df_portfolio_balances.columns)
            logger.error(f"Missing columns in portfolio balances DataFrame: {missing}")
            conn.close()
            raise Exception(f"Missing columns in portfolio balances DataFrame: {missing}")

        aggregated_df = df_portfolio_balances.groupby(['TOKEN_ADDRESS', 'TOKEN_SYMBOL', 'CATEGORY']).agg(
            TOTAL_VALUE_USD=('VALUE_USD', 'sum'),
            TOTAL_BALANCE=('BALANCE', 'sum'),
            TRADER_COUNT=('TRADER_ADDRESS', 'nunique')
        ).reset_index()

        # Add FETCH_DATE to aggregated_df
        aggregated_df['FETCH_DATE'] = fetch_date

        # Reorder columns to match Snowflake table schema
        aggregated_df = aggregated_df[['TOKEN_SYMBOL', 'TOKEN_ADDRESS', 'CATEGORY', 'TOTAL_VALUE_USD', 'TOTAL_BALANCE', 'TRADER_COUNT', 'FETCH_DATE']]

        logger.info(f"Aggregated portfolio balances into {len(aggregated_df)} records.")

    except Exception as e:
        logger.exception(f"Failed to aggregate portfolio balances: {e}")
        conn.close()
        raise  # Re-raise exception to notify Airflow of the failure

    # Drop aggregated entries with TOTAL_VALUE_USD < $1000 and TRADER_COUNT <= 2
    initial_agg_count = len(aggregated_df)
    aggregated_df = aggregated_df[(aggregated_df['TOTAL_VALUE_USD'] >= 1000) & (aggregated_df['TRADER_COUNT'] > 2)]
    final_agg_count = len(aggregated_df)
    logger.info(f"Dropped {initial_agg_count - final_agg_count} aggregated records with TOTAL_VALUE_USD < $1000 or TRADER_COUNT <= 2.")

    if aggregated_df.empty:
        logger.warning("No aggregated portfolio balances met the $1000 and TRADER_COUNT > 2 thresholds.")
        conn.close()
        raise Exception("No aggregated portfolio balances met the $1000 and TRADER_COUNT > 2 thresholds.")

    # Insert aggregated data into Snowflake
    try:
        # Define target table name
        target_table = 'TRADER_PORTFOLIO_AGG'

        # Reset index to avoid potential issues
        aggregated_df = aggregated_df.reset_index(drop=True)

        # Insert into Snowflake with use_logical_type=True to handle timezone-aware datetimes
        success, nchunks, nrows, _ = write_pandas(conn, aggregated_df, target_table, use_logical_type=True)
        if success:
            logger.info(f"Inserted {nrows} records into {target_table} table.")
        else:
            logger.error(f"Failed to insert data into {target_table} table.")
            raise Exception(f"Failed to insert data into {target_table} table.")
    except Exception as e:
        logger.exception(f"Error during data insertion: {e}")
        raise  # Re-raise exception to notify Airflow of the failure
    finally:
        # Close the Snowflake connection
        try:
            conn.close()
            logger.info("Closed Snowflake connection.")
        except Exception as e:
            logger.exception(f"Failed to close Snowflake connection: {e}")

# DAG definition
with DAG(
    'trader_portfolio_dag',
    default_args=default_args,
    description='A DAG to fetch and aggregate portfolio balances of traders from BirdEye API into Snowflake',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
) as dag:

    trader_portfolio_task = PythonOperator(
        task_id='trader_portfolio_task',
        python_callable=run_trader_portfolio,
    )

# Add a main function for standalone execution
def main():
    """
    Main function for standalone execution of the DAG script.
    """
    asyncio.run(main_async())

# Enable standalone execution
if __name__ == "__main__":
    main()
