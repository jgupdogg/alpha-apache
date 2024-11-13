# dags/identify_successful_traders_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
import sys
from utils.workflows.crypto.token_list import TOKEN_LIST

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure Airflow can find the utils package:
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
from utils.workflows.crypto.trader_identification import fetch_top_traders, identify_successful_traders

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
    'owner': 'justin',
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
SNOWFLAKE_DATABASE = 'CRYPTO'
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

logger.info(f'SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}')
logger.info(f'SNOWFLAKE_USER: {SNOWFLAKE_USER}')
logger.info(f'SNOWFLAKE_DATABASE: {SNOWFLAKE_DATABASE}')

def run_trader_identification():
    asyncio.run(main_async())

async def main_async():
    # Initialize the BirdEye SDK
    sdk = BirdEyeSDK(API_KEY, chain="solana")
    logger.info("Initialized BirdEyeSDK.")

    logger.info(f"Using TOKEN_LIST: {TOKEN_LIST}")

    # Fetch top traders for all tokens
    logger.info("Fetching top traders for all tokens...")
    try:
        df_traders = await asyncio.to_thread(fetch_top_traders, api_sdk=sdk, token_list=TOKEN_LIST, limit=10)
        logger.info(f"Fetched top traders data with {len(df_traders)} records.")
    except Exception as e:
        logger.exception(f"Failed to fetch top traders: {e}")
        return

    if df_traders.empty:
        logger.warning("No top traders data fetched.")
        return

    # Identify successful traders
    logger.info("Identifying successful traders...")
    try:
        df_successful_traders = await asyncio.to_thread(identify_successful_traders, df_traders)
        logger.info(f"Identified {len(df_successful_traders)} successful traders.")
    except Exception as e:
        logger.exception(f"Failed to identify successful traders: {e}")
        return

    if df_successful_traders.empty:
        logger.warning("No successful traders identified.")
        return

    # Connect to Snowflake
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
        return

    # Insert successful traders into Snowflake
    try:
        # Check existing traders to avoid duplicates
        existing_traders_df = pd.read_sql("SELECT ADDRESS, CATEGORY FROM TRADERS", conn)
        
        # Rename 'ADDRESS' to 'EXISTING_ADDRESS' to prevent duplication
        existing_traders_df = existing_traders_df.rename(columns={'ADDRESS': 'EXISTING_ADDRESS'})

        # Merge df_successful_traders with existing_traders_df to find new entries
        merged_df = pd.merge(
            df_successful_traders,
            existing_traders_df,
            how='left',
            left_on=['TRADER_ADDRESS', 'CATEGORY'],
            right_on=['EXISTING_ADDRESS', 'CATEGORY'],
            indicator=True,
            suffixes=('', '_existing')
        )

        # Log the columns of merged_df for debugging
        logger.info(f"Columns in merged_df after merge: {merged_df.columns.tolist()}")
        logger.info(f"First few rows of merged_df:\n{merged_df.head()}")

        # Now filter for new entries
        new_traders_df = merged_df[merged_df['_merge'] == 'left_only'].copy()

        if new_traders_df.empty:
            logger.info("No new traders to insert.")
            return

        # Drop '_merge' and 'EXISTING_ADDRESS' columns
        new_traders_df = new_traders_df.drop(columns=['_merge', 'EXISTING_ADDRESS'])

        # Rename 'TRADER_ADDRESS' to 'ADDRESS'
        new_traders_df = new_traders_df.rename(columns={'TRADER_ADDRESS': 'ADDRESS'})

        # Log the columns before reordering for debugging
        logger.info(f"Columns in new_traders_df before reordering: {new_traders_df.columns.tolist()}")
        logger.info(f"First few rows of new_traders_df:\n{new_traders_df.head()}")

        # Reorder columns to match Snowflake table schema
        new_traders_df = new_traders_df[['DATE_ADDED', 'ADDRESS', 'CATEGORY', 'TOKEN_SYMBOL', 'FREQ']]

        # Log the final columns and data for verification
        logger.info(f"Final columns for insertion: {new_traders_df.columns.tolist()}")
        logger.info(f"First few rows of new_traders_df ready for insertion:\n{new_traders_df.head()}")

        # Insert into Snowflake with use_logical_type=True to handle timezone-aware datetimes
        success, nchunks, nrows, _ = write_pandas(conn, new_traders_df, 'TRADERS', use_logical_type=True)
        if success:
            logger.info(f"Inserted {nrows} new traders into TRADERS table.")
        else:
            logger.error("Failed to insert data into TRADERS table.")
    except Exception as e:
        logger.exception(f"Error during data insertion: {e}")
    finally:
        # Close the Snowflake connection
        try:
            conn.close()
            logger.info("Closed Snowflake connection.")
        except Exception as e:
            logger.exception(f"Failed to close Snowflake connection: {e}")

# DAG definition
with DAG(
    'identify_successful_traders_dag',
    default_args=default_args,
    description='A DAG to identify successful traders from BirdEye API and insert into Snowflake',
    schedule='@daily',
    catchup=False,
) as dag:

    trader_identification_task = PythonOperator(
        task_id='trader_identification_task',
        python_callable=run_trader_identification,
    )

# Enable standalone execution
if __name__ == "__main__":
    asyncio.run(main_async())
