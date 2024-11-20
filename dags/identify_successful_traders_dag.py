# dags/identify_successful_traders_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
import sys

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.workflows.crypto.token_list import TOKEN_LIST
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

    logger.info(f"Columns in df_traders from fetch_top_traders: {df_traders.columns.tolist()}")
    logger.info(f"First few rows of df_traders:\n{df_traders.head()}")

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

    logger.info(f"Columns in df_successful_traders: {df_successful_traders.columns.tolist()}")
    logger.info(f"First few rows of df_successful_traders:\n{df_successful_traders.head()}")

    # Filter traders with FREQ >=3 using .loc to avoid SettingWithCopyWarning
    logger.info("Filtering traders with FREQ >=3...")
    df_successful_traders = df_successful_traders.loc[df_successful_traders['FREQ'] >= 3].copy()
    logger.info(f"{len(df_successful_traders)} traders after filtering with FREQ >=3.")

    if df_successful_traders.empty:
        logger.warning("No traders with FREQ >=3 after filtering.")
        return

    # Rename 'TRADER_ADDRESS' to 'ADDRESS' for consistency after identification
    if 'TRADER_ADDRESS' in df_successful_traders.columns:
        df_successful_traders = df_successful_traders.rename(columns={'TRADER_ADDRESS': 'ADDRESS'})
        logger.info("Renamed 'TRADER_ADDRESS' to 'ADDRESS' in df_successful_traders.")
    else:
        logger.error("'TRADER_ADDRESS' column not found in df_successful_traders.")
        return

    logger.info(f"Columns in df_successful_traders: {df_successful_traders.columns.tolist()}")
    logger.info(f"First few rows of df_successful_traders:\n{df_successful_traders.head()}")

    # At this point, 'CATEGORY' is already correctly assigned based on 'TOKEN_SYMBOL'

    # Convert CATEGORY and TOKEN_SYMBOL lists to comma-separated strings and ensure uniqueness
    logger.info("Converting CATEGORY and TOKEN_SYMBOL lists to comma-separated strings...")
    df_successful_traders['CATEGORY'] = df_successful_traders.groupby('ADDRESS')['CATEGORY'].transform(
        lambda x: ','.join(sorted(set(x)))
    )
    df_successful_traders['TOKEN_SYMBOL'] = df_successful_traders.groupby('ADDRESS')['TOKEN_SYMBOL'].transform(
        lambda x: ','.join(sorted(set(x)))
    )
    logger.info("Converted CATEGORY and TOKEN_SYMBOL to comma-separated strings.")

    # Drop duplicates to have one row per ADDRESS
    df_successful_traders = df_successful_traders.drop_duplicates(subset=['ADDRESS'])
    logger.info(f"{len(df_successful_traders)} unique traders after deduplication.")

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

    try:
        # Write df_successful_traders to a staging table
        staging_table = 'TRADERS_STAGING'
        logger.info(f"Writing data to staging table: {staging_table}...")
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df_successful_traders, 
            staging_table, 
            auto_create_table=True, 
            overwrite=True
        )
        if success:
            logger.info(f"Written {nrows} rows to staging table {staging_table}.")
        else:
            logger.error(f"Failed to write data to staging table {staging_table}.")
            return

        # Perform MERGE to upsert data into TRADERS table
        logger.info("Performing MERGE to upsert data into TRADERS table...")
        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_SCHEMA}.TRADERS AS target
            USING {SNOWFLAKE_SCHEMA}.{staging_table} AS source
            ON target.ADDRESS = source.ADDRESS
            WHEN MATCHED THEN
                UPDATE SET
                    FREQ = target.FREQ + source.FREQ,
                    CATEGORY = CONCAT(target.CATEGORY, ',', source.CATEGORY),
                    TOKEN_SYMBOL = CONCAT(target.TOKEN_SYMBOL, ',', source.TOKEN_SYMBOL)
            WHEN NOT MATCHED THEN
                INSERT (DATE_ADDED, ADDRESS, CATEGORY, TOKEN_SYMBOL, FREQ)
                VALUES (source.DATE_ADDED, source.ADDRESS, source.CATEGORY, source.TOKEN_SYMBOL, source.FREQ);
        """
        logger.debug(f"MERGE SQL Statement:\n{merge_sql}")
        cursor = conn.cursor()
        cursor.execute(merge_sql)
        logger.info("MERGE statement executed successfully.")
    except Exception as e:
        logger.exception(f"Error during MERGE operation: {e}")
    finally:
        # Drop the staging table
        try:
            logger.info(f"Dropping staging table: {staging_table}...")
            cursor.execute(f"DROP TABLE IF EXISTS {SNOWFLAKE_SCHEMA}.{staging_table}")
            logger.info(f"Staging table {staging_table} dropped.")
        except Exception as e:
            logger.exception(f"Failed to drop staging table {staging_table}: {e}")
        finally:
            cursor.close()
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
    description='A DAG to identify successful traders from BirdEye API and upsert into Snowflake',
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
