# dags/token_data_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
import sys
import numpy as np
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure Airflow can find the utils package:
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils', 'workflows', 'crypto'))

# Import the BirdEyeSDK and helper functions
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
from utils.workflows.crypto.token_data_processing import fetch_token_data

import pandas as pd
import snowflake.connector

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

# Fetch environment variables for Snowflake connection
SNOWFLAKE_ACCOUNT_ENV = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_REGION_ENV = os.getenv('SNOWFLAKE_REGION')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Construct SNOWFLAKE_ACCOUNT
if SNOWFLAKE_ACCOUNT_ENV and SNOWFLAKE_REGION_ENV:
    SNOWFLAKE_ACCOUNT = f"{SNOWFLAKE_ACCOUNT_ENV}.{SNOWFLAKE_REGION_ENV}"
else:
    logger.error("SNOWFLAKE_ACCOUNT or SNOWFLAKE_REGION environment variables not set.")
    raise ValueError("SNOWFLAKE_ACCOUNT or SNOWFLAKE_REGION environment variables not set.")

SNOWFLAKE_DATABASE = 'CRYPTO'  # Set database to 'CRYPTO'

logger.info(f'SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}')
logger.info(f'SNOWFLAKE_USER: {SNOWFLAKE_USER}')
logger.info(f'SNOWFLAKE_DATABASE: {SNOWFLAKE_DATABASE}')
logger.info(f'SNOWFLAKE_SCHEMA: {SNOWFLAKE_SCHEMA}')

def run_token_data():
    """
    Entry point for the PythonOperator. Runs the main_async function.
    """
    logger.info("Starting run_token_data() function.")
    asyncio.run(main_async())

def clean_param(param):
    """
    Replace 'NAN', 'nan', 'NaN' strings, NaN float values, and pd.NaT with None.
    """
    if isinstance(param, str) and param.strip().upper() == 'NAN':
        return None
    if pd.isna(param):  # This will catch NaN and NaT
        return None
    return param

async def main_async():
    """
    Main asynchronous function to fetch token data and merge into Snowflake.
    """
    logger.info("Entered main_async() function.")
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
        cursor = conn.cursor()

        # Fetch unique token addresses
        logger.info("Fetching unique token addresses from TRADER_PORTFOLIO_AGG table.")
        query = "SELECT DISTINCT TOKEN_ADDRESS FROM TRADER_PORTFOLIO_AGG"
        cursor.execute(query)
        results = cursor.fetchall()
        token_addresses = [row[0] for row in results if row[0] is not None]
        logger.info(f"Fetched {len(token_addresses)} unique token addresses from TRADER_PORTFOLIO_AGG table.")

        if not token_addresses:
            logger.warning("No token addresses found in TRADER_PORTFOLIO_AGG table.")
            conn.close()
            raise Exception("No token addresses found in TRADER_PORTFOLIO_AGG table.")

        # Fetch token data
        logger.info("Fetching token data from BirdEye API.")
        token_data_df = await fetch_token_data(sdk=sdk, token_addresses=token_addresses)
        logger.info(f"Fetched token data with {len(token_data_df)} records.")

        if token_data_df.empty:
            logger.warning("No token data fetched.")
            conn.close()
            raise Exception("No token data fetched.")

        # Add LAST_UPDATED and DATE_ADDED columns as formatted strings
        current_time = datetime.utcnow()
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        token_data_df['LAST_UPDATED'] = formatted_time
        token_data_df['DATE_ADDED'] = formatted_time
        logger.info("Added LAST_UPDATED and DATE_ADDED to token data DataFrame.")

        # Convert CREATION_TIMESTAMP using BLOCK_HUMAN_TIME
        # Ensure 'CREATION_TIMESTAMP' exists
        if 'BLOCK_HUMAN_TIME' in token_data_df.columns:
            logger.info("Converting CREATION_TIMESTAMP to datetime and making it timezone-naive.")
            token_data_df['CREATION_TIMESTAMP'] = pd.to_datetime(
                token_data_df['BLOCK_HUMAN_TIME'],
                errors='coerce',
                utc=True
            )
            # Make CREATION_TIMESTAMP timezone-naive
            token_data_df['CREATION_TIMESTAMP'] = token_data_df['CREATION_TIMESTAMP'].dt.tz_localize(None)
            # Convert to string in 'YYYY-MM-DD HH:MM:SS' format
            token_data_df['CREATION_TIMESTAMP'] = token_data_df['CREATION_TIMESTAMP'].dt.strftime('%Y-%m-%d %H:%M:%S')

            logger.debug("Sample of CREATION_TIMESTAMP values:")
            logger.debug(token_data_df['CREATION_TIMESTAMP'].head())

            # Drop 'BLOCK_HUMAN_TIME' as it's no longer needed
            token_data_df.drop(columns=['BLOCK_HUMAN_TIME'], inplace=True)
            logger.info("Dropped 'BLOCK_HUMAN_TIME' column to align with table schema.")
        else:
            logger.warning("The DataFrame does not contain the 'BLOCK_HUMAN_TIME' column.")
            token_data_df['CREATION_TIMESTAMP'] = None

        # Ensure data types are correct and handle nulls
        logger.info("Ensuring data types are correct and handling nulls.")
        token_data_df = token_data_df.astype({
            'TOKEN_ADDRESS': 'str',
            'SYMBOL': 'str',
            'DECIMALS': 'Int64',
            'NAME': 'str',
            'WEBSITE': 'str',
            'TWITTER': 'str',
            'DESCRIPTION': 'str',
            'LOGO_URI': 'str',
            'LIQUIDITY': 'float',
            'MARKET_CAP': 'float',
            'HOLDER_COUNT': 'Int64',
            'PRICE': 'float',
            'V24H_USD': 'float',
            'V_BUY_HISTORY_24H_USD': 'float',
            'V_SELL_HISTORY_24H_USD': 'float',
            'OWNER': 'str',
            'TOP10_HOLDER_PERCENT': 'float',
            'OWNER_PERCENTAGE': 'float',
            'CREATOR_PERCENTAGE': 'float',
            'LAST_UPDATED': 'str',
            'DATE_ADDED': 'str',
            'CREATION_TIMESTAMP': 'str'  # Since we converted to string
        })

        # Replace NaN with None
        token_data_df = token_data_df.where(pd.notnull(token_data_df), None)
        logger.info("Replaced NaN values with None for Snowflake compatibility.")

        # Additionally, replace 'NAN', 'nan', 'NaN' strings with None
        token_data_df = token_data_df.replace({'NAN': None, 'nan': None, 'NaN': None})
        logger.info("Replaced 'NAN' strings with None for Snowflake compatibility.")

        # Ensure columns match the STAGE_TOKEN_DATA table schema
        expected_columns = [
            'TOKEN_ADDRESS',
            'SYMBOL',
            'DECIMALS',
            'NAME',
            'WEBSITE',
            'TWITTER',
            'DESCRIPTION',
            'LOGO_URI',
            'LIQUIDITY',
            'MARKET_CAP',
            'HOLDER_COUNT',
            'PRICE',
            'V24H_USD',
            'V_BUY_HISTORY_24H_USD',
            'V_SELL_HISTORY_24H_USD',
            'CREATION_TIMESTAMP',
            'OWNER',
            'TOP10_HOLDER_PERCENT',
            'OWNER_PERCENTAGE',
            'CREATOR_PERCENTAGE',
            'LAST_UPDATED',
            'DATE_ADDED'
        ]

        # Reorder the DataFrame columns to match the expected order
        token_data_df = token_data_df[expected_columns]

        # Round float columns to 6 decimal places to prevent Snowflake float precision issues
        float_columns = [
            'LIQUIDITY',
            'MARKET_CAP',
            'PRICE',
            'V24H_USD',
            'V_BUY_HISTORY_24H_USD',
            'V_SELL_HISTORY_24H_USD',
            'TOP10_HOLDER_PERCENT',
            'OWNER_PERCENTAGE',
            'CREATOR_PERCENTAGE'
        ]
        for col in float_columns:
            if col in token_data_df.columns and token_data_df[col].dtype == 'float':
                token_data_df[col] = token_data_df[col].round(6)
                logger.debug(f"Rounded column {col} to 6 decimal places.")

        # Convert DataFrame to list of tuples
        data_tuples = [tuple(clean_param(x) for x in row) for row in token_data_df.itertuples(index=False, name=None)]
        logger.info("Cleaned data tuples by replacing 'NAN' and NaN values with None.")

        # **Data Validation Before Insertion**
        # Check for any float values in integer fields
        for idx, row in enumerate(data_tuples):
            decimals = row[2]  # DECIMALS is the 3rd field
            holder_count = row[10]  # HOLDER_COUNT is the 11th field

            # Log the type of DECIMALS and HOLDER_COUNT for debugging
            logger.debug(f"Record {idx} DECIMALS value: {decimals} (type: {type(decimals)})")
            logger.debug(f"Record {idx} HOLDER_COUNT value: {holder_count} (type: {type(holder_count)})")

            if decimals is not None and not isinstance(decimals, (int, np.integer)):
                logger.error(f"Record {idx} has non-integer DECIMALS: {decimals}")
                raise ValueError(f"Record {idx} has non-integer DECIMALS: {decimals}")

            if holder_count is not None and not isinstance(holder_count, (int, np.integer)):
                logger.error(f"Record {idx} has non-integer HOLDER_COUNT: {holder_count}")
                raise ValueError(f"Record {idx} has non-integer HOLDER_COUNT: {holder_count}")

        # Insert data into STAGE_TOKEN_DATA and perform MERGE
        try:
            # Truncate STAGE_TOKEN_DATA
            truncate_sql = f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGE_TOKEN_DATA;"
            cursor.execute(truncate_sql)
            logger.info("Truncated STAGE_TOKEN_DATA table.")

            # Prepare the INSERT statement for STAGE_TOKEN_DATA with %s placeholders
            insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGE_TOKEN_DATA (
                TOKEN_ADDRESS,
                SYMBOL,
                DECIMALS,
                NAME,
                WEBSITE,
                TWITTER,
                DESCRIPTION,
                LOGO_URI,
                LIQUIDITY,
                MARKET_CAP,
                HOLDER_COUNT,
                PRICE,
                V24H_USD,
                V_BUY_HISTORY_24H_USD,
                V_SELL_HISTORY_24H_USD,
                CREATION_TIMESTAMP,
                OWNER,
                TOP10_HOLDER_PERCENT,
                OWNER_PERCENTAGE,
                CREATOR_PERCENTAGE,
                LAST_UPDATED,
                DATE_ADDED
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """

            # Execute batch insert into STAGE_TOKEN_DATA
            cursor.executemany(insert_sql, data_tuples)
            logger.info(f"Inserted {len(data_tuples)} records into STAGE_TOKEN_DATA.")

            # Prepare the MERGE statement
            merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TOKEN_DATA AS target
            USING {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGE_TOKEN_DATA AS source
            ON target.TOKEN_ADDRESS = source.TOKEN_ADDRESS
            WHEN MATCHED THEN
                UPDATE SET
                    SYMBOL = source.SYMBOL,
                    DECIMALS = source.DECIMALS,
                    NAME = source.NAME,
                    WEBSITE = source.WEBSITE,
                    TWITTER = source.TWITTER,
                    DESCRIPTION = source.DESCRIPTION,
                    LOGO_URI = source.LOGO_URI,
                    LIQUIDITY = source.LIQUIDITY,
                    MARKET_CAP = source.MARKET_CAP,
                    HOLDER_COUNT = source.HOLDER_COUNT,
                    PRICE = source.PRICE,
                    V24H_USD = source.V24H_USD,
                    V_BUY_HISTORY_24H_USD = source.V_BUY_HISTORY_24H_USD,
                    V_SELL_HISTORY_24H_USD = source.V_SELL_HISTORY_24H_USD,
                    CREATION_TIMESTAMP = source.CREATION_TIMESTAMP,
                    OWNER = source.OWNER,
                    TOP10_HOLDER_PERCENT = source.TOP10_HOLDER_PERCENT,
                    OWNER_PERCENTAGE = source.OWNER_PERCENTAGE,
                    CREATOR_PERCENTAGE = source.CREATOR_PERCENTAGE,
                    LAST_UPDATED = source.LAST_UPDATED
            WHEN NOT MATCHED THEN
                INSERT (
                    TOKEN_ADDRESS,
                    SYMBOL,
                    DECIMALS,
                    NAME,
                    WEBSITE,
                    TWITTER,
                    DESCRIPTION,
                    LOGO_URI,
                    LIQUIDITY,
                    MARKET_CAP,
                    HOLDER_COUNT,
                    PRICE,
                    V24H_USD,
                    V_BUY_HISTORY_24H_USD,
                    V_SELL_HISTORY_24H_USD,
                    CREATION_TIMESTAMP,
                    OWNER,
                    TOP10_HOLDER_PERCENT,
                    OWNER_PERCENTAGE,
                    CREATOR_PERCENTAGE,
                    LAST_UPDATED,
                    DATE_ADDED
                ) VALUES (
                    source.TOKEN_ADDRESS,
                    source.SYMBOL,
                    source.DECIMALS,
                    source.NAME,
                    source.WEBSITE,
                    source.TWITTER,
                    source.DESCRIPTION,
                    source.LOGO_URI,
                    source.LIQUIDITY,
                    source.MARKET_CAP,
                    source.HOLDER_COUNT,
                    source.PRICE,
                    source.V24H_USD,
                    source.V_BUY_HISTORY_24H_USD,
                    source.V_SELL_HISTORY_24H_USD,
                    source.CREATION_TIMESTAMP,
                    source.OWNER,
                    source.TOP10_HOLDER_PERCENT,
                    source.OWNER_PERCENTAGE,
                    source.CREATOR_PERCENTAGE,
                    source.LAST_UPDATED,
                    source.DATE_ADDED
                );
            """

            # Perform MERGE from STAGE_TOKEN_DATA into TOKEN_DATA
            cursor.execute(merge_sql)
            logger.info("Merged data from STAGE_TOKEN_DATA into TOKEN_DATA successfully.")

        except Exception as e:
            logger.exception(f"Error during data insertion and merge: {e}")
            conn.close()
            raise

    except Exception as e:
        logger.exception(f"Error in main_async: {e}")
        conn.close()
        raise

    finally:
        # Close the Snowflake connection
        try:
            conn.close()
            logger.info("Closed Snowflake connection.")
        except Exception as e:
            logger.exception(f"Failed to close Snowflake connection: {e}")

# DAG definition
with DAG(
    'token_data_dag',
    default_args=default_args,
    description='A DAG to fetch token data from BirdEye API into Snowflake',
    schedule='@daily',
    catchup=False,
) as dag:

    token_data_task = PythonOperator(
        task_id='token_data_task',
        python_callable=run_token_data,
    )

# Add a main function for standalone execution
def main():
    """
    Main function for standalone execution of the DAG script.
    """
    logger.info("Executing main() function.")
    try:
        asyncio.run(main_async())
    except Exception as e:
        logger.exception(f"An error occurred while running main_async: {e}")

# Enable standalone execution
if __name__ == "__main__":
    main()
