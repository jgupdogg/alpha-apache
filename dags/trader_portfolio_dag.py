# dags/trader_portfolio_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
import sys
import pendulum

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
from utils.workflows.crypto.token_data_processing import fetch_token_data  # Imported to fetch token data

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Configure detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path)

local_tz = pendulum.timezone("America/New_York")  # Replace with your timezone


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

def run_trader_portfolio():
    asyncio.run(main_async())

def clean_param(param, expected_type=None):
    """
    Replace 'NAN', 'nan', 'NaN' strings, NaN float values, and pd.NaT with None.
    Convert numeric strings to floats or ints based on expected_type if provided.
    """
    if isinstance(param, str):
        if param.strip().upper() == 'NAN':
            return None
        try:
            if expected_type == 'int':
                return int(float(param))
            elif expected_type == 'float':
                return float(param)
            else:
                return param
        except ValueError:
            return param
    if pd.isna(param):  # This will catch NaN and NaT
        return None
    return param

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
        cursor = conn.cursor()

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

        # Ensure TRADER_COUNT is integer
        aggregated_df['TRADER_COUNT'] = aggregated_df['TRADER_COUNT'].astype(int)

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

    # Now, check which token addresses in the aggregated DataFrame are missing in TOKEN_DATA
    token_addresses = aggregated_df['TOKEN_ADDRESS'].dropna().unique().tolist()
    logger.info(f"Unique token addresses in aggregated data: {len(token_addresses)}")

    try:
        cursor.execute("SELECT TOKEN_ADDRESS FROM TOKEN_DATA")
        existing_token_addresses = [row[0] for row in cursor.fetchall()]
        logger.info(f"Fetched {len(existing_token_addresses)} token addresses from TOKEN_DATA table.")
    except Exception as e:
        logger.exception(f"Failed to fetch token addresses from TOKEN_DATA: {e}")
        conn.close()
        raise  # Re-raise exception to notify Airflow of the failure

    # Find missing token addresses
    missing_token_addresses = list(set(token_addresses) - set(existing_token_addresses))
    logger.info(f"Found {len(missing_token_addresses)} missing token addresses not present in TOKEN_DATA.")

    if missing_token_addresses:
        # Fetch token data for missing addresses
        logger.info("Fetching token data for missing token addresses.")
        try:
            token_data_df = await fetch_token_data(sdk=sdk, token_addresses=missing_token_addresses)
            logger.info(f"Fetched token data with {len(token_data_df)} records.")

            if token_data_df.empty:
                logger.warning("No token data fetched for missing token addresses.")
            else:
                # Add LAST_UPDATED and DATE_ADDED columns as formatted strings
                current_time = datetime.utcnow()
                formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
                token_data_df['LAST_UPDATED'] = formatted_time
                token_data_df['DATE_ADDED'] = formatted_time
                logger.info("Added LAST_UPDATED and DATE_ADDED to token data DataFrame.")

                # Convert CREATION_TIMESTAMP using BLOCK_HUMAN_TIME
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
                    'CREATION_TIMESTAMP': 'str',
                    'OWNER': 'str',
                    'TOP10_HOLDER_PERCENT': 'float',
                    'OWNER_PERCENTAGE': 'float',
                    'CREATOR_PERCENTAGE': 'float',
                    'LAST_UPDATED': 'str',
                    'DATE_ADDED': 'str'
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

                # Clean all data tuples with expected types
                # Define expected types for each column based on STAGE_TOKEN_DATA schema
                expected_types = [
                    'str', 'str', 'int', 'str', 'str', 'str', 'str', 'str',
                    'float', 'float', 'int', 'float', 'float', 'float', 'float',
                    'str', 'str', 'float', 'float', 'float', 'str', 'str'
                ]

                # Clean all data tuples
                data_tuples = [
                    tuple(clean_param(x, expected_type='float' if idx in [8,9,11,12,13,14,16,17,18,19] else 'int' if idx ==10 else None)
                          for idx, x in enumerate(row, 1))
                    for row in token_data_df.itertuples(index=False, name=None)
                ]
                logger.info("Cleaned data tuples by replacing 'NAN' and NaN values with None.")

                # Log data types for debugging
                logger.info("Inspecting data_tuples before insertion:")
                for i, row in enumerate(data_tuples[:5], 1):
                    logger.info(f"Row {i} Data Types: {[type(item) for item in row]}")
                    logger.info(f"Row {i} Data Values: {row}")

                # Insert data into STAGE_TOKEN_DATA using write_pandas
                try:
                    success, nchunks, nrows, _ = write_pandas(conn, token_data_df, 'STAGE_TOKEN_DATA', auto_create_table=False, overwrite=True)
                    if success:
                        logger.info(f"Inserted {nrows} records into STAGE_TOKEN_DATA successfully.")
                    else:
                        logger.error("Failed to insert data into STAGE_TOKEN_DATA.")
                        conn.close()
                        raise Exception("Failed to insert data into STAGE_TOKEN_DATA.")
                except Exception as e:
                    logger.exception(f"Error during write_pandas insertion into STAGE_TOKEN_DATA: {e}")
                    conn.close()
                    raise

                # Perform MERGE from STAGE_TOKEN_DATA into TOKEN_DATA
                try:
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
                    cursor.execute(merge_sql)
                    logger.info("Merged data from STAGE_TOKEN_DATA into TOKEN_DATA successfully.")
                except snowflake.connector.errors.ProgrammingError as e:
                    logger.error(f"Snowflake ProgrammingError during MERGE: {e}")
                    conn.close()
                    raise  # Re-raise exception to notify Airflow of the failure
                except Exception as e:
                    logger.exception(f"Error during MERGE operation: {e}")
                    conn.close()
                    raise

        except Exception as e:
            logger.exception(f"Failed to fetch and insert token data for missing addresses: {e}")
            conn.close()
            raise  # Re-raise exception to notify Airflow of the failure

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
        logger.exception(f"Error during data insertion into {target_table}: {e}")
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
    schedule_interval='@hourly',  # Updated schedule
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
