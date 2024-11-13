# dags/data_ingestion_dag.py

import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from typing import Dict, Optional, List
from snowflake.snowpark import Session
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import sys
from utils.workflows.tickers.data_processing import (
    process_company_data,
    normalize_profile_data,
    normalize_metrics_data,
    normalize_ratios_data,
    normalize_inside_trade,
    normalize_splits_history,
    normalize_stock_dividend,
)

# Ensure that Airflow can find the utils package:
sys.path.append(os.path.dirname(__file__))

# Configure detailed logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 11),
    'end_date': datetime(2023, 10, 11, 0, 4, 0),  # Runs for 5 minutes
}

# Define the list of tables and their corresponding normalization functions
TABLES_CONFIG = {
    "COMPANY_PROFILE": normalize_profile_data,
    "COMPANY_METRICS": normalize_metrics_data,
    "COMPANY_RATIOS": normalize_ratios_data,
    "INSIDE_TRADES": normalize_inside_trade,
    "SPLITS_HISTORY": normalize_splits_history,
    "STOCK_DIVIDENDS": normalize_stock_dividend,
}

def run_ticker_outlook():
    API_KEY = os.getenv('API_KEY')
    BASE_URL = 'https://financialmodelingprep.com/api/v4'

    SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SF_REGION = os.getenv('SNOWFLAKE_REGION')

    # Snowflake connection parameters
    SNOWFLAKE_ACCOUNT = f'{SF_ACCOUNT}.{SF_REGION}'
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

    logging.info(f'SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}')
    logging.info(f'SNOWFLAKE_USER: {SNOWFLAKE_USER}')

    async def fetch_company_outlook(http_session: aiohttp.ClientSession, symbol: str) -> Optional[Dict]:
        url = f"{BASE_URL}/company-outlook?symbol={symbol}&apikey={API_KEY}"
        logging.info(f"Fetching data for {symbol}...")
        try:
            async with http_session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch data for {symbol}: {response.status}")
                    return None
                data = await response.json()
                return data
        except Exception as e:
            logging.exception(f"Exception occurred while fetching data for {symbol}: {e}")
            return None

    async def upsert_to_snowflake(snowflake_session: Session, data: Dict, table_name: str):
        """
        Performs an upsert operation using Snowflake's MERGE statement.
        """
        symbol = data.get('symbol')
        if not symbol:
            logging.error(f"No symbol found in data for table {table_name}. Skipping upsert.")
            return

        # Prepare columns and values
        columns = list(data.keys())
        values = [f"source.{col}" for col in columns]

        # Construct the SET clause for matched rows (excluding the key column)
        set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns if col != 'symbol'])

        # Construct the MERGE statement
        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING (SELECT {', '.join([f"'{v}' AS {k}" for k, v in data.items()])}) AS source
        ON target.symbol = source.symbol
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(columns)})
            VALUES ({', '.join(values)});
        """

        try:
            snowflake_session.sql(merge_query).collect()
            logging.info(f"Upserted data into {table_name} for symbol {symbol}.")
        except Exception as e:
            logging.exception(f"Failed to upsert data into {table_name} for symbol {symbol}: {e}")

    async def predefine_tables_with_clustering(snowflake_session: Session):
        """
        Predefines all target tables with clustering on the 'symbol' column.
        Assumes that normalization functions return consistent schemas.
        """
        for table_name, normalize_func in TABLES_CONFIG.items():
            # Example schema definition based on normalization function
            # You need to adjust the columns based on actual normalization functions
            # Here, we'll assume each normalization function returns a dictionary with 'symbol' and other fields
            sample_data = {"symbol": "SAMPLE"}  # Placeholder

            try:
                # Get normalized sample data to infer columns
                normalized_sample = normalize_func(sample_data)
                normalized_sample['symbol'] = 'SAMPLE'  # Ensure symbol is included
                columns_definitions = ", ".join([f"{col} STRING" for col in normalized_sample.keys()])

                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns_definitions}
                )
                CLUSTER BY (symbol);
                """

                snowflake_session.sql(create_table_query).collect()
                logging.info(f"Table {table_name} ensured with clustering on 'symbol'.")
            except Exception as e:
                logging.exception(f"Failed to create or alter table {table_name} with clustering: {e}")

    async def process_symbol(http_session: aiohttp.ClientSession, symbol: str, snowflake_session: Session):
        data = await fetch_company_outlook(http_session, symbol)
        if not data:
            logging.warning(f"No data fetched for symbol: {symbol}")
            return

        company_data = process_company_data(data, symbol)

        # Map table names to the correct keys in company_data
        key_mapping = {
            "COMPANY_PROFILE": "profile",
            "COMPANY_METRICS": "metrics",
            "COMPANY_RATIOS": "ratios",
            "INSIDE_TRADES": "insideTrades",
            "SPLITS_HISTORY": "splitsHistory",
            "STOCK_DIVIDENDS": "stockDividends",
            "STOCK_NEWS": "stockNews",
        }

        # Iterate over each table and perform upsert
        for table_name, normalize_func in TABLES_CONFIG.items():
            key = key_mapping.get(table_name)
            table_data = company_data.get(key)
            if not table_data:
                logging.warning(f"No data for table {table_name} for symbol {symbol}. Skipping.")
                continue

            if isinstance(table_data, list):
                # For tables that have multiple records (e.g., INSIDE_TRADES)
                for entry in table_data:
                    normalized_entry = normalize_func(entry)
                    normalized_entry['symbol'] = symbol
                    await upsert_to_snowflake(snowflake_session, normalized_entry, table_name)
            else:
                # For tables that have single records (e.g., COMPANY_PROFILE)
                normalized_entry = normalize_func(table_data)
                normalized_entry['symbol'] = symbol
                await upsert_to_snowflake(snowflake_session, normalized_entry, table_name)

    async def main_async():
        # List of symbols to process
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']  # You can expand this list

        # Create Snowflake session
        try:
            connection_parameters = {
                "account": SNOWFLAKE_ACCOUNT,
                "user": SNOWFLAKE_USER,
                "password": SNOWFLAKE_PASSWORD,
                "role": SNOWFLAKE_ROLE,
                "warehouse": SNOWFLAKE_WAREHOUSE,
                "database": SNOWFLAKE_DATABASE,
                "schema": SNOWFLAKE_SCHEMA,
            }
            logging.info(f"Connection parameters: {connection_parameters}")
            # Establish a session with Snowflake
            snowflake_session = Session.builder.configs(connection_parameters).create()
            logging.info("Connection to Snowflake successful!")
        except Exception as e:
            logging.exception("Failed to connect to Snowflake")
            return  # Exit the function if connection failed

        # Predefine tables with clustering keys
        await predefine_tables_with_clustering(snowflake_session)

        # Process symbols concurrently
        async with aiohttp.ClientSession() as http_session:
            tasks = []
            for symbol in symbols:
                tasks.append(process_symbol(http_session, symbol, snowflake_session))
            await asyncio.gather(*tasks)

        # Close the Snowflake session
        try:
            snowflake_session.close()
            logging.info("Snowflake session closed.")
        except Exception as e:
            logging.exception("Failed to close Snowflake session.")

    # Run the async main function
    asyncio.run(main_async())

# DAG definition
with DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='A DAG to ingest data into Snowflake with clustering and upserts',
    schedule='@daily',
    catchup=False,  # Set to False to prevent backfilling
) as dag:

    data_ingestion_task = PythonOperator(
        task_id='data_ingestion_task',
        python_callable=run_ticker_outlook,
    )
