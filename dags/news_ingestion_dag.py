# dags/news_ingestion_dag.py

import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from typing import Dict, List
from snowflake.snowpark import Session
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys

# Import the Article class
from classes.articles import Article

# Ensure that Airflow can find the utils package:
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

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

# Global variables (moved outside of functions)
API_KEY = os.getenv('API_KEY')
STOCK_NEWS_URL = 'https://financialmodelingprep.com/api/v3/stock_news'
PRESS_RELEASES_URL = 'https://financialmodelingprep.com/api/v3/press-releases'

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = f"{os.getenv('SNOWFLAKE_ACCOUNT')}.{os.getenv('SNOWFLAKE_REGION')}"
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

logger.info(f'SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}')
logger.info(f'SNOWFLAKE_USER: {SNOWFLAKE_USER}')

def run_stock_news():
    # Since run_stock_news is called by Airflow's PythonOperator, it must be synchronous.
    # We'll run the asynchronous main_async() function using asyncio.run().
    asyncio.run(main_async())

async def fetch_news(http_session: aiohttp.ClientSession, symbols: List[str]) -> List[Dict]:
    news_data = []
    tickers = ','.join(symbols)

    # Calculate 'from' and 'to' dates for the past 24 hours
    to_date = datetime.utcnow()
    from_date = to_date - timedelta(days=1)

    # Format dates as 'YYYY-MM-DD'
    from_date_str = from_date.strftime('%Y-%m-%d')
    to_date_str = to_date.strftime('%Y-%m-%d')

    params = {
        'tickers': tickers,
        'from': from_date_str,
        'to': to_date_str,
        'apikey': API_KEY
    }

    try:
        async with http_session.get(STOCK_NEWS_URL, params=params) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch news: {response.status}")
                return news_data
            data = await response.json()
            news_data.extend(data)
    except Exception as e:
        logger.exception(f"Exception occurred while fetching news: {e}")
    return news_data

async def fetch_press_releases(http_session: aiohttp.ClientSession, symbol: str) -> List[Dict]:
    press_data = []
    url = f"{PRESS_RELEASES_URL}/{symbol}"

    # Calculate 'from' and 'to' dates for the past 24 hours
    to_date = datetime.utcnow()
    from_date = to_date - timedelta(days=1)

    # Format dates as 'YYYY-MM-DD'
    from_date_str = from_date.strftime('%Y-%m-%d')
    to_date_str = to_date.strftime('%Y-%m-%d')

    params = {
        'from': from_date_str,
        'to': to_date_str,
        'apikey': API_KEY
    }

    try:
        async with http_session.get(url, params=params) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch press releases for {symbol}: {response.status}")
                return press_data
            data = await response.json()
            filtered_data = []
            for item in data:
                date_str = item.get('date')
                if date_str:
                    item_date = parse_datetime(date_str)
                    if item_date and from_date <= item_date <= to_date:
                        filtered_data.append(item)
            press_data.extend(filtered_data)
    except Exception as e:
        logger.exception(f"Exception occurred while fetching press releases for {symbol}: {e}")
    return press_data

def parse_datetime(value: str):
    # Parse datetime string to datetime object
    date_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d'
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    logger.warning(f"Unable to parse date: {value}")
    return None

async def main_async():
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
        logger.info(f"Connection parameters: {connection_parameters}")
        # Establish a session with Snowflake
        snowflake_session = Session.builder.configs(connection_parameters).create()
        logger.info("Connection to Snowflake successful!")
    except Exception as e:
        logger.exception("Failed to connect to Snowflake")
        return  # Exit the function if connection failed

    # Get list of symbols from company_profile table
    try:
        symbols_df = snowflake_session.table("COMPANY_PROFILE").select("SYMBOL").distinct().collect()
        symbols = [row['SYMBOL'] for row in symbols_df]
        logger.info(f"Retrieved {len(symbols)} symbols from COMPANY_PROFILE.")
    except Exception as e:
        logger.exception("Failed to retrieve symbols from COMPANY_PROFILE.")
        return

    # Process symbols in batches for stock news
    batch_size = 5  # Adjust batch size as needed
    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    async with aiohttp.ClientSession() as http_session:
        # Fetch and process stock news
        for batch in symbol_batches:
            news_data = await fetch_news(http_session, batch)
            for news_item in news_data:
                try:
                    # Create an Article object
                    article = Article(
                        title=news_item.get('title'),
                        content=news_item.get('text'),
                        date_str=news_item.get('publishedDate'),
                        symbol=news_item.get('symbol'),
                        name='',  # Name can be empty or fetched from another source
                        category='news',
                        site=news_item.get('site'),
                        url=news_item.get('url')
                    )

                    # Upsert the article data to Snowflake
                    article.upsert_to_snowflake(snowflake_session)
                except Exception as e:
                    logger.exception(f"Error processing news article: {e}")

        # Fetch and process press releases
        tasks = []
        for symbol in symbols:
            tasks.append(fetch_press_releases(http_session, symbol))
        press_results = await asyncio.gather(*tasks)
        for press_data in press_results:
            for press_item in press_data:
                try:
                    # Assign 'press' to missing fields
                    url = press_item.get('url') or 'press'
                    site = press_item.get('site') or 'press'

                    # Create an Article object
                    article = Article(
                        title=press_item.get('title'),
                        content=press_item.get('text'),
                        date_str=press_item.get('date'),
                        symbol=press_item.get('symbol'),
                        name='',  # Name can be empty or fetched from another source
                        category='press',
                        site=site,
                        url=url
                    )

                    # Upsert the article data to Snowflake
                    article.upsert_to_snowflake(snowflake_session)
                except Exception as e:
                    logger.exception(f"Error processing press release: {e}")

    # Close the Snowflake session
    try:
        snowflake_session.close()
        logger.info("Snowflake session closed.")
    except Exception as e:
        logger.exception("Failed to close Snowflake session.")

# DAG definition (moved outside of any function)
with DAG(
    'news_ingestion_dag',
    default_args=default_args,
    description='A DAG to ingest stock news and press releases into Snowflake',
    schedule='@hourly',  
    catchup=False,
) as dag:

    news_ingestion_task = PythonOperator(
        task_id='news_ingestion_task',
        python_callable=run_stock_news,
    )
