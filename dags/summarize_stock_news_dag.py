# dags/summarize_stock_news_dag.py

import os
import asyncio
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from snowflake.snowpark import Session
from dotenv import load_dotenv
import hashlib
# Import the shared initialize_pinecone function
from utils.components.vectorizer.pinecone_utils import initialize_pinecone

# Configure logger using Airflow's logging system
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'justin',  # Replace with your name
    'start_date': datetime(2024, 11, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def generate_summary_id(symbol: str, date: datetime) -> str:
    """
    Generates a unique summary ID based on the symbol and date.

    Args:
        symbol (str): The stock symbol.
        date (datetime): The date of the article.

    Returns:
        str: The generated summary ID in the format 'symbol_YYYYMM'.
    """
    if date is None:
        date = datetime.now()
    date_str = date.strftime('%Y%m')  # Format as 'YYYYMM'
    return f"{symbol}_{date_str}"



async def process_symbol_articles(snowflake_session, symbol, group_df, symbol_to_name, pinecone_index):
    from classes.summary import Summary

    try:
        name = symbol_to_name.get(symbol, '')
        logger.info(f"Processing articles for symbol: {symbol} - {name}")

        # Prepare the excerpts with source IDs
        excerpts = []
        source_id_to_url = {}  # Mapping from source ID to URL
        source_ids = []  # List to store (ID, SOURCE_ID) tuples for bulk update

        # Collect all article IDs for metadata
        article_ids = group_df['ID'].tolist()
        logger.debug(f"Collected article IDs for symbol {symbol}: {article_ids}")

        # Prepare the excerpts and process each article
        for idx, row in group_df.iterrows():
            content = row['TEXT']
            category = row['CATEGORY']

            # Assign default values for 'press' category if fields are missing
            if category.lower() == 'press':
                url = row.get('URL') or 'press'
                site = row.get('SITE') or 'press'
                image = row.get('IMAGE') or 'press'
                logger.debug(f"Assigned default values for 'press' article ID {row['ID']}: URL={url}, SITE={site}, IMAGE={image}")
            else:
                url = row.get('URL', '')
                site = row.get('SITE', '')
                image = row.get('IMAGE', '')

            # Validate URL
            if not url:
                logger.warning(f"Missing URL for article ID {row['ID']}. Assigning 'unknown_url'.")
                url = 'unknown_url'

            # Generate SOURCE_ID
            if url == 'press':
                # Assign a unique SOURCE_ID for 'press' using article ID
                source_id = hashlib.md5(row['ID'].encode('utf-8')).hexdigest()[:8]
                logger.debug(f"Generated SOURCE_ID for 'press' article ID {row['ID']}: SOURCE_ID={source_id}")
            else:
                source_id = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
                logger.debug(f"Generated SOURCE_ID for 'news' article ID {row['ID']}: SOURCE_ID={source_id}")

            # Append the tuple for bulk update
            source_ids.append((row['ID'], source_id))
            source_id_to_url[source_id] = url

            # Include 'SOURCE_ID' in the excerpt
            excerpt = f"{content}\n*** SOURCE: {source_id}"
            excerpts.append(excerpt)

            # Update the row with modified values
            group_df.at[idx, 'URL'] = url
            group_df.at[idx, 'SITE'] = site
            group_df.at[idx, 'IMAGE'] = image

        combined_excerpts = '\n'.join(excerpts)

        # Create a Summary instance
        summary = Summary(
            symbol=symbol,
            name=name,
            content=combined_excerpts,
            article_ids=article_ids
        )

        # Check for existing summary in Pinecone and set existing_summary
        existing_vector = summary.retrieve_existing_summary_from_pinecone(
            pinecone_index=pinecone_index,
            namespace='summaries'
        )

        if existing_vector:
            logger.info(f"Existing summary found for symbol {symbol}. Using the existing summary.")
        else:
            logger.info(f"No existing summary found for symbol {symbol}. Generating new summary.")

        logger.info(f"Generating all representations for symbol {symbol}...")
        # Generate all representations (summary, sentiment/significance, vector)
        representations = summary.generate_all_representations()
        if representations:
            logger.info(f"All representations generated for symbol {symbol}.")

            # Upsert to Pinecone with metadata
            try:
                logger.info(f"Upserting to Pinecone for symbol {symbol}...")
                summary.upsert_to_pinecone(pinecone_index=pinecone_index, namespace='summaries')
                logger.info(f"Successfully upserted summary vector to Pinecone for symbol {symbol}.")
            except Exception as e:
                logger.exception(f"Failed to upsert summary vector to Pinecone for symbol {symbol}: {e}")

            # Update articles in Snowflake
            summary.update_articles_in_snowflake(snowflake_session)
        else:
            logger.warning(f"No representations generated for symbol {symbol}.")
    except Exception as e:
        logger.exception(f"Error in processing articles for symbol {symbol}: {e}")


async def main_async(snowflake_session, pinecone_index, test=False):
    """
    Main asynchronous pipeline to process all unsummarized stock news articles.
    
    Args:
        snowflake_session (snowflake.snowpark.Session): The Snowflake session.
        pinecone_index: The connected Pinecone index.
    """
    try:
        # Query unsummarized articles
        logger.info("Querying unsummarized articles from Snowflake...")
        query = """
        SELECT SYMBOL, SITE, PUBLISHEDDATE, TITLE, TEXT, URL, ID, CATEGORY
        FROM STOCK_NEWS
        WHERE SUMMARY_ID IS NULL AND CATEGORY IN ('news', 'press')
        """

        articles_df = snowflake_session.sql(query).to_pandas()
        logger.info(f"Fetched {len(articles_df)} unsummarized articles.")
        logger.info(f"Columns in articles_df: {articles_df.columns.tolist()}")
    except Exception as e:
        logger.exception("Failed to fetch unsummarized articles from STOCK_NEWS.")
        return

    if articles_df.empty:
        logger.info("No unsummarized articles found.")
        return

    # Get list of symbols
    symbols = articles_df['SYMBOL'].unique().tolist()
    logger.info(f"Processing {len(symbols)} symbols.")

    # Get company names for the symbols
    try:
        symbols_str = ",".join(f"'{s}'" for s in symbols)
        query = f"""
        SELECT SYMBOL, COMPANYNAME
        FROM COMPANY_PROFILE
        WHERE SYMBOL IN ({symbols_str});
        """
        logger.info(f"Fetching company names with query: {query}")
        company_names_df = snowflake_session.sql(query).to_pandas()
        symbol_to_name = dict(zip(company_names_df['SYMBOL'], company_names_df['COMPANYNAME']))
        logger.info(f"Fetched company names for {len(symbol_to_name)} symbols.")
    except Exception as e:
        logger.exception("Failed to fetch company names from COMPANY_PROFILE.")
        symbol_to_name = {symbol: '' for symbol in symbols}  # Fallback to empty names

    # Group articles by SYMBOL
    try:
        grouped_articles = articles_df.groupby('SYMBOL')
        logger.info(f"Grouped articles by symbol. Number of groups: {len(grouped_articles)}")
    except Exception as e:
        logger.exception("Failed to group articles by symbol.")
        return

    # Process articles concurrently for each symbol
    tasks = []
    for symbol, group_df in grouped_articles:
        logger.info(f"Adding processing task for symbol {symbol}")
        tasks.append(process_symbol_articles(snowflake_session, symbol, group_df, symbol_to_name, pinecone_index))

    if tasks:
        logger.info("Starting asyncio.gather for processing symbols...")
        await asyncio.gather(*tasks)
        logger.info("Completed asyncio.gather for processing symbols.")
    else:
        logger.info("No processing tasks to run.")


async def run_async_pipeline(snowflake_session, pinecone_index, test=False):
    """
    Wrapper to run the main asynchronous pipeline.
    
    Args:
        snowflake_session (snowflake.snowpark.Session): The Snowflake session.
        pinecone_index: The connected Pinecone index.
    """
    await main_async(snowflake_session, pinecone_index, test=test)


def run_summarize_stock_news_task(test=False):
    """
    The main task function that initializes environment, connects to Snowflake and Pinecone,
    and runs the asynchronous pipeline to summarize stock news articles.
    """
    import sys

    # Ensure that Airflow can find the utils package by modifying sys.path
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(dag_folder, 'utils')
    if utils_path not in sys.path:
        sys.path.append(dag_folder)
        sys.path.append(utils_path)
        logger.info(f"Added '{dag_folder}' and '{utils_path}' to sys.path.")

    # Load environment variables
    dotenv_path = os.path.abspath(os.path.join(dag_folder, '..', '.env'))
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        logger.info(f"Loaded environment variables from '{dotenv_path}'.")
    else:
        logger.error(f".env file not found at '{dotenv_path}'. Exiting task.")
        return

    # Log environment variables for debugging
    logger.info(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    logger.info(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")

    # Create Snowflake session
    try:
        connection_parameters = {
            "account": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.{os.getenv('SNOWFLAKE_REGION')}",
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "role": os.getenv('SNOWFLAKE_ROLE'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "database": os.getenv('SNOWFLAKE_DATABASE'),
            "schema": os.getenv('SNOWFLAKE_SCHEMA'),
        }
        logger.info(f"Connection parameters: {connection_parameters}")
        # Establish a session with Snowflake
        logger.info("Establishing Snowflake session...")
        snowflake_session = Session.builder.configs(connection_parameters).create()
        logger.info("Connection to Snowflake successful!")
    except Exception as e:
        logger.exception("Failed to connect to Snowflake.")
        raise  # Re-raise the exception to prevent silent failure

    # Initialize Pinecone and get the index
    pinecone_index = initialize_pinecone()
    if not pinecone_index:
        logger.error("Pinecone initialization failed. Exiting task.")
        return

    # Run the async pipeline
    try:
        logger.info("Running async pipeline...")
        asyncio.run(run_async_pipeline(snowflake_session, pinecone_index, test=test))
        logger.info("Async pipeline completed.")
    except Exception as e:
        logger.exception(f"Async pipeline failed: {e}")
    finally:
        # Close the Snowflake session
        try:
            logger.info("Closing Snowflake session...")
            snowflake_session.close()
            logger.info("Snowflake session closed.")
        except Exception as e:
            logger.exception("Failed to close Snowflake session.")


# DAG definition
with DAG(
    'summarize_stock_news_dag',
    default_args=default_args,
    description='A DAG to summarize stock news articles, update STOCK_NEWS table, and upsert summaries to Pinecone',
    schedule_interval='@hourly',  
    catchup=False,
) as dag:

    summarize_stock_news_task = PythonOperator(
        task_id='summarize_stock_news_task',
        python_callable=run_summarize_stock_news_task,
    )
