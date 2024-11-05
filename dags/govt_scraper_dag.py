# dags/govt_scraper_dag.py

import logging
import sys
import os
import hashlib  # Ensure hashlib is imported
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.workflows.govt_sites.sites.site_agg import site_parsers
from utils.components.scraper.scraper_api import make_scraper_request
from superceeded.articles import Article
from utils.components.vectorizer.pinecone_utils import initialize_pinecone
from bs4 import BeautifulSoup
from snowflake.snowpark import Session
from dotenv import load_dotenv  # Ensure you have this import

# Ensure the 'dags' directory and its subdirectories are in the system path
dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_folder)
sys.path.append(os.path.join(dag_folder, 'utils'))

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def generate_summary_id(symbol: str, url: str) -> str:
    """
    Generates a unique summary ID based on the symbol and URL.

    Args:
        symbol (str): The stock symbol or identifier.
        url (str): The URL of the article.

    Returns:
        str: The generated summary ID.
    """
    # Hash the URL to ensure the summary_id is Pinecone-compatible
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return f"{symbol}_{url_hash}"


def update_summary_fields(snowflake_session, article_id: str, summary_id: str, summary_time: datetime):
    """
    Updates SUMMARY_ID and SUMMARY_TIME for the given article ID in Snowflake.

    Args:
        snowflake_session (snowflake.snowpark.Session): The Snowflake session.
        article_id (str): The article ID to update.
        summary_id (str): The summary ID to set.
        summary_time (datetime): The summary time to set.
    """
    if not article_id:
        logger.warning("No article ID provided for update.")
        return

    # Format the ID based on its data type (assuming string IDs)
    id_str = f"'{article_id}'"

    # Format the summary_time
    summary_time_str = summary_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(summary_time, datetime) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Construct the UPDATE query
    update_query = f"""
    UPDATE tickers.public.stock_news
    SET 
        SUMMARY_ID = '{summary_id}',
        SUMMARY_TIME = '{summary_time_str}'
    WHERE ID = {id_str};
    """

    try:
        logger.info(f"Executing update query for article ID {article_id}: {update_query}")
        snowflake_session.sql(update_query).collect()
        logger.info(f"Successfully updated SUMMARY_ID and SUMMARY_TIME for article ID {article_id}.")
    except Exception as e:
        logger.exception(f"Failed to update article ID {article_id}: {e}")


def run_govt_scraper(test_mode=True):
    """
    Main function to scrape, summarize, and upsert articles from various government sites.

    Args:
        test_mode (bool): If True, only the first article per site is processed.
    """
    # Load environment variables
    load_dotenv()

    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_REGION = os.getenv('SNOWFLAKE_REGION')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

    # Initialize Pinecone and get the index
    pinecone_index = initialize_pinecone()
    if not pinecone_index:
        logger.error("Pinecone initialization failed. Exiting task.")
        return

    snowflake_session = None  # Initialize session variable

    try:
        # Create Snowflake session
        try:
            connection_parameters = {
                "account": f"{SNOWFLAKE_ACCOUNT}.{SNOWFLAKE_REGION}",
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

        for symbol, site_info in site_parsers.items():
            logger.info(f"Processing {site_info['name']}...")

            try:
                # Load the site HTML content via the scraper service
                site_html = make_scraper_request(site_info['link'], use_proxy=True)
                soup = BeautifulSoup(site_html, 'html.parser')

                # Extract article links using the site's specific parser
                links = site_info['links_parse'](soup)

                if not links:
                    logger.warning(f"No links found for {site_info['name']}.")
                    continue

                # In test mode, only process the first link
                if test_mode:
                    links = links[:1]

                processed_articles = 0  # Counter for processed articles per site

                for link_data in links:
                    try:
                        if processed_articles >= 5:
                            logger.info(f"Reached the maximum of 5 articles for {site_info['name']}. Moving to the next site.")
                            break  # Exit the loop for this site

                        url = link_data.get('link')
                        if not url:
                            logger.warning(f"No URL found for link data: {link_data}. Skipping.")
                            continue

                        # Generate summary ID for the article
                        summary_id = generate_summary_id(symbol, url)

                        # Check if the summary already exists in Pinecone
                        existing_vector = pinecone_index.fetch(
                            ids=[summary_id],
                            namespace='summaries'
                        )

                        if existing_vector and existing_vector.get('vectors') and summary_id in existing_vector['vectors']:
                            logger.info(f"Summary already exists for article {url}. Skipping further articles from {site_info['name']}.")
                            break  # Stop processing more articles from this site

                        # Scrape the article page
                        article_html = make_scraper_request(url, use_proxy=True)
                        article_soup = BeautifulSoup(article_html, 'html.parser')

                        # Parse the article content using the site's specific parser
                        article_data = site_info['article_parse'](article_soup)

                        # Merge link_data and article_data, and add the site symbol
                        full_article_data = {
                            **link_data,
                            **article_data,
                            'symbol': symbol,
                            'name': site_info['name'],
                            'category': 'govt',  # Set category to 'govt'
                            'site': site_info['name'],  # Add site name if needed
                            'url': url,  # Add URL
                            'content': article_data.get('content', ''),  # Ensure content is available
                        }

                        # Create an Article object from the merged data
                        article = Article.from_dict(full_article_data)

                        # Assign summary ID and time to the article
                        article.summary_id = summary_id
                        article.summary_time = datetime.now()

                        # Generate all necessary representations (summary, sentiment/significance, vector)
                        try:
                            article.generate_all_representations()
                        except TypeError as te:
                            logger.error(
                                f"TypeError during summary generation for article {url}: {te}",
                                exc_info=True  # Include stack trace in the log
                            )
                            # Optionally, you can set summary fields to None or skip upserting
                            # Here, we'll skip upserting this article
                            continue
                        except Exception as e:
                            logger.error(
                                f"Error during summary generation for article {url}: {e}",
                                exc_info=True  # Include stack trace in the log
                            )
                            # Decide whether to skip or proceed; here, we'll skip
                            continue

                        # Upsert the article data to Pinecone
                        try:
                            article.upsert_to_pinecone(pinecone_index=pinecone_index, namespace='summaries')
                        except Exception as e:
                            logger.error(
                                f"Error upserting article to Pinecone for {url}: {e}",
                                exc_info=True
                            )
                            # Decide whether to skip updating Snowflake; here, we'll proceed

                        # Upsert the article data to Snowflake
                        try:
                            article.upsert_to_snowflake(snowflake_session)
                        except Exception as e:
                            logger.error(
                                f"Error upserting article to Snowflake for {url}: {e}",
                                exc_info=True
                            )
                            # Decide whether to skip updating summary fields; here, we'll proceed

                        # Update the article in Snowflake with SUMMARY_ID and SUMMARY_TIME
                        try:
                            update_summary_fields(snowflake_session, article.id, article.summary_id, article.summary_time)
                        except Exception as e:
                            logger.error(
                                f"Error updating summary fields in Snowflake for article {article.id}: {e}",
                                exc_info=True
                            )
                            # Proceed regardless

                        # Increment the processed articles counter
                        processed_articles += 1

                        logger.info(f"Processed article {article.id} from {site_info['name']}.")

                    except Exception as e:
                        logger.error(
                            f"Error processing article from {site_info['name']} ({link_data.get('link', 'No URL')}): {str(e)}",
                            exc_info=True  # Include stack trace in the log
                        )

                if test_mode:
                    logger.info(f"Test mode: Processed one article from {site_info['name']}")

            except Exception as e:
                logger.error(
                    f"Error processing site {site_info['name']} ({site_info['link']}): {str(e)}",
                    exc_info=True  # Include stack trace in the log
                )

    except Exception as e:
        logger.exception(f"Unexpected error in run_govt_scraper: {e}")

    finally:
        # Close the Snowflake session
        if snowflake_session:
            try:
                snowflake_session.close()
                logger.info("Snowflake session closed.")
            except Exception as e:
                logger.exception("Failed to close Snowflake session.")

        logger.info("Scraping process completed.")


# Default arguments for the DAG
default_args = {
    'owner': 'your_name',  # Replace with your name or team name
    'depends_on_past': False,
    'email': ['youremail@example.com'],  # Replace with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'govt_scraper_dag',
    default_args=default_args,
    description='DAG to scrape, summarize, and upsert government site articles to Pinecone and Snowflake',
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
    max_active_runs=1,
)


def run_govt_scraper_task(**kwargs):
    """
    Wrapper function to execute the run_govt_scraper with desired parameters.
    """
    run_govt_scraper(test_mode=False)  # Set test_mode as required


# Define the PythonOperator
scraper_task = PythonOperator(
    task_id='run_govt_scraper',
    python_callable=run_govt_scraper_task,
    dag=dag,
)
