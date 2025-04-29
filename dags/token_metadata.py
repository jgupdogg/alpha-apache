from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from sqlalchemy import text
from typing import List, Set

# Import our custom DB tasks and Token class
from db_manager import DBManager
from utils.token_class import Token

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(
    dag_id='token_data_refresh_dag',
    default_args=default_args,
    schedule_interval='0 */12 * * *',  # Run every 12 hours
    catchup=False,
    tags=['solana', 'tokens', 'metadata', 'market_data'],
)
def token_data_refresh_dag():
    
    @task(task_id="get_unique_tokens")
    def get_unique_tokens() -> List[str]:
        """
        Query the database for all unique token addresses from both swapfromtoken and swaptotoken fields.
        
        Returns:
            List of unique token addresses
        """
        logger.info("Getting unique token addresses from the database")
        
        # Create database manager
        db = DBManager(db_name="solana_insiders_silver")
        
        # Query for all unique swapfromtoken values
        from_query = """
            SELECT DISTINCT swapfromtoken
            FROM helius_txns_clean
            WHERE swapfromtoken IS NOT NULL
        """
        
        # Query for all unique swaptotoken values
        to_query = """
            SELECT DISTINCT swaptotoken
            FROM helius_txns_clean
            WHERE swaptotoken IS NOT NULL
        """
        
        try:
            # Execute queries
            from_tokens = db.execute_query(from_query)
            to_tokens = db.execute_query(to_query)
            
            # Extract token addresses from query results
            from_set = {row.get('swapfromtoken') for row in from_tokens if row.get('swapfromtoken')}
            to_set = {row.get('swaptotoken') for row in to_tokens if row.get('swaptotoken')}
            
            # Combine into a single set of unique tokens
            all_tokens = from_set.union(to_set)
            
            # Convert set to list
            token_list = list(all_tokens)
            
            logger.info(f"Found {len(token_list)} unique token addresses")
            
            # Close the database connection
            db.quit()
            
            return token_list
            
        except Exception as e:
            logger.error(f"Error getting unique token addresses: {str(e)}")
            db.quit()
            return []
    
    @task(task_id="filter_recent_tokens")
    def filter_recent_tokens(token_list: List[str]) -> List[str]:
        """
        Filter out tokens that have been updated in the last 2 days.
        
        Args:
            token_list: List of token addresses
            
        Returns:
            Filtered list of token addresses
        """
        logger.info(f"Filtering {len(token_list)} tokens to exclude recently updated ones")
        
        # Skip if no tokens
        if not token_list or len(token_list) == 0:
            logger.info("No tokens to filter")
            return []
        
        # Create database manager
        db = DBManager(db_name="solana")
        
        # Calculate timestamp for 2 days ago
        two_days_ago = datetime.utcnow() - timedelta(days=2)
        
        # Query for tokens that have been updated within the last 2 days
        query = """
            SELECT DISTINCT token_address
            FROM token_metadata
            WHERE token_address IN :token_addresses
            AND scraped_at > :two_days_ago
        """
        
        try:
            # Execute query with parameters
            # Note: We need to convert the list to a tuple for SQL IN clause
            params = {
                "token_addresses": tuple(token_list),
                "two_days_ago": two_days_ago
            }
            
            recent_tokens = db.execute_query(query, params=params)
            
            # Extract token addresses from query results
            recent_set = {row.get('token_address') for row in recent_tokens if row.get('token_address')}
            
            # Filter out recently updated tokens
            filtered_tokens = [t for t in token_list if t not in recent_set]
            
            logger.info(f"Filtered out {len(recent_set)} recently updated tokens")
            logger.info(f"Remaining tokens to process: {len(filtered_tokens)}")
            
            # Close the database connection
            db.quit()
            
            return filtered_tokens
            
        except Exception as e:
            logger.error(f"Error filtering recent tokens: {str(e)}")
            db.quit()
            return token_list  # Return original list if filtering fails
    
    @task(task_id="fetch_token_data")
    def fetch_token_data(token_list: List[str]) -> int:
        """
        Fetch metadata and market data for each token in the list.
        
        Args:
            token_list: List of token addresses
            
        Returns:
            Number of tokens processed successfully
        """
        logger.info(f"Fetching data for {len(token_list)} tokens")
        
        # Skip if no tokens
        if not token_list or len(token_list) == 0:
            logger.info("No tokens to process")
            return 0
        
        # Create database manager
        db = DBManager(db_name="solana")
        successful_count = 0
        
        # Process tokens in batches to avoid memory issues
        batch_size = 50
        for i in range(0, len(token_list), batch_size):
            batch = token_list[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}, with {len(batch)} tokens")
            
            for token_address in batch:
                if not token_address:
                    continue
                    
                try:
                    # Create token object
                    token = Token(token_address, db_manager=db)
                    
                    # Get metadata
                    metadata = token.get_token_metadata(force_refresh=True)
                    
                    # Get market data
                    market_data = token.get_token_market_data(force_refresh=True)
                    
                    # Log results
                    if metadata:
                        token_name = metadata.get('name', 'Unknown')
                        token_symbol = metadata.get('symbol', 'Unknown')
                        logger.info(f"Metadata for {token_address}: {token_name} ({token_symbol})")
                    else:
                        logger.warning(f"No metadata returned for token {token_address}")
                        
                    if market_data:
                        price = market_data.get('price_usd', 'Unknown')
                        mcap = market_data.get('market_cap', 'Unknown')
                        logger.info(f"Market data for {token_address}: Price=${price}, MCap=${mcap}")
                    else:
                        logger.warning(f"No market data returned for token {token_address}")
                    
                    # Count as successful if either metadata or market data was retrieved
                    if metadata or market_data:
                        successful_count += 1
                        
                except Exception as e:
                    logger.error(f"Error processing token {token_address}: {str(e)}")
        
        # Close the database connection
        db.quit()
        
        logger.info(f"Successfully processed data for {successful_count} out of {len(token_list)} tokens")
        return successful_count
    
    # Define task flow
    all_tokens = get_unique_tokens()
    filtered_tokens = filter_recent_tokens(all_tokens)
    processed_count = fetch_token_data(filtered_tokens)

# Instantiate the DAG
dag_instance = token_data_refresh_dag()