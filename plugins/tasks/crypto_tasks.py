# plugins/tasks/crypto_tasks.py

import logging
from typing import List, Dict, Optional
from collections import defaultdict

from airflow.decorators import task
from airflow.hooks.base import BaseHook

# Import your generic fetch/store function
from utils.crypto.birdeye_generic_fetch_store import birdeye_generic_fetch_store
from airflow.exceptions import AirflowSkipException
from db_manager import DBManager  # Adjust import path as necessary

logger = logging.getLogger(__name__)


# Pre-fetch connections to avoid doing it inside each task
def get_db_api_credentials():
    db_conn = BaseHook.get_connection("pipeline_db")
    api_conn = BaseHook.get_connection("birdseye_api")
    return db_conn.get_uri(), api_conn.password  # Assuming API key is stored as password


DB_URL, API_KEY = get_db_api_credentials()


@task(pool='birdeye_pool', task_id='task_fetch_token_trending')
def task_fetch_token_trending(
    blockchains: Optional[List[Dict[str, str]]] = None,
    limit: int = 20,
    max_offset: int = 20, 
    table_name: str = "token_trending_raw",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches trending token data from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data in the target table is considered fresh.

    :param blockchains: List of blockchain-address pairs.
    :param limit: Number of records to fetch per API call.
    :param max_offset: Maximum offset for pagination.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """

    logger.info(f"Starting task_fetch_token_trending for table '{table_name}' with freshness threshold {freshness_hours} hours.")

    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible

    try:
        # 1. Check if the table data is fresh
        if db_manager.is_table_fresh(table_name, freshness_hours, timestamp_column="timestamp"):
            logger.info(
                f"Data in table '{table_name}' is fresh within the last {freshness_hours} hours. Skipping fetch."
            )
        
        else:
            logger.info(f"Data in table '{table_name}' is stale. Proceeding to fetch token trending data.")

            # 2. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,
                addresses=blockchains,
                method_name="get_token_trending",
                sdk_class="token",
                response_keys=["data", "tokens"],
                method_params={},  # Adjust as necessary
                address_key_in_method=None,  # Not used for this endpoint
                table_name=table_name,
                limit=limit,
                max_offset=max_offset,
            )
            logger.info("Completed fetching token trending data.")
    
    except Exception as e:
        logger.error("Error fetching token trending data.", exc_info=True)
        raise
    finally:
        # Ensure the database connection is closed
        db_manager.quit()



@task(pool='birdeye_pool', task_id='task_fetch_whales')
def task_fetch_whales(
    addresses: Optional[List[Dict[str, str]]] = None,
    limit: int = 100,
    max_offset: int = 200,
    table_name: str = "list_whales",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches whale holder data from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data in the target table is considered fresh.

    :param addresses: List of blockchain-address pairs, e.g.,
                      [{"chain": "ethereum", "address": "0xABCDEF1234567890"}, ...]
    :param limit: Number of records to fetch per API call.
    :param max_offset: Maximum offset for pagination.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """

    logger.info(f"Starting task_fetch_whales for table '{table_name}' with freshness threshold {freshness_hours} hours.")

    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible

    try:
        # 1. Check if the table data is fresh
        if db_manager.is_table_fresh(table_name, freshness_hours, timestamp_column="timestamp"):
            logger.info(
                f"Data in table '{table_name}' is fresh within the last {freshness_hours} hours. Skipping fetch."
            )
        
        else:
            logger.info(f"Data in table '{table_name}' is stale. Proceeding to fetch whale holder data.")

            # 2. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,
                addresses=addresses,
                method_name="get_token_top_holders",
                sdk_class="token",
                response_keys=["data", "items"],
                table_name=table_name,
                identifier="address",
                limit=limit,
                max_offset=max_offset,
                address_key_in_method="address",
                method_params={},  # Empty dict as per YAML
            )
            logger.info("Completed fetching list whales data.")
    
    except Exception as e:
        logger.error("Error fetching list whales data.", exc_info=True)
        raise
    
    finally:
        # Ensure the database connection is closed
        db_manager.quit()
    



@task(pool='birdeye_pool', task_id='task_fetch_gainers_losers')
def task_fetch_gainers_losers(
    blockchain: str,
    offset: int = 20,
    limit: int = 10,
    table_name: str = "top_traders_gainers_losers_raw",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches gainers and losers data from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data in the target table is considered fresh.

    :param blockchain: The blockchain identifier, e.g., "ethereum", "solana".
    :param offset: The pagination offset.
    :param limit: The number of records to fetch per API call.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """

    logger.info(f"Starting task_fetch_gainers_losers for table '{table_name}' with freshness threshold {freshness_hours} hours.")

    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible

    try:

        logger.info(f"Data in table '{table_name}' is stale. Proceeding to fetch gainers and losers data.")

        # 2. Perform the fetch
        birdeye_generic_fetch_store(
            db_url=DB_URL,
            api_key=API_KEY,
            addresses=[{"blockchain": blockchain}],  # List containing a single blockchain
            method_name="get_gainers_losers",
            sdk_class="trader",
            response_keys=["data", "items"],
            table_name=table_name,
            method_params={},
            address_key_in_method=None,          # Not used for this endpoint
            identifier=None,                     # Not used for this endpoint
            chain_in_data_keys=False,            # Adjust based on your requirements
            max_offset=None,                     # Handled via pagination
            limit=limit,
            explode_columns=['items'],
            single_record=False
        )
        logger.info(f"Successfully fetched Gainers and Losers for Blockchain: {blockchain}, Offset: {offset}")
        
    except Exception as e:
        logger.error(f"Failed to fetch Gainers and Losers for Blockchain: {blockchain}, Offset: {offset}. Error: {e}", exc_info=True)
        raise
    finally:
        # Ensure the database connection is closed
        db_manager.quit()
    
@task(pool='birdeye_pool', task_id='task_fetch_swaps')
def task_fetch_swaps(
    addresses: Optional[List[Dict[str, str]]] = None,
    limit: int = 100,
    max_offset: int = 500,
    table_name: str = "trades_seek_by_time_raw",
    after_time: int = 0,
    freshness_hours: int = 24  # <--- new parameter
) -> None:
    """
    Fetches trade data (swaps) from the Birdeye API and stores it in PostgreSQL.
    Skips if the table is considered 'fresh' within the last `freshness_hours`.
    Processes addresses in batches of 100 to avoid issues with large input lists.
    """
    db_manager = DBManager(db_url=DB_URL)

    # 1. Check freshness
    if db_manager.is_table_fresh(table_name, freshness_hours, timestamp_column="timestamp"):
        logger.info(
            f"Table '{table_name}' is within {freshness_hours}h threshold. Skipping fetch."
        )
        db_manager.quit()
        return
    else:
        logger.info(f"Table '{table_name}' is stale. Proceeding to fetch swaps data.")

        try:
            # Split the addresses into batches of 100 if provided; else use None as a single batch.
            if addresses:
                batches = [addresses[i:i+100] for i in range(0, len(addresses), 100)]
            else:
                batches = [None]

            total_batches = len(batches)
            for batch_index, batch in enumerate(batches, start=1):
                birdeye_generic_fetch_store(
                    db_url=DB_URL,
                    api_key=API_KEY,
                    addresses=batch,
                    method_name="get_trades_seek_by_time",
                    sdk_class="trader",
                    response_keys=["data", "items"],
                    table_name=table_name,
                    identifier="address",
                    limit=limit,
                    max_offset=max_offset,
                    address_key_in_method="address",
                    method_params={"after_time": after_time},
                    explode_columns=["quote", "base"],
                    single_record=False
                )
                logger.info(f"Completed batch {batch_index} of {total_batches} for table '{table_name}'")
            logger.info("Completed fetching all batches of swaps data.")
        except Exception as e:
            logger.error("Error fetching swaps data.", exc_info=True)
            raise
    db_manager.quit()



@task(pool='birdeye_pool', task_id='task_fetch_wallet_balances')
def task_fetch_wallet_balances(
    addresses: Optional[List[Dict[str, str]]] = None,
    table_name: str = "account_balances_raw",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches wallet balances from the Birdeye API and stores them in PostgreSQL.
    Skips fetching if the data in the target table is considered fresh.

    :param addresses: List of blockchain-address pairs.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """

    logger.info(f"Starting task_fetch_wallet_balances for table '{table_name}' with freshness threshold {freshness_hours} hours.")

    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible

    try:
        # 1. Check if the table data is fresh
        if db_manager.is_table_fresh(table_name, freshness_hours, timestamp_column="timestamp"):
            logger.info(
                f"Data in table '{table_name}' is fresh within the last {freshness_hours} hours. Skipping fetch."
            )

        else:
            # 2. Check if addresses are provided
            if not addresses:
                logger.warning("No wallet addresses provided for fetching balances. Skipping task.")
                raise AirflowSkipException("No wallet addresses provided; skipping task_fetch_wallet_balances.")

            logger.info(f"Data in table '{table_name}' is stale. Proceeding to fetch wallet balances for {len(addresses)} addresses.")

            # 3. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,
                addresses=addresses,
                method_name="get_token_list",
                sdk_class="wallet",
                response_keys=["data", "items"],
                table_name=table_name,
                identifier="wallet",
                limit=None,
                max_offset=None,
                address_key_in_method="wallet",
                method_params={},  # As per YAML configuration
                explode_columns=["items"],
                single_record=False
            )
            logger.info("Completed fetching wallet balances.")
    
    except Exception as e:
        logger.error(f"Failed to fetch wallet balances. Error: {e}", exc_info=True)
        raise
    finally:
        # Ensure the database connection is closed
        db_manager.quit()
    


@task(pool='birdeye_pool', task_id='task_fetch_metadata_single')
def task_fetch_metadata_single(
    addresses: Optional[List[Dict[str, str]]] = None,
    table_name: str = "token_metadata_raw",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches metadata for a list of token addresses from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data for individual tokens is considered fresh.

    :param addresses: List of blockchain-address pairs, e.g.,
                      [{"blockchain": "ethereum", "address": "0xABCDEF1234567890"}, ...]
    :param batch_size: Number of address lists to process in each batch.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """
    
    logger.info(f"Starting task_fetch_metadata for table '{table_name}' with freshness threshold {freshness_hours} hours.")
    
    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible
    
    try:
        # 1. Check if addresses are provided
        if not addresses:
            logger.warning("No addresses provided for metadata fetching. Skipping task.")
            raise AirflowSkipException("No addresses provided; skipping task_fetch_metadata.")

        # 2. Extract blockchain and address list
        blockchain = addresses[0].get("blockchain")
        if not blockchain:
            logger.error("Blockchain information missing in addresses.")
            raise ValueError("Blockchain information missing in addresses.")
        
        address_list = [addr["address"] for addr in addresses if "address" in addr]
        
        
        if not address_list:
            logger.warning("No valid addresses found in input. Skipping task.")
            raise AirflowSkipException("No valid addresses found; skipping task_fetch_metadata.")

        # 3. Filter for freshness
        stale_tokens = db_manager.filter_for_freshness(
            table_name=table_name,
            blockchain=blockchain,
            addresses=address_list,
            freshness_hours=freshness_hours,
            timestamp_column="timestamp",
            block_column="blockchain",
            address_column="address"
        )

        if not stale_tokens:
            logger.info("All token metadata is fresh. Skipping fetch.")
            raise AirflowSkipException("All token metadata is fresh; skipping task_fetch_metadata.")

        else:
            logger.info(f"{len(stale_tokens)} tokens need fetching. Proceeding...")
    
            # 5. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,  # Ensure API_KEY is defined globally or passed appropriately
                addresses=addresses,
                method_name="get_token_metadata",
                sdk_class="token",
                response_keys=["data"],
                table_name=table_name,
                identifier="address",
                max_offset=10,
                address_key_in_method="address",
                method_params={},  # Adjust if the API requires additional parameters
                explode_columns=["metadata", "extensions"]
            )
            logger.info("Completed fetching metadata.")

    except AirflowSkipException as e:
        logger.info(str(e))
        # Re-raise to let Airflow handle the skip
        raise
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}", exc_info=True)
        raise

    finally:
        # Ensure the database connection is closed
        db_manager.quit()

@task(pool='birdeye_pool', task_id='task_fetch_metadata')
def task_fetch_metadata(
    addresses: Optional[List[Dict[str, str]]] = None,
    batch_size: int = 50,
    table_name: str = "token_metadata_raw",
    freshness_hours: int = 24  # New parameter for freshness threshold
) -> None:
    """
    Fetches metadata for a list of token addresses from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data for individual tokens is considered fresh.

    :param addresses: List of blockchain-address pairs, e.g.,
                      [{"blockchain": "ethereum", "address": "0xABCDEF1234567890"}, ...]
    :param batch_size: Number of address lists to process in each batch.
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """
    
    logger.info(f"Starting task_fetch_metadata for table '{table_name}' with freshness threshold {freshness_hours} hours.")
    
    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible
    
    try:
        # 1. Check if addresses are provided
        if not addresses:
            logger.warning("No addresses provided for metadata fetching. Skipping task.")
            raise AirflowSkipException("No addresses provided; skipping task_fetch_metadata.")

        # 2. Extract blockchain and address list
        blockchain = addresses[0].get("blockchain")
        if not blockchain:
            logger.error("Blockchain information missing in addresses.")
            raise ValueError("Blockchain information missing in addresses.")
        
        address_list = [addr["address"] for addr in addresses if "address" in addr]
        
        
        if not address_list:
            logger.warning("No valid addresses found in input. Skipping task.")
            raise AirflowSkipException("No valid addresses found; skipping task_fetch_metadata.")

        # 3. Filter for freshness
        stale_tokens = db_manager.filter_for_freshness(
            table_name=table_name,
            blockchain=blockchain,
            addresses=address_list,
            freshness_hours=freshness_hours,
            timestamp_column="timestamp",
            block_column="blockchain",
            address_column="address"
        )

        if not stale_tokens:
            logger.info("All token metadata is fresh. Skipping fetch.")
            raise AirflowSkipException("All token metadata is fresh; skipping task_fetch_metadata.")

        else:
            logger.info(f"{len(stale_tokens)} tokens need fetching. Proceeding...")
    
            # 4. Transform addresses into batches
            blockchain_dict = defaultdict(list)
            for entry in stale_tokens:
                blockchain = entry.get("blockchain")
                address = entry.get("address")
                if blockchain and address:
                    blockchain_dict[blockchain].append(address)

            transformed = []
            for blockchain, addr_list in blockchain_dict.items():
                # Split the address list into chunks of 'batch_size'
                for i in range(0, len(addr_list), batch_size):
                    batch_addresses = addr_list[i:i + batch_size]
                    list_address = ",".join(batch_addresses)
                    transformed.append({
                        "blockchain": blockchain,
                        "list_address": list_address
                    })
                    logger.info(f"Transformed blockchain '{blockchain}' with addresses: {list_address}")

            logger.info(f"Transformed addresses into {len(transformed)} blockchain-address_list pairs.")

            # log api key
            logger.info(f"API Key: {API_KEY}")
            
            # 5. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,  # Ensure API_KEY is defined globally or passed appropriately
                addresses=transformed,
                method_name="get_token_metadata_multiple",
                sdk_class="token",
                response_keys=["data"],
                table_name=table_name,
                identifier="list_address",
                max_offset=10,
                address_key_in_method="list_address",
                method_params={},  # Adjust if the API requires additional parameters
                explode_columns=["metadata", "extensions"]
            )
            logger.info("Completed fetching metadata.")

    except AirflowSkipException as e:
        logger.info(str(e))
        # Re-raise to let Airflow handle the skip
        raise
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}", exc_info=True)
        raise

    finally:
        # Ensure the database connection is closed
        db_manager.quit()
    
    
    

@task(pool='birdeye_pool', task_id='task_fetch_token_market_data')
def task_fetch_token_market_data(
    tokens: Optional[List[Dict[str, str]]] = None,
    table_name: str = "token_market_data_raw",
    freshness_hours: int = 1  # Adjusted threshold as per your requirement
) -> None:
    """
    Fetches token market data from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data for individual tokens is considered fresh.
    
    :param tokens: List of dictionaries containing 'blockchain' and 'address'.
    :param table_name: Target table name in PostgreSQL for market data.
    :param freshness_hours: Number of hours to consider data as fresh.
    """
    
    logger.info(f"Starting task_fetch_token_market_data for table '{table_name}' with freshness threshold {freshness_hours} hours.")
    
    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible
    
    try:
        # 1. Check if tokens are provided
        if not tokens:
            logger.warning("No tokens provided for fetching market data. Skipping task.")
            raise AirflowSkipException("No tokens provided; skipping task_fetch_token_market_data.")

        # 2. Extract blockchain and address list
        blockchain = tokens[0].get("blockchain")
        if not blockchain:
            logger.error("Blockchain information missing in tokens.")
            raise ValueError("Blockchain information missing in tokens.")
        
        address_list = [token["address"] for token in tokens if "address" in token]
        if not address_list:
            logger.warning("No valid addresses found in tokens. Skipping task.")
            raise AirflowSkipException("No valid addresses found; skipping task_fetch_token_market_data.")

        # 3. Filter for freshness
        stale_tokens = db_manager.filter_for_freshness(
            table_name=table_name,
            blockchain=blockchain,
            addresses=address_list,
            freshness_hours=freshness_hours,
            timestamp_column="timestamp",
            block_column="blockchain",
            address_column="address"
        )

        if not stale_tokens:
            logger.info("All token market data is fresh. Skipping fetch.")
            raise AirflowSkipException("All token market data is fresh; skipping task_fetch_token_market_data.")

        else:
            logger.info(f"{len(stale_tokens)} tokens need fetching. Proceeding...")
        
            # 4. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,  # Ensure API_KEY is defined globally or passed appropriately
                addresses=stale_tokens,
                method_name="get_token_market_data",
                sdk_class="token",
                response_keys=["data"],
                table_name=table_name,
                identifier="address",
                address_key_in_method="address",
                method_params={},  # Adjust if the API requires additional parameters
                explode_columns=[],  # No columns to explode for this endpoint
                single_record=True  # Single record response as per YAML configuration
            )
            logger.info("Completed fetching token market data.")
    
    except AirflowSkipException as e:
        logger.info(str(e))
        # Re-raise to let Airflow handle the skip
        raise
    except Exception as e:
        logger.error(f"Error fetching token market data: {e}", exc_info=True)
        raise
    
    finally:
        # Ensure the database connection is closed
        db_manager.quit()
        



@task(pool='birdeye_pool', task_id='task_fetch_token_security')
def task_fetch_token_security(
    addresses: Optional[List[Dict[str, str]]] = None,
    table_name: str = "token_security_raw",
    freshness_hours: int = 2  # New parameter for freshness threshold
) -> None:
    """
    Fetches security information for a list of token addresses from the Birdeye API and stores it in PostgreSQL.
    Skips fetching if the data in the target table is considered fresh.

    :param addresses: List of blockchain-address pairs, e.g.,
                      [{"blockchain": "ethereum", "address": "0xABCDEF1234567890"}, ...]
    :param table_name: Target table name in PostgreSQL.
    :param freshness_hours: Number of hours to consider data as fresh.
    """
    
    logger.info(f"Starting task_fetch_token_security for table '{table_name}' with freshness threshold {freshness_hours} hours.")
    
    # Instantiate DBManager
    db_manager = DBManager(db_url=DB_URL)  # Ensure DB_URL is accessible
    
    try:
        # 1. Check if addresses are provided
        if not addresses:
            logger.warning("No addresses provided for security info fetching. Skipping task.")
            raise AirflowSkipException("No addresses provided; skipping task_fetch_token_security.")

        # 2. Extract blockchain and addresses
        blockchain = addresses[0].get("blockchain")
        if not blockchain:
            logger.error("Blockchain information missing in addresses.")
            raise ValueError("Blockchain information missing in addresses.")
        
        address_list = [addr["address"] for addr in addresses if "address" in addr]
        if not address_list:
            logger.warning("No valid addresses found in input. Skipping task.")
            raise AirflowSkipException("No valid addresses found; skipping task_fetch_token_security.")

        # 3. Filter for freshness
        stale_addresses = db_manager.filter_for_freshness(
            table_name=table_name,
            blockchain=blockchain,
            addresses=address_list,
            freshness_hours=freshness_hours,
            timestamp_column="timestamp",
            block_column="blockchain",
            address_column="address"
        )

        if not stale_addresses:
            logger.info("All addresses are fresh. Skipping fetch.")
            raise AirflowSkipException("All addresses are fresh; skipping task_fetch_token_security.")

        else:
            logger.info(f"{len(stale_addresses)} addresses need fetching. Proceeding...")
        
            # 4. Perform the fetch
            birdeye_generic_fetch_store(
                db_url=DB_URL,
                api_key=API_KEY,  # Ensure API_KEY is defined globally or passed appropriately
                addresses=stale_addresses,
                method_name="get_token_security",
                sdk_class="token",
                response_keys=["data"],
                table_name=table_name,
                identifier="address",
                address_key_in_method="address",
                method_params={},  # Adjust if the API requires additional parameters
                explode_columns=[],  # No columns to explode for this endpoint
                single_record=True  # Single record response as per YAML configuration
            )
            logger.info("Completed fetching security information.")

    except AirflowSkipException as e:
        logger.info(str(e))
        # Re-raise to let Airflow handle the skip
        raise
    except Exception as e:
        logger.error(f"Error fetching security information: {e}", exc_info=True)
        raise

    finally:
        # Ensure the database connection is closed
        db_manager.quit()