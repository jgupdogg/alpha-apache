# plugins/db_manager.py

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook


logger = logging.getLogger(__name__)

def remove_nul_chars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes NUL (0x00) characters from all string columns in the DataFrame.
    If a column does not have a `.str` accessor (for example if it’s not a plain Series),
    it will use a lambda to replace NUL characters.
    """
    # Only process object (string) type columns
    for col in df.select_dtypes(include=['object']).columns:
        # If the column has a 'str' accessor, use it
        if hasattr(df[col], "str"):
            df[col] = df[col].astype(str).str.replace('\x00', '', regex=False)
        else:
            # Otherwise, use apply() to convert each element to a string and replace NUL characters.
            df[col] = df[col].apply(lambda x: str(x).replace('\x00', ''))
    return df


# Pre-fetch connections to avoid doing it inside each task
def get_db_api_credentials():
    db_conn = BaseHook.get_connection("pipeline_db")
    api_conn = BaseHook.get_connection("birdseye_api")
    return db_conn.get_uri(), api_conn.password  # Assuming API key is stored as password

DB_URL, API_KEY = get_db_api_credentials()

class DBManager:
    def __init__(self, db_name:str = "pipeline_db"):
        # logger.info(f"Initializing DBManager with URL: {db_name}")
        
        db_url = f"postgresql+psycopg2://postgres:St0ck!adePG@localhost:5432/{db_name}"

        self.engine = create_engine(db_url, echo=False)
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        
    def cast_unsigned_to_signed(self, df: pd.DataFrame) -> pd.DataFrame:
        unsigned_int_types = ['uint8', 'uint16', 'uint32', 'uint64']
        for col in df.select_dtypes(include=unsigned_int_types).columns:
            original_dtype = df[col].dtype
            df[col] = df[col].astype('int64')
            # logger.debug(f"Column '{col}' casted from {original_dtype} to int64.")
        return df

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(schema=schema)
        return table_name in tables  
    
    
    def insert_raw_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Always appends data to the specified table, ignoring extra columns that are not in the
        table definition. For any column in the table not present in the DataFrame, a blank value is inserted.
        """
        try:
            # Normalize column names to lowercase
            df.columns = [str(col).lower() for col in df.columns]
            
            # Convert unsigned integer types to signed
            df = self.cast_unsigned_to_signed(df)
            
            # Remove NUL characters from string fields
            df = remove_nul_chars(df)
            
            # Replace NaN values with None so missing values are inserted as NULL
            df = df.where(pd.notnull(df), None)
            
            # Replace "<nil>" string values with None
            for col in df.columns:
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x == '<nil>' else x)
            
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            if table_name in existing_tables:
                # Get list of columns from the target table
                table_columns_info = inspector.get_columns(table_name)
                table_columns = [col_info['name'].lower() for col_info in table_columns_info]
                # logger.info(f"Target table '{table_name}' columns: {table_columns}")
                
                # For each target column not in df, add it with a default value (None)
                for col in table_columns:
                    if col not in df.columns:
                        df[col] = None
                
                # Restrict the DataFrame to only the target table columns
                df = df[table_columns]
            else:
                logger.info(f"Table '{table_name}' does not exist. It will be created with the DataFrame columns.")
            
            if_exists_mode = 'append' if table_name in existing_tables else 'replace'

            # Replace remaining None values with 0 for numeric columns
            # Note: You might want to be more selective about which columns get fillna(0)
            df = df.where(pd.notnull(df), None).fillna(0)
            
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists_mode,
                index=False
            )
            logger.info(f"Inserted {len(df)} records into '{table_name}' (if_exists='{if_exists_mode}').")
        except SQLAlchemyError as e:
            logger.error(f"Error inserting raw data into '{table_name}': {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error in insert_raw_dataframe: {e}", exc_info=True)
            
    def execute_query(self, query: str, params: dict = None, df: bool = False):
        """
        Executes a SQL query with parameters and returns the results.
        
        Parameters:
            query (str): The SQL query to execute.
            params (dict, optional): Dictionary of parameter values to bind to the query.
            df (bool): If True, returns the results as a pandas DataFrame. Defaults to False.
        
        Returns:
            List[Dict[str, Any]] or pd.DataFrame: Query results as a list of dictionaries or as a DataFrame.
        """
        try:
            with self.engine.connect() as connection:
                if params is not None:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                    
                # Fetch all results and convert to list of dicts
                if result.returns_rows:
                    results = [dict(row) for row in result]
                else:
                    results = []
                            
                if df:
                    return pd.DataFrame(results)
                return results

        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {query}\nParams: {params}\nException: {e}", exc_info=True)
            return pd.DataFrame() if df else []
        except Exception as e:
            logger.error(f"Unexpected error executing query: {query}\nParams: {params}\nException: {e}", exc_info=True)
            return pd.DataFrame() if df else []
        
    def is_table_fresh(self, table_name: str, freshness_hours: int, timestamp_column: str = "timestamp") -> bool:
        """
        Checks if the latest record in `table_name` has a timestamp within the last `freshness_hours`.
        Returns True if table is fresh, False otherwise.
        """
        if not self.table_exists(table_name):
            logger.info(f"Table '{table_name}' does not exist. Marking as stale.")
            return False

        # Build a query to get the max timestamp.
        query = f"SELECT MAX({timestamp_column}) as max_ts FROM {table_name};"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                max_ts = result['max_ts']  # could be None if table is empty

            if not max_ts:
                logger.info(f"Table '{table_name}' is empty. Marking as stale.")
                return False

            # Compare max timestamp to current time minus freshness_hours
            cutoff = datetime.utcnow() - timedelta(hours=freshness_hours)

            return max_ts >= cutoff

        except SQLAlchemyError as e:
            logger.error(f"Error checking table freshness for '{table_name}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking table freshness: {e}", exc_info=True)
            return False

    def filter_for_freshness(
        self,
        table_name: str,
        blockchain: str,
        addresses: List[str],
        freshness_hours: int,
        timestamp_column: str = "timestamp",
        block_column: str = "blockchain",
        address_column: str = "address"
    ) -> List[Dict[str, str]]:
        """
        Given a list of addresses on a single blockchain, returns only those
        whose max timestamp is older than `freshness_hours`. If an address is
        missing entirely from the table, that address is also considered "stale"
        (i.e., needing a fetch).

        :param table_name: Name of the DB table storing token security or similar data.
        :param blockchain: The blockchain name (e.g., "ethereum").
        :param addresses: List of address strings to check for freshness.
        :param freshness_hours: Threshold in hours; data older than this is stale.
        :param timestamp_column: Column holding timestamp information.
        :param block_column: Column holding blockchain name.
        :param address_column: Column holding address data.
        :return: List of addresses that need to be (re)fetched (i.e., stale).
        """
        # logger.info(f"Filtering for freshness in '{table_name}' for {len(addresses)} addresses on {blockchain}.")

        # Early exit if no addresses
        if not addresses:
            logger.warning("No addresses provided to filter_for_freshness.")
            return []

        # Check if the table exists
        if not self.table_exists(table_name):
            logger.info(f"Table '{table_name}' does not exist. All addresses are considered stale.")
            # Convert to list of dicts with blockchain and address
            return [{"blockchain": blockchain, "address": addr} for addr in addresses]

        # Convert list to tuple string for SQL IN clause
        # Make sure to handle quotes and escaping properly if these are hex addresses
        address_list_str = ", ".join(f"'{addr}'" for addr in addresses)

        # Build a query to get max timestamps per address
        query = f"""
            SELECT {address_column} AS address,
                   MAX({timestamp_column}) AS max_ts
            FROM {table_name}
            WHERE {block_column} = '{blockchain}'
              AND {address_column} IN ({address_list_str})
            GROUP BY 1
        """

        logger.info(f"Executing freshness filter query:\n{query}")

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(query), {"blockchain": blockchain}).fetchall()
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error while filtering for freshness: {e}", exc_info=True)
            # On error, assume all addresses are stale
            return [{"blockchain": blockchain, "address": addr} for addr in addresses]
        except Exception as e:
            logger.error(f"Unexpected error in filter_for_freshness: {e}", exc_info=True)
            # On error, assume all addresses are stale
            return [{"blockchain": blockchain, "address": addr} for addr in addresses]

        # Convert result rows to dict: {address: max_ts, ...}
        address_to_timestamp = {row["address"]: row["max_ts"] for row in rows if row["max_ts"]}

        # Determine which addresses are missing (i.e., not in the results)
        missing_addrs = set(addresses) - set(address_to_timestamp.keys())

        # Calculate cutoff
        cutoff = datetime.utcnow() - timedelta(hours=freshness_hours)

        # Evaluate addresses with existing timestamps
        stale_addrs = []
        for addr, ts in address_to_timestamp.items():
            if ts < cutoff:
                # stale
                stale_addrs.append(addr)

        # Combine missing addresses with stale addresses
        stale_addrs.extend(list(missing_addrs))

        logger.info(
            f"Out of {len(addresses)} addresses, {len(stale_addrs)} are stale or missing: {stale_addrs[:5]}..."
        )

        # Return as list of dicts
        return [{"blockchain": blockchain, "address": addr} for addr in stale_addrs]

    def quit(self):
        """
        Closes the database connection.
        """
        self.engine.dispose()
        self.Session.remove()
        logger.info("Database connection closed.")
