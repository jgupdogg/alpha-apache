# plugins/hooks/dune_hook.py
from airflow.hooks.base import BaseHook
from dune_client.client import DuneClient
from dune_client.types import QueryParameter
from dune_client.query import QueryBase
import pandas as pd
from typing import Union, List, Dict, Any
from datetime import datetime
import logging

#setup logging
logging.basicConfig(level=logging.INFO)


class DuneHook(BaseHook):
    def __init__(self, dune_conn_id='dune_default'):
        super().__init__()
        self.dune_conn_id = dune_conn_id
        self._client = None
        
    def get_conn(self):
        """
        Retrieves connection information and initializes the Dune client.
        
        Returns:
            DuneClient: Initialized Dune Analytics client with proper timeout
        """
        if self._client is not None:
            return self._client
            
        conn = self.get_connection(self.dune_conn_id)
        api_key = conn.password or conn.extra_dejson.get('api_key')
        
        if not api_key:
            raise ValueError(f"No API key found in connection {self.dune_conn_id}")
        
        # Create client with a 120-second (2 minute) timeout
        self._client = DuneClient(api_key)
        self._client.request_timeout = 120  # Set timeout to 120 seconds
        return self._client
    
                
    # Add timeout parameter to the DuneClient methods in DuneHook
    def get_traders_swap_history_by_tokens(self, 
                                    trader_addresses: str,
                                    token_addresses: str,
                                    limit: int = 50,
                                    timeout: int = 120) -> pd.DataFrame:
        """Get aggregated swap history for a specific trader and token.
        
        Args:
            trader_addresses: Trader wallet address
            token_address: Token mint address to filter for (input or output)
            limit: Maximum number of results to return
            timeout: Timeout in seconds for the request (default: 120)
            
        Returns:
            DataFrame containing aggregated swap history for the trader and specified token
        """
        query = QueryBase(
            name="swap_history_by_trader_and_token",
            query_id=4884715,  # Assuming same query ID, update if needed
            params=[
                QueryParameter.text_type(name="token_addresses", value=token_addresses),
                QueryParameter.text_type(name="trader_addresses", value=trader_addresses),
                QueryParameter.number_type(name="limit", value=limit)
            ],
        )
        
        # Add timeout to the request
        client = self.get_conn()
        client.request_timeout = timeout
        return client.run_query_dataframe(query)

    def get_trader_pnl(self, 
                  trader_addresses: str,
                  num_days: int = 30,
                  timeout: int = 120) -> pd.DataFrame:
        """Get PNL (Profit and Loss) metrics for specified Solana traders.
        
        Args:
            trader_addresses: Comma-separated list of trader wallet addresses
            num_days: Number of days to look back for trading history
            timeout: Timeout in seconds for the request (default: 120)
            
        Returns:
            DataFrame containing PNL metrics for the specified traders
        """
        query = QueryBase(
            name="trader_pnl",
            query_id=4911507,
            params=[
                QueryParameter.text_type(name="trader_addresses", value=trader_addresses),
                QueryParameter.number_type(name="num_days", value=num_days)
            ],
        )
        
        # Add timeout to the request
        client = self.get_conn()
        client.request_timeout = timeout
        return client.run_query_dataframe(query)

    def get_token_sellers_grouped(self,
                             token_address: str,
                             start_date: datetime,
                             end_date: datetime,
                             min_usd: float = 1000,
                             count: int = 10,
                             timeout: int = 120) -> pd.DataFrame:
        """Get token sellers grouped by signer with transaction counts and totals.
        
        Args:
            token_address: Token mint address
            start_date: Start date for the query
            end_date: End date for the query
            min_usd: Minimum USD value per transaction and total
            count: Minimum number of transactions
            timeout: Timeout in seconds for the request (default: 120)
            
        Returns:
            DataFrame containing sellers with transaction counts and totals
        """
        # Format dates for Dune
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        query = QueryBase(
            name="token_sellers_grouped",
            query_id=4885555, 
            params=[
                QueryParameter.text_type(name="addresses", value=token_address),
                QueryParameter.text_type(name="start_date", value=start_date_str),
                QueryParameter.text_type(name="end_date", value=end_date_str),
                QueryParameter.number_type(name="count", value=count),
                QueryParameter.number_type(name="min_usd", value=min_usd)
            ],
        )
        
        # Add timeout to the request
        client = self.get_conn()
        client.request_timeout = timeout
        return client.run_query_dataframe(query)
    
    def get_recent_token_seller_pnl(self,
                               token_address: str,
                               start_time: str,
                               end_time: str,
                               min_usd: float = 10.0,
                               num_traders: int = 10,
                               num_trader_txns: int = 50,
                               timeout: int = 180) -> pd.DataFrame:
        """Get PNL analysis for recent sellers of a specific token.
        
        Args:
            token_address: Token mint address to analyze
            start_time: Start time for seller identification (format: 'YYYY-MM-DD HH:MM')
            end_time: End time for seller identification (format: 'YYYY-MM-DD HH:MM')
            min_usd: Minimum USD value for trades to be considered
            num_traders: Maximum number of traders to analyze
            num_trader_txns: Maximum number of transactions to consider per trader
            timeout: Timeout in seconds for the request (default: 180)
            
        Returns:
            DataFrame containing PNL analysis for recent sellers of the specified token
        """
        
        # log params
        print(f"Token Address: {token_address}")
        print(f"Start Time: {start_time}")
        print(f"End Time: {end_time}")
        print(f"Min USD: {min_usd}")
        print(f"Num Traders: {num_traders}")
        print(f"Num Trader Txns: {num_trader_txns}")
        
        query = QueryBase(
            name="recent_token_seller_pnl",
            query_id=4911230,
            params=[
                QueryParameter.text_type(name="token_address", value=token_address),
                QueryParameter.text_type(name="start_time", value=start_time),
                QueryParameter.text_type(name="end_time", value=end_time),
                QueryParameter.number_type(name="min_usd", value=min_usd),
                QueryParameter.number_type(name="num_traders", value=num_traders),
                QueryParameter.number_type(name="num_trader_txns", value=num_trader_txns)
            ],
        )
        
        # Set timeout for the request
        client = self.get_conn()
        client.request_timeout = timeout
        return client.run_query_dataframe(query)

    def run_custom_query(self, query_id: int, params: List[Dict[str, Any]], 
                        name: str = "custom_query", timeout: int = 120) -> pd.DataFrame:
        """Run a custom Dune query with specified parameters.
        
        Args:
            query_id: The Dune query ID
            params: List of parameter dictionaries, each containing:
                    {'type': 'text|number|date|enum', 'name': param_name, 'value': param_value}
            name: Optional name for the query
            timeout: Timeout in seconds for the request (default: 120)
            
        Returns:
            DataFrame containing query results
        """
        # Convert the parameter dictionaries to QueryParameter objects
        query_params = []
        for param in params:
            if param['type'] == 'text':
                query_params.append(QueryParameter.text_type(name=param['name'], value=param['value']))
            elif param['type'] == 'number':
                query_params.append(QueryParameter.number_type(name=param['name'], value=param['value']))
            elif param['type'] == 'date':
                query_params.append(QueryParameter.date_type(name=param['name'], value=param['value']))
            elif param['type'] == 'enum':
                query_params.append(QueryParameter.enum_type(name=param['name'], value=param['value']))
        
        query = QueryBase(
            name=name,
            query_id=query_id,
            params=query_params
        )
        
        # Add timeout to the request
        client = self.get_conn()
        client.request_timeout = timeout
        return client.run_query_dataframe(query)
    
    def execute_query(self, query_id: int, parameters: dict, timeout: int = 120) -> pd.DataFrame:
        """
        Execute a Dune Analytics query with the specified parameters.
        
        Args:
            query_id (int): Dune query ID to execute
            parameters (dict): Dictionary of parameter names and values
            timeout (int): Timeout in seconds for the request (default: 120)
            
        Returns:
            pd.DataFrame: Results of the query as a DataFrame
        """
        # Convert parameters dictionary into QueryParameter objects
        params = []
        for name, value in parameters.items():
            if isinstance(value, (int, float)):
                params.append(QueryParameter.number_type(name=name, value=value))
            elif isinstance(value, str):
                params.append(QueryParameter.text_type(name=name, value=value))
            elif isinstance(value, list):
                # Handle list parameters (assumes list of strings)
                params.append(QueryParameter.text_array_type(name=name, value=value))
            elif isinstance(value, bool):
                params.append(QueryParameter.boolean_type(name=name, value=value))
            else:
                # Default to string for other types
                params.append(QueryParameter.text_type(name=name, value=str(value)))
        
        # Create the query object
        query = QueryBase(
            name=f"dune_query_{query_id}",
            query_id=query_id,
            params=params,
        )
        
        # Configure timeout and execute query
        client = self.get_conn()
        client.request_timeout = timeout
        
        try:
            return client.run_query_dataframe(query)
        except Exception as e:
            logging.error(f"Error executing Dune query {query_id}: {e}")
            # Return empty DataFrame on error
            return pd.DataFrame()
    
    def run_custom_query(self, query_id: int, params: List[Dict[str, Any]], 
                        name: str = "custom_query") -> pd.DataFrame:
        """Run a custom Dune query with specified parameters.
        
        Args:
            query_id: The Dune query ID
            params: List of parameter dictionaries, each containing:
                    {'type': 'text|number|date|enum', 'name': param_name, 'value': param_value}
            name: Optional name for the query
            
        Returns:
            DataFrame containing query results
        """
        # Convert the parameter dictionaries to QueryParameter objects
        query_params = []
        for param in params:
            if param['type'] == 'text':
                query_params.append(QueryParameter.text_type(name=param['name'], value=param['value']))
            elif param['type'] == 'number':
                query_params.append(QueryParameter.number_type(name=param['name'], value=param['value']))
            elif param['type'] == 'date':
                query_params.append(QueryParameter.date_type(name=param['name'], value=param['value']))
            elif param['type'] == 'enum':
                query_params.append(QueryParameter.enum_type(name=param['name'], value=param['value']))
        
        query = QueryBase(
            name=name,
            query_id=query_id,
            params=query_params
        )
        
        return self.get_conn().run_query_dataframe(query)