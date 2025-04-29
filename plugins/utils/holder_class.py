import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime, timezone, UTC
import time
from hooks.dune_hook import DuneHook

# Configure logging
logger = logging.getLogger("HolderAnalyzer")

class Holder:
    """
    A simplified class to manage and analyze holder (wallet) information on the Solana blockchain.
    Integrates with DBManager and BirdEyeSDK to fetch and store wallet data.
    """
    
    def __init__(self, address: str, blockchain: str = "solana", 
                 db_name: str = "solana", api_key: Optional[str] = None, 
                 db_manager=None, birdeye_sdk=None):
        """
        Initialize a Holder object.
        
        Args:
            address (str): The wallet address
            blockchain (str): The blockchain name (default: "solana")
            db_name (str): The database name to use (default: "solana")
            api_key (str, optional): BirdEye API key. If None, will attempt to get from environment
            db_manager: Optional existing DBManager instance 
            birdeye_sdk: Optional existing BirdEyeSDK instance
        """
        self.address = address
        self.blockchain = blockchain
        self.db_name = db_name
        self.metrics_table = "trader_metrics"
        self.txn_table = "wallet_trade_history" 

        
        # Use provided instances or create new ones
        if db_manager:
            self.db = db_manager
        else:
            from db_manager import DBManager
            self.db = DBManager(db_name=self.db_name)
        
        if birdeye_sdk:
            self.birdeye = birdeye_sdk
        else:
            from utils.crypto.birdseye_sdk import BirdEyeSDK
            self.birdeye = BirdEyeSDK(chain=self.blockchain)
    
    def get_token_list(self, force_refresh: bool = True) -> pd.DataFrame:
        """
        Get list of tokens held by this wallet address from BirdEye API.
        
        Args:
            force_refresh (bool): Force refresh from API even if data exists in DB
            
        Returns:
            pd.DataFrame: DataFrame of tokens held by this wallet
        """
        if not force_refresh:
            # Check if we have recent data in the database (within last hour)
            query = f"""
            SELECT * FROM wallet_token_list 
            WHERE wallet_address = '{self.address}' 
            AND blockchain = '{self.blockchain}'
            AND scraped_at > NOW() - INTERVAL '1 hour'
            ORDER BY scraped_at DESC
            """
            
            try:
                result = self.db.execute_query(query, df=True)
                
                if not result.empty:
                    logger.info(f"Retrieved token list for wallet {self.address} from database")
                    return result
            except Exception as e:
                logger.info(f"No existing token list data found: {e}")
        
        try:
            # Ensure table exists
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS wallet_token_list (
                wallet_address TEXT NOT NULL,
                blockchain TEXT NOT NULL,
                token_address TEXT NOT NULL,
                token_symbol TEXT,
                token_name TEXT,
                amount NUMERIC,
                ui_amount NUMERIC,
                decimals INTEGER,
                price_usd NUMERIC,
                value_usd NUMERIC,
                scraped_at TIMESTAMP NOT NULL,
                PRIMARY KEY (wallet_address, blockchain, token_address, scraped_at)
            )
            """)
            
            # Call API
            response = self.birdeye.wallet.get_token_list(wallet=self.address)
            
            if response and 'data' in response and 'items' in response['data'] and response.get('success', False):
                # Extract token list from the response
                tokens_data = response['data']['items']
                
                if not tokens_data:
                    logger.warning(f"No tokens found for wallet {self.address}")
                    return pd.DataFrame()
                
                # Prepare data for database
                current_time = datetime.now(timezone.utc)
                records = []
                
                for token in tokens_data:
                    record = {
                        'wallet_address': self.address,
                        'blockchain': self.blockchain,
                        'token_address': token.get('address'),
                        'token_symbol': token.get('symbol'),
                        'token_name': token.get('name'),
                        'amount': token.get('amount'),
                        'ui_amount': token.get('ui_amount'),
                        'decimals': token.get('decimals'),
                        'price_usd': token.get('price_usd'),
                        'value_usd': token.get('value_usd'),
                        'scraped_at': current_time
                    }
                    records.append(record)
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                
                # Store in database
                self.db.insert_raw_dataframe('wallet_token_list', df)
                logger.info(f"Retrieved and stored {len(records)} tokens for wallet {self.address}")
                
                return df
            else:
                logger.warning(f"Failed to get token list from BirdEye API for {self.address}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting token list for wallet {self.address}: {e}")
            return pd.DataFrame()
    
    def get_trade_history(self, tx_type: str = "swap", total_trades: int = 100, 
                         before_time: int = 0, after_time: int = 0,
                         force_refresh: bool = True, sleep_between_calls: float = 0.5) -> pd.DataFrame:
        """
        Get trading history for this wallet address from BirdEye API, handling pagination.
        
        Args:
            tx_type (str): Type of transaction (default: "swap")
            total_trades (int): Total number of trades to retrieve, will make multiple API calls if needed
            before_time (int): Unix timestamp to get trades before (default: 0, no limit)
            after_time (int): Unix timestamp to get trades after (default: 0, no limit)
            force_refresh (bool): Force refresh from API even if data exists in DB
            sleep_between_calls (float): Time to sleep between API calls to avoid rate limiting
            
        Returns:
            pd.DataFrame: DataFrame of trades for this wallet
        """
        if not force_refresh:
            # Build query based on time parameters
            time_conditions = []
            if before_time > 0:
                time_conditions.append(f"block_unix_time < {before_time}")
            if after_time > 0:
                time_conditions.append(f"block_unix_time > {after_time}")
            
            time_clause = " AND ".join(time_conditions) if time_conditions else ""
            if time_clause:
                time_clause = f" AND {time_clause}"
            
            query = f"""
            SELECT * FROM wallet_trade_history 
            WHERE wallet_address = '{self.address}' 
            AND blockchain = '{self.blockchain}'
            AND tx_type = '{tx_type}'{time_clause}
            ORDER BY block_unix_time DESC
            LIMIT {total_trades}
            """
            
            try:
                result = self.db.execute_query(query, df=True)
                
                if not result.empty and len(result) >= total_trades:
                    logger.info(f"Retrieved {len(result)} trades for wallet {self.address} from database")
                    return result
            except Exception as e:
                logger.info(f"No existing trade history data found: {e}")
        
        try:
            # Ensure table exists
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS wallet_trade_history (
                wallet_address TEXT NOT NULL,
                blockchain TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                block_unix_time BIGINT,
                tx_type TEXT,
                source TEXT,
                owner TEXT,
                
                quote_symbol TEXT,
                quote_decimals INTEGER,
                quote_address TEXT,
                quote_amount NUMERIC,
                quote_ui_amount NUMERIC,
                quote_price NUMERIC,
                quote_type TEXT,
                quote_type_swap TEXT,
                
                base_symbol TEXT,
                base_decimals INTEGER,
                base_address TEXT,
                base_amount NUMERIC,
                base_ui_amount NUMERIC,
                base_price NUMERIC,
                base_type TEXT,
                base_type_swap TEXT,
                
                fetched_at TIMESTAMP NOT NULL,
                PRIMARY KEY (wallet_address, blockchain, tx_hash)
            )
            """)
            
            # Initialize variables for pagination
            offset = 0
            limit_per_call = min(100, total_trades)  # API limit is 100 per call
            all_records = []
            has_next = True
            
            while has_next and len(all_records) < total_trades:
                # Calculate remaining trades to fetch
                remaining = total_trades - len(all_records)
                current_limit = min(limit_per_call, remaining)
                
                logger.info(f"Fetching trades for {self.address} (offset: {offset}, limit: {current_limit})")
                
                # Call API
                response = self.birdeye.trader.get_trades_seek_by_time(
                    address=self.address,
                    offset=offset,
                    limit=current_limit,
                    tx_type=tx_type,
                    before_time=before_time,
                    after_time=after_time
                )
                
                if not (response and 'data' in response and 'items' in response['data'] and response.get('success', False)):
                    logger.warning(f"Failed to get trade history from BirdEye API for {self.address} at offset {offset}")
                    break
                
                # Extract trades from the response
                trades_data = response['data']['items']
                has_next = response['data'].get('hasNext', False)
                
                if not trades_data:
                    logger.info(f"No more trades found for wallet {self.address} at offset {offset}")
                    break
                
                # Process fetched trades
                for trade in trades_data:
                    record = {
                        'wallet_address': self.address,
                        'blockchain': self.blockchain,
                        'tx_hash': trade.get('tx_hash'),
                        'block_unix_time': trade.get('block_unix_time'),
                        'tx_type': trade.get('tx_type'),
                        'source': trade.get('source'),
                        'owner': trade.get('owner'),
                        
                        # Quote (from) token details
                        'quote_symbol': trade.get('quote', {}).get('symbol'),
                        'quote_decimals': trade.get('quote', {}).get('decimals'),
                        'quote_address': trade.get('quote', {}).get('address'),
                        'quote_amount': trade.get('quote', {}).get('amount'),
                        'quote_ui_amount': trade.get('quote', {}).get('ui_amount'),
                        'quote_price': trade.get('quote', {}).get('price'),
                        'quote_type': trade.get('quote', {}).get('type'),
                        'quote_type_swap': trade.get('quote', {}).get('type_swap'),
                        
                        # Base (to) token details
                        'base_symbol': trade.get('base', {}).get('symbol'),
                        'base_decimals': trade.get('base', {}).get('decimals'),
                        'base_address': trade.get('base', {}).get('address'),
                        'base_amount': trade.get('base', {}).get('amount'),
                        'base_ui_amount': trade.get('base', {}).get('ui_amount'),
                        'base_price': trade.get('base', {}).get('price'),
                        'base_type': trade.get('base', {}).get('type'),
                        'base_type_swap': trade.get('base', {}).get('type_swap'),
                    }
                    all_records.append(record)
                
                # Update offset for next call
                offset += len(trades_data)
                
                # Sleep to avoid rate limiting
                if has_next and len(all_records) < total_trades:
                    time.sleep(sleep_between_calls)
            
            # Add the same fetched_at timestamp to all records
            fetched_at = datetime.now(timezone.utc)
            for record in all_records:
                record['fetched_at'] = fetched_at
            
            if not all_records:
                logger.warning(f"No trades found for wallet {self.address}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(all_records)
            
            # Store in database (handling duplicates)
            for record in all_records:
                try:
                    # Insert with ON CONFLICT DO UPDATE to handle duplicates
                    # This will update existing records with new data
                    insert_query = f"""
                    INSERT INTO wallet_trade_history 
                    (wallet_address, blockchain, tx_hash, block_unix_time, tx_type, source, owner,
                     quote_symbol, quote_decimals, quote_address, quote_amount, quote_ui_amount, 
                     quote_price, quote_type, quote_type_swap,
                     base_symbol, base_decimals, base_address, base_amount, base_ui_amount, 
                     base_price, base_type, base_type_swap, fetched_at)
                    VALUES 
                    ('{record['wallet_address']}', '{record['blockchain']}', '{record['tx_hash']}',
                     {record['block_unix_time'] or 'NULL'}, 
                     '{record['tx_type'] or ''}', '{record['source'] or ''}', '{record['owner'] or ''}',
                     '{record['quote_symbol'] or ''}', {record['quote_decimals'] or 'NULL'}, 
                     '{record['quote_address'] or ''}', {record['quote_amount'] or 'NULL'}, 
                     {record['quote_ui_amount'] or 'NULL'}, {record['quote_price'] or 'NULL'},
                     '{record['quote_type'] or ''}', '{record['quote_type_swap'] or ''}',
                     '{record['base_symbol'] or ''}', {record['base_decimals'] or 'NULL'}, 
                     '{record['base_address'] or ''}', {record['base_amount'] or 'NULL'}, 
                     {record['base_ui_amount'] or 'NULL'}, {record['base_price'] or 'NULL'},
                     '{record['base_type'] or ''}', '{record['base_type_swap'] or ''}',
                     '{record['fetched_at']}')
                    ON CONFLICT (wallet_address, blockchain, tx_hash) 
                    DO UPDATE SET
                     fetched_at = '{record['fetched_at']}'
                    """
                    self.db.execute_query(insert_query)
                except Exception as e:
                    logger.debug(f"Error inserting trade {record.get('tx_hash')}: {e}")
            
            logger.info(f"Retrieved and stored {len(all_records)} trades for wallet {self.address}")
            
            return df
                
        except Exception as e:
            logger.error(f"Error getting trade history for wallet {self.address}: {e}")
            return pd.DataFrame()
    
    def get_transactions(self, num_txns: int = 100, force_update: bool = False) -> pd.DataFrame:
        """
        Retrieve transactions for this address and store them in the database.
        Checks if we already have recent data (within 24 hours) before fetching.
        
        Parameters:
            num_txns (int): Number of transactions to retrieve
            force_update (bool): If True, bypass the 24-hour threshold check
            
        Returns:
            pd.DataFrame: DataFrame containing the transactions
        """
        # Check if we already have recent transactions
        query = f"""
        SELECT MAX(block_unix_time) as latest_time
        FROM wallet_trade_history
        WHERE wallet_address = '{self.address}'
        """
        result = self.db.execute_query(query)
        
        latest_time = None
        if result and result[0] and 'latest_time' in result[0]:
            latest_time = result[0]['latest_time']
        
        current_time = int(datetime.now(timezone.utc).timestamp())
        time_threshold = 86400  # 24 hours in seconds
        
        # Default after_time will be 0 (get all transactions)
        after_time = 0
        
        if latest_time:
            # Calculate how long ago the last transaction was
            time_since_last_tx = current_time - latest_time
            
            # If we have recent data and not forcing update, just return DB data
            if time_since_last_tx < time_threshold and not force_update:
                logger.info(f"Using existing data for {self.address}. Last update was {time_since_last_tx / 3600:.2f} hours ago.")
                query = f"""
                SELECT * FROM {self.txn_table}
                WHERE wallet_address = '{self.address}'
                AND blockchain = '{self.blockchain}'
                ORDER BY block_unix_time DESC
                LIMIT {num_txns}
                """
                return self.db.execute_query(query, df=True)
            
            # Otherwise, set after_time to only get new transactions
            after_time = latest_time
        
        # Use the existing get_trade_history method to fetch transactions
        return self.get_trade_history(
            tx_type="swap",
            total_trades=num_txns,
            after_time=after_time,
            force_refresh=force_update,
            sleep_between_calls=0.5
        )
        
    def calculate_pnl(self, time_period: str = 'all') -> Dict[str, Any]:
        """
        Calculate PnL metrics for this address and store in the metrics table.
        
        Parameters:
            time_period (str): Time period for calculation: 'all', 'day', 'week', 'month', 'year'
            
        Returns:
            Dict[str, Any]: Dictionary containing PnL metrics
        """
        # Ensure trader_metrics table exists
        self.db.execute_query("""
        CREATE TABLE IF NOT EXISTS trader_metrics (
            address TEXT NOT NULL,
            blockchain TEXT NOT NULL,
            time_period TEXT NOT NULL,
            timestamp BIGINT NOT NULL,
            pnl NUMERIC,
            unrealized_pnl NUMERIC,
            total_pnl NUMERIC,
            volume NUMERIC,
            trade_count INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            win_rate NUMERIC,
            PRIMARY KEY (address, blockchain, time_period, timestamp)
        )
        """)
        
        # Define time filters based on the period
        current_time = int(datetime.now(timezone.utc).timestamp())
        time_filters = {
            'all': 0,
            'day': current_time - 86400,
            'week': current_time - 86400 * 7,
            'month': current_time - 86400 * 30,
            'year': current_time - 86400 * 365
        }
        
        after_time = time_filters.get(time_period.lower(), 0)
        
        # Query transactions for the specified period
        query = f"""
        SELECT * FROM wallet_trade_history
        WHERE wallet_address = '{self.address}'
        AND blockchain = '{self.blockchain}'
        AND block_unix_time >= {after_time}
        ORDER BY block_unix_time ASC
        """
        
        txn_df = self.db.execute_query(query, df=True)
        
        if txn_df.empty:
            logger.info(f"No transactions found for {self.address} in the {time_period} period")
            # Create empty metrics record
            metrics = {
                'address': self.address,
                'blockchain': self.blockchain,
                'time_period': time_period,
                'timestamp': current_time,
                'pnl': 0,
                'unrealized_pnl': 0,
                'total_pnl': 0,
                'volume': 0,
                'trade_count': 0,
                'win_count': 0,
                'loss_count': 0,
                'win_rate': 0
            }
            
            # Store metrics in the database
            metrics_df = pd.DataFrame([metrics])
            self.db.insert_raw_dataframe('trader_metrics', metrics_df)
            
            return metrics
        
        # Calculate metrics using FIFO method
        metrics = self._calculate_metrics_fifo(txn_df)
        
        # Add address and time period to metrics
        metrics['address'] = self.address
        metrics['blockchain'] = self.blockchain
        metrics['time_period'] = time_period
        metrics['timestamp'] = current_time
        
        # Store metrics in the database
        metrics_df = pd.DataFrame([metrics])
        self.db.insert_raw_dataframe('trader_metrics', metrics_df)
        
        return metrics
    
    def calculate_pnl(self, time_period: str = 'all') -> Dict[str, Any]:
        """
        Calculate PnL metrics for this address and store in the metrics table.
        Focuses only on realized gains using transaction data.
        
        Parameters:
            time_period (str): Time period for calculation: 'all', 'day', 'week', 'month', 'year'
            
        Returns:
            Dict[str, Any]: Dictionary containing PnL metrics
        """
        # Ensure trader_metrics table exists
        self.db.execute_query("""
        CREATE TABLE IF NOT EXISTS trader_metrics (
            address TEXT NOT NULL,
            blockchain TEXT NOT NULL,
            time_period TEXT NOT NULL,
            timestamp BIGINT NOT NULL,
            realized_pnl NUMERIC,
            volume NUMERIC,
            trade_count INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            win_rate NUMERIC,
            PRIMARY KEY (address, blockchain, time_period, timestamp)
        )
        """)
        
        # Define time filters based on the period
        current_time = int(datetime.now(timezone.utc).timestamp())
        time_filters = {
            'all': 0,
            'day': current_time - 86400,
            'week': current_time - 86400 * 7,
            'month': current_time - 86400 * 30,
            'year': current_time - 86400 * 365
        }
        
        after_time = time_filters.get(time_period.lower(), 0)
        
        # Query transactions for the specified period
        query = f"""
        SELECT * FROM {self.txn_table}
        WHERE wallet_address = '{self.address}'
        AND blockchain = '{self.blockchain}'
        AND block_unix_time >= {after_time}
        ORDER BY block_unix_time ASC
        """
        
        txn_df = self.db.execute_query(query, df=True)
        
        if txn_df.empty:
            logger.info(f"No transactions found for {self.address} in the {time_period} period")
            # Create empty metrics record
            metrics = {
                'address': self.address,
                'blockchain': self.blockchain,
                'time_period': time_period,
                'timestamp': current_time,
                'realized_pnl': 0,
                'volume': 0,
                'trade_count': 0,
                'win_count': 0,
                'loss_count': 0,
                'win_rate': 0
            }
            
            # Store metrics in the database
            metrics_df = pd.DataFrame([metrics])
            self.db.insert_raw_dataframe(self.metrics_table, metrics_df)
            
            return metrics
        
        # Prepare the dataframe for PnL calculation
        # Map DB column names to the ones expected by process_trader_trades
        txn_df = self._prepare_trades_for_pnl(txn_df)
        
        # Calculate PnL using transaction data
        portfolio, realized_pnl, pnl_history = self._process_trader_trades(txn_df)
        
        # Calculate metrics based on results
        metrics = self._calculate_metrics_from_pnl(portfolio, realized_pnl, pnl_history, txn_df)
        
        # Add address and time period to metrics
        metrics['address'] = self.address
        metrics['blockchain'] = self.blockchain
        metrics['time_period'] = time_period
        metrics['timestamp'] = current_time
        
        # Store metrics in the database
        metrics_df = pd.DataFrame([metrics])
        self.db.insert_raw_dataframe(self.metrics_table, metrics_df)
        
        return metrics
    
    def calculate_pnl(self, time_period: str = 'all') -> Dict[str, Any]:
        """
        Calculate PnL metrics for this address and store in the metrics table.
        Includes rate of return calculations for trader comparison.
        
        Parameters:
            time_period (str): Time period for calculation: 'all', 'day', 'week', 'month', 'year'
            
        Returns:
            Dict[str, Any]: Dictionary containing PnL metrics
        """
        # Ensure trader_metrics table exists with the new columns
        self.db.execute_query("""
        CREATE TABLE IF NOT EXISTS trader_metrics (
            address TEXT NOT NULL,
            blockchain TEXT NOT NULL,
            time_period TEXT NOT NULL,
            timestamp BIGINT NOT NULL,
            realized_pnl NUMERIC,
            volume NUMERIC,
            trade_count INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            win_rate NUMERIC,
            roi_on_volume NUMERIC,
            avg_profit_per_trade NUMERIC,
            PRIMARY KEY (address, blockchain, time_period, timestamp)
        )
        """)
        
        # Define time filters based on the period
        current_time = int(datetime.now(timezone.utc).timestamp())
        time_filters = {
            'all': 0,
            'day': current_time - 86400,
            'week': current_time - 86400 * 7,
            'month': current_time - 86400 * 30,
            'year': current_time - 86400 * 365
        }
        
        after_time = time_filters.get(time_period.lower(), 0)
        
        # Query transactions for the specified period
        query = f"""
        SELECT * FROM {self.txn_table}
        WHERE wallet_address = '{self.address}'
        AND blockchain = '{self.blockchain}'
        AND block_unix_time >= {after_time}
        ORDER BY block_unix_time ASC
        """
        
        txn_df = self.db.execute_query(query, df=True)
        
        if txn_df.empty:
            logger.info(f"No transactions found for {self.address} in the {time_period} period")
            # Create empty metrics record
            metrics = {
                'address': self.address,
                'blockchain': self.blockchain,
                'time_period': time_period,
                'timestamp': current_time,
                'realized_pnl': 0,
                'volume': 0,
                'trade_count': 0,
                'win_count': 0,
                'loss_count': 0,
                'win_rate': 0
            }
            
            # Store metrics in the database
            metrics_df = pd.DataFrame([metrics])
            self.db.insert_raw_dataframe(self.metrics_table, metrics_df)
            
            return metrics
        
        # Prepare the dataframe for PnL calculation
        # Map DB column names to the ones expected by process_trader_trades
        txn_df = self._prepare_trades_for_pnl(txn_df)
        
        # Calculate PnL using transaction data
        portfolio, realized_pnl, pnl_history = self._process_trader_trades(txn_df)
        
        # After calculating basic metrics, add rate of return metrics
        metrics = self._calculate_metrics_from_pnl(portfolio, realized_pnl, trade_pnl_df, txn_df)
        
        # Calculate ROI metrics
        if metrics['volume'] > 0:
            # ROI on volume: PnL as a percentage of total volume traded
            metrics['roi_on_volume'] = metrics['realized_pnl'] / metrics['volume']
        else:
            metrics['roi_on_volume'] = 0
        
        # Average profit per trade
        if metrics['trade_count'] > 0:
            metrics['avg_profit_per_trade'] = metrics['realized_pnl'] / metrics['trade_count']
        else:
            metrics['avg_profit_per_trade'] = 0
        
        # Add address and time period to metrics
        metrics['address'] = self.address
        metrics['blockchain'] = self.blockchain
        metrics['time_period'] = time_period
        metrics['timestamp'] = current_time
        
        # Store metrics in the database
        metrics_df = pd.DataFrame([metrics])
        self.db.insert_raw_dataframe(self.metrics_table, metrics_df)
        
        return metrics


    def _prepare_trades_for_pnl(self, txn_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare transaction data for PnL calculation by mapping column names.
        """
        # Make a copy to avoid modifying the original
        df = txn_df.copy()
        
        # Rename columns to match the expected format for process_trader_trades
        column_mapping = {
            'block_unix_time': 'timestamp',
            'quote_symbol': 'quote.symbol',
            'quote_decimals': 'quote.decimals',
            'quote_address': 'quote.address',
            'quote_amount': 'quote.amount',
            'quote_ui_amount': 'quote.ui_amount',
            'quote_price': 'quote.price',
            'quote_nearest_price': 'quote.nearest_price',
            'quote_type': 'quote.type',
            'quote_type_swap': 'quote.type_swap',
            'base_symbol': 'base.symbol',
            'base_decimals': 'base.decimals',
            'base_address': 'base.address',
            'base_amount': 'base.amount',
            'base_ui_amount': 'base.ui_amount',
            'base_price': 'base.price', 
            'base_nearest_price': 'base.nearest_price',
            'base_type': 'base.type',
            'base_type_swap': 'base.type_swap'
        }
        
        # Apply the column renaming
        df = df.rename(columns=column_mapping)
        
        # Ensure required columns exist and have valid data
        for col in column_mapping.values():
            if col not in df.columns:
                df[col] = None
        
        # Fill missing prices with nearest_price where available
        df['quote.price'] = df['quote.price'].fillna(df['quote.nearest_price'])
        df['base.price'] = df['base.price'].fillna(df['base.nearest_price'])
        
        # Replace NaN values with safe defaults
        for col in df.columns:
            if 'price' in col:
                # Default to 0 for missing prices - will be handled in the PnL calculation
                df[col] = df[col].fillna(0)
            elif 'ui_amount' in col:
                # Default to 0 for missing amounts
                df[col] = df[col].fillna(0)
        
        return df
    
    def _process_trader_trades(self, trader_df):
        """
        Calculate realized PnL by replaying trades and tracking portfolio state.
        Correctly tracks both profits and losses.
        """
        # Ensure the trades are processed in time order
        trader_df = trader_df.sort_values('timestamp')
        
        # Portfolio tracks token holdings and their cost basis
        portfolio = {}
        realized_pnl = 0.0
        pnl_history = []
        trade_pnl_history = []
        
        for idx, row in trader_df.iterrows():
            trade_time = row['timestamp']
            trade_realized_pnl = 0.0
            
            try:
                # Get asset details - regardless of type_swap
                base_token_addr = row['base.address']
                base_token_symbol = row['base.symbol']
                base_ui_amount = float(row['base.ui_amount']) if pd.notna(row['base.ui_amount']) else 0
                base_price = float(row['base.price'] or row['base.nearest_price']) if pd.notna(row['base.price'] or row['base.nearest_price']) else 0
                
                quote_token_addr = row['quote.address']
                quote_token_symbol = row['quote.symbol']
                quote_ui_amount = float(row['quote.ui_amount']) if pd.notna(row['quote.ui_amount']) else 0
                quote_price = float(row['quote.price'] or row['quote.nearest_price']) if pd.notna(row['quote.price'] or row['quote.nearest_price']) else 0
                
                # Determine which asset is being sold vs bought based on type_swap
                if row['quote.type_swap'] == 'from':
                    # SELLING quote to BUY base
                    sell_token_addr = quote_token_addr
                    sell_token_symbol = quote_token_symbol
                    sell_qty = quote_ui_amount
                    sell_price = quote_price
                    
                    buy_token_addr = base_token_addr
                    buy_token_symbol = base_token_symbol
                    buy_qty = base_ui_amount
                    
                elif row['quote.type_swap'] == 'to':
                    # SELLING base to BUY quote
                    sell_token_addr = base_token_addr
                    sell_token_symbol = base_token_symbol
                    sell_qty = base_ui_amount
                    sell_price = base_price
                    
                    buy_token_addr = quote_token_addr
                    buy_token_symbol = quote_token_symbol
                    buy_qty = quote_ui_amount
                    
                else:
                    logger.warning(f"Unknown trade type for row {idx} at {trade_time}")
                    continue
                
                # Skip trades with missing essential data
                if sell_qty <= 0 or buy_qty <= 0 or not sell_token_addr or not buy_token_addr:
                    logger.warning(f"Skipping invalid trade at {trade_time}: Missing essential data")
                    continue
                
                # Initialize portfolio entries if they don't exist
                if sell_token_addr not in portfolio:
                    portfolio[sell_token_addr] = {
                        'quantity': 0,
                        'cost_basis': 0,
                        'symbol': sell_token_symbol
                    }
                
                if buy_token_addr not in portfolio:
                    portfolio[buy_token_addr] = {
                        'quantity': 0,
                        'cost_basis': 0,
                        'symbol': buy_token_symbol
                    }
                
                # Process sale - this is where we calculate PnL
                # If we don't have the token or not enough, log a warning but continue
                token_info = portfolio[sell_token_addr]
                actual_sell_qty = min(sell_qty, token_info['quantity'])
                
                # Initialize proceeds variable
                proceeds = 0
                
                if actual_sell_qty > 0 and sell_price > 0:
                    # Calculate cost basis for the sold portion
                    avg_cost = token_info['cost_basis'] / token_info['quantity'] if token_info['quantity'] > 0 else 0
                    cost_of_sold = avg_cost * actual_sell_qty
                    
                    # Calculate proceeds
                    proceeds = actual_sell_qty * sell_price
                    
                    # Calculate PnL for this trade
                    trade_realized_pnl = proceeds - cost_of_sold
                    realized_pnl += trade_realized_pnl
                    
                    # Record the trade's PnL regardless of profit or loss
                    trade_pnl_history.append({
                        'timestamp': trade_time,
                        'pnl': trade_realized_pnl,
                        'is_profit': trade_realized_pnl > 0
                    })
                    
                    # Update portfolio for the sold token
                    token_info['quantity'] -= actual_sell_qty
                    token_info['cost_basis'] -= cost_of_sold
                    
                    # Ensure we don't have negative values due to rounding
                    if np.isclose(token_info['quantity'], 0) or token_info['quantity'] < 0:
                        token_info['quantity'] = 0
                        token_info['cost_basis'] = 0
                
                # Process purchase
                if buy_qty > 0:
                    # If we didn't sell enough or at all, use market price for the buy
                    if actual_sell_qty < sell_qty or actual_sell_qty == 0:
                        # We don't have enough to sell, so this is effectively a market buy
                        buy_price = base_price if buy_token_addr == base_token_addr else quote_price
                        cost_for_buy = buy_qty * buy_price if buy_price > 0 else 0
                    else:
                        # Normal case - use the proceeds from the sale
                        cost_for_buy = proceeds
                    
                    # Update portfolio for the bought token
                    portfolio[buy_token_addr]['quantity'] += buy_qty
                    portfolio[buy_token_addr]['cost_basis'] += cost_for_buy
                
            except Exception as e:
                logger.error(f"Error processing trade at {trade_time}: {e}", exc_info=True)
                continue
            
            # Record a snapshot of the state after this trade
            pnl_history.append({
                'timestamp': trade_time,
                'realized_pnl': realized_pnl,
                'portfolio_snapshot': {k: v.copy() for k, v in portfolio.items()}
            })
        
        pnl_history_df = pd.DataFrame(pnl_history)
        trade_pnl_df = pd.DataFrame(trade_pnl_history)
        
        return portfolio, realized_pnl, trade_pnl_df

    def _calculate_metrics_from_pnl(self, portfolio, realized_pnl, trade_pnl_df, txn_df):
        """
        Calculate detailed metrics from PnL results, focusing on realized gains.
        """
        # Initialize metrics dictionary
        metrics = {
            'realized_pnl': realized_pnl,
            'volume': 0,
            'trade_count': len(txn_df),
            'win_count': 0,
            'loss_count': 0,
            'win_rate': 0
        }
        
        # Calculate trade volume
        for _, row in txn_df.iterrows():
            base_amount = float(row['base.ui_amount']) if pd.notna(row['base.ui_amount']) else 0
            base_price = float(row['base.price'] or row['base.nearest_price']) if pd.notna(row['base.price'] or row['base.nearest_price']) else 0
            
            quote_amount = float(row['quote.ui_amount']) if pd.notna(row['quote.ui_amount']) else 0
            quote_price = float(row['quote.price'] or row['quote.nearest_price']) if pd.notna(row['quote.price'] or row['quote.nearest_price']) else 0
            
            # Use the side with valid price data
            if quote_price > 0:
                metrics['volume'] += quote_amount * quote_price
            elif base_price > 0:
                metrics['volume'] += base_amount * base_price
        
        # Count profitable and losing trades
        if not trade_pnl_df.empty:
            # Explicitly count based on positive/negative PnL values
            for _, row in trade_pnl_df.iterrows():
                if row['pnl'] > 0:
                    metrics['win_count'] += 1
                elif row['pnl'] < 0:
                    metrics['loss_count'] += 1
            
            # Calculate win rate
            total_pnl_trades = metrics['win_count'] + metrics['loss_count']
            metrics['win_rate'] = metrics['win_count'] / total_pnl_trades if total_pnl_trades > 0 else 0
        
        return metrics
    
    def close(self):
        """
        Close database connections and clean up resources.
        """
        if hasattr(self, 'db') and self.db:
            self.db.quit()
        logger.info(f"Closed Holder resources for {self.address}")