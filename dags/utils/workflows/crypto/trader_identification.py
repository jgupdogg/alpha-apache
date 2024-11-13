# utils/trader_identification.py

import requests
import pandas as pd
from typing import Dict
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
import logging

# Configure detailed logging
logger = logging.getLogger(__name__)

def fetch_top_traders(api_sdk: BirdEyeSDK, token_list: Dict[str, Dict[str, str]], limit: int = 10) -> pd.DataFrame:
    data_list = []
    for category, tokens in token_list.items():
        for token_symbol, token_address in tokens.items():
            try:
                response = api_sdk.token.get_top_traders(
                    address=token_address,
                    time_frame="24h",
                    sort_by="volume",
                    sort_type="desc",
                    offset=0,
                    limit=limit
                )
                logger.info(f"API response for {token_symbol}: {response}")

                traders = response.get('data', {}).get('items', [])

                if not traders:
                    logger.warning(f"No trader data found for token {token_symbol} ({token_address}).")
                    continue

                for trader in traders:
                    trader_address = trader.get('owner')
                    if trader_address is None:
                        logger.warning(f"Trader without 'owner' field: {trader}")
                        continue

                    data = {
                        'CATEGORY': category,
                        'TOKEN_SYMBOL': token_symbol,
                        'TRADER_ADDRESS': trader_address
                    }
                    data_list.append(data)
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP Error fetching data for token {token_symbol} ({token_address}): {e}")
            except Exception as e:
                logger.exception(f"Unexpected error fetching data for token {token_symbol} ({token_address}): {e}")

    df = pd.DataFrame(data_list)
    logger.info(f"Columns in df_traders from fetch_top_traders: {df.columns.tolist()}")
    logger.info(f"First few rows of df_traders:\n{df.head()}")

    return df

def identify_successful_traders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifies traders who appear in top traders lists of more than one token and adds a frequency count.

    Args:
        df (pd.DataFrame): DataFrame containing 'CATEGORY', 'TOKEN_SYMBOL', and 'TRADER_ADDRESS'.

    Returns:
        pd.DataFrame: DataFrame with 'DATE_ADDED', 'TRADER_ADDRESS', 'CATEGORY', 'TOKEN_SYMBOL', and 'FREQ' columns.
    """
    # Ensure required columns are present
    required_columns = {'TRADER_ADDRESS', 'TOKEN_SYMBOL', 'CATEGORY'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"DataFrame must contain columns: {missing}")

    # Group by trader address and collect the categories and tokens they are associated with
    trader_groups = df.groupby('TRADER_ADDRESS').agg({
        'CATEGORY': lambda x: list(set(x)),        # Unique categories
        'TOKEN_SYMBOL': lambda x: list(set(x))    # Unique tokens
    }).reset_index()

    # Calculate frequency
    trader_groups['FREQ'] = trader_groups['TOKEN_SYMBOL'].apply(len)

    # Filter traders who appear in more than one token's top traders list
    successful_traders = trader_groups[trader_groups['FREQ'] > 1].copy()

    # Flatten the lists into comma-separated strings
    successful_traders['CATEGORY'] = successful_traders['CATEGORY'].apply(lambda x: ','.join(x))
    successful_traders['TOKEN_SYMBOL'] = successful_traders['TOKEN_SYMBOL'].apply(lambda x: ','.join(x))

    # Add DATE_ADDED column
    successful_traders['DATE_ADDED'] = pd.Timestamp.utcnow()

    # Select required columns
    successful_traders = successful_traders[['DATE_ADDED', 'TRADER_ADDRESS', 'CATEGORY', 'TOKEN_SYMBOL', 'FREQ']]

    return successful_traders   