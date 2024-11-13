import requests
import pandas as pd
from typing import Dict, List
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
import logging

# Configure detailed logging
logger = logging.getLogger(__name__)
def fetch_top_traders(api_sdk: BirdEyeSDK, token_list: Dict[str, str], limit: int = 10) -> pd.DataFrame:
    """
    Fetches top traders for each token in the token list and compiles the data into a pandas DataFrame.

    Args:
        api_sdk (BirdEyeSDK): An instance of the BirdEyeSDK.
        token_list (Dict[str, str]): Dictionary mapping token symbols to their addresses.
        limit (int): Number of top traders to retrieve per token.

    Returns:
        pd.DataFrame: DataFrame containing trader data across all tokens.
    """
    data_list = []
    for token_name, token_address in token_list.items():
        try:
            response = api_sdk.token.get_top_traders(
                address=token_address,
                time_frame="24h",
                sort_by="volume",
                sort_type="desc",
                offset=0,
                limit=limit
            )
            traders = response.get('data', {}).get('items', [])

            if not traders:
                print(f"No trader data found for token {token_name} ({token_address}).")
                continue

            for trader in traders:
                data = {
                    'TOKEN_SYMBOL': token_name,
                    'TRADER_ADDRESS': trader.get('owner')
                }
                data_list.append(data)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error fetching data for token {token_name} ({token_address}): {e}")
        except Exception as e:
            print(f"Unexpected error fetching data for token {token_name} ({token_address}): {e}")

    df = pd.DataFrame(data_list)
    return df   
    


def get_traders_with_min_occurrences(df: pd.DataFrame, min_occurrences: int = 2) -> pd.DataFrame:
    """
    Identifies trader addresses that appear at least 'min_occurrences' times in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing trader addresses.
        min_occurrences (int): Minimum number of occurrences required.

    Returns:
        pd.DataFrame: DataFrame with trader addresses and their counts.
    """
    # Ensure the 'Trader Address' column exists
    if 'TRADER_ADDRESS' not in df.columns:
        raise ValueError("DataFrame must contain a 'TRADER_ADDRESS' column.")

    # Count occurrences of each trader address
    address_counts = df['TRADER_ADDRESS'].value_counts()

    # Filter addresses with counts >= min_occurrences
    frequent_traders = address_counts[address_counts >= min_occurrences].reset_index()
    frequent_traders.columns = ['TRADER_ADDRESS', 'COUNT']

    return frequent_traders


def get_portfolio_balances(api_sdk: BirdEyeSDK, trader_addresses: List[str]) -> pd.DataFrame:
    """
    Retrieves the current portfolio balances for a list of trader addresses.

    Args:
        api_sdk (BirdEyeSDK): An instance of the BirdEyeSDK.
        trader_addresses (List[str]): List of trader wallet addresses.

    Returns:
        pd.DataFrame: DataFrame containing portfolio balances for each trader.
    """
    # Create an empty list to store portfolio data
    portfolio_data = []

    # Iterate over each trader address
    for trader in trader_addresses:
        try:
            # Fetch the token list for the trader's wallet
            response = api_sdk.wallet.get_token_list(wallet=trader)

            # Extract the list of tokens
            tokens = response.get('data', {}).get('items', [])

            if not tokens:
                logger.warning(f"No tokens found for trader {trader}.")
                continue

            # Iterate over each token and collect balance information
            for token in tokens:
                token_address = token.get('address')
                token_symbol = token.get('symbol')
                balance = float(token.get('uiAmount', 0))
                price_usd = float(token.get('priceUsd', 0))
                value_usd = float(token.get('valueUsd', 0))

                portfolio_entry = {
                    'TRADER_ADDRESS': trader,
                    'TOKEN_SYMBOL': token_symbol,
                    'TOKEN_ADDRESS': token_address,
                    'BALANCE': balance,
                    'PRICE_USD': price_usd,
                    'VALUE_USD': value_usd
                }
                portfolio_data.append(portfolio_entry)

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error fetching portfolio for trader {trader}: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error fetching portfolio for trader {trader}: {e}")

    # Create a DataFrame from the portfolio data
    portfolio_df = pd.DataFrame(portfolio_data)

    logger.info(f"Columns in portfolio_df: {portfolio_df.columns.tolist()}")
    logger.info(f"First few rows of portfolio_df:\n{portfolio_df.head()}")

    return portfolio_df  