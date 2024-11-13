# utils/workflows/crypto/trader_processing.py

import requests
import pandas as pd
from typing import List
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
import logging

# Configure detailed logging
logger = logging.getLogger(__name__)

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

            # Check if response is successful
            if not response.get('success', False):
                logger.warning(f"Unsuccessful response for trader {trader}: {response}")
                continue

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
