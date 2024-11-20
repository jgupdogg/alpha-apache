# utils/workflows/crypto/token_data_processing.py

import logging
from typing import List
from utils.workflows.crypto.birdseye_sdk import BirdEyeSDK
import pandas as pd
import asyncio
import aiohttp

logger = logging.getLogger(__name__)

async def fetch_token_data(sdk: BirdEyeSDK, token_addresses: List[str]) -> pd.DataFrame:
    token_data_list = []
    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

    async def fetch_data_for_token(session, address):
        data = {}
        try:
            base_url = sdk.BASE_URL
            headers = sdk.headers

            token_security_url = f"{base_url}/defi/token_security?address={address}"
            token_creation_info_url = f"{base_url}/defi/token_creation_info?address={address}"
            token_overview_url = f"{base_url}/defi/token_overview?address={address}"

            async with semaphore:
                # Fetch token_security
                async with session.get(token_security_url, headers=headers) as response:
                    token_security = await response.json()
                    if token_security.get('success'):
                        security_data = token_security.get('data') or {}
                        data['TOP10_HOLDER_PERCENT'] = security_data.get('top10HolderPercent')
                        data['OWNER_PERCENTAGE'] = security_data.get('ownerPercentage')
                        data['CREATOR_PERCENTAGE'] = security_data.get('creatorPercentage')
                    else:
                        logger.warning(f"Failed to fetch token_security for address {address}: {token_security.get('message')}")

                # Fetch token_creation_info
                async with session.get(token_creation_info_url, headers=headers) as response:
                    token_creation_info = await response.json()
                    if token_creation_info.get('success'):
                        creation_data = token_creation_info.get('data') or {}
                        data['BLOCK_HUMAN_TIME'] = creation_data.get('blockHumanTime')
                        data['OWNER'] = creation_data.get('owner')
                    else:
                        logger.warning(f"Failed to fetch token_creation_info for address {address}: {token_creation_info.get('message')}")

                # Fetch token_overview
                async with session.get(token_overview_url, headers=headers) as response:
                    token_overview = await response.json()
                    if token_overview.get('success'):
                        overview_data = token_overview.get('data') or {}
                        data['TOKEN_ADDRESS'] = overview_data.get('address')
                        data['SYMBOL'] = overview_data.get('symbol')
                        
                        # Ensure DECIMALS is an integer
                        decimals = overview_data.get('decimals')
                        if decimals is not None:
                            try:
                                data['DECIMALS'] = int(decimals)
                            except ValueError:
                                logger.error(f"Invalid DECIMALS value for address {address}: {decimals}")
                                data['DECIMALS'] = None
                        else:
                            data['DECIMALS'] = None

                        data['NAME'] = overview_data.get('name')
                        extensions = overview_data.get('extensions') or {}
                        data['WEBSITE'] = extensions.get('website')
                        data['TWITTER'] = extensions.get('twitter')
                        data['DESCRIPTION'] = extensions.get('description')
                        data['LOGO_URI'] = overview_data.get('logoURI')

                        # Ensure HOLDER_COUNT is an integer
                        holder = overview_data.get('holder')
                        if holder is not None:
                            try:
                                data['HOLDER_COUNT'] = int(holder)
                            except ValueError:
                                logger.error(f"Invalid HOLDER_COUNT value for address {address}: {holder}")
                                data['HOLDER_COUNT'] = None
                        else:
                            data['HOLDER_COUNT'] = None

                        data['LIQUIDITY'] = overview_data.get('liquidity')
                        data['MARKET_CAP'] = overview_data.get('mc')
                        data['PRICE'] = overview_data.get('price')
                        data['V24H_USD'] = overview_data.get('v24hUSD')
                        data['V_BUY_HISTORY_24H_USD'] = overview_data.get('vBuyHistory24hUSD')
                        data['V_SELL_HISTORY_24H_USD'] = overview_data.get('vSellHistory24hUSD')
                    else:
                        logger.warning(f"Failed to fetch token_overview for address {address}: {token_overview.get('message')}")

                # Only add data if TOKEN_ADDRESS is present
                if data.get('TOKEN_ADDRESS'):
                    token_data_list.append(data)
                else:
                    logger.warning(f"No TOKEN_ADDRESS found for address {address}, skipping.")

                logging.info(f"Fetched data for token {address}: {data}")
                
        except Exception as e:
            logger.exception(f"Unexpected error fetching data for token {address}: {e}")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data_for_token(session, address) for address in token_addresses]
        await asyncio.gather(*tasks)

    token_df = pd.DataFrame(token_data_list)
    return token_df
