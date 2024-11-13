import requests
from typing import Optional, Dict, Any

class BirdEyeSDK:
    BASE_URL = "https://public-api.birdeye.so"

    def __init__(self, api_key: str, chain: str = "solana"):
        self.api_key = api_key
        self.chain = chain
        self.headers = {
            "accept": "application/json",
            "X-API-KEY": self.api_key,
            "x-chain": self.chain
        }
        self.defi = self.DefiEndpoints(self)
        self.token = self.TokenEndpoints(self)
        self.wallet = self.WalletEndpoints(self)
        self.trader = self.TraderEndpoints(self)

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None) -> Dict[str, Any]:
        url = f"{base_url or self.BASE_URL}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def _post(self, endpoint: str, data: Dict[str, Any], params: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None) -> Dict[str, Any]:
        url = f"{base_url or self.BASE_URL}{endpoint}"
        headers = self.headers.copy()
        headers["content-type"] = "application/json"
        response = requests.post(url, headers=headers, json=data, params=params)
        response.raise_for_status()
        return response.json()

    class DefiEndpoints:
        def __init__(self, sdk: 'BirdEyeSDK'):
            self.sdk = sdk
            self.base_url = f"{sdk.BASE_URL}/defi"

        def get_multi_price(self, list_address: str, check_liquidity: Optional[float] = None, include_liquidity: Optional[bool] = None) -> Dict[str, Any]:
            endpoint = "/multi_price"
            data = {"list_address": list_address}
            params = {}
            if check_liquidity is not None:
                params['check_liquidity'] = check_liquidity
            if include_liquidity is not None:
                params['include_liquidity'] = str(include_liquidity).lower()
            response = self.sdk._post(endpoint, data, params, base_url=self.base_url)
            return response

        def get_historical_price(self, address: str, address_type: str = "token", type: str = "15m",
                                 time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            endpoint = "/history_price"
            params = {"address": address, "address_type": address_type, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_historical_price_unix(self, address: str, unixtime: Optional[int] = None) -> Dict[str, Any]:
            endpoint = "/historical_price_unix"
            params = {"address": address}
            if unixtime is not None:
                params['unixtime'] = unixtime
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_trades_token(self, address: str, offset: int = 0, limit: int = 50,
                             tx_type: str = "swap", sort_type: str = "desc") -> Dict[str, Any]:
            endpoint = "/txs/token"
            params = {"address": address, "offset": offset, "limit": limit, "tx_type": tx_type, "sort_type": sort_type}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_trades_pair(self, address: str, offset: int = 0, limit: int = 50,
                            tx_type: str = "swap", sort_type: str = "desc") -> Dict[str, Any]:
            endpoint = "/txs/pair"
            params = {"address": address, "offset": offset, "limit": limit, "tx_type": tx_type, "sort_type": sort_type}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv(self, address: str, type: str = "15m",
                      time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            endpoint = "/ohlcv"
            params = {"address": address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv_pair(self, address: str, type: str = "15m",
                           time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            endpoint = "/ohlcv/pair"
            params = {"address": address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_ohlcv_base_quote(self, base_address: str, quote_address: str, type: str = "15m",
                                 time_from: Optional[int] = None, time_to: Optional[int] = None) -> Dict[str, Any]:
            endpoint = "/ohlcv/base_quote"
            params = {"base_address": base_address, "quote_address": quote_address, "type": type}
            if time_from is not None:
                params['time_from'] = time_from
            if time_to is not None:
                params['time_to'] = time_to
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_price_volume_multi(self, list_address: str, type: str = "24h") -> Dict[str, Any]:
            endpoint = "/price_volume/multi"
            data = {"list_address": list_address, "type": type}
            response = self.sdk._post(endpoint, data, base_url=self.base_url)
            return response

    class TokenEndpoints:
        def __init__(self, sdk: 'BirdEyeSDK'):
            self.sdk = sdk
            self.base_url = f"{sdk.BASE_URL}/defi"

        def get_token_security(self, address: str) -> Dict[str, Any]:
            endpoint = "/token_security"
            params = {"address": address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_overview(self, address: str) -> Dict[str, Any]:
            endpoint = "/token_overview"
            params = {"address": address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_creation_info(self, address: str) -> Dict[str, Any]:
            endpoint = "/token_creation_info"
            params = {"address": address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_trending(self, sort_by: str = "rank", sort_type: str = "asc",
                               offset: int = 0, limit: int = 20) -> Dict[str, Any]:
            endpoint = "/token_trending"
            params = {"sort_by": sort_by, "sort_type": sort_type, "offset": offset, "limit": limit}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_new_listing(self, time_to: Optional[int] = None, limit: int = 10,
                            meme_platform_enabled: bool = False) -> Dict[str, Any]:
            endpoint = "/v2/tokens/new_listing"
            params = {
                "limit": limit,
                "meme_platform_enabled": str(meme_platform_enabled).lower()
            }
            if time_to is not None:
                params['time_to'] = time_to
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_top_traders(self, address: str, time_frame: str = "24h", sort_type: str = "desc",
                            sort_by: str = "volume", offset: int = 0, limit: int = 10) -> Dict[str, Any]:
            endpoint = "/v2/tokens/top_traders"
            params = {
                "address": address,
                "time_frame": time_frame,
                "sort_type": sort_type,
                "sort_by": sort_by,
                "offset": offset,
                "limit": limit
            }
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_metadata_multiple(self, list_address: str) -> Dict[str, Any]:
            endpoint = "/v3/token/meta-data/multiple"
            params = {"list_address": list_address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_market_data(self, address: str) -> Dict[str, Any]:
            endpoint = "/v3/token/market-data"
            params = {"address": address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_trade_data_multiple(self, list_address: str) -> Dict[str, Any]:
            endpoint = "/v3/token/trade-data/multiple"
            params = {"list_address": list_address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_top_holders(self, address: str, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
            endpoint = "/v3/token/holder"
            params = {"address": address, "offset": offset, "limit": limit}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

    class WalletEndpoints:
        def __init__(self, sdk: 'BirdEyeSDK'):
            self.sdk = sdk
            self.base_url = sdk.BASE_URL

        def get_token_list(self, wallet: str) -> Dict[str, Any]:
            endpoint = "/v1/wallet/token_list"
            params = {"wallet": wallet}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_token_balance(self, wallet: str, token_address: str) -> Dict[str, Any]:
            endpoint = "/v1/wallet/token_balance"
            params = {"wallet": wallet, "token_address": token_address}
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

        def get_transaction_history(self, wallet: str, limit: int = 100, before: Optional[str] = None) -> Dict[str, Any]:
            endpoint = "/v1/wallet/tx_list"
            params = {"wallet": wallet, "limit": limit}
            if before is not None:
                params['before'] = before
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response

    class TraderEndpoints:
        def __init__(self, sdk: 'BirdEyeSDK'):
            self.sdk = sdk
            self.base_url = sdk.BASE_URL

        def get_gainers_losers(self, type: str = "1W", sort_by: str = "PnL", sort_type: str = "desc",
                               offset: int = 0, limit: int = 10) -> Dict[str, Any]:
            endpoint = "/top_traders/gainers-losers"
            params = {
                "type": type,
                "sort_by": sort_by,
                "sort_type": sort_type,
                "offset": offset,
                "limit": limit
            }
            response = self.sdk._get(endpoint, params, base_url=self.base_url)
            return response
        
