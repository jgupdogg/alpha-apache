# sql_queries.py

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Calculate cutoff time as 6 hours ago from UTC now
cutoff_time = datetime.utcnow() - timedelta(hours=12)
cutoff_time_str = cutoff_time.strftime('%Y-%m-%d %H:%M:%S')


# DAG parameters with the updated SQL query

def get_query(query_name, table_name=None, HOLDS_COUNT_THRESHOLD=None, SWAPS_FREQ_THRESHOLD=None, SWAPS_USD_THRESHOLD=None):
    QUERIES = {
        "POPULAR_TRADES": f"""
        
            WITH swap_parsed AS (
                SELECT
                    tx_hash,
                    owner,
                    "blockchain",
                    CASE 
                        WHEN "quote.type_swap" = 'to' THEN "quote.address"
                        ELSE "base.address"
                    END AS swap_to_address,
                    
                    CASE 
                        WHEN "quote.type_swap" = 'to' THEN "base.address"
                        ELSE "quote.address"
                    END AS swap_from_address
                FROM {table_name}
                WHERE 
                    tx_type = 'swap'
                    AND timestamp >= '{cutoff_time_str}'
            ),
            
            to_aggregated AS (
                SELECT
                    "blockchain",
                    swap_to_address AS asset_address,
                    COUNT(DISTINCT owner) AS unique_owners_to
                FROM swap_parsed
                GROUP BY "blockchain", swap_to_address
            ),
            
            from_aggregated AS (
                SELECT
                    "blockchain",
                    swap_from_address AS asset_address,
                    COUNT(DISTINCT owner) AS unique_owners_from
                FROM swap_parsed
                GROUP BY "blockchain", swap_from_address
            )
            
            SELECT 
                COALESCE(to_aggregated.blockchain, from_aggregated.blockchain) AS blockchain,
                COALESCE(to_aggregated.asset_address, from_aggregated.asset_address) AS address
            FROM to_aggregated
            FULL OUTER JOIN from_aggregated
                ON to_aggregated.asset_address = from_aggregated.asset_address
                AND to_aggregated.blockchain = from_aggregated.blockchain
            WHERE 
                COALESCE(to_aggregated.unique_owners_to, 0) >= {SWAPS_FREQ_THRESHOLD}
                OR COALESCE(from_aggregated.unique_owners_from, 0) >= {SWAPS_FREQ_THRESHOLD}
            ORDER BY 
                (COALESCE(to_aggregated.unique_owners_to, 0) + COALESCE(from_aggregated.unique_owners_from, 0)) DESC;
                
        """,

        "POPULAR_HOLDS": f"""
        
        SELECT blockchain, address, SUM(valueusd) as total_usd, COUNT(wallet) as num_wallets
            FROM {table_name}
            WHERE timestamp = (SELECT MAX(timestamp) FROM {table_name})
            AND valueusd > {SWAPS_USD_THRESHOLD}
            GROUP BY 1, 2
            HAVING COUNT(wallet) > {HOLDS_COUNT_THRESHOLD}
            ORDER BY total_usd DESC
            
            """,
            
        "COMMON_LIST_WHALES":f"""
        
        SELECT DISTINCT blockchain, owner as address, owner as wallet, COUNT(*) as frequency
                FROM list_whales_raw
                WHERE timestamp = (SELECT MAX(timestamp) FROM list_whales_raw)
                GROUP BY 1, 2, 3
                HAVING COUNT(*) >= {HOLDS_COUNT_THRESHOLD}
                ORDER BY frequency DESC
                
                """, 
                
        "GET_LATEST_GMGN_TRADERS":f"""
                SELECT address, blockchain
            FROM gmgn_traders
            WHERE timestamp = (
                SELECT MAX(timestamp) FROM gmgn_traders
            )
        """,
        
        
    }
    
    return QUERIES[query_name]


