import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import task
from db_manager import DBManager

@task(task_id="check_whale_notifications")
def check_whale_notifications() -> list:
    """
    Identifies transactions that qualify for notifications based on criteria:
     - Transaction has not been processed yet (processed = FALSE)
     - Token has snifscore >= 70
     - Token has smart followers >= 1
     - Token has name and symbol available
    """
    db_manager = DBManager(db_name='solana')
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get unprocessed transactions with qualifying token metadata
    query = """
    WITH unprocessed_txns AS (
        SELECT
            t.raw_id,
            t.user_address,
            t.swapfromtoken,
            t.swaptotoken,
            t.swapfromamount,
            t.swaptoamount,
            t.signature,
            t.processed,
            t.timestamp
        FROM helius_txns_clean t
        WHERE t.processed = False
        ORDER BY t.timestamp DESC
        LIMIT 10
        
    ),
    solsniffer_data AS (
        SELECT 
            token_address,  -- Using correct field name token_address
            snifscore
        FROM solsniffer_token_data
        WHERE snifscore IS NOT NULL 
        AND CAST(snifscore AS NUMERIC) >= 70
    ),
    market_data AS (
        SELECT 
            token_address,
            price,
            liquidity,
            market_cap,
            fdv
        FROM token_market_data
        WHERE market_cap IS NOT NULL 
        AND CAST(market_cap AS NUMERIC) > 0
    ),
    token_creation AS (
        SELECT 
            token_address,
            block_human_time AS deploy_time
        FROM token_creation_info
    ),
    follower_counts AS (
        SELECT 
            token_address,
            x_handle,
            COUNT(DISTINCT follower_handle) AS follower_count
        FROM x_followers
        WHERE x_handle IS NOT NULL AND x_handle != ''
        AND follower_handle IS NOT NULL
        GROUP BY token_address, x_handle
        HAVING COUNT(DISTINCT follower_handle) >= 1
    ),
    token_metadata_info AS (
        SELECT DISTINCT ON (token_address)
            token_address,
            blockchain AS chain,
            token_symbol AS symbol,
            token_name,
            website,
            twitter,
            scraped_at
        FROM token_metadata
        WHERE token_address IS NOT NULL
        AND token_symbol IS NOT NULL AND token_symbol != ''
        ORDER BY token_address, scraped_at DESC
    ),
    buy_txns AS (
        SELECT 
            ut.raw_id,
            ut.user_address,
            ut.swapfromtoken,
            ut.swaptotoken,
            ut.swapfromamount,
            ut.swaptoamount,
            ut.signature,
            ut.timestamp,
            sd.token_address,
            md.price,
            md.market_cap,
            md.fdv,
            md.liquidity,
            sd.snifscore,
            tc.deploy_time,
            tm.token_name,
            tm.symbol,
            tm.chain,
            tm.website,
            tm.twitter,
            fc.x_handle,
            fc.follower_count,
            'buy' AS transaction_type
        FROM unprocessed_txns ut
        JOIN solsniffer_data sd ON ut.swaptotoken = sd.token_address
        JOIN market_data md ON ut.swaptotoken = md.token_address
        JOIN token_metadata_info tm ON ut.swaptotoken = tm.token_address
        JOIN follower_counts fc ON ut.swaptotoken = fc.token_address
        LEFT JOIN token_creation tc ON ut.swaptotoken = tc.token_address
        WHERE ut.swaptotoken NOT IN ('So11111111111111111111111111111111111111112')
    ),
    sell_txns AS (
        SELECT 
            ut.raw_id,
            ut.user_address,
            ut.swapfromtoken,
            ut.swaptotoken,
            ut.swapfromamount,
            ut.swaptoamount,
            ut.signature,
            ut.timestamp,
            sd.token_address,
            md.price,
            md.market_cap,
            md.fdv,
            md.liquidity,
            sd.snifscore,
            tc.deploy_time,
            tm.token_name,
            tm.symbol,
            tm.chain,
            tm.website,
            tm.twitter,
            fc.x_handle,
            fc.follower_count,
            'sell' AS transaction_type
        FROM unprocessed_txns ut
        JOIN solsniffer_data sd ON ut.swapfromtoken = sd.token_address
        JOIN market_data md ON ut.swapfromtoken = md.token_address
        JOIN token_metadata_info tm ON ut.swapfromtoken = tm.token_address
        JOIN follower_counts fc ON ut.swapfromtoken = fc.token_address
        LEFT JOIN token_creation tc ON ut.swapfromtoken = tc.token_address
        WHERE ut.swapfromtoken NOT IN ('So11111111111111111111111111111111111111112')
    )
    SELECT * FROM buy_txns
    UNION ALL
    SELECT * FROM sell_txns
    """
    
    logging.info("Querying for unprocessed transactions that meet notification criteria")
    df_qualifying_txns = db_manager.execute_query(query, df=True)
    
    # Extract signatures from the qualifying transactions to mark as processed
    examined_signatures = []
    if not df_qualifying_txns.empty:
        examined_signatures = df_qualifying_txns['signature'].tolist()
        logging.info(f"Found {len(df_qualifying_txns)} qualifying transactions to process")
    else:
        logging.info("No qualifying transactions found.")
        db_manager.quit()
        return []
    
    # For each qualifying transaction, get historical context
    candidates = []
    for _, row in df_qualifying_txns.iterrows():
        token_address = row.get('token_address')  # Updated field name
        
        # Check if we've already processed a candidate for this token in this batch
        if any(c.get('token') == token_address for c in candidates):
            logging.info(f"Skipping duplicate candidate for token {row.get('token_name')} ({token_address})")
            continue
        
        # Query historical context - buyers over 1h, 6h, 24h
        buyers_query = f"""
        SELECT
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') AS buyers_1h,
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '6 hours') AS buyers_6h,
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '24 hours') AS buyers_24h
        FROM helius_txns_clean
        WHERE swaptotoken = '{token_address}'
        """
        
        # Query historical context - sellers over 1h, 6h, 24h
        sellers_query = f"""
        SELECT
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') AS sellers_1h,
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '6 hours') AS sellers_6h,
            COUNT(DISTINCT user_address) FILTER (WHERE timestamp > NOW() - INTERVAL '24 hours') AS sellers_24h
        FROM helius_txns_clean
        WHERE swapfromtoken = '{token_address}'
        """
        
        df_buyers = db_manager.execute_query(buyers_query, df=True)
        df_sellers = db_manager.execute_query(sellers_query, df=True)
        
        # Default values
        buyers_1h = 0
        buyers_6h = 0
        buyers_24h = 0
        sellers_1h = 0
        sellers_6h = 0
        sellers_24h = 0
        
        if not df_buyers.empty:
            buyers_1h = int(df_buyers.iloc[0].get('buyers_1h', 0) or 0)
            buyers_6h = int(df_buyers.iloc[0].get('buyers_6h', 0) or 0)
            buyers_24h = int(df_buyers.iloc[0].get('buyers_24h', 0) or 0)
            
        if not df_sellers.empty:
            sellers_1h = int(df_sellers.iloc[0].get('sellers_1h', 0) or 0)
            sellers_6h = int(df_sellers.iloc[0].get('sellers_6h', 0) or 0)
            sellers_24h = int(df_sellers.iloc[0].get('sellers_24h', 0) or 0)
        
        # Create candidate with all necessary information
        candidate = {
            "timestamp": now_str,
            "token": token_address,
            "token_name": row.get('token_name'),
            "symbol": row.get('symbol'),
            "transaction_type": row.get('transaction_type'),
            "signature": row.get('signature'),
            "user_address": row.get('user_address'),
            "amount": row.get('swaptoamount') if row.get('transaction_type') == 'buy' else row.get('swapfromamount'),
            "x_handle": row.get('x_handle'),
            "follower_count": int(row.get('follower_count', 0)),
            "market_cap": float(row.get('market_cap', 0)),
            "price": float(row.get('price', 0)),
            "liquidity": float(row.get('liquidity', 0)),
            "fdv": float(row.get('fdv', 0)),
            "deploy_time": row.get('deploy_time'),
            "snifscore": float(row.get('snifscore', 0)),
            "chain": row.get('chain'),
            "website": row.get('website'),
            "twitter": row.get('twitter'),
            "buyers_1h": buyers_1h,
            "buyers_6h": buyers_6h,
            "buyers_24h": buyers_24h,
            "sellers_1h": sellers_1h,
            "sellers_6h": sellers_6h,
            "sellers_24h": sellers_24h
        }
        
        candidates.append(candidate)
    
    # Mark only the examined transactions as processed
    if examined_signatures:
        signatures_str = "', '".join(examined_signatures)
        update_query = f"""
        UPDATE helius_txns_clean
        SET processed = TRUE
        WHERE signature IN ('{signatures_str}')
        """
        db_manager.execute_query(update_query)
        logging.info(f"Marked {len(examined_signatures)} examined transactions as processed")
    
    db_manager.quit()
    return candidates


# Consolidated query that gets all needed data at once
token_details_query_template = """
    WITH solsniffer_data AS (
        SELECT 
            token_address,
            snifscore
        FROM solsniffer_token_data
        WHERE token_address IN ({token_list})
        AND snifscore IS NOT NULL 
        AND CAST(snifscore AS NUMERIC) > 0
    ),
    market_data AS (
        SELECT 
            token_address,
            price,
            liquidity,
            market_cap,
            fdv
        FROM token_market_data
        WHERE market_cap IS NOT NULL 
        AND CAST(market_cap AS NUMERIC) > 0
        AND token_address IN ({token_list})
    ),
    token_creation AS (
        SELECT 
            token_address,
            block_human_time AS deploy_time
        FROM token_creation_info
        WHERE token_address IN ({token_list})
    ),
    follower_counts AS (
        SELECT 
            token_address,
            x_handle,
            COUNT(DISTINCT follower_handle) AS follower_count
        FROM x_followers
        WHERE token_address IN ({token_list})
        AND x_handle IS NOT NULL AND x_handle != ''
        AND follower_handle IS NOT NULL
        GROUP BY token_address, x_handle
        HAVING COUNT(DISTINCT follower_handle) > 0
    ),
    token_metadata_info AS (
        SELECT DISTINCT ON (token_address)
            token_address,
            blockchain AS chain,
            token_symbol AS symbol,
            token_name,
            website,
            twitter,
            scraped_at
        FROM token_metadata
        WHERE token_address IN ({token_list})
        AND token_symbol IS NOT NULL AND token_symbol != ''
        ORDER BY token_address, scraped_at DESC
    )
    SELECT 
        sd.token_address as address,
        tm.token_name,
        tm.symbol,
        tc.deploy_time,
        md.price,
        md.market_cap,
        md.liquidity,
        md.fdv,
        fc.x_handle,
        sd.snifscore,
        fc.follower_count,
        tm.chain,
        tm.website,
        tm.twitter
    FROM solsniffer_data sd
    JOIN token_metadata_info tm ON sd.token_address = tm.token_address
    JOIN follower_counts fc ON sd.token_address = fc.token_address
    JOIN market_data md ON sd.token_address = md.token_address
    LEFT JOIN token_creation tc ON sd.token_address = tc.token_address
"""

def fetch_token_details(candidates: list):
    """Fetch token details from database for candidate tokens"""
    db_manager = DBManager(db_name='solana_insiders_silver')
    
    # Gather candidate token addresses
    tokens = list({cand["token"] for cand in candidates})
    token_list_str = ", ".join(f"'{token}'" for token in tokens)
    
    # Query token details
    token_details_query = token_details_query_template.format(token_list=token_list_str)
    logging.info(f"Fetching token details for {len(tokens)} tokens.")
    df_token_details = db_manager.execute_query(token_details_query, df=True)
    
    db_manager.quit()
    return df_token_details


def process_candidate_data(candidates: list, df_token_details: pd.DataFrame) -> pd.DataFrame:
    """Process and filter candidate data"""
    # Convert candidates to DataFrame
    df_candidates = pd.DataFrame(candidates)
    
    # Log before merge to help diagnose issues
    logging.info(f"Candidates shape before merge: {df_candidates.shape}")
    logging.info(f"Token details shape before merge: {df_token_details.shape}")
    
    # Merge data
    df_merged = pd.merge(df_candidates, df_token_details, left_on="token", right_on="address", how="inner")
    
    # Log after merge to help diagnose issues
    logging.info(f"Merged data shape: {df_merged.shape}")
    
    # Filter out banned tokens
    banned_terms = ["SOL", "USDT", "USDC", "WSOL", "WBTC"]
    df_filtered = df_merged[~df_merged['token_name'].isin(banned_terms)]
    
    # Convert string numeric fields to actual numeric values
    if not df_filtered.empty:
        try:
            df_filtered['market_cap'] = pd.to_numeric(df_filtered['market_cap'], errors='coerce')
            df_filtered['snifscore'] = pd.to_numeric(df_filtered['snifscore'], errors='coerce')
            df_filtered['price'] = pd.to_numeric(df_filtered['price'], errors='coerce')
            df_filtered['liquidity'] = pd.to_numeric(df_filtered['liquidity'], errors='coerce')
            df_filtered['fdv'] = pd.to_numeric(df_filtered['fdv'], errors='coerce')
        except Exception as e:
            logging.warning(f"Error converting numeric fields: {e}")
    
    # Additional strict filtering - only keep rows with complete data
    df_filtered = df_filtered[
        df_filtered['token_name'].notna() & 
        df_filtered['token_name'].str.strip().ne('') &
        df_filtered['symbol'].notna() & 
        df_filtered['symbol'].str.strip().ne('') &
        df_filtered['market_cap'].notna() & 
        (df_filtered['market_cap'] > 0) &
        df_filtered['snifscore'].notna() & 
        (df_filtered['snifscore'] > 0) &
        df_filtered['x_handle'].notna() & 
        df_filtered['x_handle'].str.strip().ne('') &
        df_filtered['follower_count'].notna() & 
        (df_filtered['follower_count'] > 0)
    ]
    
    logging.info(f"Data shape after filtering for complete data: {df_filtered.shape}")
    
    if df_filtered.empty:
        logging.info("All candidate notifications were filtered out.")
        return pd.DataFrame()

    # Keep necessary columns including new fields
    df_filtered = df_filtered.rename(columns={"token": "address"})
    df_notifications = df_filtered[['timestamp', 'address', 'token_name', 'symbol', 'time_interval',
                             'num_users_bought', 'num_users_sold', 'follower_count', 'chain', 
                             'deploy_time', 'x_handle', 'market_cap', 'snifscore',
                             'price', 'liquidity', 'fdv', 'website', 'twitter']]
    
    return df_notifications

@task(task_id="store_notifications")
def store_notifications(candidates: list) -> str:
    """
    Store notifications for qualifying transactions and generate formatted messages.
    """
    if not candidates:
        logging.info("No candidate notifications to process.")
        return "No new whale notifications triggered."

    # Log candidates for debugging
    logging.info(f"Processing {len(candidates)} candidates")
    
    db_manager = DBManager(db_name='solana_insiders_gold')
    db_silver = DBManager(db_name='solana_insiders_silver')
    
    # Group candidates by token to avoid duplicate notifications
    token_groups = {}
    for candidate in candidates:
        token = candidate.get('token')
        if token not in token_groups:
            token_groups[token] = candidate
    
    # Process each unique token
    new_notifications = []
    for token, candidate in token_groups.items():
        
        # Get user_address and update transaction status
        user_address = update_transaction_status(candidate.get('signature'), True, db_silver)
        
        # Get trader source information
        trader_address, trader_source = get_trader_info(user_address, db_manager)
        
        # Store the notification
        try:
            # Format for insertion with new fields
            notification_record = {
                'token_address': token,
                'timestamp': candidate.get('timestamp'),
                'token_name': candidate.get('token_name'),
                'symbol': candidate.get('symbol'),
                'buyers_1h': candidate.get('buyers_1h', 0),
                'buyers_6h': candidate.get('buyers_6h', 0),
                'buyers_24h': candidate.get('buyers_24h', 0),
                'sellers_1h': candidate.get('sellers_1h', 0),
                'sellers_6h': candidate.get('sellers_6h', 0),
                'sellers_24h': candidate.get('sellers_24h', 0),
                'transaction_type': candidate.get('transaction_type', ''),
                'signature': candidate.get('signature'),
                'snifscore': candidate.get('snifscore', 0),
                'price': candidate.get('price', 0),
                'market_cap': candidate.get('market_cap', 0),
                'liquidity': candidate.get('liquidity', 0),
                'fdv': candidate.get('fdv', 0),
                'follower_count': candidate.get('follower_count', 0),
                'x_handle': candidate.get('x_handle', ''),
                'twitter': candidate.get('twitter', ''),
                'website': candidate.get('website', ''),
                'deploy_time': candidate.get('deploy_time'),
                'trader_address': trader_address,
                'trader_source': trader_source
            }
            
            # Insert into database
            df_notification = pd.DataFrame([notification_record])
            db_manager.insert_raw_dataframe("whale_notifications", df_notification)
            
            # Add to list of new notifications to format
            new_notifications.append(notification_record)
            logging.info(f"Stored notification for {candidate.get('token_name')}")
            
        except Exception as e:
            logging.error(f"Error storing notification for {token}: {e}")
    
    # Generate notification messages
    if not new_notifications:
        logging.info("No new notifications to send.")
        return "No new whale notifications triggered."
    
    messages = []
    for notification in new_notifications:
        message = format_notification_message(notification)
        messages.append(message)
        logging.info(f"Generated message for {notification.get('token_name')}")
    
    # Join messages with separator
    joined_messages = "\n\n---\n\n".join(messages)
    logging.info(f"Generated combined notification message of length {len(joined_messages)}")
    
    db_manager.quit()
    db_silver.quit()
    
    return joined_messages


def format_notification_message(row):
    """Format a notification message with colored dots and historical activity"""
    # Basic token info
    token_name = row['token_name'] if pd.notnull(row.get('token_name')) else "Unknown Token"
    token_symbol = row['symbol'] if pd.notnull(row.get('symbol')) else token_name
    token_address = row.get('token') if pd.notnull(row.get('token')) else row.get('address', '')
    
    # Transaction type
    transaction_type = row.get('transaction_type', '')
    if transaction_type == 'buy':
        signal_dot = "🟢"  # Green dot for buy
    elif transaction_type == 'sell':
        signal_dot = "🔴"  # Red dot for sell
    else:
        signal_dot = "🟡"  # Yellow dot for neutral
    
    # Historical stats
    buyers_1h = int(row.get('buyers_1h', 0))
    buyers_6h = int(row.get('buyers_6h', 0))
    buyers_24h = int(row.get('buyers_24h', 0))
    sellers_1h = int(row.get('sellers_1h', 0))
    sellers_6h = int(row.get('sellers_6h', 0))
    sellers_24h = int(row.get('sellers_24h', 0))
    
    # Trader information
    trader_address = row.get('trader_address')
    trader_source = row.get('trader_source')
    
    # Format deploy time if available
    deploy_time = row['deploy_time'] if pd.notnull(row.get('deploy_time')) else None
    age_text = ""
    is_new_token = False
    
    if deploy_time:
        try:
            # Convert both to datetime objects
            deploy_dt = pd.to_datetime(deploy_time)
            current_dt = pd.Timestamp.now()
            
            # Make both timezone naive in UTC
            if deploy_dt.tzinfo is not None:
                deploy_dt = deploy_dt.tz_localize(None)
            if current_dt.tzinfo is not None:
                current_dt = current_dt.tz_localize(None)
            
            time_diff = current_dt - deploy_dt
            
            # Simple age formatting
            if time_diff.days > 365:
                years = time_diff.days // 365
                age_text = f"{years}yr"
            elif time_diff.days > 30:
                months = time_diff.days // 30
                age_text = f"{months}mo"
            elif time_diff.days > 0:
                age_text = f"{time_diff.days}d"
            else:
                hours = time_diff.seconds // 3600
                age_text = f"{hours}h"
                
            # Flag very new tokens (less than 24 hours)
            if time_diff.total_seconds() < 86400:
                is_new_token = True
        except Exception as e:
            logging.warning(f"Error formatting token age: {e}")
    
    # Create a human-readable timestamp for the current time
    timestamp = datetime.now().strftime("%I:%M %p - %b %d, %Y")
    
    # Build message header with transaction detection
    message = f"""{signal_dot} <https://dexscreener.com/solana/{token_address}|*{token_name}* ({token_symbol})>"""
    
    # Add "NEW TOKEN!" indicator for tokens less than 24 hours old
    if is_new_token:
        message += " 🔥🔥 *NEW TOKEN!* 🔥🔥"
    
    # Add activity summary
    activity_summary = []
    if buyers_1h > 0 or sellers_1h > 0:
        activity_summary.append(f"1h: {buyers_1h} buys, {sellers_1h} sells")
    if buyers_6h > 0 or sellers_6h > 0:
        activity_summary.append(f"6h: {buyers_6h} buys, {sellers_6h} sells")
    if buyers_24h > 0 or sellers_24h > 0:
        activity_summary.append(f"24h: {buyers_24h} buys, {sellers_24h} sells")
    
    if activity_summary:
        message += "\n📊 " + " | ".join(activity_summary)

    # Add bullet list for additional information with indentation and icons
    bullet_points = []
    
    # Add price with dollar emoji if available (NEW)
    if 'price' in row and pd.notnull(row.get('price')):
        price = float(row['price'])
        bullet_points.append(f"    💲 Price: ${price:.6f}")
    
    # Add market cap with money bag emoji if available
    if 'market_cap' in row and pd.notnull(row.get('market_cap')):
        market_cap = float(row['market_cap'])
        bullet_points.append(f"    💰 MarketCap: ${market_cap:,.0f}")
    
    # Add liquidity with droplet emoji if available (NEW)
    if 'liquidity' in row and pd.notnull(row.get('liquidity')):
        liquidity = float(row['liquidity'])
        bullet_points.append(f"    💧 Liquidity: ${liquidity:,.0f}")
    
    # Add FDV with star emoji if available (NEW)
    if 'fdv' in row and pd.notnull(row.get('fdv')):
        fdv = float(row['fdv'])
        bullet_points.append(f"    ⭐ Fully Diluted Value: ${fdv:,.0f}")
    
    # Add website with globe emoji if available (NEW)
    if 'website' in row and pd.notnull(row.get('website')):
        website = row['website']
        if website and not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        bullet_points.append(f"    🌐 <{website}|Website>")
    
    # Add X handle and smart followers with followers icon (IMPROVED)
    x_handle = row['x_handle'] if pd.notnull(row.get('x_handle')) else ""
    twitter = row['twitter'] if pd.notnull(row.get('twitter')) else x_handle
    follower_count = int(row['follower_count']) if 'follower_count' in row and pd.notnull(row.get('follower_count')) else 0
    
    if twitter:
        if not twitter.startswith('@'):
            twitter = '@' + twitter
        bullet_points.append(f"    👥 <https://twitter.com/{twitter.lstrip('@')}|{twitter}> ({follower_count} Smart Followers)")
    elif x_handle:
        bullet_points.append(f"    👥 <https://twitter.com/{x_handle}|@{x_handle}> ({follower_count} Smart Followers)")
    
    # Add trader information
    if trader_address and trader_source:
        bullet_points.append(f"    🧠 Trader: <https://solscan.io/account/{trader_address}|{trader_source}>")
    elif trader_address:
        bullet_points.append(f"    🧠 Trader: <https://solscan.io/account/{trader_address}|Unknown>")
    
    # Add sniff score with shield emoji
    snifscore = float(row['snifscore']) if pd.notnull(row.get('snifscore')) else None
    if snifscore is not None:
        # Determine qualitative rating
        security_rating = ""
        if snifscore >= 90:
            security_rating = "Excellent"
        elif snifscore >= 80:
            security_rating = "Great"
        elif snifscore >= 70:
            security_rating = "Good"
        
        # Format security score as raw number with rating if applicable
        if security_rating:
            bullet_points.append(f"    🛡️ <https://solsniffer.com/scanner/{token_address}|Security: {int(snifscore)} ({security_rating})>")
        else:
            bullet_points.append(f"    🛡️ <https://solsniffer.com/scanner/{token_address}|Security: {int(snifscore)}>")
    
    # Add token age with clock emoji if available
    if age_text:
        bullet_points.append(f"    ⏰ Token age: {age_text}")
    
    # Add bullet points
    if bullet_points:
        message += "\n\n" + "\n".join(bullet_points)
    
    # Add timestamp in plain text format
    message += f"\n\n{timestamp}"
    
    return message



def update_transaction_status(signature, notification_sent, db_silver):
    """Update transaction notification status"""
    query = f"""
    UPDATE helius_txns_clean
    SET notification_sent = {notification_sent}
    WHERE signature = '{signature}'
    """
    db_silver.execute_query(query)

# Add new function to get trader information
def get_trader_info(user_address, db_gold):
    """Get trader source information from helius_addresses table"""
    if not user_address:
        return None, None
    
    query = f"""
    SELECT address, source
    FROM helius_addresses
    WHERE address = '{user_address}'
    ORDER BY last_updated DESC
    LIMIT 1
    """
    
    result = db_gold.execute_query(query, df=True)
    
    if result.empty:
        return user_address, None
    
    return result.iloc[0]['address'], result.iloc[0]['source']

# Update the transaction status function to also return user_address
def update_transaction_status(signature, notification_sent, db_silver):
    """Update transaction notification status and get user_address"""
    # First, get the user_address for this transaction
    user_query = f"""
    SELECT user_address 
    FROM helius_txns_clean
    WHERE signature = '{signature}'
    LIMIT 1
    """
    user_result = db_silver.execute_query(user_query, df=True)
    user_address = None
    if not user_result.empty:
        user_address = user_result.iloc[0]['user_address']
    
    # Update the notification status
    update_query = f"""
    UPDATE helius_txns_clean
    SET notification_sent = {notification_sent}
    WHERE signature = '{signature}'
    """
    db_silver.execute_query(update_query)
    
    return user_address

# Update the store_notifications function to include trader information
@task(task_id="store_notifications")
def store_notifications(candidates: list) -> str:
    """
    Store notifications for qualifying transactions and generate formatted messages.
    """
    if not candidates:
        logging.info("No candidate notifications to process.")
        return "No new whale notifications triggered."

    # Log candidates for debugging
    logging.info(f"Processing {len(candidates)} candidates")
    
    db_manager = DBManager(db_name='solana_insiders_gold')
    db_silver = DBManager(db_name='solana_insiders_silver')
    
    # Group candidates by token to avoid duplicate notifications
    token_groups = {}
    for candidate in candidates:
        token = candidate.get('token')
        if token not in token_groups:
            token_groups[token] = candidate
    
    # Process each unique token
    new_notifications = []
    for token, candidate in token_groups.items():
        
        # Get user_address and update transaction status
        user_address = update_transaction_status(candidate.get('signature'), True, db_silver)
        
        # Get trader source information
        trader_address, trader_source = get_trader_info(user_address, db_manager)
        
        # Store the notification
        try:
            # Format for insertion
            notification_record = {
                'token_address': token,
                'timestamp': candidate.get('timestamp'),
                'token_name': candidate.get('token_name'),
                'symbol': candidate.get('symbol'),
                'buyers_1h': candidate.get('buyers_1h', 0),
                'buyers_6h': candidate.get('buyers_6h', 0),
                'buyers_24h': candidate.get('buyers_24h', 0),
                'sellers_1h': candidate.get('sellers_1h', 0),
                'sellers_6h': candidate.get('sellers_6h', 0),
                'sellers_24h': candidate.get('sellers_24h', 0),
                'transaction_type': candidate.get('transaction_type', ''),
                'signature': candidate.get('signature'),
                'snifscore': candidate.get('snifscore', 0),
                'market_cap': candidate.get('market_cap', 0),
                'follower_count': candidate.get('follower_count', 0),
                'x_handle': candidate.get('x_handle', ''),
                'deploy_time': candidate.get('deploy_time'),
                'trader_address': trader_address,
                'trader_source': trader_source
            }
            
            # Insert into database
            df_notification = pd.DataFrame([notification_record])
            db_manager.insert_raw_dataframe("whale_notifications", df_notification)
            
            # Add to list of new notifications to format
            new_notifications.append(notification_record)
            logging.info(f"Stored notification for {candidate.get('token_name')}")
            
        except Exception as e:
            logging.error(f"Error storing notification for {token}: {e}")
    
    # Generate notification messages
    if not new_notifications:
        logging.info("No new notifications to send.")
        return "No new whale notifications triggered."
    
    messages = []
    for notification in new_notifications:
        message = format_notification_message(notification)
        messages.append(message)
        logging.info(f"Generated message for {notification.get('token_name')}")
    
    # Join messages with separator
    joined_messages = "\n\n---\n\n".join(messages)
    logging.info(f"Generated combined notification message of length {len(joined_messages)}")
    
    db_manager.quit()
    db_silver.quit()
    
    return joined_messages