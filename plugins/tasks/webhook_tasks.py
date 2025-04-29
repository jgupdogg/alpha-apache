from airflow.decorators import task
from datetime import datetime
import os
import logging

from dotenv import load_dotenv
from db_manager import DBManager

# Import the Helius WebhooksAPI
from helius import WebhooksAPI
import pandas as pd

@task
def register_or_update_webhook():
    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    # Retrieve required environment variables
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        raise Exception("HELIUS_API_KEY not set in environment.")
    
    # Initialize Helius API client
    webhooks_api = WebhooksAPI(api_key)
    
    # Get existing webhooks first
    existing_webhooks = webhooks_api.get_all_webhooks()
    
    # Determine webhook URL - either from existing webhook or environment
    webhook_url = None
    if existing_webhooks and len(existing_webhooks) > 0:
        # Use the URL from the first existing webhook
        webhook_url = existing_webhooks[0].get("webhookURL")
        logging.info(f"Using existing webhook URL: {webhook_url}")
    else:
        # Fall back to environment variable if no existing webhooks
        webhook_url = os.getenv("WEBHOOK_URL")
        if not webhook_url:
            raise Exception("No existing webhooks found and WEBHOOK_URL not set in environment.")
        logging.info(f"No existing webhooks found. Using WEBHOOK_URL from environment: {webhook_url}")


    # Fetch the latest account addresses
    def fetch_addresses_from_db() -> list:
        
        try:
            # Connect to bronze database
            bronze_db = DBManager(db_name="solana_insiders_bronze")

            # Query dex3 traders
            dex3_query = """
            WITH ranked_traders AS (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY address ORDER BY updated_at DESC) as rank
            FROM dex3_raw_traders
            WHERE 
                pnl_30d > 0
                AND win_rate_30d > 0.5
                AND (txns_30d / 30.0) < 10
                AND (txns_30d / 30.0) > 0.25
                AND updated_at::timestamp > (CURRENT_DATE - INTERVAL '4 weeks')
            )
            SELECT
            address,
            'dex3' AS source,
            updated_at::text AS last_updated
            FROM ranked_traders
            WHERE rank = 1
            ORDER BY last_updated DESC;
            """
            dex3_traders = bronze_db.execute_query(dex3_query, df=True)

            # Query gmgn traders
            gmgn_query = """
            WITH ranked_traders AS (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY wallet_address ORDER BY capture_time DESC) as rank
            FROM gmgn_raw_traders
            WHERE 
                realized_profit_30d > 0
                AND (winrate_7d > 0.5 OR winrate_7d IS NULL)
                AND (txs_30d / 30.0) BETWEEN 0.25 AND 10
                AND capture_time > (CURRENT_DATE - INTERVAL '4 weeks')
            )
            SELECT
            address,
            'gmgn' AS source,
            capture_time::text AS last_updated
            FROM ranked_traders
            WHERE rank = 1
            ORDER BY last_updated DESC;
            """
            gmgn_traders = bronze_db.execute_query(gmgn_query, df=True)

            # Connect to silver database
            silver_db = DBManager(db_name="solana_insiders_silver")

            # Query watchlist traders
            watchlist_query = """
            WITH watchlist_traders AS (
            SELECT 
                wallet as address,
                total_pnl,
                realized_pnl,
                trades_per_day,
                fetched_at,
                ROW_NUMBER() OVER (PARTITION BY wallet ORDER BY fetched_at DESC) as rank
            FROM successful_trader_pnl
            WHERE 
                total_pnl > 0
                AND realized_pnl > 0
                AND trades_per_day < 10
                AND fetched_at > (CURRENT_DATE - INTERVAL '4 weeks')
            )
            SELECT
            address,
            'watchlist' AS source,
            fetched_at::text AS last_updated
            FROM watchlist_traders
            WHERE rank = 1
            ORDER BY last_updated DESC;
            """
            watchlist_traders = silver_db.execute_query(watchlist_query, df=True)

            # Combine all results
            all_traders = pd.concat([dex3_traders, gmgn_traders, watchlist_traders], ignore_index=True)

            # Sort by last_updated (descending) and keep only the most recent entry for each trader_id
            all_traders = all_traders.sort_values('last_updated', ascending=False)
            all_traders = all_traders.drop_duplicates(subset=['address'], keep='first')

            # Sort by source and last_updated for display
            all_traders = all_traders.sort_values(['source', 'last_updated'], ascending=[True, False])
                    
            db=DBManager(db_name="solana_insiders_gold")    
            db.insert_raw_dataframe(
                table_name='helius_addresses',
                df=pd.DataFrame(all_traders)
            )            
            
            addresses = all_traders['address'].to_list()
            logging.info(f"Fetched {len(addresses)} trader addresses from the database.")
            return addresses
        
        except Exception as e:
            logging.error(f"Error fetching trader addresses: {e}")
            import traceback
            logging.error(traceback.format_exc())
            raise
        finally:
            db.quit()

        
    account_addresses = fetch_addresses_from_db()
    if not account_addresses:
        raise Exception("No account addresses were found in the database.")
    logging.info("Fetched %d account addresses from the database.", len(account_addresses))

    transaction_types = ["SWAP"]
    webhook_type = os.getenv("WEBHOOK_TYPE", "raw")
    auth_header = os.getenv("WEBHOOK_AUTH_HEADER")  # Optional

    # Find or create a webhook
    target_webhook = None
    for wh in existing_webhooks:
        # We'll update the first webhook we find, preferring one with a matching URL
        if wh.get("webhookURL") == webhook_url:
            target_webhook = wh
            break
    
    # If no webhook with matching URL, use the first one if available
    if not target_webhook and existing_webhooks:
        target_webhook = existing_webhooks[0]
        logging.info(f"No webhook with URL {webhook_url} found. Using webhook ID: {target_webhook['webhookID']}")

    if target_webhook:
        webhook_id = target_webhook["webhookID"]
        updated_webhook = webhooks_api.edit_webhook(
            webhook_id=webhook_id,
            webhook_url=webhook_url,
            transaction_types=transaction_types,
            account_addresses=account_addresses,
            webhook_type=webhook_type,
            auth_header=auth_header
        )
        logging.info("Updated webhook: %s", updated_webhook)
    else:
        try:
            new_webhook = webhooks_api.create_webhook(
                webhook_url=webhook_url,
                transaction_types=transaction_types,
                account_addresses=account_addresses,
                webhook_type=webhook_type,
                auth_header=auth_header
            )
            logging.info("Created webhook: %s", new_webhook)
        except ValueError as e:
            error_message = str(e)
            if "reached webhook limit" in error_message:
                logging.warning("Webhook limit reached. Attempting to update an existing webhook instead.")
                if existing_webhooks:
                    fallback_webhook = existing_webhooks[0]
                    updated_webhook = webhooks_api.edit_webhook(
                        webhook_id=fallback_webhook["webhookID"],
                        webhook_url=webhook_url,
                        transaction_types=transaction_types,
                        account_addresses=account_addresses,
                        webhook_type=webhook_type,
                        auth_header=auth_header
                    )
                    logging.info("Updated fallback webhook: %s", updated_webhook)
                else:
                    logging.error("No existing webhook to update even though limit was reached.")
                    raise
            else:
                raise