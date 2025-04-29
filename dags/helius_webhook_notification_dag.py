from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import logging
from sqlalchemy import text

# Import our custom DB tasks and DBManager
from db_manager import DBManager

# Import the notification tasks
from tasks.notification_tasks import check_whale_notifications, store_notifications

# Import X and SolSniffer tasks - using our updated versions
from tasks.token_tasks import task_fetch_token_data 

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

def branch_on_notification(message, **context):
    """Branch based on whether there are notifications to send"""
    logging.info(f"Branching based on notifications message: {message}")
    
    # Check if the message indicates no notifications
    if isinstance(message, str) and message.strip() == "No new whale notifications triggered.":
        return "skip_notification"
    elif isinstance(message, list):
        # If it's a list, check if it's empty or contains only None values
        valid_messages = [m for m in message if m is not None]
        if not valid_messages:
            return "skip_notification"
    else:
        # If we have any content, send the notification
        return "send_notification"

@dag(
    dag_id='helius_webhook_notification_dag',
    default_args=default_args,
    schedule_interval=None,  # Run only when triggered externally.
    catchup=False,
    tags=['helius', 'transform', 'notification'],
)
def helius_webhook_notification_dag():
    # === Step 1: Data Processing and Upsert ===
    query = "SELECT id, payload, processed FROM helius_hook WHERE processed = false"
    
    # Execute the SQL query at the top level so that its result is unwrapped.
    db = DBManager(db_name="solana_insiders_bronze")
    raw_rows = db.execute_query(query, df=False)
    
    @task(task_id="process_and_upsert_txns")
    def process_and_upsert_txns(raw_rows: list) -> list:
        logger.info("Processing %d raw rows.", len(raw_rows))
        transformed = []
        for row in raw_rows:
            row_id = row.get("id")
            payload_val = row.get("payload")
            if isinstance(payload_val, str):
                try:
                    payload_val = json.loads(payload_val)
                except Exception as e:
                    logger.error("Error parsing JSON for id %s: %s", row_id, e)
                    continue

            if not isinstance(payload_val, list) or len(payload_val) == 0:
                logger.warning("Row id %s: payload is empty or not a list.", row_id)
                continue

            tx = payload_val[0]
            token_transfers = tx.get("tokenTransfers", [])
            if not token_transfers:
                logger.warning("Row id %s: no tokenTransfers found.", row_id)
                continue

            first_tt = token_transfers[0]
            last_tt = token_transfers[-1]
            ts = tx.get("timestamp")
            ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else None

            transformed_record = {
                "raw_id": row_id,
                "user_address": first_tt.get("fromUserAccount"),
                "swapfromtoken": first_tt.get("mint"),
                "swapfromamount": first_tt.get("tokenAmount"),
                "swaptotoken": last_tt.get("mint"),
                "swaptoamount": last_tt.get("tokenAmount"),  # Corrected field name
                "signature": tx.get("signature"),
                "source": tx.get("source"),
                "timestamp": ts_str,
                "processed": False,
                "notification_sent": False,
            }
            transformed.append(transformed_record)
        logger.info("Transformation complete. Transformed %d records.", len(transformed))

        if not transformed:
            logger.info("No rows processed.")
            return []

        # Upsert the transformed data into helius_txns_clean.
        df = pd.DataFrame(transformed)
        # Update to use the new database name
        dbm = DBManager(db_name="solana_insiders_silver")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS helius_txns_clean (
                raw_id INT,
                user_address VARCHAR(255),
                swapFromToken VARCHAR(255),
                swapFromAmount NUMERIC,
                swapToToken VARCHAR(255),
                swapToAmount NUMERIC,
                signature VARCHAR(255) PRIMARY KEY,
                source VARCHAR(255),
                timestamp TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                notification_sent BOOLEAN DEFAULT FALSE
            )
        """
        
        # filter out source of pump_fun so downstream fetching dont break
        df = df[df["source"] != "PUMP_FUN"]
        
        # Execute the DDL and ignore the lack of returned rows.
        dbm.execute_query(create_table_sql)
        dbm.insert_raw_dataframe("helius_txns_clean", df)
        
        # Mark raw rows as processed in bronze db.
        update_sql = "UPDATE helius_hook SET processed = true WHERE id = :id"
        for record in transformed:
            try:
                with db.engine.begin() as conn:
                    conn.execute(text(update_sql), {"id": record["raw_id"]})
                logger.info("Marked record %s as processed.", record.get("raw_id"))
            except Exception as e:
                logger.error("Error marking record %s as processed: %s", record.get("raw_id"), e)
        
        # Extract unique token addresses for further processing
        token_set = set()
        for record in transformed:
            if record.get("swapfromtoken"):
                token_set.add(record.get("swapfromtoken"))
            if record.get("swaptotoken"):
                token_set.add(record.get("swaptotoken"))
        
        unique_tokens = list(token_set)
        logger.info(f"Extracted {len(unique_tokens)} unique token addresses for processing.")
        
        dbm.quit()
        
        # log the unique tokens
        logger.info(f"Unique tokens to process: {unique_tokens}")
        return unique_tokens
    
    token_list = process_and_upsert_txns(raw_rows)
    
    # Replace all these tasks with the single consolidated task
    updated_tokens = task_fetch_token_data(token_list)
    
    # Notifications
    candidates = check_whale_notifications()
    notifications_message = store_notifications(candidates)
    
    branch = BranchPythonOperator(
        task_id='branch_notification',
        python_callable=branch_on_notification,
        op_kwargs={'message': "{{ ti.xcom_pull(task_ids='store_notifications') }}"},
        provide_context=True
    )
    
    send_notification = SlackWebhookOperator(
        task_id='send_notification',
        slack_webhook_conn_id='slack_webhook',  # Must match your Airflow connection name.
        message="{{ ti.xcom_pull(task_ids='store_notifications') }}",
        channel="#alpha-notifications"
    )
    
    skip_notification = EmptyOperator(task_id='skip_notification')
    
    # === Set Task Dependencies ===
    token_list >> updated_tokens >> candidates >> notifications_message >> branch
    branch >> [send_notification, skip_notification]

# Instantiate the DAG.
dag_instance = helius_webhook_notification_dag()