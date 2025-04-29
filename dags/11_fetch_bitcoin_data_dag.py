################################

# This is a simple DAG to query the FMP API for bitcoin volume and price data, and store it locally

###############################


# dags/fetch_bitcoin_data_dag.py
import os
import logging
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Import your DBManager from your db_manager module
from db_manager import DBManager

# Default DAG arguments
default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 25),  # Adjust the start date as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

# Schedule: run at minute 1 of every hour (i.e., a minute after the hour)
@dag(
    dag_id='fetch_bitcoin_data_dag',
    default_args=default_args,
    schedule_interval='1 * * * *',  # Every hour at minute 1
    catchup=False,
    tags=['bitcoin', 'price', 'hourly']
)
def fetch_bitcoin_data_dag():

    @task(task_id="fetch_bitcoin_data")
    def fetch_bitcoin_data() -> list:
        """
        Pings the financialmodelingprep.com API endpoint for the BTCUSD hourly chart
        and returns the data as a list of dictionaries.
        """
        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            raise ValueError("FMP_API_KEY is not set in the environment variables.")
            
        url = f"https://financialmodelingprep.com/api/v3/historical-chart/1hour/BTCUSD?&apikey={api_key}"
        logging.info(f"Fetching data from {url}")
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        data = response.json()
        logging.info(f"Fetched {len(data)} records from API.")
        return data

    @task(task_id="format_data")
    def format_data(data: list) -> list:
        """
        Converts the API JSON data into a DataFrame, converts the 'date' field (assumed to be
        Eastern Standard Time) to a timestamp string in the format "YYYY-MM-DD HH:MM:SS-05",
        and returns the formatted data as a list of dictionaries.
        """
        if not data:
            logging.info("No data received to format.")
            return []

        df = pd.DataFrame(data)
        # Ensure that the 'date' column is present
        if 'date' not in df.columns:
            raise KeyError("Expected 'date' field in API response.")

        # Convert the 'date' column to datetime (assuming the string is in "%Y-%m-%d %H:%M:%S")
        df['timestamp'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S")

        # Localize to US/Eastern time (which gives an offset, e.g., -05:00)
        eastern = pytz.timezone("US/Eastern")
        df['timestamp'] = df['timestamp'].apply(lambda dt: eastern.localize(dt))

        # Format the timestamp as "YYYY-MM-DD HH:MM:SS-05"
        # The default strftime with %z produces "-0500"; we trim the last two zeros.
        df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S%z").apply(lambda s: s[:-2])

        # Optionally drop the original 'date' column since we now have 'timestamp'
        df.drop(columns=["date"], inplace=True)

        logging.info("Data formatted successfully.")
        return df.to_dict(orient="records")

    @task(task_id="get_latest_timestamp")
    def get_latest_timestamp() -> str:
        """
        Retrieves the latest (maximum) timestamp from the target database table.
        Returns None if the table is empty.
        """
        db_manager = DBManager()
        query = "SELECT MAX(timestamp) as latest_ts FROM btc_price_hourly;"
        result_df = db_manager.execute_query(query, df=True)
        if result_df.empty or pd.isna(result_df.iloc[0]['latest_ts']):
            logging.info("No existing timestamp found in btc_price_hourly table.")
            return None
        latest_ts = result_df.iloc[0]['latest_ts']
        logging.info(f"Latest timestamp in DB: {latest_ts}")
        return latest_ts

    @task(task_id="filter_new_data")
    def filter_new_data(formatted_data: list, latest_ts: str) -> list:
        """
        Filters out any rows from the formatted data that have a timestamp less than or equal to the
        latest timestamp already stored in the database.
        """
        from datetime import datetime

        def parse_ts(ts_str: str) -> datetime:
            # Append "00" to the offset (e.g., "-05" becomes "-0500") so that strptime can parse it.
            return datetime.strptime(ts_str + "00", "%Y-%m-%d %H:%M:%S%z")

        if latest_ts is None:
            logging.info("No latest timestamp provided. All records are considered new.")
            return formatted_data

        latest_dt = parse_ts(latest_ts)
        new_data = []
        for row in formatted_data:
            row_dt = parse_ts(row['timestamp'])
            if row_dt > latest_dt:
                new_data.append(row)
        logging.info(f"Filtered data: {len(new_data)} new records found out of {len(formatted_data)} total.")
        return new_data

    @task(task_id="insert_data")
    def insert_data(new_data: list) -> str:
        """
        Inserts the new data (if any) into the 'btc_price_hourly' table using the DBManager.
        """
        if not new_data:
            logging.info("No new data to insert.")
            return "No new data inserted."

        df = pd.DataFrame(new_data)
        db_manager = DBManager()
        table_name = "btc_price_hourly"
        logging.info(f"Inserting {len(df)} new records into table '{table_name}'.")
        db_manager.insert_raw_dataframe(table_name, df)
        return f"Inserted {len(df)} new records into '{table_name}'."

    # Define the task pipeline
    raw_data = fetch_bitcoin_data()
    formatted = format_data(raw_data)
    latest_timestamp = get_latest_timestamp()
    new_records = filter_new_data(formatted, latest_timestamp)
    insert_result = insert_data(new_records)

    # Optionally, you can set dependencies or log the final result.
    insert_result  # This ensures that the insert_data task is executed.

# Instantiate the DAG
dag_instance = fetch_bitcoin_data_dag()
