# plugins/custom/tasks/db_tasks.py

import logging
from airflow.utils.trigger_rule import TriggerRule
from typing import Optional
from airflow.models import TaskInstance
import pandas as pd
from typing import List, Dict, Any
from airflow.decorators import task


logger = logging.getLogger(__name__)

@task(pool='birdeye_pool', task_id='process_unique_addresses')
def process_unique_addresses(
    list1: List[Dict[str, Any]],
    list2: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Combines the results of popular whale holds and trades, removes duplicates,
    and returns a list of unique addresses.
    """
    logger = logging.getLogger("airflow.task")

    logger.info("Starting to process unique addresses from holds and trades.")

    # Convert lists of dicts to pandas DataFrames
    df_holds = pd.DataFrame(list1)
    df_trades = pd.DataFrame(list2)

    logger.info(f"Holds DataFrame shape: {df_holds.shape}")
    logger.info(f"Trades DataFrame shape: {df_trades.shape}")

    # Select relevant columns
    if not df_holds.empty:
        df_holds = df_holds[['blockchain', 'address']]
    else:
        df_holds = pd.DataFrame(columns=['blockchain', 'address'])
        logger.warning("Holds DataFrame is empty.")

    if not df_trades.empty:
        df_trades = df_trades[['blockchain', 'address']]
    else:
        df_trades = pd.DataFrame(columns=['blockchain', 'address'])
        logger.warning("Trades DataFrame is empty.")

    # Concatenate and drop duplicates
    unique_df = pd.concat([df_holds, df_trades], axis=0).drop_duplicates()

    logger.info(f"Unique addresses DataFrame shape: {unique_df.shape}")

    # Convert back to list of dicts
    unique_records = unique_df.to_dict(orient='records')

    logger.info(f"Number of unique addresses: {len(unique_records)}")

    return unique_records



def decide_branch(task1, task2, **kwargs) -> str:
    """
    Decides which branch to follow based on the 'is_stale' condition.

    Returns:
        str: The task ID to follow ('fetch_whales' or 'skip_whales').
    """
    ti: Optional[TaskInstance] = kwargs.get('ti')
    if ti is None:
        raise ValueError("TaskInstance (ti) not found in context.")

    is_stale = ti.xcom_pull(task_ids='is_data_stale')
    logger.info(f"Branching decision based on is_stale={is_stale}")

    if is_stale:
        return task1
    else:
        return task2
    
    

@task(task_id='skip_task', trigger_rule=TriggerRule.ONE_SUCCESS)
def skip_task(task_name: str):
    """
    Skips the specified task.
    """
    logger.info(f"Skipping task: {task_name}")
    return f"Skipped {task_name}"