# utils/data_utils.py

import re
import pandas as pd
from pandas.api.types import is_unsigned_integer_dtype
from typing import Dict, List  # Add this import

import random

def get_random_pause() -> float:
    """
    Returns a random float between 2 and 3.5 seconds.
    Useful for introducing a realistic delay between page navigations
    to avoid suspicion or rate-limiting.
    """
    return random.uniform(2, 3.5)

def cast_unsigned_to_signed(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if is_unsigned_integer_dtype(df[col]):
            df[col] = df[col].astype('int64')
    return df


def camel_to_snake(name: str) -> str:
    """
    Convert camelCase string to snake_case.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


