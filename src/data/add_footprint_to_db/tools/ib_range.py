from typing import Dict
from datetime import time
import pandas as pd


def calculate_ib_range_metrics(
        day_df: pd.DataFrame,
        ib_high: float,
        ib_low: float,
        ib_start_time: time
) -> Dict[str, float]:
    """
    Calculates the Initial Balance (IB) Range metrics (USD value and percentage).
    Attempts to find the exact opening price at ib_start_time.
    """

    # 1. Determine the opening price
    # Attempt 1: Exact match at ib_start_time (e.g., 09:30)
    ib_candle = day_df[day_df.index.time == ib_start_time]

    if not ib_candle.empty:
        ib_open = float(ib_candle.iloc[0]['open'])
    else:
        # Attempt 2: Fallback to the first available candle if exact start time is missing
        if not day_df.empty:
            ib_open = float(day_df.iloc[0]['open'])
        else:
            return {"ib_range_usd": 0.0, "ib_range_pct": 0.0}

    # 2. Perform calculations
    range_usd = ib_high - ib_low

    if ib_open == 0:
        range_pct = 0.0
    else:
        range_pct = (range_usd / ib_open) * 100

    return {
        "ib_range_usd": round(range_usd, 4),
        "ib_range_pct": round(range_pct, 4)
    }
