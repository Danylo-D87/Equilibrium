import pandas as pd
from datetime import time
from typing import Dict


def calculate_ib_breakout(day_df: pd.DataFrame, ib_high: float, ib_low: float, ib_end_time: time) -> Dict[str, bool]:
    """
    Checks if the Initial Balance (IB) High or Low was broken AFTER the IB period ended.

    Args:
        day_df: DataFrame with candles for the entire day (index must be datetime).
        ib_high: The maximum price of the Initial Balance.
        ib_low: The minimum price of the Initial Balance.
        ib_end_time: The time when the IB period ends (e.g., 10:30).

    Returns:
        Dict: {'ib_high_broken': bool, 'ib_low_broken': bool}
    """

    # 1. Slice data occurring AFTER the IB end time
    post_ib_data = day_df[day_df.index.time > ib_end_time]

    if post_ib_data.empty:
        # If no data exists after IB (e.g., partial day), assume no breakout
        # print(f"[calculate_ib_breakout] No post-IB data available.")
        return {
            "ib_high_broken": False,
            "ib_low_broken": False
        }

    # 2. Vectorized check (performance optimization)
    # Check if any candle High is greater than IB High
    high_broken = (post_ib_data['high'] > ib_high).any()

    # Check if any candle Low is lower than IB Low
    low_broken = (post_ib_data['low'] < ib_low).any()

    # 3. Return native Python bools (avoid numpy.bool_ for JSON serialization)
    return {
        "ib_high_broken": bool(high_broken),
        "ib_low_broken": bool(low_broken)
    }
