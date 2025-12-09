import pandas as pd
from datetime import time
from typing import Dict


def calculate_ib_levels(day_df: pd.DataFrame, ib_start_time: time, ib_end_time: time) -> Dict[str, float]:
    """
    Calculates High, Low, and Range for the Initial Balance (IB) period.

    Args:
        day_df: DataFrame with data for a single day.
        ib_start_time: IB start time (e.g., 09:30).
        ib_end_time: IB end time (e.g., 10:29).

    Returns:
        Dict: {'ib_high': float, 'ib_low': float, 'ib_range': float}
        Returns empty dict if no data found.
    """

    # 1. Filter data by time range
    # 'between_time' is inclusive, so configuration should ideally use 10:29 for minute bars
    ib_data = day_df.between_time(ib_start_time, ib_end_time)

    if ib_data.empty:
        return {}

    # 2. Find extremes
    # Important: Cast to float to avoid numpy types (JSON serialization issues)
    ib_high = float(ib_data['high'].max())
    ib_low = float(ib_data['low'].min())
    ib_range = ib_high - ib_low

    return {
        "ib_high": ib_high,
        "ib_low": ib_low,
        "ib_range": ib_range
    }
