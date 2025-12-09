import pandas as pd
from datetime import time


def calculate_ib_volume(
        day_df: pd.DataFrame,
        ib_start_time: time,
        ib_end_time: time
) -> float:
    """
    Calculates the total volume traded during the Initial Balance period.

    Args:
        day_df: DataFrame of the day (index - datetime).
        ib_start_time: IB Start (e.g., 09:30).
        ib_end_time: IB End (e.g., 10:30).
    """
    if day_df.empty:
        return 0.0

    # Filter data strictly by IB time
    # 'between_time' is inclusive, which is acceptable for volume aggregation
    ib_data = day_df.between_time(ib_start_time, ib_end_time)

    total_volume = float(ib_data['volume'].sum())

    return float(total_volume)
