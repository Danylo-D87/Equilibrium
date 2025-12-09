from typing import Dict
import pandas as pd


def calculate_ib_reversion(
    df: pd.DataFrame,
    ib_high: float,
    ib_low: float
) -> Dict[str, bool]:
    """
    Checks if the price reverted (traded back) into the Initial Balance zone
    (between IB Low and IB High) during the given period.
    """
    if df.empty:
        return {"hit_ib": False}

    # Determine price range for the period
    period_high = df['high'].max()
    period_low = df['low'].min()

    # Intersection check:
    # (Period Low <= IB High) AND (Period High >= IB Low)
    # This implies that at least part of the price action was inside the IB range
    hit_ib = (period_low <= ib_high) and (period_high >= ib_low)

    return {"hit_ib": bool(hit_ib)}
