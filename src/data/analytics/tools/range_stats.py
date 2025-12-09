from typing import Dict
import pandas as pd


def calculate_range_volume_stats(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculates average volatility and liquidity metrics for the selected period.
    """
    # If no data exists, return empty dict
    if df.empty:
        return {}

    return {
        # Average IB size in USD (Mean).
        # Indicates the average volatility of the first hour in price points.
        "avg_ib_range_usd": round(df["ib_range_usd"].mean(), 2),

        # Average IB size in Percentage (Mean).
        # A more universal metric allowing comparison across different assets.
        "avg_ib_range_pct": round(df["ib_range_pct"].mean(), 3),

        # Average Volume during the first hour.
        # Converted to int for cleaner display.
        "avg_ib_volume": int(df["ib_vol"].mean())
    }
