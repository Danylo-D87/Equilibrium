from typing import Dict, Any
import pandas as pd


def _calculate_subset(df: pd.DataFrame, prefix: str) -> Dict[str, float]:
    """
    Calculates the probability of retesting IB Mid GIVEN that a breakout occurred.
    """
    col_hit_mid = f"{prefix}hit_ib_mid"
    col_high_broken = f"{prefix}high_broken"
    col_low_broken = f"{prefix}low_broken"

    if col_hit_mid not in df.columns:
        return {}

    # 1. Find all days where a breakout occurred (Denominator)
    # We don't care about direction (Up or Down), just that the range was broken.
    breakout_days_df = df[(df[col_high_broken] == True) | (df[col_low_broken] == True)]

    total_breakouts = len(breakout_days_df)

    if total_breakouts == 0:
        return {"prob_ib_mid_retest": 0.0}

    # 2. Count days where a retest happened (Numerator)
    # The logic in the extraction Tool already guarantees hit_ib_mid=True only occurs post-breakout.
    retest_count = len(breakout_days_df[breakout_days_df[col_hit_mid] == True])

    # 3. Calculate Percentage
    prob = (retest_count / total_breakouts) * 100

    return {
        "prob_ib_mid_retest": round(prob, 1)
    }


def calculate_retest_stats(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"session": {}, "full_day": {}}

    return {
        "session": _calculate_subset(df, "session_"),
        "full_day": _calculate_subset(df, "full_")
    }
