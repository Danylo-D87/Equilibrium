"""
IB Breakout Statistics Calculator.
Calculates breakout statistics for both the Intraday Session and Full Day.
"""
import pandas as pd
from typing import Dict, Any


def _calculate_subset(high_series: pd.Series, low_series: pd.Series) -> Dict[str, float]:
    """
    Helper function. Takes two boolean series (High broken and Low broken)
    and calculates probability metrics.
    """
    # If no data exists (length is 0), return empty dict
    if len(high_series) == 0:
        return {}

    # Enforce boolean type to avoid errors with 0/1 integers or None
    high = high_series.astype(bool)
    low = low_series.astype(bool)

    # 1. Independent Probabilities
    # Simply calculates how often High was broken (regardless of Low)
    high_prob = high.mean() * 100
    low_prob = low.mean() * 100

    # 2. Scenarios (Mutually Exclusive Events)

    # "Two Sided" (Choppy/Rotational): Both High AND Low were broken
    two_sided = (high & low).mean() * 100

    # "One Sided" (Trend): Either High OR Low was broken, but NOT both (XOR)
    one_sided = (high ^ low).mean() * 100

    # "No Breakout" (Inside Day): Neither High NOR Low were broken
    no_breakout = (~(high | low)).mean() * 100

    # Return results rounded to 1 decimal place
    return {
        "break_high_chance": round(high_prob, 1),
        "break_low_chance": round(low_prob, 1),
        "one_sided_chance": round(one_sided, 1),
        "two_sided_chance": round(two_sided, 1),
        "no_breakout_chance": round(no_breakout, 1),
    }


def calculate_breakout_probabilities(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Main function. Splits data into Session and Full Day subsets before calculation.
    """
    # Check for empty DataFrame
    if df.empty:
        return {"session": {}, "full_day": {}}

    # 1. Calculation for SESSION (until 16:00)
    # Extract session columns. Use empty series as fallback to prevent errors.
    session_stats = _calculate_subset(
        df.get('session_high_broken', pd.Series(dtype=bool)),
        df.get('session_low_broken', pd.Series(dtype=bool))
    )

    # 2. Calculation for FULL DAY (until 23:59)
    full_stats = _calculate_subset(
        df.get('full_high_broken', pd.Series(dtype=bool)),
        df.get('full_low_broken', pd.Series(dtype=bool))
    )

    # Return grouped result
    return {
        "session": session_stats,
        "full_day": full_stats
    }
