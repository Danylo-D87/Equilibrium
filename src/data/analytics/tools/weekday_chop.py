from typing import Dict, Any
import pandas as pd

WEEKDAYS_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def _calculate_subset(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    """
    Helper function to calculate 'Chop' (Two-Sided) statistics for a specific prefix.
    """
    col_high = f"{prefix}high_broken"
    col_low = f"{prefix}low_broken"

    if col_high not in df.columns or col_low not in df.columns:
        return {}

    # Calculate Two Sided (High AND Low broken)
    is_chop = df[col_high] & df[col_low]

    # Group by Weekday
    grouped = is_chop.groupby(df["weekday"]).mean() * 100

    result = {}
    for day in WEEKDAYS_ORDER:
        val = grouped.get(day, 0.0)

        # Count of sessions for this specific day
        count = len(df[df["weekday"] == day])

        result[day] = {
            "two_sided_prob": round(val, 1),
            "sessions_count": count
        }
    return result


def calculate_weekday_chop_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculates the probability of a Two-Sided Breakout (Choppy Day) by day of the week.
    Returns structure: { "session": {...}, "full_day": {...} }
    """
    if df.empty or "date" not in df.columns:
        return {"session": {}, "full_day": {}}

    # Add weekday column if missing
    if "weekday" not in df.columns:
        df["weekday"] = pd.to_datetime(df["date"]).dt.day_name()

    return {
        "session": _calculate_subset(df, prefix="session_"),
        "full_day": _calculate_subset(df, prefix="full_")
    }
