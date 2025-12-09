from typing import Dict, Any
import pandas as pd

WEEKDAYS_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def _calculate_subset(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    """
    Helper function to calculate target hits (0.5x, 1x, 2x) for a specific prefix.
    """
    # Map: DataFrame Column -> Response Key
    cols_map = {
        f"{prefix}ext_05x": "hit_05x_prob",
        f"{prefix}ext_1x": "hit_1x_prob",
        f"{prefix}ext_2x": "hit_2x_prob"
    }

    existing_cols = [c for c in cols_map.keys()]

    if not existing_cols:
        return {}

    # Grouping
    grouped = df.groupby("weekday")[existing_cols].mean() * 100

    result = {}
    for day in WEEKDAYS_ORDER:
        day_data = {}

        if day in grouped.index:
            row = grouped.loc[day]
            for raw_col, clean_key in cols_map.items():
                day_data[clean_key] = round(row.get(raw_col, 0), 1)
        else:
            for clean_key in cols_map.values():
                day_data[clean_key] = 0.0

        result[day] = day_data

    return result


def calculate_weekday_targets_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculates the probability of hitting extension targets by day of the week.
    Returns structure: { "session": {...}, "full_day": {...} }
    """
    if df.empty or "date" not in df.columns:
        return {"session": {}, "full_day": {}}

    if "weekday" not in df.columns:
        df["weekday"] = pd.to_datetime(df["date"]).dt.day_name()

    return {
        "session": _calculate_subset(df, prefix="session_"),
        "full_day": _calculate_subset(df, prefix="full_")
    }
