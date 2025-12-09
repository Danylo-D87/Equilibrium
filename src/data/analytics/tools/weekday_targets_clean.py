from typing import Dict, Any
import pandas as pd

WEEKDAYS_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def _calculate_subset_clean(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    """
    Calculates targets EXCLUDING days with a Two-Sided breakout (Choppy days).
    """
    col_high = f"{prefix}high_broken"
    col_low = f"{prefix}low_broken"

    # Column Mapping
    cols_map = {
        f"{prefix}ext_05x": "hit_05x_prob",
        f"{prefix}ext_1x": "hit_1x_prob",
        f"{prefix}ext_2x": "hit_2x_prob"
    }

    # Check for column existence
    if col_high not in df.columns or col_low not in df.columns:
        return {}

    # === MAIN FILTER ===
    # We keep only rows where we did NOT break both sides.
    # Logic: (High Broken AND Low Broken) == False
    is_two_sided = df[col_high] & df[col_low]
    clean_df = df[~is_two_sided]  # Tilde (~) implies logic NOT

    if clean_df.empty:
        return {day: {k: 0.0 for k in cols_map.values()} for day in WEEKDAYS_ORDER}

    # Standard logic applied to clean_df
    grouped = clean_df.groupby("weekday")[list(cols_map.keys())].mean() * 100

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

        # Optional: Add count to see how many "clean" days occurred
        day_data["clean_sessions_count"] = len(clean_df[clean_df["weekday"] == day])

        result[day] = day_data

    return result


def calculate_weekday_targets_clean_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculates target statistics excluding days with dual breakouts.
    """
    if df.empty or "date" not in df.columns:
        return {"session": {}, "full_day": {}}

    if "weekday" not in df.columns:
        df["weekday"] = pd.to_datetime(df["date"]).dt.day_name()

    return {
        "session": _calculate_subset_clean(df, prefix="session_"),
        "full_day": _calculate_subset_clean(df, prefix="full_")
    }
