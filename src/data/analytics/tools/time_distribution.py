from typing import Dict, Any
import pandas as pd
from datetime import datetime


def _bucket_time_30m(time_str: str) -> str | None:
    """Rounds time to the nearest 30-minute bucket (e.g., 10:45 -> 10:30)."""
    if not isinstance(time_str, str):
        return None
    try:
        t = datetime.strptime(time_str, "%H:%M")
        # If minutes < 30, bucket to 00, otherwise to 30
        minute = 0 if t.minute < 30 else 30
        return f"{t.hour:02d}:{minute:02d}"
    except ValueError:
        return None


def _calculate_distribution(series: pd.Series, end_time_str: str) -> Dict[str, float]:
    START_TIME_STR = "10:30"

    # 1. Prepare Data
    times = series.dropna()
    valid_times = times[(times >= START_TIME_STR) & (times <= end_time_str)]

    # [IMPORTANT] Do not return {} here even if valid_times is empty.
    # We want to return a full grid initialized with zeros.

    buckets = valid_times.apply(_bucket_time_30m)
    counts = buckets.value_counts()
    total_events = len(valid_times)

    # 2. Generate FULL Grid
    start_h = int(START_TIME_STR.split(":")[0])
    start_m = int(START_TIME_STR.split(":")[1])
    grid_end_h = int(end_time_str.split(":")[0])  # e.g., 23

    dist = {}

    for h in range(start_h, grid_end_h + 1):
        for m in [0, 30]:
            if h == start_h and m < start_m:
                continue  # Skip times before start (e.g., before 10:30)

            time_key = f"{h:02d}:{m:02d}"
            if time_key > end_time_str:
                break

            # If no data exists, count is 0
            count = counts.get(time_key, 0)
            percentage = (count / total_events * 100) if total_events > 0 else 0
            dist[time_key] = round(percentage, 1)

    return dist


def calculate_time_distribution(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Main function. Returns { "session": {...}, "full_day": {...} }
    """
    if df.empty:
        return {"session": {}, "full_day": {}}

    # Event Map: { Frontend Key : [Database Columns] }
    # For Breakout, we combine High and Low as we are interested in "time of balance break"
    events_map = {
        "breakout": ["time_break_high", "time_break_low"],
        "hit_05x": ["time_hit_05x"],
        "hit_1x": ["time_hit_1x"],
        "hit_2x": ["time_hit_2x"]
    }

    result = {
        "session": {},
        "full_day": {}
    }

    for key, cols in events_map.items():
        # Aggregate data from all columns (e.g., Break High + Break Low)
        combined_series = pd.Series(dtype=object)

        for col in cols:
            if col in df.columns:
                combined_series = pd.concat([combined_series, df[col]])

        if combined_series.empty:
            continue

        # Calculate for Session (until 16:00)
        result["session"][key] = _calculate_distribution(combined_series, end_time_str="16:00")

        # Calculate for Full Day (until 23:59)
        result["full_day"][key] = _calculate_distribution(combined_series, end_time_str="23:59")

    return result
