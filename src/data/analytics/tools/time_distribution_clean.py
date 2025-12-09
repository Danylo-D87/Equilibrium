from typing import Dict, Any
import pandas as pd
from datetime import datetime


def _bucket_time_30m(time_str: str) -> str | None:
    if not isinstance(time_str, str):
        return None
    try:
        t = datetime.strptime(time_str, "%H:%M")
        minute = 0 if t.minute < 30 else 30
        return f"{t.hour:02d}:{minute:02d}"
    except ValueError:
        return None


def _calculate_distribution(series: pd.Series, end_time_str: str) -> Dict[str, float]:
    START_TIME_STR = "10:30"
    times = series.dropna()
    valid_times = times[(times >= START_TIME_STR) & (times <= end_time_str)]

    buckets = valid_times.apply(_bucket_time_30m)
    counts = buckets.value_counts()
    total_events = len(valid_times)

    start_h, start_m = 10, 30
    grid_end_h = int(end_time_str.split(":")[0])

    dist = {}
    for h in range(start_h, grid_end_h + 1):
        for m in [0, 30]:
            if h == start_h and m < start_m:
                continue
            time_key = f"{h:02d}:{m:02d}"
            if time_key > end_time_str:
                break

            count = counts.get(time_key, 0)
            percentage = (count / total_events * 100) if total_events > 0 else 0
            dist[time_key] = round(percentage, 1)
    return dist


def calculate_time_distribution_clean(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculates time distribution ONLY for days where NO Two-Sided Breakout occurred.
    Filters separately for Session and Full Day.
    """
    if df.empty:
        return {"session": {}, "full_day": {}}

    events_map = {
        "breakout": ["time_break_high", "time_break_low"],
        "hit_05x": ["time_hit_05x"],
        "hit_1x": ["time_hit_1x"],
        "hit_2x": ["time_hit_2x"]
    }

    result = {"session": {}, "full_day": {}}

    # === FILTERING ===
    # 1. For Session: exclude days where session_high_broken AND session_low_broken
    chop_session_mask = df["session_high_broken"] & df["session_low_broken"]
    df_clean_session = df[~chop_session_mask]

    # 2. For Full Day: exclude days where full_high_broken AND full_low_broken
    chop_full_mask = df["full_high_broken"] & df["full_low_broken"]
    df_clean_full = df[~chop_full_mask]

    for key, cols in events_map.items():
        # --- Session Calculation ---
        combined_sess = pd.Series(dtype=object)
        for col in cols:
            if col in df_clean_session.columns:
                combined_sess = pd.concat([combined_sess, df_clean_session[col]])

        if not combined_sess.empty:
            result["session"][key] = _calculate_distribution(combined_sess, end_time_str="16:00")

        # --- Full Day Calculation ---
        combined_full = pd.Series(dtype=object)
        for col in cols:
            if col in df_clean_full.columns:
                combined_full = pd.concat([combined_full, df_clean_full[col]])

        if not combined_full.empty:
            result["full_day"][key] = _calculate_distribution(combined_full, end_time_str="23:59")

    return result
