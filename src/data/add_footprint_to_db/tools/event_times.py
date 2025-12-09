from typing import Dict, Optional
import pandas as pd


def find_event_times(
        df: pd.DataFrame,
        ib_high: float,
        ib_low: float,
        ib_range: float
) -> Dict[str, Optional[str]]:
    """
    Identifies the exact time (HH:MM) for each breakout type and target hit.
    """
    result = {
        "time_break_high": None,
        "time_break_low": None,
        "time_hit_05x": None,
        "time_hit_1x": None,
        "time_hit_2x": None
    }

    if df.empty:
        return result

    # 1. BREAKOUT HIGH
    mask_high = df['high'] > ib_high
    if mask_high.any():
        result["time_break_high"] = mask_high.idxmax().strftime("%H:%M")

    # 2. BREAKOUT LOW
    mask_low = df['low'] < ib_low
    if mask_low.any():
        result["time_break_low"] = mask_low.idxmax().strftime("%H:%M")

    # --- Targets (Usually taking the first hit in any direction) ---
    targets = {
        "05x_high": ib_high + (0.5 * ib_range),
        "05x_low": ib_low - (0.5 * ib_range),
        "1x_high": ib_high + ib_range,
        "1x_low": ib_low - ib_range,
        "2x_high": ib_high + (2 * ib_range),
        "2x_low": ib_low - (2 * ib_range),
    }

    # Search for the "first occurrence" of hitting a target (High or Low)

    # 0.5x Target
    mask_05 = (df['high'] >= targets["05x_high"]) | (df['low'] <= targets["05x_low"])
    if mask_05.any():
        result["time_hit_05x"] = mask_05.idxmax().strftime("%H:%M")

    # 1.0x Target
    mask_1x = (df['high'] >= targets["1x_high"]) | (df['low'] <= targets["1x_low"])
    if mask_1x.any():
        result["time_hit_1x"] = mask_1x.idxmax().strftime("%H:%M")

    # 2.0x Target
    mask_2x = (df['high'] >= targets["2x_high"]) | (df['low'] <= targets["2x_low"])
    if mask_2x.any():
        result["time_hit_2x"] = mask_2x.idxmax().strftime("%H:%M")

    return result
