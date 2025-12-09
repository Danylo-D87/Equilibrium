from typing import Dict
import pandas as pd


def calculate_ib_mid_retest(
        df: pd.DataFrame,
        ib_high: float,
        ib_low: float
) -> Dict[str, bool]:
    """
    Checks if the price retraced to the IB Mid level AFTER breaking out of the IB range.

    Logic:
    1. Find the first candle that closed (or wicked) outside the IB limits.
    2. Check if the price touched IB Mid at any time starting from that candle onwards.
    """
    if df.empty:
        return {"hit_ib_mid": False}

    ib_mid = (ib_high + ib_low) / 2

    # 1. Identify breakout moments (High > IBH or Low < IBL)
    # Note: Checks candle High/Low to catch wicks
    breakout_mask = (df['high'] > ib_high) | (df['low'] < ib_low)

    # If no breakout occurred, a post-breakout retest is impossible
    if not breakout_mask.any():
        return {"hit_ib_mid": False}

    # 2. Find the time of the FIRST breakout
    # idxmax returns the index of the first True value
    first_breakout_idx = breakout_mask.idxmax()

    # 3. Slice data: consider everything starting FROM the first breakout candle
    # (Including the breakout candle itself, to account for "SFP" or immediate rejection)
    post_breakout_df = df.loc[first_breakout_idx:]

    # 4. Check if price touched Mid in this tail
    # Condition: Low <= Mid <= High
    hit_mid_mask = (post_breakout_df['low'] <= ib_mid) & (post_breakout_df['high'] >= ib_mid)

    did_retest = hit_mid_mask.any()

    return {"hit_ib_mid": bool(did_retest)}
