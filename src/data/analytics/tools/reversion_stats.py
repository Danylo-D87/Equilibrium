from typing import Dict
import pandas as pd


def calculate_reversion_stats(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculates the probability of price returning to the IB range after the session closes.
    """
    col_hit = "after_hours_hit_ib"

    if col_hit not in df.columns:
        return {}

    # General probability
    prob = df[col_hit].mean() * 100

    return {
        "prob_return_to_ib_after_session": round(prob, 1)
    }
