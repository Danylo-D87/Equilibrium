from typing import Dict, Any
import pandas as pd


def _calculate_subset(df: pd.DataFrame, prefix: str) -> Dict[str, float]:
    """
    Calculates statistics for a specific column set (either session_ or full_).
    """
    # Dynamically generate column names based on prefix (session_ or full_)
    col_05x = f"{prefix}ext_05x"
    col_1x = f"{prefix}ext_1x"      # e.g., session_ext_1x
    col_2x = f"{prefix}ext_2x"      # e.g., session_ext_2x
    col_coeff = f"{prefix}ext_coeff"  # e.g., session_ext_coeff

    # Safety check: ensure required columns exist in the DataFrame
    if col_1x not in df.columns:
        return {}  # Return empty result to avoid KeyError

    # Calculate mean for boolean columns (True=1, False=0).
    # 0.7 * 100 = 70.0%. This gives the probability of the event in percentages.
    prob_05x = df[col_1x].mean() * 100
    prob_1x = df[col_1x].mean() * 100
    prob_2x = df[col_2x].mean() * 100

    # Calculate arithmetic mean for extension coefficients.
    # Indicates how many "IBs" the price travels on average (e.g., 1.45x).
    avg_coeff = df[col_coeff].mean()

    # Return dictionary with rounded values for cleaner display
    return {
        "prob_hit_05x": round(prob_05x, 1),
        "prob_hit_1x": round(prob_1x, 1),      # Rounded to 1 decimal (e.g., 65.4%)
        "prob_hit_2x": round(prob_2x, 1),      # Rounded to 1 decimal (e.g., 20.1%)
        "avg_extension_coeff": round(avg_coeff, 2)  # Rounded to 2 decimals (e.g., 1.45x)
    }


def calculate_extension_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Main entry point. Invokes calculations twice: for the Intraday Session and the Full Day.
    """
    # If no data exists, return empty structure to prevent frontend crashes
    if df.empty:
        return {"session": {}, "full_day": {}}

    # Return dictionary with two keys, calling the helper function with different prefixes
    return {
        "session": _calculate_subset(df, "session_"),  # Calculate for Intraday Session (until 16:00)
        "full_day": _calculate_subset(df, "full_")     # Calculate for Full Day (until 23:59)
    }
