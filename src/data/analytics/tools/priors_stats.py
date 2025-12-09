from typing import Dict, Any
import pandas as pd


def _calculate_subset(df: pd.DataFrame, prefix: str) -> Dict[str, float]:
    """
    Calculates statistics regarding Prior Day Levels (PDH/PDL).
    """
    # Dynamically form column names (e.g., session_hit_pdh or full_hit_pdh)
    col_hit_pdh = f"{prefix}hit_pdh"      # Did we touch PDH?
    col_hit_pdl = f"{prefix}hit_pdl"      # Did we touch PDL?
    col_high_broken = f"{prefix}high_broken"  # IB High Broken? (Condition for conversion)
    col_low_broken = f"{prefix}low_broken"    # IB Low Broken? (Condition for conversion)

    # Exit if required columns are missing
    if col_hit_pdh not in df.columns:
        return {}

    # IMPORTANT: Filter days. We only need days where PDH exists (not NaN).
    # For the very first day in the database, PDH is None, so we exclude it.
    valid_df = df[df['pdh'].notna()].copy()

    # If no data remains after filtering, return empty dict
    if valid_df.empty:
        return {}

    # Total valid days for percentage calculation
    total_valid_days = len(valid_df)

    # --- 1. SIMPLE PROBABILITIES (General Probability) ---
    # Question: "In what % of cases does price touch PDH during the day?" (Context independent)

    # Count days where hit_pdh == True
    count_hit_pdh = len(valid_df[valid_df[col_hit_pdh] == True])
    # Divide by total days and multiply by 100
    prob_hit_pdh = (count_hit_pdh / total_valid_days * 100) if total_valid_days > 0 else 0.0

    # Same for PDL
    count_hit_pdl = len(valid_df[valid_df[col_hit_pdl] == True])
    prob_hit_pdl = (count_hit_pdl / total_valid_days * 100) if total_valid_days > 0 else 0.0

    # --- 2. CONDITIONAL PROBABILITIES (Conversion Rate) ---
    # Question: "IF we have already broken IB High, what is the probability we reach PDH?"

    # Step A: Find all days where IB High was broken (Denominator)
    ibh_broken_days = valid_df[valid_df[col_high_broken] == True]
    total_ibh = len(ibh_broken_days)

    # Step B: Among broken days, find those that actually hit PDH (Numerator)
    hit_pdh_cond_count = len(ibh_broken_days[ibh_broken_days[col_hit_pdh] == True])

    # Step C: Calculate % (Conversion from Breakout to Target)
    prob_pdh_cond = (hit_pdh_cond_count / total_ibh * 100) if total_ibh > 0 else 0.0

    # Same logic for the Low side (IB Low Break -> PDL Hit)
    ibl_broken_days = valid_df[valid_df[col_low_broken] == True]
    total_ibl = len(ibl_broken_days)
    hit_pdl_cond_count = len(ibl_broken_days[ibl_broken_days[col_hit_pdl] == True])

    prob_pdl_cond = (hit_pdl_cond_count / total_ibl * 100) if total_ibl > 0 else 0.0

    # Return all 4 metrics
    return {
        # Simple probabilities (how often we test levels)
        "prob_hit_pdh": round(prob_hit_pdh, 1),
        "prob_hit_pdl": round(prob_hit_pdl, 1),

        # Conditional probabilities (breakout efficiency)
        "prob_pdh_if_ibh_broken": round(prob_pdh_cond, 1),
        "prob_pdl_if_ibl_broken": round(prob_pdl_cond, 1)
    }


def calculate_priors_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Main function. Splits calculations into Session and Full Day.
    """
    if df.empty:
        return {"session": {}, "full_day": {}}

    return {
        "session": _calculate_subset(df, "session_"),
        "full_day": _calculate_subset(df, "full_")
    }
