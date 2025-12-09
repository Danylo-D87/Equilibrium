from typing import Dict, Optional, Any


def calculate_prior_levels_interaction(
        session_high: float,
        session_low: float,
        previous_day_stats: Optional[Dict[str, float]]
) -> Dict[str, Any]:
    """
    Determines Previous Day High (PDH) and Previous Day Low (PDL) levels
    and checks if they were tested during the current session.

    Args:
        session_high: High of the current session.
        session_low: Low of the current session.
        previous_day_stats: Dictionary {'high': float, 'low': float} from yesterday.
                            Can be None if it's the first day in history.

    Returns:
        Dict containing level prices and boolean flags indicating hits.
    """

    # 1. Handle the FIRST day (when no history exists)
    if previous_day_stats is None:
        return {
            "pdh": None,
            "pdl": None,
            "hit_pdh": False,
            "hit_pdl": False
        }

    # 2. Retrieve levels
    pdh = previous_day_stats.get('high')
    pdl = previous_day_stats.get('low')

    # Safety check for missing/corrupt data
    if pdh is None or pdl is None:
        return {
            "pdh": None, "pdl": None,
            "hit_pdh": False, "hit_pdl": False
        }

    # 3. Check interaction: did we touch these levels today?
    # PDH is hit if current session High >= Previous Day High
    hit_pdh = bool(session_high >= pdh)

    # PDL is hit if current session Low <= Previous Day Low
    hit_pdl = bool(session_low <= pdl)

    return {
        "pdh": float(pdh),
        "pdl": float(pdl),
        "hit_pdh": hit_pdh,
        "hit_pdl": hit_pdl
    }
