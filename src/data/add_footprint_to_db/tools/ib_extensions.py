from typing import Dict


def calculate_extension_coefficient(
        ib_high: float,
        ib_low: float,
        ib_range: float,
        session_high: float,
        session_low: float
) -> float:
    """
    Calculates the extension coefficient (how many times the IB range
    was projected in the breakout direction).
    """
    if ib_range == 0:
        return 0.0

    max_ext_up = max(0.0, session_high - ib_high)
    max_ext_down = max(0.0, ib_low - session_low)
    max_move = max(max_ext_up, max_ext_down)

    return round(max_move / ib_range, 2)


def calculate_ib_extension_metrics(
        ib_high: float,
        ib_low: float,
        ib_range: float,
        session_high: float,
        session_low: float,
) -> Dict[str, any]:
    """
    Aggregates extension metrics. Includes calculation for 0.5x, 1x, and 2x targets.
    """

    # Targets for x0.5
    target_up_05x = ib_high + (0.5 * ib_range)
    target_down_05x = ib_low - (0.5 * ib_range)

    # Targets for x1
    target_up_1x = ib_high + ib_range
    target_down_1x = ib_low - ib_range

    # Targets for x2
    target_up_2x = ib_high + (2 * ib_range)
    target_down_2x = ib_low - (2 * ib_range)

    # Check if targets were hit
    hit_05x = bool((session_high >= target_up_05x) or (session_low <= target_down_05x))
    hit_1x = bool((session_high >= target_up_1x) or (session_low <= target_down_1x))
    hit_2x = bool((session_high >= target_up_2x) or (session_low <= target_down_2x))

    coeff = calculate_extension_coefficient(
        ib_high, ib_low, ib_range, session_high, session_low
    )

    return {
        "ib_ext_05x": hit_05x,
        "ib_ext_1x": hit_1x,
        "ib_ext_2x": hit_2x,
        "ib_ext_coeff": coeff
    }
