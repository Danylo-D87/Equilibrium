config = {
    # Symbols to track
    "symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],

    # Start date for full history download
    "full_history_start_date": "2020-01-01",

    # Initial Balance (IB) Configuration (NY Time)
    "ib_start_time": "9:30",
    "ib_end_time": "10:29",

    # Active Session Configuration (NY Time)
    "session_start_time": "9:30",
    "session_end_time": "16:29",

    # ------------------------------------
    # List of required keys in the JSONB metrics column.
    # Used for data integrity checks.
    "required_keys": [
        "ib_high",
        "ib_low",
        "ib_range",
        "ib_range_usd",
        "ib_range_pct",
        "ib_vol",

        # === SESSION METRICS (до 16:00) ===
        "session_high_broken",
        "session_low_broken",

        "session_ext_05x",
        "session_ext_1x",
        "session_ext_2x",
        "session_ext_coeff",

        "session_hit_pdh",
        "session_hit_pdl",

        "session_hit_ib_mid",

        # === FULL DAY METRICS (до 23:59) ===
        "full_high_broken",
        "full_low_broken",

        "full_ext_05x",
        "full_ext_1x",
        "full_ext_2x",
        "full_ext_coeff",

        "full_hit_pdh",
        "full_hit_pdl",

        "full_hit_ib_mid",

        # Metadata & Timing
        "pdh",
        "pdl",
        "after_hours_hit_ib",
        "time_break_high",
        "time_break_low",
        "time_hit_05x",
        "time_hit_1x",
        "time_hit_2x",
    ],
}
