from datetime import datetime, date, timedelta
import pytz

# 1. List of all available analytics periods (Constant)
# We iterate through this list to generate cache keys
ANALYTICS_PERIODS = [
    "YTD",
    "last_730_days",
    "last_365_days",
    "last_180_days",
    "last_90_days",
    "last_60_days",
    "last_30_days",
    "last_14_days",
    "last_7_days",
]


def get_date_range_for_period(period_name: str) -> tuple[date, date] | None:
    """
    Parses a period string (e.g., 'last_30_days')
    Returns a tuple (start_date, end_date)
    """

    ny_tz = pytz.timezone('America/New_York')
    today = datetime.now(ny_tz).date()
    # End date is always yesterday (completed session)
    end_date = today - timedelta(days=1)

    # Logic for "YTD" (Year To Date / All History)
    if period_name == "YTD":
        return date(2020, 1, 1), end_date

    # Logic for "last_X_days"
    # We split the string by underscores: 'last_30_days' -> ['last', '30', 'days']
    try:
        parts = period_name.split('_')
        days_ago_str = parts[1]  # Get the middle part ('30')
        days = int(days_ago_str)

        start_date = today - timedelta(days=days)
        return start_date, end_date

    except (IndexError, ValueError):
        print(f"⚠️ [Periods] Unknown period format: {period_name}. Defaulting to 30 days.")
        return None
