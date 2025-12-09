from datetime import time


def parse_time(time_str: str) -> time | None:
    """
    Converts the string '9:30' or '16:00' to a datetime.time object.
    """
    try:
        hour, minute = map(int, time_str.split(':'))
        return time(hour, minute)
    except ValueError:
        # In case there is an error in the config
        print(f"[Time] ⚠️ Time format error: {time_str}.")
        return None