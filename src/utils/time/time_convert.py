import pandas as pd
import pytz


def convert_to_ny_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DF with a UTC index.
    Returns a copy of the DF with an index in New York time.
    """

    ny_tz = pytz.timezone('America/New_York')
    utc_tz = pytz.utc

    if df.empty:
        return df

    df = df.copy()
    # If the index has no time zone, assume it is UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize(utc_tz)

    # Convert to NY
    df.index = df.index.tz_convert(ny_tz)
    return df


def convert_to_ua_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DF with a UTC index.
    Returns a copy of the DF with an index in Ukraine time.
    """

    ua_tz = pytz.timezone('Ukraine/Kyiv')
    utc_tz = pytz.utc

    if df.empty:
        return df

    df = df.copy()
    # If the index has no time zone, assume it is UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize(utc_tz)

    # Convert to UA
    df.index = df.index.tz_convert(ua_tz)
    return df
