"""
Market Data Loader Service (ETL).

This module is responsible for downloading historical data (candles) from
crypto exchanges via CCXT and saving them to the "raw" database (market_data).

Functionality:
1. Checks the last recorded candle in the DB for each asset.
2. Downloads missing data from the exchange (Binance).
3. Stops strictly at 00:00 UTC of the current day (to avoid saving incomplete days).
4. Uses an Upsert (merge) mechanism to prevent duplicates.

DB Data Format (table 'candles'):
-------------------------------------------------------
| Column    | Type      | Description                 |
-------------------------------------------------------
| symbol    | String    | Ticker (e.g., 'BTC/USDT')   |
| timestamp | DateTime  | Open/Close time (UTC)       |
| open      | Float     | Opening price               |
| high      | Float     | Highest price               |
| low       | Float     | Lowest price                |
| close     | Float     | Closing price               |
| volume    | Float     | Trading volume              |
-------------------------------------------------------
"""

import asyncio
from datetime import datetime, timezone, timedelta

import ccxt.async_support as ccxt
import pytz
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import config
from src.db.models import Candle


config_load = {
    "timeframe": "1m",
    "exchange": "binance",
    "market_type": "swap",  # or 'spot' if needed
}


class RawDataLoaderService:
    def __init__(self, conf: dict, main_config: dict = config):
        date_str = main_config.get("full_history_start_date", "2020-01-01")
        self.full_history_start_date = self._parse_date_to_datetime(date_str)
        self.symbol_list = main_config["symbols"]
        self.timeframe = conf["timeframe"]
        self.market_type = conf["market_type"]

        # Initialize exchange dynamically
        exchange_class = getattr(ccxt, conf["exchange"])
        self.exchange = exchange_class({
            'enableRateLimit': True,
            'options': {
                'defaultType': self.market_type
            }
        })

    @staticmethod
    def _parse_date_to_datetime(date_str: str) -> datetime:
        """Converts a date string to a datetime object (UTC)."""
        y, m, d = map(int, date_str.split('-'))
        return datetime(y, m, d, 0, 0, 0, tzinfo=timezone.utc)

    @staticmethod
    async def get_last_db_timestamp(session: AsyncSession, symbol: str) -> datetime | None:
        """Retrieves the timestamp of the last recorded candle for a specific symbol."""
        query = select(func.max(Candle.timestamp)).where(Candle.symbol == symbol)
        result = await session.execute(query)
        last_timestamp = result.scalar()

        if last_timestamp and last_timestamp.tzinfo is None:
            last_timestamp = last_timestamp.replace(tzinfo=timezone.utc)

        return last_timestamp

    async def sync_data(self, session: AsyncSession):
        """Main loop: iterates through all assets and updates them."""
        print(f"[RawDataLoaderService] 🚀 Starting synchronization for {len(self.symbol_list)} assets...")

        # Define NY timezone
        ny_tz = pytz.timezone('America/New_York')

        # Current time in NY
        now_ny = datetime.now(ny_tz)

        # Truncate to the start of the day (00:00:00 NY)
        target_end_dt_ny = now_ny.replace(hour=0, minute=0, second=0, microsecond=0)

        # Convert this moment (00:00 NY) back to UTC to get the exchange timestamp
        target_end_dt_utc = target_end_dt_ny.astimezone(timezone.utc)

        target_end_ts = int(target_end_dt_utc.timestamp() * 1000)  # timestamp ms

        for symbol in self.symbol_list:
            print(f"\n[RawDataLoaderService] 🔄 Processing {symbol}...")

            # A. Where to start?
            last_date = await self.get_last_db_timestamp(session, symbol)

            if last_date:
                since_dt = last_date + timedelta(minutes=1)
                print(f"[RawDataLoaderService]    📅 Found in DB. Resuming from: {since_dt}")
            else:
                since_dt = self.full_history_start_date
                print(f"[RawDataLoaderService]    📅 No data. Downloading full history from: {since_dt}")

            since_ts = int(since_dt.timestamp() * 1000)

            if since_ts >= target_end_ts:
                print(f"[RawDataLoaderService]    ✅ {symbol} is up to date. Skipping.")
                continue

            # B. Download Loop
            total_loaded = 0

            while since_ts < target_end_ts:
                try:
                    candles = await self.exchange.fetch_ohlcv(
                        symbol,
                        self.timeframe,
                        since_ts,
                        limit=1000
                    )

                    if not candles:
                        print("[RawDataLoaderService]    ⚠️ Exchange returned no data.")
                        break

                    # Filter out future candles (safety check)
                    valid_candles = [c for c in candles if c[0] < target_end_ts]

                    if not valid_candles:
                        break

                    # Convert to DB models
                    objects = []
                    for c in valid_candles:
                        # Convert to UTC, then strip timezone info (make naive)
                        dt_object = datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc).replace(tzinfo=None)

                        objects.append(Candle(
                            symbol=symbol,
                            timestamp=dt_object,
                            open=c[1], high=c[2], low=c[3], close=c[4], volume=c[5],
                        ))

                    # Upsert
                    for obj in objects:
                        await session.merge(obj)

                    await session.commit()

                    count = len(valid_candles)
                    total_loaded += count

                    # Move cursor
                    last_candle_ts = valid_candles[-1][0]
                    since_ts = last_candle_ts + 60000

                    print(
                        f"[RawDataLoaderService]    📥 +{count} candles. (Last: {datetime.fromtimestamp(last_candle_ts / 1000, tz=timezone.utc)})")

                except Exception as e:
                    print(f"[RawDataLoaderService]    ❌ Error downloading {symbol}: {e}")
                    await asyncio.sleep(5)

            print(f"[RawDataLoaderService]    🏁 {symbol}: Added {total_loaded} records.")

        await self.exchange.close()
        print("\n[RawDataLoaderService] ✅ All assets synchronized!")


async def run_data_sync(session: AsyncSession):
    """
    Called from main.py (at startup and by scheduler).
    Initializes the service with config and starts the process.
    """
    print("⏳ [Scheduler] [RawDataLoaderService] Starting data update...")

    # config is taken from this file (global variable above)
    loader = RawDataLoaderService(config_load)

    await loader.sync_data(session)
    print("✅ [Scheduler] [RawDataLoaderService] Update completed.")
