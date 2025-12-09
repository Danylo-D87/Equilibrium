"""
This module aggregates all basic calculation functions required for metrics.
"""
from datetime import datetime, time
from typing import List

import pandas as pd
import pytz
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.add_footprint_to_db.tools.event_times import find_event_times
from src.data.add_footprint_to_db.tools.ib_mid_retest import calculate_ib_mid_retest
from src.data.add_footprint_to_db.tools.ib_reversion import calculate_ib_reversion
from src.data.add_footprint_to_db.tools.pdh_pdl import calculate_prior_levels_interaction
from src.data.add_footprint_to_db.tools.ib_extensions import calculate_ib_extension_metrics
from src.data.add_footprint_to_db.tools.ib_range import calculate_ib_range_metrics
from src.data.add_footprint_to_db.tools.ib_volume import calculate_ib_volume
from src.data.add_footprint_to_db.tools.ib_breakout import calculate_ib_breakout
from src.data.add_footprint_to_db.tools.ib_high_low_range import calculate_ib_levels
from src.db.database import RawSessionLocal
from src.db.models import Candle, Statistic
from src.utils.symbols import get_available_symbols
from src.utils.time.time_convert import convert_to_ny_time


class CorePreparation:
    def __init__(self, conf: dict):
        self.conf = conf

        # Initialize time constants only (they do not change between runs)
        self.ib_start = self._parse_time(conf["ib_start_time"])
        self.ib_end = self._parse_time(conf["ib_end_time"])

        self.session_start = self._parse_time(conf["session_start_time"])
        self.session_end = self._parse_time(conf["session_end_time"])

    @staticmethod
    def _parse_time(time_str: str) -> time:
        h, m = map(int, time_str.split(':'))
        return time(h, m)

    @staticmethod
    async def receive_raw_data_from_db(symbol: str, start_dt, end_dt) -> pd.DataFrame:
        """Loads candles for the specified dynamic period."""
        print(f"   📥 [Core] Loading candles for {symbol} from {start_dt.date()} to {end_dt.date()}...")

        async with RawSessionLocal() as session:
            query = select(Candle).where(
                and_(
                    Candle.symbol == symbol,
                    Candle.timestamp >= start_dt,
                    Candle.timestamp <= end_dt)
            ).order_by(Candle.timestamp)

            results = await session.execute(query)
            candles = results.scalars().all()

        if not candles:
            return pd.DataFrame()

        df = pd.DataFrame([{
            "timestamp": c.timestamp,
            "open": c.open, "high": c.high,
            "low": c.low, "close": c.close, "volume": c.volume,
        } for c in candles])

        df.set_index("timestamp", inplace=True)

        return df

    @staticmethod
    async def save_to_db(session: AsyncSession, symbol: str, date_obj, metrics_data: dict):
        """Saves data to the 'metrics' JSON column."""
        stat = Statistic(
            symbol=symbol,
            date=date_obj,
            metrics=metrics_data,
        )
        await session.merge(stat)
        await session.commit()

    async def run_preparation(self, session: AsyncSession, start_date: datetime, end_date: datetime, symbols: List[str] = None):
        """
        Main method.
        Dates come from external source (SmartRunner).
        """
        ny_tz = pytz.timezone('America/New_York')
        utc_tz = pytz.utc

        # 1. Define limits in UTC
        start_ny = ny_tz.localize(start_date.replace(hour=0, minute=0, second=0))
        end_ny = ny_tz.localize(end_date.replace(hour=23, minute=59, second=59))

        start_utc = start_ny.astimezone(utc_tz).replace(tzinfo=None)
        end_utc = end_ny.astimezone(utc_tz).replace(tzinfo=None)

        # 2. Define asset list
        # If symbols not passed (None) — fetch all from DB
        if not symbols:
            symbols = await get_available_symbols()

        print(f"⚙️ [Core] Data request for period (NY Time): {start_date.date()} -> {end_date.date()}")
        print(f"   🕒 DB Search Range (UTC): {start_utc} -> {end_utc}")

        for symbol in symbols:
            # 3. Load Data
            df_utc = await self.receive_raw_data_from_db(symbol, start_utc, end_utc)

            if df_utc.empty:
                print(f"   ⚠️ [Core] No data for {symbol}.")
                continue

            # 4. Conversion
            df = convert_to_ny_time(df_utc)

            processed_count = 0

            # Variable to store PREVIOUS day stats ===
            previous_day_stats = None

            # 5. Day Loop
            for date_obj, day_df in df.groupby(df.index.date):

                # === EXCLUDE WEEKENDS ===
                # 5 = Saturday, 6 = Sunday
                if date_obj.weekday() in [5, 6]:
                    continue
                # ============================

                # Filter tails (to exclude extra days caught due to UTC shift)
                if date_obj < start_date.date() or date_obj > end_date.date():
                    continue

                if len(day_df) < 30:
                    continue

                # Full Day Data
                full_day_df = day_df

                # Post-IB Data (Full Day)
                post_ib_full_df = full_day_df.loc[full_day_df.index.time > self.ib_end]

                # Post-IB Data (Session Only)
                post_ib_session = post_ib_full_df.loc[post_ib_full_df.index.time <= self.session_end]

                # ------ Tools (Calculations) ------

                # 1. IB Levels
                ib_lvl = calculate_ib_levels(
                    day_df=day_df,
                    ib_start_time=self.ib_start,
                    ib_end_time=self.ib_end
                )

                if not ib_lvl:
                    continue

                # 2. Breakouts: Use session data for session, full day for full stats
                ib_breakout_session = calculate_ib_breakout(
                    day_df=post_ib_session,  # Only post-IB until 16:00
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    ib_end_time=self.ib_end
                )

                ib_breakout_full = calculate_ib_breakout(
                    day_df=post_ib_full_df, # Only post-IB until end of day
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    ib_end_time=self.ib_end
                )

                # 3. Calculate Range
                ib_range_metrics = calculate_ib_range_metrics(
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    day_df=day_df,
                    ib_start_time=self.ib_start,
                )

                # 4. Calculate Volume
                ib_vol = calculate_ib_volume(
                    day_df=day_df,
                    ib_start_time=self.ib_start,
                    ib_end_time=self.ib_end
                )

                # 5. Calculate Extensions
                # --- Session Only ---
                sh_sess = post_ib_session['high'].max()
                sl_sess = post_ib_session['low'].min()

                ib_ext_session = calculate_ib_extension_metrics(
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    ib_range=ib_lvl["ib_range"],
                    session_high=sh_sess,
                    session_low=sl_sess
                )

                # --- Full Day ---
                sh_full = post_ib_full_df['high'].max()
                sl_full = post_ib_full_df['low'].min()

                ib_ext_full = calculate_ib_extension_metrics(
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    ib_range=ib_lvl["ib_range"],
                    session_high=sh_full,
                    session_low=sl_full
                )

                # 6. PDH / PDL
                # Interaction check: did we sweep yesterday's high during session or later?
                priors_session = calculate_prior_levels_interaction(
                    session_high=sh_sess,
                    session_low=sl_sess,
                    previous_day_stats=previous_day_stats
                )

                priors_full = calculate_prior_levels_interaction(
                    session_high=sh_full,
                    session_low=sl_full,
                    previous_day_stats=previous_day_stats
                )

                # --- [DEBUG] Selective print for check ---
                if processed_count == 0:
                    max_full = post_ib_full_df.index.max().time() if not post_ib_full_df.empty else "None"
                    print(
                        f"      👉 [Check] {date_obj}: Full DF max time: {max_full} | Session max time: {self.session_end}")

                # === 7. Reversion (After Hours Analysis) ===
                # Period strictly AFTER main session close
                # i.e., from session_end to end of day
                after_hours_df = full_day_df.loc[full_day_df.index.time > self.session_end]

                reversion_metrics = calculate_ib_reversion(
                    df=after_hours_df,
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"]
                )

                # === 8. IB MID RETEST ===
                # Check if price returned to center

                # Session
                mid_retest_session = calculate_ib_mid_retest(
                    df=post_ib_session,
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"]
                )

                # Full Day
                mid_retest_full = calculate_ib_mid_retest(
                    df=post_ib_full_df,
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"]
                )

                # 9. Event Times
                # === [NEW] 9. EVENT TIMING ===
                # Search for breakout times across the entire period (10:30 -> 23:59)
                timing_metrics = find_event_times(
                    df=post_ib_full_df,
                    ib_high=ib_lvl["ib_high"],
                    ib_low=ib_lvl["ib_low"],
                    ib_range=ib_lvl["ib_range"]
                )

                # ------ Save ------
                full_metrics = {
                    # Base (Common)
                    "ib_high": ib_lvl["ib_high"],
                    "ib_low": ib_lvl["ib_low"],
                    "ib_range": ib_lvl["ib_range"],
                    "ib_range_usd": ib_range_metrics["ib_range_usd"],
                    "ib_range_pct": ib_range_metrics["ib_range_pct"],
                    "ib_vol": ib_vol,
                    "time_break_high": timing_metrics["time_break_high"],
                    "time_break_low": timing_metrics["time_break_low"],
                    "time_hit_05x": timing_metrics["time_hit_05x"],
                    "time_hit_1x": timing_metrics["time_hit_1x"],
                    "time_hit_2x": timing_metrics["time_hit_2x"],

                    # === SESSION METRICS (until 16:00) ===
                    "session_high_broken": ib_breakout_session["ib_high_broken"],
                    "session_low_broken": ib_breakout_session["ib_low_broken"],

                    "session_ext_05x": ib_ext_session["ib_ext_05x"],
                    "session_ext_1x": ib_ext_session["ib_ext_1x"],
                    "session_ext_2x": ib_ext_session["ib_ext_2x"],
                    "session_ext_coeff": ib_ext_session["ib_ext_coeff"],

                    "session_hit_pdh": priors_session["hit_pdh"],
                    "session_hit_pdl": priors_session["hit_pdl"],

                    "session_hit_ib_mid": mid_retest_session["hit_ib_mid"],

                    # === FULL DAY METRICS (until 23:59) ===
                    "full_high_broken": ib_breakout_full["ib_high_broken"],
                    "full_low_broken": ib_breakout_full["ib_low_broken"],

                    "full_ext_05x": ib_ext_full["ib_ext_05x"],
                    "full_ext_1x": ib_ext_full["ib_ext_1x"],
                    "full_ext_2x": ib_ext_full["ib_ext_2x"],
                    "full_ext_coeff": ib_ext_full["ib_ext_coeff"],

                    "full_hit_pdh": priors_full["hit_pdh"],
                    "full_hit_pdl": priors_full["hit_pdl"],

                    # Metadata (PDH/PDL prices common for the day)
                    "pdh": priors_full["pdh"],
                    "pdl": priors_full["pdl"],

                    "full_hit_ib_mid": mid_retest_full["hit_ib_mid"],

                    # === AFTER HOURS ===
                    "after_hours_hit_ib": reversion_metrics["hit_ib"],

                }

                await self.save_to_db(session, symbol, date_obj, full_metrics)

                # === [NEW] Update "Previous Day" for next iteration ===
                # Current High/Low becomes "Previous" for tomorrow
                pdh = full_day_df["high"].max()
                pdl = full_day_df["low"].min()

                previous_day_stats = {
                    'high': pdh,
                    'low': pdl
                }
                # ================================================================
                processed_count += 1

            print(f"✅ [Core] {symbol}: Updated {processed_count} full days.")
