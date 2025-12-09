from datetime import datetime, date, timedelta

import pytz
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.add_footprint_to_db.core import CorePreparation
from src.db.models import Statistic
from src.config import config
from src.utils.symbols import get_available_symbols


class StatDataLoader:
    def __init__(self):
        self.core = CorePreparation(config)

    @staticmethod
    async def _check_data_integrity(session: AsyncSession, symbol: str) -> bool:
        """
        Checks if a full recalculation is required.
        Returns True if issues (gaps or missing keys) are found.
        """

        # 1. Check: Is there any data at all?
        count_query = select(func.count()).where(Statistic.symbol == symbol)
        total_rows = (await session.execute(count_query)).scalar() or 0

        if total_rows == 0:
            print(f"   ⚠️ [SmartRunner] {symbol}: Statistics empty. Full start required.")
            return True

        # 2. Check: Are there 'gaps' in JSON?
        # Since we use JSONB, we can check for key existence.

        # Fetch the oldest record
        oldest_query = select(Statistic).where(Statistic.symbol == symbol).order_by(Statistic.date.asc()).limit(1)
        oldest_record = (await session.execute(oldest_query)).scalar_one_or_none()

        if oldest_record:
            # CHECK FOR NEW METRICS HERE
            required_keys = config.get("required_keys", [])

            # SAFETY: If NULL in DB, treat as empty dict
            existing_metrics = oldest_record.metrics or {}

            for key in required_keys:
                if key not in existing_metrics:
                    print(f"   ⚠️ [SmartRunner] {symbol}: Found old record missing key '{key}'. Recalculating.")
                    return True

        return False

    @staticmethod
    async def _get_last_date(session: AsyncSession, symbol: str) -> date | None:
        """Retrieves the date of the last record in statistics."""
        query = select(func.max(Statistic.date)).where(Statistic.symbol == symbol)
        return (await session.execute(query)).scalar()

    async def run_integrity_check(self, session: AsyncSession):

        print("🚀 [SmartRunner] Starting smart statistics update...")

        # Config returns string "2020-01-01", we need datetime
        full_history_str = config.get("full_history_start_date", "2020-01-01")
        full_history_start = datetime.strptime(full_history_str, "%Y-%m-%d")

        # Get symbols
        symbols = await get_available_symbols()

        ny_tz = pytz.timezone('America/New_York')

        today = datetime.now(ny_tz).date()
        yesterday = today - timedelta(days=1)

        for symbol in symbols:
            print(f"\n[SmartRunner] 🔍 Analyzing {symbol}...")

            # 1. Check if full recalc is needed
            # (missing data or new metrics added).
            need_full_recalc = await self._check_data_integrity(session, symbol)

            start_dt = None
            end_dt = datetime.now()

            # Decision: Full rewrite -> use start date from config.
            # No -> check last record.
            if need_full_recalc:
                print(f"   [SmartRunner] 🔄 Mode: FULL RECALC (from {full_history_start.date()}) FOR {symbol}.")
                start_dt = full_history_start
            else:
                last_date = await self._get_last_date(session, symbol)

                if last_date:
                    if last_date >= yesterday:
                        print("   [SmartRunner] ✅ Data is up to date. Skipping.")
                        continue

                    # Start from the day after the last one
                    start_dt = datetime.combine(last_date, datetime.min.time()) + timedelta(days=1)
                    print(f"   [SmartRunner] ➕ Mode: APPEND (from {start_dt.date()})")

            # Run CorePreparation only for this symbol and period.
            await self.core.run_preparation(
                session=session,
                start_date=start_dt,
                end_date=end_dt,
                symbols=[symbol]  # Pass explicitly
            )

        print("\n🏁 [SmartRunner] All tasks completed.")


async def run_stats_sync(session: AsyncSession):
    """
    Wrapper to start statistics calculation.
    """
    print("⏳ [Scheduler] [StatDataLoader] Starting metrics recalculation...")
    loader = StatDataLoader()
    await loader.run_integrity_check(session)
    print("✅ [Scheduler] [StatDataLoader] Recalculation completed.")
