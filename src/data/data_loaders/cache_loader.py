from sqlalchemy.ext.asyncio import AsyncSession

from src.data.analytics.core import AnalyticsCore
from src.db.redis import set_cache
from src.utils.symbols import get_available_symbols

from src.utils.periods import ANALYTICS_PERIODS, get_date_range_for_period


class CacheLoaderService:
    def __init__(self):
        self.analytics = AnalyticsCore()

    async def update_all_analytics_cache(self, session: AsyncSession):
        print("💾 [CacheLoader] Starting Redis cache update...")

        symbols = await get_available_symbols()

        # Use imported list of periods
        periods_list = ANALYTICS_PERIODS

        total_keys = 0

        for symbol in symbols:
            first_stat_date = await self.analytics.get_first_date(session, symbol)

            if not first_stat_date:
                print(f"⚠️ [CacheLoader] No data for {symbol}, skipping.")
                continue

            # Create a list of available periods for this asset (to send to frontend later)
            available_periods_for_symbol = []

            # Iterate through period names ['last_30_days', 'last_7_days'...]
            for period_name in periods_list:

                date_range = get_date_range_for_period(period_name)

                if not date_range:
                    continue

                start_dt, end_dt = date_range

                # --- FILTERING LOGIC ---

                # Rule:
                # 1. "YTD" (from_2020) is always kept (this is "All History" mode).
                # 2. For fixed periods (last_X_days):
                #    If requested start (start_dt) is EARLIER than coin's actual start (first_stat_date),
                #    then the period is "too large" for this coin. Skip it.

                if period_name != "YTD":
                    # Add a small buffer (e.g., 2 days) so we don't show
                    # "30 days" button if the coin is exactly 29 days old.
                    if start_dt < first_stat_date:
                        # Period is too long for this coin
                        # (e.g., requesting 730 days, but coin is 300 days old)
                        continue

                # -------------------------

                report = await self.analytics.get_analytics(
                    session, symbol, start_dt, end_dt
                )

                if "error" in report:
                    continue

                redis_key = f"analytics:{symbol}:{period_name}"
                await set_cache(redis_key, report)

                # Add to success list
                available_periods_for_symbol.append(period_name)
                total_keys += 1

                # (OPTIONAL) Save list of available buttons for frontend
                # redis_key_meta = f"analytics:{symbol}:available_periods"
                # await set_cache(redis_key_meta, available_periods_for_symbol)

            print(f"✅ [CacheLoader] Completed. Updated {total_keys} keys.")


async def run_cache_update(session: AsyncSession):
    loader = CacheLoaderService()
    await loader.update_all_analytics_cache(session)
