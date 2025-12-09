from src.db.database import RawSessionLocal, StatsSessionLocal
from src.data.data_loaders.raw_data_loader import run_data_sync
from src.data.data_loaders.footprint_data_loader import run_stats_sync
from src.data.data_loaders.cache_loader import run_cache_update


async def run_full_synchronization():
    """
    Runs the full data update cycle:
    1. First, download fresh candles (Raw Data).
    2. Only upon success - update statistics (Stats).
    3. Finally, aggregate results into Redis cache.
    """
    print("🔄 [Pipeline] Starting full update cycle...")

    try:
        # Step 1: Candles
        async with RawSessionLocal() as raw_session:
            await run_data_sync(raw_session)

        # Step 2: Statistics (Stats)
        async with StatsSessionLocal() as stats_session:
            await run_stats_sync(stats_session)

            # Step 3: Redis Aggregation
            await run_cache_update(stats_session)

        print("🏁 [Pipeline] Cycle completed successfully.")

    except Exception as e:
        print(f"❌ [Pipeline] Critical error during update: {e}")
