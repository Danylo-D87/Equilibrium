import asyncio
import sys
from contextlib import asynccontextmanager

from sqlalchemy import delete, text
from src.db.models import Statistic
from src.db.database import get_stats_session  # Ensure correct import path


@asynccontextmanager
async def session_manager():
    """
    Converts async_generator (get_stats_session) into a
    context manager that supports 'async with'.
    """
    # Create generator
    gen = get_stats_session()
    session = None
    try:
        # Retrieve session (equivalent to yield session)
        session = await anext(gen)
        yield session
    except StopAsyncIteration:
        raise RuntimeError("Failed to retrieve session from get_stats_session")
    finally:
        # Close generator (executes finally/close block in get_stats_session)
        await gen.aclose()


async def clear_database():
    print("⚠️  WARNING: This script will completely DELETE all collected statistics (footprint data).")
    confirm = input("Are you sure? Type 'yes' to confirm: ")

    if confirm.lower() != 'yes':
        print("❌ Cancelled.")
        return

    # Use wrapper instead of direct call
    async with session_manager() as session:
        try:
            print("⏳ Deleting data...")

            # Option 1: Delete via ORM
            await session.execute(delete(Statistic))

            # Option 2: If huge dataset (TRUNCATE) - uncomment for PostgreSQL
            # await session.execute(text("TRUNCATE TABLE statistics RESTART IDENTITY CASCADE;"))

            await session.commit()
            print("✅ Table 'Statistic' successfully cleared.")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error during deletion: {e}")


if __name__ == "__main__":
    # Fix for Windows asyncio loop policy
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(clear_database())
