from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from src.db.config import settings


# 1. Initialize Engines
# echo=False (set to True for SQL query debugging in console)
raw_engine = create_async_engine(settings.raw_db_url, echo=False)
stats_engine = create_async_engine(settings.stats_db_url, echo=False)

# 2. Session Factories
# expire_on_commit=False is mandatory for async SQLAlchemy
RawSessionLocal = async_sessionmaker(raw_engine, expire_on_commit=False)
StatsSessionLocal = async_sessionmaker(stats_engine, expire_on_commit=False)


# 3. Dependencies for FastAPI and scripts
async def get_raw_session():
    async with RawSessionLocal() as session:
        yield session


async def get_stats_session():
    async with StatsSessionLocal() as session:
        yield session


# 4. Base class for ORM models
class Base(DeclarativeBase):
    pass
