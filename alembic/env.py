import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context


import sys
import os

# 1. Додаємо шлях до кореня проекту, щоб Python бачив папку src
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# 2. Імпортуємо налаштування та моделі
from src.db.config import settings
from src.db.database import Base  # Важливо імпортувати саме той Base, від якого спадкуються моделі
# (Можна також імпортувати самі моделі, щоб переконатися, що вони зареєстровані:
from src.db.models import Statistic

# 3. Вказуємо Alembic-у, де шукати структуру таблиць
target_metadata = Base.metadata

# 4. Підміняємо URL в конфігу на той, що в .env (для бази статистики)
config = context.config
config.set_main_option("sqlalchemy.url", settings.stats_db_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
