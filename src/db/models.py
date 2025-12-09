from datetime import date, datetime

from sqlalchemy import String, Numeric, Date, DateTime, JSON
from sqlalchemy.orm import Mapped, mapped_column

from src.db.database import Base


# Table for raw market data (candles)
class Candle(Base):
    __tablename__ = "candles"

    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)

    # Numeric with high precision for crypto prices
    open: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False))
    close: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False))
    low: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False))
    high: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False))
    volume: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False))


# Table for calculated daily statistics
class Statistic(Base):
    __tablename__ = "statistics"

    symbol: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)

    # JSONB column to store flexible metrics structure
    metrics: Mapped[dict] = mapped_column(JSON, default={})
