from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.data.data_loaders.full_data_update import run_full_synchronization
from src.routers import analytics

# --- Metadata for Documentation ---
tags_metadata = [
    {
        "name": "Analytics",
        "description": "Endpoints for retrieving statistical reports and market data metrics.",
    },
    {
        "name": "Health",
        "description": "Service health checks.",
    },
]

description = """
# Equilibrium API 🚀

Quantitative Analytics Engine for Financial Markets. 

This API provides advanced statistical analysis for intraday trading strategies, focusing on:
* **Initial Balance (IB)** mechanics.
* **Volatility analysis** and extension targets.
* **Time-based probabilities** (Heatmaps).
* **Seasonality patterns** across trading sessions.

## Architecture
The system operates on a sophisticated data pipeline:
1. **Raw Data Layer:** Ingests high-frequency OHLCV data from exchanges.
2. **Processing Core:** Asynchronous engine calculates probabilities using Pandas & NumPy.
3. **Caching Layer:** Redis stores pre-computed reports for millisecond-latency access.

*Built for high-performance financial analysis.*
"""

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Server starting...")
    # 1. Background task
    asyncio.create_task(run_full_synchronization())
    # 2. Scheduler
    scheduler.add_job(run_full_synchronization, CronTrigger(hour=0, minute=5, timezone="America/New_York"))
    scheduler.start()
    yield
    print("🛑 Server shutting down...")
    scheduler.shutdown()


app = FastAPI(
    title="Equilibrium API",
    description=description,
    version="1.0.0",
    openapi_tags=tags_metadata,
    lifespan=lifespan
)

app.include_router(analytics.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Health"])
async def root():
    """
    **Health Check Endpoint.**

    Returns the current status of the API service.
    """
    return {"status": "ok", "service": "Crypto Data Lake"}
