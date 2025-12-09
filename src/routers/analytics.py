from fastapi import APIRouter, HTTPException, Query
from src.db.redis import get_cache
from src.config import config
from src.schemas import AssetsResponse, AnalyticsReportResponse

router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"]
)


@router.get(
    "/assets",
    response_model=AssetsResponse,
    summary="Get Available Assets",
    description="Returns a list of all trading pairs currently tracked and analyzed by the system."
)
async def get_available_assets():
    symbols = config.get("symbols", [])
    return {"assets": symbols}


@router.get(
    "/get-report",
    response_model=AnalyticsReportResponse,
    summary="Get Quantitative Report",
    description="""
    Retrieves a comprehensive statistical report for a specific asset and time period.

    The report includes:
    - **Breakout Probabilities**: Chances of breaking IB High/Low.
    - **Extension Targets**: Probability of hitting 0.5x, 1x, 2x extensions.
    - **Timing Heatmaps**: Distribution of significant market events by time of day.
    - **Seasonality**: Day-of-week performance analysis.

    *Data is cached in Redis for high performance.*
    """,
    responses={
        404: {
            "description": "Report not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Report not generated yet for this period."}
                }
            }
        }
    }
)
async def get_analytics_data(
        symbol: str = Query(..., example="BTC/USDT", description="Trading pair symbol (e.g., BTC/USDT)"),
        period: str = Query(..., example="last_30_days",
                            description="Time period for analysis (e.g., last_30_days, YTD)")
):
    print(f"🔍 [API] Requesting: Symbol='{symbol}', Period='{period}'")

    redis_key = f"analytics:{symbol}:{period}"
    data = await get_cache(redis_key)

    if not data:
        print(f"❌ [API] Key not found: {redis_key}")
        raise HTTPException(status_code=404, detail="Report not generated yet for this period.")

    return data
