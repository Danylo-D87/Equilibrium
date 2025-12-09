from typing import Dict, Optional, Any, List
from pydantic import BaseModel, Field


class Asset(BaseModel):
    id: str = Field(..., example="BTC/USDT", description="Trading pair symbol")
    name: str = Field(..., example="Bitcoin", description="Human-readable name")


class AssetsResponse(BaseModel):
    assets: List[str] = Field(..., example=["BTC/USDT", "ETH/USDT"])


# --- Components for Report ---

class BreakoutStats(BaseModel):
    break_high_chance: float = Field(..., description="Probability of breaking the IB High")
    break_low_chance: float = Field(..., description="Probability of breaking the IB Low")
    one_sided_chance: float = Field(..., description="Probability of a trend day (one side broken)")
    two_sided_chance: float = Field(..., description="Probability of a choppy day (both sides broken)")
    no_breakout_chance: float = Field(..., description="Probability of staying inside IB")


class ExtensionStats(BaseModel):
    prob_hit_05x: float = Field(..., description="Probability of hitting 0.5x extension")
    prob_hit_1x: float = Field(..., description="Probability of hitting 1.0x extension")
    prob_hit_2x: float = Field(..., description="Probability of hitting 2.0x extension")
    avg_extension_coeff: float = Field(..., description="Average extension coefficient")


class PriorsStats(BaseModel):
    prob_hit_pdh: float = Field(..., description="Probability of hitting Previous Day High")
    prob_hit_pdl: float = Field(..., description="Probability of hitting Previous Day Low")
    prob_pdh_if_ibh_broken: float = Field(..., description="Conditional prob: Hit PDH if IB High is broken")
    prob_pdl_if_ibl_broken: float = Field(..., description="Conditional prob: Hit PDL if IB Low is broken")


class SessionData(BaseModel):
    # We include fields from various calculators here
    # Using specific models creates nice nested docs

    # Breakouts
    break_high_chance: Optional[float] = None
    break_low_chance: Optional[float] = None
    one_sided_chance: Optional[float] = None
    two_sided_chance: Optional[float] = None
    no_breakout_chance: Optional[float] = None

    # Extensions
    prob_hit_05x: Optional[float] = None
    prob_hit_1x: Optional[float] = None
    prob_hit_2x: Optional[float] = None
    avg_extension_coeff: Optional[float] = None

    # Priors
    prob_hit_pdh: Optional[float] = None
    prob_hit_pdl: Optional[float] = None
    prob_pdh_if_ibh_broken: Optional[float] = None
    prob_pdl_if_ibl_broken: Optional[float] = None

    # Retest
    prob_ib_mid_retest: Optional[float] = None

    # Complex dictionaries (Heatmaps)
    weekday_chop: Dict[str, Any] = Field(default_factory=dict, description="Chop stats by weekday")
    weekday_targets: Dict[str, Any] = Field(default_factory=dict, description="Target hits by weekday")
    weekday_targets_clean: Dict[str, Any] = Field(default_factory=dict, description="Clean trend stats by weekday")
    time_heatmap: Dict[str, Any] = Field(default_factory=dict, description="Breakout timing distribution")
    time_heatmap_clean: Dict[str, Any] = Field(default_factory=dict, description="Clean timing distribution")


class AnalyticsReportResponse(BaseModel):
    symbol: str = Field(..., example="BTC/USDT")
    period_start: str = Field(..., example="2024-01-01")
    period_end: str = Field(..., example="2024-02-01")
    total_days_analyzed: int = Field(..., example=30)

    prob_return_to_ib_after_session: Optional[float] = Field(None, description="Reversion probability after close")

    avg_ib_range_usd: float
    avg_ib_range_pct: float
    avg_ib_volume: float

    session: SessionData = Field(..., description="Statistics for the active session (until 16:00 NY)")
    full_day: SessionData = Field(..., description="Statistics for the full trading day (until 23:59 NY)")
