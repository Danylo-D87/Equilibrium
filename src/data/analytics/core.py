from datetime import date
from typing import Dict, Any, List, Optional

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd

from src.data.analytics.tools.time_distribution_clean import calculate_time_distribution_clean
from src.data.analytics.tools.weekday_targets_clean import calculate_weekday_targets_clean_stats
from src.data.analytics.tools.time_distribution import calculate_time_distribution
from src.data.analytics.tools.retest_stats import calculate_retest_stats
from src.data.analytics.tools.reversion_stats import calculate_reversion_stats
from src.data.analytics.tools.weekday_chop import calculate_weekday_chop_stats
from src.data.analytics.tools.weekday_targets import calculate_weekday_targets_stats
from src.data.analytics.tools.priors_stats import calculate_priors_stats
from src.data.analytics.tools.extension_stats import calculate_extension_stats
from src.data.analytics.tools.range_stats import calculate_range_volume_stats
from src.db.models import Statistic
from src.data.analytics.tools.ib_breakout_stats import calculate_breakout_probabilities


class AnalyticsCore:
    """
    Orchestrator class for analytics.
    Responsible for fetching data from the DB (Statistic model) and passing it to mathematical functions.
    """

    @staticmethod
    async def get_first_date(session: AsyncSession, symbol: str) -> Optional[date]:
        """
        Quickly finds the date of the very first statistic entry for a symbol.
        Used to filter out irrelevant periods.
        """
        query = select(func.min(Statistic.date)).where(Statistic.symbol == symbol)
        result = await session.execute(query)
        return result.scalar()

    @staticmethod
    async def _fetch_stats_data(
            session: AsyncSession,
            symbol: str,
            start_date: date,
            end_date: date
    ):
        """
        Private method: Executes SELECT query to the database.
        """
        # 1. Build the query
        query = select(Statistic).where(
            and_(
                Statistic.symbol == symbol,
                Statistic.date >= start_date,  # Filter by start date
                Statistic.date <= end_date     # Filter by end date
            )
        )
        # 2. Execute
        result = await session.execute(query)

        # 3. Return clean list of objects
        return result.scalars().all()

    async def get_analytics(
            self,
            session: AsyncSession,
            symbol: str,
            start_date: date,
            end_date: date
    ) -> Dict[str, Any]:
        """
        Generates a report for a single symbol over a specified period.
        Used by CacheLoader.
        """

        # Step 1: Fetch data from DB
        stats_data = await self._fetch_stats_data(session, symbol, start_date, end_date)

        if not stats_data:
            return {"error": "No data found"}

        actual_start = stats_data[0].date
        actual_end = stats_data[-1].date

        # 2. Convert to DataFrame + Add date
        metrics_list = []
        for row in stats_data:
            # Create a copy of the metrics dictionary
            m = row.metrics.copy()
            # Add date as a regular field (needed for weekday_stats)
            m["date"] = row.date
            metrics_list.append(m)

        if not metrics_list:
            return {"error": "Empty metrics data"}

        df = pd.DataFrame(metrics_list)

        if df.empty:
            return {"error": "Empty DataFrame"}

        # ==========================================
        # CALCULATIONS
        # ==========================================

        # 1. General (IB Range, Volume) - common for the entire day
        range_stats = calculate_range_volume_stats(df)

        # 2. Breakouts (Session + Full Day)
        breakout_stats = calculate_breakout_probabilities(df)

        # 3. Extensions (Session + Full Day)
        ext_stats = calculate_extension_stats(df)

        # 4. Priors / Context (Session + Full Day)
        priors_stats = calculate_priors_stats(df)

        # 5. Weekday Seasonality (Separate calculators)
        wd_chop = calculate_weekday_chop_stats(df)  # { "session": {...}, "full_day": {...} }
        wd_targets = calculate_weekday_targets_stats(df)  # { "session": {...}, "full_day": {...} }

        # [NEW] Clean Stats (Excluding Chop)
        wd_targets_clean = calculate_weekday_targets_clean_stats(df)

        # 6. Reversion Stats
        rev_stats = calculate_reversion_stats(df)

        # 7. IB Mid Retest Stats
        retest_stats = calculate_retest_stats(df)

        # 8. Time Distribution (Timing Heatmap)
        time_stats = calculate_time_distribution(df)

        time_stats_clean = calculate_time_distribution_clean(df)

        # ==========================================
        # PREPARING VARIABLES (For convenience)
        # ==========================================

        # Session Sub-dictionaries
        s_brk = breakout_stats.get("session", {})
        s_ext = ext_stats.get("session", {})
        s_pri = priors_stats.get("session", {})
        s_retest = retest_stats.get("session", {})

        # Full Day Sub-dictionaries
        f_brk = breakout_stats.get("full_day", {})
        f_ext = ext_stats.get("full_day", {})
        f_pri = priors_stats.get("full_day", {})
        f_retest = retest_stats.get("full_day", {})

        # ==========================================
        # RESPONSE FORMATION (MANUAL UNPACKING)
        # ==========================================

        res = {
            # --- METADATA ---
            "symbol": symbol,
            "total_days_analyzed": len(df),
            "period_start": str(actual_start),
            "period_end": str(actual_end),
            "prob_return_to_ib_after_session": rev_stats.get("prob_return_to_ib_after_session"),

            # --- COMMON STATS ---
            "avg_ib_range_usd": range_stats.get("avg_ib_range_usd"),
            "avg_ib_range_pct": range_stats.get("avg_ib_range_pct"),
            "avg_ib_volume": range_stats.get("avg_ib_volume"),

            # --- SESSION STATS (Intraday / until 16:00) ---
            "session": {
                # Breakouts
                "break_high_chance": s_brk.get("break_high_chance"),
                "break_low_chance": s_brk.get("break_low_chance"),
                "one_sided_chance": s_brk.get("one_sided_chance"),
                "two_sided_chance": s_brk.get("two_sided_chance"),
                "no_breakout_chance": s_brk.get("no_breakout_chance"),

                # Extensions
                "prob_hit_05x": s_ext.get("prob_hit_05x"),
                "prob_hit_1x": s_ext.get("prob_hit_1x"),
                "prob_hit_2x": s_ext.get("prob_hit_2x"),
                "avg_extension_coeff": s_ext.get("avg_extension_coeff"),

                # Priors
                "prob_hit_pdh": s_pri.get("prob_hit_pdh"),
                "prob_hit_pdl": s_pri.get("prob_hit_pdl"),
                "prob_pdh_if_ibh_broken": s_pri.get("prob_pdh_if_ibh_broken"),
                "prob_pdl_if_ibl_broken": s_pri.get("prob_pdl_if_ibl_broken"),

                "weekday_chop": wd_chop.get("session", {}),
                "weekday_targets": wd_targets.get("session", {}),

                "weekday_targets_clean": wd_targets_clean.get("session", {}),

                "prob_ib_mid_retest": s_retest.get("prob_ib_mid_retest"),

                "time_heatmap": time_stats.get("session", {}),
                "time_heatmap_clean": time_stats_clean.get("session", {}),
            },

            # --- FULL DAY STATS (Closing / until 23:59) ---
            "full_day": {
                # Breakouts
                "break_high_chance": f_brk.get("break_high_chance"),
                "break_low_chance": f_brk.get("break_low_chance"),
                "one_sided_chance": f_brk.get("one_sided_chance"),
                "two_sided_chance": f_brk.get("two_sided_chance"),
                "no_breakout_chance": f_brk.get("no_breakout_chance"),

                # Extensions
                "prob_hit_05x": f_ext.get("prob_hit_05x"),
                "prob_hit_1x": f_ext.get("prob_hit_1x"),
                "prob_hit_2x": f_ext.get("prob_hit_2x"),
                "avg_extension_coeff": f_ext.get("avg_extension_coeff"),

                # Priors
                "prob_hit_pdh": f_pri.get("prob_hit_pdh"),
                "prob_hit_pdl": f_pri.get("prob_hit_pdl"),
                "prob_pdh_if_ibh_broken": f_pri.get("prob_pdh_if_ibh_broken"),
                "prob_pdl_if_ibl_broken": f_pri.get("prob_pdl_if_ibl_broken"),

                "weekday_chop": wd_chop.get("full_day", {}),
                "weekday_targets": wd_targets.get("full_day", {}),
                "weekday_targets_clean": wd_targets_clean.get("full_day", {}),

                "prob_ib_mid_retest": f_retest.get("prob_ib_mid_retest"),

                "time_heatmap": time_stats.get("full_day", {}),
                "time_heatmap_clean": time_stats_clean.get("full_day", {}),
            },
        }

        return res
