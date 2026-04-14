from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

from value_screener.application.investment_quality_view import build_investment_quality_from_snapshot
from value_screener.domain.buffett import BuffettAssessor
from value_screener.domain.graham import GrahamAssessor
from value_screener.domain.investment_quality import InvestmentQualityAnalyzer
from value_screener.domain.snapshot import StockFinancialSnapshot


class ScreeningApplicationService:
    """应用服务：对快照并列执行格雷厄姆、巴菲特与价值质量（与详情页规则一致，含行业周期口径）。"""

    def __init__(self) -> None:
        self._graham = GrahamAssessor()
        self._buffett = BuffettAssessor()
        self._investment_quality = InvestmentQualityAnalyzer()

    def _screen_one(
        self,
        snap: StockFinancialSnapshot,
        industry: str | None,
    ) -> dict[str, object]:
        g = self._graham.assess(snap)
        b = self._buffett.assess(snap)
        iq = build_investment_quality_from_snapshot(
            self._investment_quality,
            snap,
            industry=industry,
            ts_code=snap.symbol,
        )
        prov: dict[str, object] = {
            "data_source": snap.data_source,
            "trade_cal_date": snap.trade_cal_date,
            "financials_end_date": snap.financials_end_date,
            "market_cap": float(snap.market_cap),
        }
        if snap.dv_ratio is not None:
            prov["dv_ratio"] = float(snap.dv_ratio)
        if snap.dv_ttm is not None:
            prov["dv_ttm"] = float(snap.dv_ttm)
        return {
            "symbol": snap.symbol,
            "provenance": prov,
            "graham": asdict(g),
            "buffett": asdict(b),
            "investment_quality": iq,
        }

    def screen(
        self,
        items: list[StockFinancialSnapshot],
        *,
        industry_by_symbol: dict[str, str] | None = None,
        parallel: bool = True,
    ) -> list[dict[str, object]]:
        """
        批跑算分；价值质量与 security_reference 行业对齐（缺行业则按一般工商业口径）。
        parallel=True 时用线程池并行单票算分（I/O 与释 GIL 段叠加吞吐）。
        """

        if not items:
            return []
        ind = industry_by_symbol or {}
        if not parallel or len(items) < 4:
            return [self._screen_one(s, ind.get(s.symbol) or None) for s in items]

        max_workers = min(16, len(items), max(4, (os.cpu_count() or 4) * 2))

        def _work(snap: StockFinancialSnapshot) -> dict[str, object]:
            return self._screen_one(snap, ind.get(snap.symbol) or None)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            return list(pool.map(_work, items))
