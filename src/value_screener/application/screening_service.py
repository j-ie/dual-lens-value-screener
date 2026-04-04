from __future__ import annotations

from dataclasses import asdict

from value_screener.domain.buffett import BuffettAssessor
from value_screener.domain.graham import GrahamAssessor
from value_screener.domain.snapshot import StockFinancialSnapshot


class ScreeningApplicationService:
    """应用服务：对快照列表并列执行格雷厄姆与巴菲特评估（不合并为单一总分）。"""

    def __init__(self) -> None:
        self._graham = GrahamAssessor()
        self._buffett = BuffettAssessor()

    def screen(self, items: list[StockFinancialSnapshot]) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for snap in items:
            g = self._graham.assess(snap)
            b = self._buffett.assess(snap)
            results.append(
                {
                    "symbol": snap.symbol,
                    "provenance": {
                        "data_source": snap.data_source,
                        "trade_cal_date": snap.trade_cal_date,
                        "financials_end_date": snap.financials_end_date,
                        "market_cap": float(snap.market_cap),
                    },
                    "graham": asdict(g),
                    "buffett": asdict(b),
                }
            )
        return results
