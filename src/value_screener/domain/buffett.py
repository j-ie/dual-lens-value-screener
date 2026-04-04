from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from value_screener.domain.scoring_params import BuffettScoringParams
from value_screener.domain.snapshot import StockFinancialSnapshot


@dataclass(frozen=True, slots=True)
class BuffettAssessment:
    """巴菲特（后期）维度：资本回报、杠杆与现金流质量的初版代理。"""

    score: float
    roe: float | None
    debt_to_equity: float | None
    ocf_to_net_income: float | None
    notes: dict[str, Any]


class BuffettAssessor:
    """
    质量启发式（初版，非「护城河」主观判断）：
    - ROE
    - 杠杆（有息负债/权益，缺失时用负债合计/权益作粗代理并标注）
    - 经营现金流相对净利润
    """

    def __init__(self, params: BuffettScoringParams | None = None) -> None:
        self._p = params if params is not None else BuffettScoringParams.from_env()

    def assess(self, s: StockFinancialSnapshot) -> BuffettAssessment:
        notes: dict[str, Any] = {}
        p = self._p
        roe = self._roe(s)
        de, crude = self._debt_to_equity(s)
        if crude:
            notes["debt_proxy_crude"] = True
        ocf_ni = self._ocf_to_ni(s)
        if ocf_ni is not None and ocf_ni < p.weak_cash_conversion_below:
            notes["weak_cash_conversion"] = True

        score = self._score(roe, de, ocf_ni)
        return BuffettAssessment(
            score=round(score, 2),
            roe=round(roe, 4) if roe is not None else None,
            debt_to_equity=round(de, 4) if de is not None else None,
            ocf_to_net_income=round(ocf_ni, 4) if ocf_ni is not None else None,
            notes=notes,
        )

    @staticmethod
    def _roe(s: StockFinancialSnapshot) -> float | None:
        if s.total_equity is None or s.net_income_ttm is None:
            return None
        if s.total_equity <= 0:
            return None
        return s.net_income_ttm / s.total_equity

    def _debt_to_equity(self, s: StockFinancialSnapshot) -> tuple[float | None, bool]:
        if s.total_equity is None or s.total_equity <= 0:
            return None, False
        if s.interest_bearing_debt is not None:
            return s.interest_bearing_debt / s.total_equity, False
        if s.total_liabilities is not None:
            return s.total_liabilities / s.total_equity, True
        return None, False

    @staticmethod
    def _ocf_to_ni(s: StockFinancialSnapshot) -> float | None:
        if s.net_income_ttm is None or s.operating_cash_flow_ttm is None:
            return None
        if abs(s.net_income_ttm) < 1e-9:
            return None
        return s.operating_cash_flow_ttm / s.net_income_ttm

    def _score(
        self,
        roe: float | None,
        debt_to_equity: float | None,
        ocf_to_ni: float | None,
    ) -> float:
        """0–100：越高越偏「好生意/高质量」初版启发式。"""
        p = self._p
        parts: list[float] = []
        weights: list[float] = []

        if roe is not None:
            ex = p.roe_excellent
            if roe >= ex:
                parts.append(100.0)
            elif roe <= 0:
                parts.append(0.0)
            else:
                parts.append(max(0.0, min(100.0, roe / ex * 100.0)) if ex > 0 else 0.0)
            weights.append(p.roe_weight)

        if debt_to_equity is not None:
            lo, hi = p.debt_low, p.debt_high
            if debt_to_equity <= lo:
                parts.append(100.0)
            elif debt_to_equity >= hi:
                parts.append(0.0)
            else:
                span = hi - lo
                parts.append(max(0.0, 100.0 * (hi - debt_to_equity) / span) if span > 0 else 0.0)
            weights.append(p.debt_weight)

        if ocf_to_ni is not None:
            if ocf_to_ni >= 1.0:
                parts.append(100.0)
            elif ocf_to_ni <= 0:
                parts.append(0.0)
            else:
                parts.append(max(0.0, min(100.0, ocf_to_ni * 100.0)))
            weights.append(p.ocf_to_ni_weight)

        if not parts:
            return 0.0
        wsum = sum(weights)
        return sum(x * w for x, w in zip(parts, weights, strict=True)) / wsum
