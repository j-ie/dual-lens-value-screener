from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from value_screener.domain.scoring_params import GrahamScoringParams
from value_screener.domain.snapshot import StockFinancialSnapshot


@dataclass(frozen=True, slots=True)
class GrahamAssessment:
    """格雷厄姆维度：折价与安全边际代理（非精确内在价值）。"""

    score: float
    ncav: float | None
    market_cap_to_ncav: float | None
    current_ratio: float | None
    price_to_book: float | None
    notes: dict[str, Any]


class GrahamAssessor:
    """
    格雷厄姆式规则（初版）：
    - NCAV ≈ 流动资产 − 负债合计（简化；经典文献对负债口径有不同严格度）
    - 市值相对 NCAV 越低越偏「烟蒂」倾向
    - 流动比率、市净率作为辅助
    """

    def __init__(self, params: GrahamScoringParams | None = None) -> None:
        self._p = params if params is not None else GrahamScoringParams.from_env()

    def assess(self, s: StockFinancialSnapshot) -> GrahamAssessment:
        notes: dict[str, Any] = {}
        ncav = self._compute_ncav(s)
        m_to_ncav = None
        if ncav is not None and ncav > 0 and s.market_cap > 0:
            m_to_ncav = round(s.market_cap / ncav, 4)
            if m_to_ncav < 1.0:
                notes["net_net_tendency"] = True
        elif ncav is not None and ncav <= 0:
            notes["ncav_non_positive"] = True

        current_ratio = self._current_ratio(s)
        ptb = self._price_to_book(s)

        score = self._score(m_to_ncav, current_ratio, ptb, ncav is not None)
        return GrahamAssessment(
            score=round(score, 2),
            ncav=round(ncav, 2) if ncav is not None else None,
            market_cap_to_ncav=m_to_ncav,
            current_ratio=round(current_ratio, 4) if current_ratio is not None else None,
            price_to_book=round(ptb, 4) if ptb is not None else None,
            notes=notes,
        )

    @staticmethod
    def _compute_ncav(s: StockFinancialSnapshot) -> float | None:
        if s.total_current_assets is None:
            return None
        if s.total_liabilities is None:
            return None
        return s.total_current_assets - s.total_liabilities

    @staticmethod
    def _current_ratio(s: StockFinancialSnapshot) -> float | None:
        if s.total_current_assets is None or s.total_current_liabilities is None:
            return None
        if s.total_current_liabilities == 0:
            return None
        return s.total_current_assets / s.total_current_liabilities

    @staticmethod
    def _price_to_book(s: StockFinancialSnapshot) -> float | None:
        if s.total_equity is None or s.total_equity <= 0:
            return None
        return s.market_cap / s.total_equity

    def _score(
        self,
        m_to_ncav: float | None,
        current_ratio: float | None,
        ptb: float | None,
        has_ncav: bool,
    ) -> float:
        """0–100：越高越符合「格雷厄姆式便宜/安全边际」初版启发式。"""
        p = self._p
        parts: list[float] = []
        weights: list[float] = []

        if m_to_ncav is not None:
            lo, hi = p.mcap_ncav_full_score_at_most, p.mcap_ncav_zero_score_at_least
            if m_to_ncav <= lo:
                parts.append(100.0)
            elif m_to_ncav >= hi:
                parts.append(0.0)
            else:
                span = hi - lo
                parts.append(max(0.0, 100.0 * (hi - m_to_ncav) / span) if span > 0 else 0.0)
            weights.append(p.weight_ncav)
        elif has_ncav:
            parts.append(p.ncav_missing_partial_score)
            weights.append(p.weight_ncav)

        if current_ratio is not None:
            cap = p.current_ratio_cap
            capped = min(current_ratio, cap)
            parts.append((capped / cap) * 100.0 if cap > 0 else 0.0)
            weights.append(p.weight_current_ratio)

        if ptb is not None:
            lo, hi = p.pb_full_score_at_most, p.pb_zero_score_at_least
            if ptb <= lo:
                parts.append(100.0)
            elif ptb >= hi:
                parts.append(0.0)
            else:
                span = hi - lo
                parts.append(max(0.0, 100.0 * (hi - ptb) / span) if span > 0 else 0.0)
            weights.append(p.weight_pb)

        if not parts:
            return 0.0
        wsum = sum(weights)
        return sum(x * w for x, w in zip(parts, weights, strict=True)) / wsum
