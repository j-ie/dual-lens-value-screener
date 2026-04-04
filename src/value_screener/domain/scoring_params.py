"""评分启发式阈值与权重（可环境变量覆盖，便于调参与单测注入）。"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class GrahamScoringParams:
    """格雷厄姆维度数值参数。"""

    mcap_ncav_full_score_at_most: float = 0.67
    mcap_ncav_zero_score_at_least: float = 2.0
    current_ratio_cap: float = 3.0
    weight_ncav: float = 0.5
    weight_current_ratio: float = 0.2
    weight_pb: float = 0.3
    pb_full_score_at_most: float = 0.5
    pb_zero_score_at_least: float = 3.0
    ncav_missing_partial_score: float = 20.0

    @classmethod
    def default(cls) -> GrahamScoringParams:
        return cls()

    @classmethod
    def from_env(cls) -> GrahamScoringParams:
        p = cls.default()
        if raw := os.environ.get("VALUE_SCREENER_GRAHAM_MCAP_NCAV_FULL", "").strip():
            try:
                p = replace(p, mcap_ncav_full_score_at_most=float(raw))
            except ValueError:
                pass
        if raw := os.environ.get("VALUE_SCREENER_GRAHAM_MCAP_NCAV_ZERO", "").strip():
            try:
                p = replace(p, mcap_ncav_zero_score_at_least=float(raw))
            except ValueError:
                pass
        if raw := os.environ.get("VALUE_SCREENER_GRAHAM_PB_FULL", "").strip():
            try:
                p = replace(p, pb_full_score_at_most=float(raw))
            except ValueError:
                pass
        if raw := os.environ.get("VALUE_SCREENER_GRAHAM_PB_ZERO", "").strip():
            try:
                p = replace(p, pb_zero_score_at_least=float(raw))
            except ValueError:
                pass
        return p


@dataclass(frozen=True, slots=True)
class BuffettScoringParams:
    """巴菲特维度数值参数。"""

    roe_excellent: float = 0.20
    roe_weight: float = 0.45
    debt_low: float = 0.3
    debt_high: float = 2.0
    debt_weight: float = 0.30
    ocf_to_ni_weight: float = 0.25
    weak_cash_conversion_below: float = 0.5

    @classmethod
    def default(cls) -> BuffettScoringParams:
        return cls()

    @classmethod
    def from_env(cls) -> BuffettScoringParams:
        p = cls.default()
        if raw := os.environ.get("VALUE_SCREENER_BUFFETT_ROE_EXCELLENT", "").strip():
            try:
                p = replace(p, roe_excellent=float(raw))
            except ValueError:
                pass
        if raw := os.environ.get("VALUE_SCREENER_BUFFETT_DEBT_LOW", "").strip():
            try:
                p = replace(p, debt_low=float(raw))
            except ValueError:
                pass
        if raw := os.environ.get("VALUE_SCREENER_BUFFETT_DEBT_HIGH", "").strip():
            try:
                p = replace(p, debt_high=float(raw))
            except ValueError:
                pass
        return p
