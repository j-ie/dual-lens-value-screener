from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TripleCompositeParams:
    """三元综合分：巴菲特 / 格雷厄姆 / 第三套 权重（环境变量，与 combined 独立）。"""

    weight_buffett: float
    weight_graham: float
    weight_third: float

    def cache_fingerprint(self) -> str:
        return (
            f"tri_wb={self.weight_buffett:.6g}|tri_wg={self.weight_graham:.6g}|tri_wt={self.weight_third:.6g}"
        )

    @classmethod
    def from_env(cls) -> TripleCompositeParams:
        wb = _env_float("VALUE_SCREENER_TRIPLE_WEIGHT_BUFFETT", 1.0 / 3.0)
        wg = _env_float("VALUE_SCREENER_TRIPLE_WEIGHT_GRAHAM", 1.0 / 3.0)
        wt = _env_float("VALUE_SCREENER_TRIPLE_WEIGHT_THIRD", 1.0 / 3.0)
        if wb < 0 or wg < 0 or wt < 0:
            raise ValueError("VALUE_SCREENER_TRIPLE_WEIGHT_* 不可为负")
        s = wb + wg + wt
        if abs(s - 1.0) > 0.02:
            raise ValueError(
                "VALUE_SCREENER_TRIPLE_WEIGHT_BUFFETT + GRAHAM + THIRD 须约等于 1 "
                f"（当前和为 {s}）",
            )
        return cls(weight_buffett=wb, weight_graham=wg, weight_third=wt)


@dataclass(frozen=True, slots=True)
class ThirdLensSubWeights:
    """第三套内部：成长分位与估值分位权重。"""

    weight_growth: float
    weight_valuation: float

    @classmethod
    def from_env(cls) -> ThirdLensSubWeights:
        g = _env_float("VALUE_SCREENER_THIRD_LENS_WEIGHT_GROWTH", 0.5)
        v = _env_float("VALUE_SCREENER_THIRD_LENS_WEIGHT_VALUATION", 0.5)
        if g < 0 or v < 0:
            raise ValueError("VALUE_SCREENER_THIRD_LENS_WEIGHT_* 不可为负")
        s = g + v
        if abs(s - 1.0) > 0.02:
            raise ValueError(
                "VALUE_SCREENER_THIRD_LENS_WEIGHT_GROWTH + VALUATION 须约等于 1 "
                f"（当前和为 {s}）",
            )
        return cls(weight_growth=g, weight_valuation=v)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return float(raw)
