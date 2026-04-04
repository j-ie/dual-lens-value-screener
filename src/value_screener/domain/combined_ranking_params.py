from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CombinedRankingParams:
    """综合排序与门槛（环境变量配置，启动时校验）。"""

    weight_buffett: float
    weight_graham: float
    gate_min_buffett: float | None
    gate_min_graham: float | None
    gate_min_combined: float | None
    tiebreak: str  # min_dim | sum_bg

    def cache_fingerprint(self) -> str:
        def _fv(v: float | None) -> str:
            return "none" if v is None else f"{v:.6g}"

        parts = [
            f"wb={self.weight_buffett:.6g}",
            f"wg={self.weight_graham:.6g}",
            f"tb={_fv(self.gate_min_buffett)}",
            f"tg={_fv(self.gate_min_graham)}",
            f"tc={_fv(self.gate_min_combined)}",
            f"tbk={self.tiebreak}",
        ]
        return "|".join(parts)

    @classmethod
    def from_env(cls) -> CombinedRankingParams:
        wb = _env_float("VALUE_SCREENER_COMBINED_WEIGHT_BUFFETT", 0.5)
        wg = _env_float("VALUE_SCREENER_COMBINED_WEIGHT_GRAHAM", 0.5)
        s = wb + wg
        if wb < 0 or wg < 0:
            raise ValueError("VALUE_SCREENER_COMBINED_WEIGHT_* 不可为负")
        if abs(s - 1.0) > 0.02:
            raise ValueError(
                "VALUE_SCREENER_COMBINED_WEIGHT_BUFFETT + VALUE_SCREENER_COMBINED_WEIGHT_GRAHAM "
                f"须约等于 1（当前和为 {s}）",
            )
        tb = _env_optional_float("VALUE_SCREENER_GATE_MIN_BUFFETT")
        tg = _env_optional_float("VALUE_SCREENER_GATE_MIN_GRAHAM")
        tc = _env_optional_float("VALUE_SCREENER_GATE_MIN_COMBINED")
        raw_tbk = os.environ.get("VALUE_SCREENER_COMBINED_TIEBREAK", "min_dim").strip().lower()
        if raw_tbk not in ("min_dim", "sum_bg"):
            raise ValueError("VALUE_SCREENER_COMBINED_TIEBREAK 须为 min_dim 或 sum_bg")
        return cls(
            weight_buffett=wb,
            weight_graham=wg,
            gate_min_buffett=tb,
            gate_min_graham=tg,
            gate_min_combined=tc,
            tiebreak=raw_tbk,
        )


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return float(raw)


def _env_optional_float(name: str) -> float | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    return float(raw)


def snapshot_ttl_seconds() -> int:
    """财务快照缓存 TTL；<=0 表示禁用 DB 缓存命中（仍写入）。"""

    raw = os.environ.get("VALUE_SCREENER_SNAPSHOT_TTL_SECONDS", "86400").strip()
    try:
        return int(raw)
    except ValueError:
        return 86400


def snapshot_cache_enabled() -> bool:
    return os.environ.get("VALUE_SCREENER_SNAPSHOT_CACHE_ENABLED", "1").strip() not in (
        "0",
        "false",
        "no",
    )
