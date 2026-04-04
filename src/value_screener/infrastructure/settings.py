from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AShareIngestionSettings:
    """A 股拉数与批跑配置（环境变量 + 默认值）。"""

    tushare_token: str | None
    primary_backend: str
    max_symbols: int | None
    request_sleep_seconds: float

    @classmethod
    def from_env(cls) -> AShareIngestionSettings:
        token = os.environ.get("TUSHARE_TOKEN", "").strip() or None
        primary = os.environ.get("VALUE_SCREENER_PRIMARY", "tushare").strip().lower()
        if primary not in {"tushare", "akshare"}:
            primary = "tushare"
        max_sym = os.environ.get("VALUE_SCREENER_MAX_SYMBOLS")
        max_symbols = int(max_sym) if max_sym and max_sym.isdigit() else None
        sleep_s = float(os.environ.get("VALUE_SCREENER_REQUEST_SLEEP", "0.12"))
        return cls(
            tushare_token=token,
            primary_backend=primary,
            max_symbols=max_symbols,
            request_sleep_seconds=max(0.0, sleep_s),
        )
