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
    tushare_max_workers: int
    tushare_max_retries: int
    tushare_retry_backoff_seconds: float

    @classmethod
    def from_env(cls) -> AShareIngestionSettings:
        token = os.environ.get("TUSHARE_TOKEN", "").strip() or None
        primary = os.environ.get("VALUE_SCREENER_PRIMARY", "tushare").strip().lower()
        if primary not in {"tushare", "akshare"}:
            primary = "tushare"
        max_sym = os.environ.get("VALUE_SCREENER_MAX_SYMBOLS")
        max_symbols = int(max_sym) if max_sym and max_sym.isdigit() else None
        sleep_s = float(os.environ.get("VALUE_SCREENER_REQUEST_SLEEP", "0.12"))
        # 默认 4：未显式配置时启用有界并发，否则与旧版单线程无异、全市场仍极慢。
        # 限流敏感环境可设 VALUE_SCREENER_TUSHARE_MAX_WORKERS=1。
        workers_raw = os.environ.get("VALUE_SCREENER_TUSHARE_MAX_WORKERS", "4").strip()
        tushare_max_workers = int(workers_raw) if workers_raw.isdigit() else 4
        tushare_max_workers = max(1, min(tushare_max_workers, 64))
        retries_raw = os.environ.get("VALUE_SCREENER_TUSHARE_MAX_RETRIES", "2").strip()
        tushare_max_retries = int(retries_raw) if retries_raw.isdigit() else 2
        tushare_max_retries = max(0, min(tushare_max_retries, 10))
        backoff_s = float(os.environ.get("VALUE_SCREENER_TUSHARE_RETRY_BACKOFF", "0.5"))
        return cls(
            tushare_token=token,
            primary_backend=primary,
            max_symbols=max_symbols,
            request_sleep_seconds=max(0.0, sleep_s),
            tushare_max_workers=tushare_max_workers,
            tushare_max_retries=tushare_max_retries,
            tushare_retry_backoff_seconds=max(0.0, backoff_s),
        )
