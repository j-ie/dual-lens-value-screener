"""财报同步任务：参数指纹与 universe 指纹（用于 ingestion_job 唯一键与一致性提示）。"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


def financial_statement_job_params_hash(
    *,
    since_years: int,
    max_symbols: int | None,
    api_start: str,
    api_end: str,
) -> str:
    """稳定序列化后取 SHA256 十六进制前 32 位，作为 params_hash 列。"""

    payload: dict[str, Any] = {
        "since_years": int(since_years),
        "max_symbols": max_symbols,
        "api_start": str(api_start),
        "api_end": str(api_end),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def universe_fingerprint(symbols: list[str]) -> str:
    """全市场列表指纹（换行拼接），用于检测 universe 变更。"""

    body = "\n".join(symbols).encode("utf-8")
    return hashlib.sha256(body).hexdigest()[:32]


def default_scheduled_date(*, tz_name: str) -> date:
    """调度日：指定时区下的日历日（默认上海）。"""

    return datetime.now(ZoneInfo(tz_name)).date()
