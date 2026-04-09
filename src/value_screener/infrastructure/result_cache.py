from __future__ import annotations

import hashlib
import json
import os
import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TTL = 120

# 分页 JSON 形状变更时递增，避免 Redis 返回缺字段的旧缓存
RESULT_CACHE_ENRICH_VER = "enrich_v9"


@lru_cache(maxsize=1)
def _redis_client():  # type: ignore[no-untyped-def]
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return None
    import redis

    return redis.Redis.from_url(url, decode_responses=True)


def cache_ttl_seconds() -> int:
    raw = os.environ.get("CACHE_TTL_SECONDS", str(DEFAULT_TTL))
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_TTL


def ai_results_cache_fingerprint(
    has_ai_analysis: bool | None,
    ai_score_min: float | None,
) -> str:
    """AI 持久化筛选维度，用于 Redis 分页键。"""

    parts: list[str] = []
    if has_ai_analysis is True:
        parts.append("hai1")
    if ai_score_min is not None:
        parts.append(f"amin:{ai_score_min:.6g}")
    return "|".join(parts) if parts else ""


def company_name_cache_fingerprint(company_name: str | None) -> str:
    """公司名称模糊检索，用于 Redis 分页键（摘要避免键过长）。"""

    from value_screener.infrastructure.company_name_search import normalized_company_search_term

    n = normalized_company_search_term(company_name)
    if not n:
        return ""
    digest = hashlib.sha256(n.encode("utf-8")).hexdigest()[:16]
    return f"cn:{digest}"


def iq_decisions_cache_fingerprint(iq_decisions: list[str] | None) -> str:
    """价值质量结论多选筛选，用于 Redis 分页键。"""

    if not iq_decisions:
        return ""
    parts = sorted({x.strip() for x in iq_decisions if x and str(x).strip()})
    if not parts:
        return ""
    return "iq:" + "|".join(parts)


def valuation_filters_cache_fingerprint(
    market_cap_min: float | None,
    market_cap_max: float | None,
    dividend_yield_min: float | None,
    dividend_yield_max: float | None,
) -> str:
    """市值/股息率区间筛选，用于 Redis 分页键。"""

    parts: list[str] = []
    if market_cap_min is not None:
        parts.append(f"mcmin:{market_cap_min:.6g}")
    if market_cap_max is not None:
        parts.append(f"mcmax:{market_cap_max:.6g}")
    if dividend_yield_min is not None:
        parts.append(f"dvmin:{dividend_yield_min:.6g}")
    if dividend_yield_max is not None:
        parts.append(f"dvmax:{dividend_yield_max:.6g}")
    return "|".join(parts) if parts else ""


def industries_cache_fingerprint(industries: list[str] | None) -> str:
    """稳定编码行业多选，用于 Redis 键（与查询 OR 语义一致）。"""

    if not industries:
        return ""
    norm = sorted({i.strip() for i in industries if i and i.strip()})
    if not norm:
        return ""
    digest = hashlib.sha256("|".join(norm).encode("utf-8")).hexdigest()[:20]
    return f"ind:{digest}"


def cache_key(
    run_id: int,
    page: int,
    page_size: int,
    sort_key: str,
    order: str,
    filter_fingerprint: str = "",
) -> str:
    # sort_key 含 ai_score；filter_fingerprint 含 has_ai_analysis / ai_score_min（见 ai_results_cache_fingerprint）
    fp = filter_fingerprint or "none"
    return (
        f"scr:{RESULT_CACHE_ENRICH_VER}:r{run_id}:p{page}:s{page_size}:k{sort_key}:o{order}:f{fp}"
    )


def cache_get_json(key: str) -> dict[str, Any] | None:
    client = _redis_client()
    if client is None:
        return None
    raw = client.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def cache_set_json(key: str, payload: dict[str, Any], ttl: int) -> None:
    if ttl <= 0:
        return
    client = _redis_client()
    if client is None:
        return
    client.setex(key, ttl, json.dumps(payload, ensure_ascii=False))


def invalidate_screening_run_results_cache(run_id: int) -> int:
    """删除某 run 下分页结果缓存键（第三套/AI 更新后避免短期命中旧页）。"""

    client = _redis_client()
    if client is None:
        return 0
    rid = int(run_id)
    pattern = f"scr:*:r{rid}:*"
    deleted = 0
    try:
        for key in client.scan_iter(match=pattern, count=200):
            client.delete(key)
            deleted += 1
    except Exception as exc:
        logger.warning("invalidate_screening_run_results_cache 失败 run_id=%s: %s", rid, exc)
    return deleted
