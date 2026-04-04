from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

DEFAULT_TTL = 120

# 分页 JSON 形状变更时递增，避免 Redis 返回缺字段的旧缓存
RESULT_CACHE_ENRICH_VER = "enrich_v1"


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


def cache_key(
    run_id: int,
    page: int,
    page_size: int,
    sort_key: str,
    order: str,
) -> str:
    return (
        f"scr:{RESULT_CACHE_ENRICH_VER}:r{run_id}:p{page}:s{page_size}:k{sort_key}:o{order}"
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
