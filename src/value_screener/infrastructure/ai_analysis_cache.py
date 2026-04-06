from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

_mem_lock = threading.Lock()
_mem_store: dict[str, tuple[float, dict[str, Any]]] = {}


@lru_cache(maxsize=1)
def _redis_client():  # type: ignore[no-untyped-def]
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return None
    import redis

    return redis.Redis.from_url(url, decode_responses=True)


def ai_analysis_cache_key(context_hash: str, model: str, prompt_version: str) -> str:
    raw = f"{context_hash}|{model}|{prompt_version}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:40]
    return f"scr:ai:{prompt_version}:{digest}"


def ai_cache_get(key: str) -> dict[str, Any] | None:
    client = _redis_client()
    if client is not None:
        try:
            raw = client.get(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ai cache redis get: %s", exc)
            raw = None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    now = time.time()
    with _mem_lock:
        hit = _mem_store.get(key)
        if hit is None:
            return None
        exp, payload = hit
        if exp < now:
            del _mem_store[key]
            return None
        return dict(payload)


def ai_cache_set(key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    if ttl_seconds <= 0:
        return
    client = _redis_client()
    if client is not None:
        try:
            client.setex(key, ttl_seconds, json.dumps(payload, ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001
            logger.warning("ai cache redis set: %s", exc)
        return

    exp = time.time() + float(ttl_seconds)
    with _mem_lock:
        _mem_store[key] = (exp, dict(payload))
        if len(_mem_store) > 512:
            _evict_mem_expired_unlocked(now=time.time())


def _evict_mem_expired_unlocked(*, now: float) -> None:
    dead = [k for k, (exp, _) in _mem_store.items() if exp < now]
    for k in dead:
        del _mem_store[k]
