"""批跑编排：拉数 → 算分 → 可选写入 MySQL。"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, Literal

from sqlalchemy.engine import Engine

from value_screener.application.batch_screening_service import (
    BatchScreeningApplicationService,
    BatchScreeningResult,
)
from value_screener.application.persist_screening_run import (
    mark_screening_run_failed,
    persist_batch_screening,
    persist_batch_screening_for_run,
)
from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.factory import build_composite_provider
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.settings import AShareIngestionSettings

logger = logging.getLogger(__name__)


def _build_throttled_progress_sink(
    engine: Engine,
    run_id: int,
    *,
    min_interval_seconds: float = 0.85,
    min_index_step: int = 15,
) -> Callable[[dict[str, Any]], None]:
    """
    将批跑进度节流写入 DB 并打日志，避免全市场时每秒数千次 UPDATE / 日志行。
    首只、末只、算分/完成阶段始终刷新。
    """

    repo = ScreeningRepository(engine)
    last_t = 0.0
    last_idx = -1

    def sink(payload: dict[str, Any]) -> None:
        nonlocal last_t, last_idx
        now = time.monotonic()
        cur = int(payload.get("progress_current") or 0)
        tot = int(payload.get("progress_total") or 0)
        phase = str(payload.get("progress_phase") or "")
        force = (
            phase in ("done", "score")
            or cur <= 1
            or (tot > 0 and cur >= tot)
            or (cur - last_idx >= min_index_step)
            or (now - last_t >= min_interval_seconds)
        )
        if not force:
            return
        patch = {
            "progress_percent": int(payload.get("progress_percent") or 0),
            "progress_current": cur,
            "progress_total": tot,
            "progress_phase": phase,
            "progress_symbol": str(payload.get("progress_symbol") or ""),
        }
        try:
            repo.merge_meta_json_patch(run_id, patch)
        except Exception:
            logger.warning("run_id=%s 写入进度 meta 失败（可忽略单次）", run_id, exc_info=True)
        logger.info(
            "batch-screen run_id=%s 进度 %s/%s (%s%%) phase=%s %s",
            run_id,
            cur,
            tot,
            patch["progress_percent"],
            phase,
            patch["progress_symbol"] or "-",
        )
        last_t = now
        last_idx = cur

    return sink


def execute_batch_screen(
    *,
    max_symbols: int | None,
    symbols: list[str] | None,
    primary_backend: Literal["tushare", "akshare"],
    persist: bool,
    on_batch_progress: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[BatchScreeningResult, int | None]:
    """
    执行一次批跑；persist 为 True 时写入新 screening_run 并返回 run_id。
    """

    base = AShareIngestionSettings.from_env()
    settings = AShareIngestionSettings(
        tushare_token=base.tushare_token,
        primary_backend=primary_backend,
        max_symbols=base.max_symbols,
        request_sleep_seconds=base.request_sleep_seconds,
    )
    provider = build_composite_provider(settings)
    batch_svc = BatchScreeningApplicationService(provider, ScreeningApplicationService())
    result = batch_svc.run(
        symbols=symbols,
        max_symbols=max_symbols,
        on_batch_progress=on_batch_progress,
    )
    if not persist:
        return result, None
    engine = get_engine()
    prov = result.meta.get("provider")
    run_id = persist_batch_screening(
        engine,
        result,
        provider_label=str(prov) if prov else None,
    )
    return result, run_id


def run_batch_screen_background(run_id: int, max_symbols: int | None) -> None:
    """HTTP 异步批跑：在后台线程/任务中执行并写入已创建的 run_id。"""

    try:
        engine = get_engine()
        progress_sink = _build_throttled_progress_sink(engine, run_id)
        result, _ = execute_batch_screen(
            max_symbols=max_symbols,
            symbols=None,
            primary_backend="tushare",
            persist=False,
            on_batch_progress=progress_sink,
        )
        prov = result.meta.get("provider")
        persist_batch_screening_for_run(
            engine,
            run_id,
            result,
            provider_label=str(prov) if prov else None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("异步 batch-screen run_id=%s 失败", run_id)
        try:
            mark_screening_run_failed(get_engine(), run_id, str(exc))
        except Exception as mark_exc:  # noqa: BLE001
            logger.exception("标记 run 失败时出错 run_id=%s: %s", run_id, mark_exc)
