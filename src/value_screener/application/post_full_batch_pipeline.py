"""全市场批跑成功后的后置流水线：第三套/三元分、Top N 的 DCF+AI 落库。"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.attach_third_lens_scores import attach_third_lens_for_run
from value_screener.application.company_ai_analysis import (
    CompanyAiAnalysisApplicationService,
    CompanyAiDetailError,
    CompanyAiUnavailableError,
    CompanyAiUpstreamError,
)
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.infrastructure.result_cache import invalidate_screening_run_results_cache
from value_screener.infrastructure.screening_repository import ScreeningRepository
from value_screener.infrastructure.settings import CompanyAiAnalysisSettings, PostFullBatchPipelineSettings

logger = logging.getLogger(__name__)

# 仅「明确仍在跑」的阶段；third_lens_* 为短过渡态，若进程崩溃会误锁 UI，故不列入。
POST_PIPELINE_BUSY_PHASES: frozenset[str] = frozenset({"starting", "ai_running"})


def _post_pipeline_stale_minutes() -> float:
    raw = os.environ.get("VALUE_SCREENER_POST_PIPELINE_STALE_MINUTES", "45").strip()
    try:
        m = float(raw) if raw else 45.0
    except ValueError:
        m = 45.0
    return max(5.0, min(m, 24 * 60.0))


def _touch_post_pipeline_activity(patch: dict[str, Any]) -> dict[str, Any]:
    """合并写入「最近活动时间」，供 is_post_pipeline_busy 与前端判断长耗时 AI 循环未僵死。"""

    out = dict(patch)
    out["post_pipeline_activity_at"] = datetime.now(timezone.utc).isoformat()
    return out


def _post_pipeline_staleness_reference(meta: dict[str, Any]) -> datetime | None:
    """取 started_at 与 activity_at 中较新者，避免 Top N AI 整批超过 45 分钟时被误判为可重复排队。"""

    ts: list[datetime] = []
    for key in ("post_pipeline_activity_at", "post_pipeline_started_at"):
        parsed = _parse_started_at(meta.get(key))
        if parsed is not None:
            ts.append(parsed)
    return max(ts) if ts else None


def _parse_started_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        # 来自 meta 的 ISO 字符串（可能带 Z）
        normalized = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_post_pipeline_busy(meta: dict[str, Any] | None) -> bool:
    """
    是否应禁止再次排队后置任务（与前端「后置任务」置灰、HTTP 409 对齐）。
    以 post_pipeline_activity_at 与 post_pipeline_started_at 中较新者为锚：超过
    VALUE_SCREENER_POST_PIPELINE_STALE_MINUTES（默认 45）分钟无更新仍卡在 starting/ai_running 视为僵尸锁，允许重试。
    """

    if not meta:
        return False
    phase_raw = meta.get("post_pipeline_phase")
    phase = str(phase_raw).strip() if phase_raw is not None else ""
    if phase not in POST_PIPELINE_BUSY_PHASES:
        return False
    ref = _post_pipeline_staleness_reference(meta)
    if ref is None:
        return False
    age_sec = (datetime.now(timezone.utc) - ref).total_seconds()
    if age_sec > _post_pipeline_stale_minutes() * 60.0:
        return False
    return True


def run_post_full_batch_pipeline(
    engine: Engine,
    run_id: int,
    settings: PostFullBatchPipelineSettings | None = None,
) -> dict[str, Any]:
    """
    在独立线程中调用；勿与同一 run 的其它写库并发。

    顺序：可选 attach_third_lens → 按综合分取 Top N → 逐只调用与详情页一致的 AI 分析（内含 DCF）。
    """

    cfg = settings or PostFullBatchPipelineSettings.from_env()
    repo = ScreeningRepository(engine)
    ranking = CombinedRankingParams.from_env()
    summary: dict[str, Any] = {
        "run_id": run_id,
        "third_lens": None,
        "ai_target": 0,
        "ai_ok": 0,
        "ai_failed": 0,
        "skipped_ai_reason": None,
    }

    started = datetime.now(timezone.utc).isoformat()
    repo.merge_meta_json_patch(
        run_id,
        _touch_post_pipeline_activity(
            {
                "post_pipeline_phase": "starting",
                "post_pipeline_started_at": started,
            },
        ),
    )

    if cfg.attach_third_lens:
        try:
            tl_meta = attach_third_lens_for_run(engine, run_id)
            summary["third_lens"] = tl_meta
            repo.merge_meta_json_patch(
                run_id,
                _touch_post_pipeline_activity(
                    {
                        "post_pipeline_phase": "third_lens_done",
                        "post_pipeline_third_lens_updated_rows": tl_meta.get("updated"),
                    },
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("post_pipeline attach_third_lens 失败 run_id=%s", run_id)
            summary["third_lens"] = {"error": str(exc)}
            repo.merge_meta_json_patch(
                run_id,
                _touch_post_pipeline_activity(
                    {
                        "post_pipeline_phase": "third_lens_failed",
                        "post_pipeline_third_lens_error": str(exc)[:500],
                    },
                ),
            )
    else:
        repo.merge_meta_json_patch(
            run_id,
            _touch_post_pipeline_activity({"post_pipeline_phase": "third_lens_skipped"}),
        )

    invalidate_screening_run_results_cache(run_id)

    n_ai = cfg.ai_top_n
    summary["ai_target"] = n_ai
    if n_ai <= 0:
        summary["skipped_ai_reason"] = "ai_top_n=0（环境变量 VALUE_SCREENER_POST_FULL_BATCH_AI_TOP_N）"
        finished = datetime.now(timezone.utc).isoformat()
        repo.merge_meta_json_patch(
            run_id,
            _touch_post_pipeline_activity(
                {
                    "post_pipeline_phase": "done",
                    "post_pipeline_finished_at": finished,
                    "post_pipeline_ai_ok": 0,
                    "post_pipeline_ai_failed": 0,
                    "post_pipeline_ai_skip_reason": summary["skipped_ai_reason"][:500],
                },
            ),
        )
        return summary

    with engine.connect() as conn:
        symbols = repo.list_top_symbols_by_combined(conn, run_id, ranking=ranking, limit=n_ai)
        symbol_pick = "combined_gates"
        if not symbols:
            relaxed = repo.list_top_symbols_weighted_desc_coverage_only(
                conn, run_id, ranking=ranking, limit=n_ai
            )
            if relaxed:
                symbols = relaxed
                symbol_pick = "weighted_coverage_only"
                logger.warning(
                    "post_pipeline 综合榜门槛下无标的，已回退为 coverage_ok+加权分排序 Top N run_id=%s n=%s",
                    run_id,
                    len(symbols),
                )
            else:
                summary["skipped_ai_reason"] = (
                    "no_eligible_symbols：综合门槛与回退列表均无标的（请检查 coverage_ok 与 gate 环境变量）"
                )
                finished = datetime.now(timezone.utc).isoformat()
                repo.merge_meta_json_patch(
                    run_id,
                    _touch_post_pipeline_activity(
                        {
                            "post_pipeline_phase": "done",
                            "post_pipeline_finished_at": finished,
                            "post_pipeline_ai_skip_reason": summary["skipped_ai_reason"][:500],
                            "post_pipeline_ai_symbol_pick": symbol_pick,
                        },
                    ),
                )
                return summary

    repo.merge_meta_json_patch(
        run_id,
        _touch_post_pipeline_activity({"post_pipeline_ai_symbol_pick": symbol_pick}),
    )

    ai_ready = CompanyAiAnalysisSettings.from_env()
    if not ai_ready.is_ready():
        gaps = ai_ready.readiness_gaps_zh()
        summary["skipped_ai_reason"] = "；".join(gaps) if gaps else "AI 未就绪"
        logger.info("post_pipeline 跳过 AI run_id=%s：%s", run_id, summary["skipped_ai_reason"])
        finished = datetime.now(timezone.utc).isoformat()
        repo.merge_meta_json_patch(
            run_id,
            _touch_post_pipeline_activity(
                {
                    "post_pipeline_phase": "done",
                    "post_pipeline_finished_at": finished,
                    "post_pipeline_ai_ok": 0,
                    "post_pipeline_ai_failed": 0,
                    "post_pipeline_ai_skip_reason": (summary["skipped_ai_reason"] or "")[:500],
                },
            ),
        )
        invalidate_screening_run_results_cache(run_id)
        return summary

    logger.info(
        "post_pipeline 开始调用 AI run_id=%s 标的数=%s symbol_pick=%s",
        run_id,
        len(symbols),
        symbol_pick,
    )

    ai_svc = CompanyAiAnalysisApplicationService(engine)
    ok = 0
    failed = 0
    for idx, code in enumerate(symbols):
        repo.merge_meta_json_patch(
            run_id,
            _touch_post_pipeline_activity(
                {
                    "post_pipeline_phase": "ai_running",
                    "post_pipeline_ai_index": idx + 1,
                    "post_pipeline_ai_total": len(symbols),
                    "post_pipeline_ai_symbol": code,
                },
            ),
        )
        try:
            ai_svc.analyze(run_id, code)
            ok += 1
        except CompanyAiUnavailableError as exc:
            summary["skipped_ai_reason"] = str(exc)
            logger.warning("post_pipeline AI 中途不可用 run_id=%s: %s", run_id, exc)
            break
        except (CompanyAiDetailError, CompanyAiUpstreamError) as exc:
            failed += 1
            logger.warning("post_pipeline AI 单标失败 run_id=%s %s: %s", run_id, code, exc)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.exception("post_pipeline AI 未预期错误 run_id=%s %s", run_id, code)

        if cfg.ai_sleep_seconds > 0 and idx + 1 < len(symbols):
            time.sleep(cfg.ai_sleep_seconds)

    summary["ai_ok"] = ok
    summary["ai_failed"] = failed
    finished = datetime.now(timezone.utc).isoformat()
    final_patch: dict[str, Any] = {
        "post_pipeline_phase": "done",
        "post_pipeline_finished_at": finished,
        "post_pipeline_ai_ok": ok,
        "post_pipeline_ai_failed": failed,
    }
    if summary.get("skipped_ai_reason"):
        final_patch["post_pipeline_ai_skip_reason"] = str(summary["skipped_ai_reason"])[:500]
    else:
        final_patch["post_pipeline_ai_skip_reason"] = None
    repo.merge_meta_json_patch(run_id, _touch_post_pipeline_activity(final_patch))
    invalidate_screening_run_results_cache(run_id)
    logger.info(
        "post_pipeline 完成 run_id=%s third_lens=%s ai_ok=%s ai_failed=%s",
        run_id,
        "yes" if cfg.attach_third_lens else "skip",
        ok,
        failed,
    )
    return summary
