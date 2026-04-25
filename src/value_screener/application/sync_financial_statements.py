from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.application.financial_statement_job_params import (
    default_scheduled_date,
    financial_statement_job_params_hash,
    universe_fingerprint,
)
from value_screener.application.financial_statement_window import (
    end_date_in_window,
    statement_api_date_bounds,
    utc_now,
)
from value_screener.infrastructure.financial_statement_repository import FinancialStatementRepository
from value_screener.infrastructure.ingestion_job_repository import IngestionJobRepository
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository
from value_screener.infrastructure.settings import AShareIngestionSettings
from value_screener.infrastructure.tushare_financial_statement_fetcher import TushareFinancialStatementFetcher
from value_screener.infrastructure.tushare_provider import TushareAShareProvider
from value_screener.infrastructure.tushare_rate_limiter import TushareRateLimiter

logger = logging.getLogger(__name__)

_DATA_SOURCE = "tushare"


def resume_start_index(
    symbols: list[str],
    *,
    resume: bool,
    cursor_ts_code: str | None,
) -> int:
    """由有序 universe 与游标（下一待处理 ts_code）得到起始下标，供单测与续跑。"""

    if not resume or not cursor_ts_code:
        return 0
    try:
        return symbols.index(cursor_ts_code)
    except ValueError:
        return 0


def _resolve_universe(
    engine: Engine,
    token: str,
    settings: AShareIngestionSettings,
    rate_limiter: TushareRateLimiter | None,
) -> list[str]:
    ref = ReferenceMasterRepository(engine)
    with engine.connect() as conn:
        codes = ref.list_active_ts_codes(conn)
    if codes:
        return sorted(codes)
    prov = TushareAShareProvider(
        token,
        request_sleep_seconds=settings.request_sleep_seconds,
        max_workers=1,
        max_retries=settings.tushare_max_retries,
        retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    raw = prov.list_universe()
    return sorted(raw)


def _filter_window(rows: list[dict[str, Any]], *, start: str, end: str) -> list[dict[str, Any]]:
    """
    仅按报告期 end_date 是否落在 [start, end] 过滤。
    不按 report_type 或 MMDD 后缀排除：年报(1231)、半年报(0630 等)、季报(0331/0630/0930/1231) 只要在窗内均保留。
    """

    out: list[dict[str, Any]] = []
    for r in rows:
        ed = str(r.get("end_date") or "").strip()
        if end_date_in_window(ed, start=start, end=end):
            out.append(r)
    return out


def _merged_end_date_suffix_counts(
    inc: list[dict[str, Any]],
    bal: list[dict[str, Any]],
    cf: list[dict[str, Any]],
) -> dict[str, int]:
    """三表合并后的唯一 end_date，按 MMDD 后缀计数（用于核对季末/半年末是否落库）。"""

    seen: set[str] = set()
    for bucket in (inc, bal, cf):
        for r in bucket:
            ed = str(r.get("end_date") or "").strip()
            if len(ed) == 8 and ed.isdigit():
                seen.add(ed)
    counts: dict[str, int] = {}
    for ed in seen:
        suf = ed[4:]
        counts[suf] = counts.get(suf, 0) + 1
    return dict(sorted(counts.items()))


def _sync_one_symbol(
    fetcher: TushareFinancialStatementFetcher,
    repo: FinancialStatementRepository,
    engine: Engine,
    ts_code: str,
    start: str,
    end: str,
    fetched_at: datetime,
) -> tuple[str, int, int, int, dict[str, int]]:
    inc = _filter_window(fetcher.fetch_income(ts_code, start, end), start=start, end=end)
    bal = _filter_window(fetcher.fetch_balancesheet(ts_code, start, end), start=start, end=end)
    cf = _filter_window(fetcher.fetch_cashflow(ts_code, start, end), start=start, end=end)
    with engine.begin() as conn:
        repo.upsert_income_rows(conn, inc, data_source=_DATA_SOURCE, fetched_at=fetched_at)
        repo.upsert_balance_rows(conn, bal, data_source=_DATA_SOURCE, fetched_at=fetched_at)
        repo.upsert_cashflow_rows(conn, cf, data_source=_DATA_SOURCE, fetched_at=fetched_at)
    suffix_hist = _merged_end_date_suffix_counts(inc, bal, cf)
    return ts_code, len(inc), len(bal), len(cf), suffix_hist


def sync_financial_statements_to_mysql(
    engine: Engine,
    settings: AShareIngestionSettings,
    token: str,
    *,
    max_symbols: int | None = None,
    since_years: int = 5,
    scheduled_date: date | None = None,
    resume: bool = False,
    reset_job: bool = True,
) -> dict[str, Any]:
    """
    按 TuShare 三表接口拉取近 since_years 年财报，写入 fs_income / fs_balance / fs_cashflow。
    单标的失败记入 failures，不中断全市场；支持按调度日任务游标续跑与全局限流。
    """

    if not token or not token.strip():
        raise ValueError("TUSHARE_TOKEN 不能为空")
    # 默认语义：每次触发都创建新任务并从头同步，避免复用旧游标造成“看起来没重跑”。
    if reset_job:
        resume = False
    start, end = statement_api_date_bounds(since_years=since_years)
    sched = scheduled_date or default_scheduled_date(tz_name=settings.fs_sync_schedule_tz)
    params_hash = financial_statement_job_params_hash(
        since_years=since_years,
        max_symbols=max_symbols,
        api_start=start,
        api_end=end,
    )
    job_type = IngestionJobRepository.financial_statement_job_type()
    job_repo = IngestionJobRepository(engine)

    limiter = TushareRateLimiter(
        settings.tushare_rpm_effective_cap(),
        zone_name=settings.fs_sync_schedule_tz,
    )
    symbols = _resolve_universe(engine, token.strip(), settings, limiter)
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[:max_symbols]
    total = len(symbols)
    ufp = universe_fingerprint(symbols)

    with engine.begin() as conn:
        if reset_job:
            job_repo.delete_job(
                conn,
                job_type=job_type,
                scheduled_date=sched,
                params_hash=params_hash,
            )
        job_row = job_repo.ensure_job(
            conn,
            job_type=job_type,
            scheduled_date=sched,
            params_hash=params_hash,
            universe_fingerprint_value=ufp,
        )

    if total == 0:
        logger.warning("标的列表为空，跳过同步")
        return {
            "universe": 0,
            "since_years": since_years,
            "api_start": start,
            "api_end": end,
            "ok": 0,
            "failures": [],
            "workers": 1,
            "scheduled_date": sched.isoformat(),
            "params_hash": params_hash,
            "ingestion_job_id": job_row.id,
        }

    restarted_from_completed = False
    if (
        resume
        and not reset_job
        and job_row.status == "completed"
        and job_row.cursor_ts_code is None
    ):
        logger.info(
            "财报同步检测到同参数任务已完成，自动重建任务并重新拉取最新窗口数据 "
            "job_type=%s scheduled_date=%s params_hash=%s",
            job_type,
            sched.isoformat(),
            params_hash,
        )
        with engine.begin() as conn:
            job_repo.delete_job(
                conn,
                job_type=job_type,
                scheduled_date=sched,
                params_hash=params_hash,
            )
            job_row = job_repo.ensure_job(
                conn,
                job_type=job_type,
                scheduled_date=sched,
                params_hash=params_hash,
                universe_fingerprint_value=ufp,
            )
        restarted_from_completed = True

    if (
        job_row.universe_fingerprint
        and job_row.universe_fingerprint != ufp
        and not reset_job
    ):
        logger.warning(
            "ingestion_job universe 指纹与本次不一致（可能 universe 已变），仍按游标续跑: job=%s current=%s",
            job_row.universe_fingerprint,
            ufp,
        )

    idx_start = resume_start_index(
        symbols, resume=resume, cursor_ts_code=job_row.cursor_ts_code
    )
    if resume and job_row.cursor_ts_code and job_row.cursor_ts_code not in symbols:
        logger.warning(
            "游标 ts_code=%s 不在本次 universe 中，自第一只重试",
            job_row.cursor_ts_code,
        )

    fetcher = TushareFinancialStatementFetcher(
        token.strip(),
        request_sleep_seconds=settings.request_sleep_seconds,
        max_retries=settings.tushare_max_retries,
        retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
        rate_limiter=limiter,
    )
    repo = FinancialStatementRepository(engine)
    fetched_at = utc_now()
    failures: list[dict[str, Any]] = []
    ok_count = 0
    job_id = job_row.id

    for i in range(idx_start, total):
        code = symbols[i]
        pos = i + 1
        try:
            _, ni, nb, nc, suf = _sync_one_symbol(
                fetcher, repo, engine, code, start, end, fetched_at
            )
            ok_count += 1
            next_cursor: str | None = symbols[i + 1] if i + 1 < total else None
            next_status = "completed" if next_cursor is None else "running"
            with engine.begin() as conn:
                job_repo.update_progress(
                    conn,
                    job_id=job_id,
                    cursor_ts_code=next_cursor,
                    status=next_status,
                )
            logger.info(
                "财报同步 %s/%s %s income=%s balance=%s cashflow=%s end_date_mmdd=%s",
                pos,
                total,
                code,
                ni,
                nb,
                nc,
                suf,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("标的 %s 财报同步失败（%s/%s）: %s", code, pos, total, exc)
            failures.append({"ts_code": code, "error": str(exc)})

    return {
        "universe": total,
        "since_years": since_years,
        "api_start": start,
        "api_end": end,
        "ok": ok_count,
        "failures": failures,
        "workers": 1,
        "scheduled_date": sched.isoformat(),
        "params_hash": params_hash,
        "ingestion_job_id": job_id,
        "resumed_from_index": idx_start,
        "restarted_from_completed": restarted_from_completed,
    }
