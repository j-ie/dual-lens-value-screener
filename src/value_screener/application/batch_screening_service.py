from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Engine

from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure

if TYPE_CHECKING:
    from value_screener.infrastructure.provider_protocol import FinancialDataProvider


@dataclass(frozen=True, slots=True)
class BatchScreeningResult:
    """批跑输出：与 POST /v1/screen 单项结构一致的 results + 失败列表。"""

    results: list[dict[str, Any]]
    failures: list[dict[str, Any]]
    meta: dict[str, Any]
    # 与 results 顺序一致，仅供持久化装配混合字段，不作为公开 HTTP 载荷契约。
    snapshots_for_persist: tuple[StockFinancialSnapshot, ...] = ()


def _chunk_symbols(symbols: Sequence[str], chunk_size: int | None) -> list[list[str]]:
    if not chunk_size or chunk_size <= 0:
        return [list(symbols)]
    out: list[list[str]] = []
    seq = list(symbols)
    for i in range(0, len(seq), chunk_size):
        out.append(seq[i : i + chunk_size])
    return out


class BatchScreeningApplicationService:
    """全市场或子集：拉数 → 现有双维度算分。"""

    def __init__(
        self,
        provider: FinancialDataProvider,
        screening: ScreeningApplicationService,
        *,
        screening_engine: Engine | None = None,
    ) -> None:
        self._provider = provider
        self._screening = screening
        self._screening_engine = screening_engine

    def run(
        self,
        symbols: list[str] | None,
        max_symbols: int | None,
        *,
        on_batch_progress: Callable[[dict[str, Any]], None] | None = None,
        chunk_size: int | None = None,
        on_chunk_screened: (
            Callable[[list[dict[str, Any]], list[StockFinancialSnapshot]], None] | None
        ) = None,
    ) -> BatchScreeningResult:
        if symbols is None:
            symbols = self._provider.list_universe()
        if max_symbols is not None and max_symbols > 0:
            symbols = symbols[:max_symbols]

        industry_map: dict[str, str] = {}
        if self._screening_engine is not None and symbols:
            ref = ReferenceMasterRepository(self._screening_engine)
            with self._screening_engine.connect() as conn:
                industry_map = ref.fetch_industry_map(conn, symbols)

        total_syms = len(symbols)
        use_chunks = (
            chunk_size is not None
            and chunk_size > 0
            and on_chunk_screened is not None
            and total_syms > 0
        )
        chunks = _chunk_symbols(symbols, chunk_size if use_chunks else None)

        all_screened: list[dict[str, Any]] = []
        all_snaps_ordered: list[StockFinancialSnapshot] = []
        all_fails: list[dict[str, Any]] = []
        snapshot_ok_total = 0
        snapshot_failed_total = 0
        offset = 0

        for ch in chunks:
            off = offset

            def _fetch_progress(idx: int, tot: int, sym: str, base: int = off) -> None:
                if on_batch_progress is None:
                    return
                cur = base + idx
                pct = min(99, int(100 * cur / total_syms)) if total_syms else 100
                on_batch_progress(
                    {
                        "progress_phase": "fetch",
                        "progress_current": cur,
                        "progress_total": total_syms,
                        "progress_symbol": sym,
                        "progress_percent": pct,
                    }
                )

            fetched = self._provider.fetch_snapshots(
                ch,
                on_progress=_fetch_progress if on_batch_progress is not None else None,
            )
            snaps = [x for x in fetched if isinstance(x, StockFinancialSnapshot)]
            fails = [x for x in fetched if isinstance(x, SymbolFetchFailure)]
            snapshot_ok_total += len(snaps)
            snapshot_failed_total += len(fails)
            all_fails.extend(asdict(f) for f in fails)
            offset += len(ch)

            if on_batch_progress is not None and snaps:
                on_batch_progress(
                    {
                        "progress_phase": "score",
                        "progress_current": min(offset, total_syms),
                        "progress_total": total_syms,
                        "progress_symbol": "",
                        "progress_percent": min(99, int(100 * offset / total_syms)) if total_syms else 99,
                    }
                )

            screened = self._screening.screen(snaps, industry_by_symbol=industry_map)
            all_screened.extend(screened)
            all_snaps_ordered.extend(snaps)
            if use_chunks and screened:
                on_chunk_screened(screened, snaps)

        if on_batch_progress is not None:
            n = len(all_screened)
            on_batch_progress(
                {
                    "progress_phase": "done",
                    "progress_current": n,
                    "progress_total": max(n, 1),
                    "progress_symbol": "",
                    "progress_percent": 100,
                }
            )

        return BatchScreeningResult(
            results=all_screened,
            failures=all_fails,
            meta={
                "universe_requested": total_syms,
                "snapshot_ok": snapshot_ok_total,
                "snapshot_failed": snapshot_failed_total,
                "provider": self._provider.backend_name,
            },
            snapshots_for_persist=tuple(all_snaps_ordered),
        )
