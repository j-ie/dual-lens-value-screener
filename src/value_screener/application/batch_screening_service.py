from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from value_screener.application.screening_service import ScreeningApplicationService
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


class BatchScreeningApplicationService:
    """全市场或子集：拉数 → 现有双维度算分。"""

    def __init__(
        self,
        provider: FinancialDataProvider,
        screening: ScreeningApplicationService,
    ) -> None:
        self._provider = provider
        self._screening = screening

    def run(
        self,
        symbols: list[str] | None,
        max_symbols: int | None,
        *,
        on_batch_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> BatchScreeningResult:
        if symbols is None:
            symbols = self._provider.list_universe()
        if max_symbols is not None and max_symbols > 0:
            symbols = symbols[:max_symbols]

        def _fetch_progress(idx: int, tot: int, sym: str) -> None:
            if on_batch_progress is None:
                return
            pct = min(99, int(100 * idx / tot)) if tot else 100
            on_batch_progress(
                {
                    "progress_phase": "fetch",
                    "progress_current": idx,
                    "progress_total": tot,
                    "progress_symbol": sym,
                    "progress_percent": pct,
                }
            )

        fetched = self._provider.fetch_snapshots(
            symbols,
            on_progress=_fetch_progress if on_batch_progress is not None else None,
        )
        snaps = [x for x in fetched if isinstance(x, StockFinancialSnapshot)]
        fails = [x for x in fetched if isinstance(x, SymbolFetchFailure)]

        if on_batch_progress is not None:
            n_snap = len(snaps)
            tot_scr = max(n_snap, 1)
            on_batch_progress(
                {
                    "progress_phase": "score",
                    "progress_current": n_snap,
                    "progress_total": tot_scr,
                    "progress_symbol": "",
                    "progress_percent": 99,
                }
            )

        screened = self._screening.screen(snaps)

        if on_batch_progress is not None:
            n = len(screened)
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
            results=screened,
            failures=[asdict(f) for f in fails],
            meta={
                "universe_requested": len(symbols),
                "snapshot_ok": len(snaps),
                "snapshot_failed": len(fails),
                "provider": self._provider.backend_name,
            },
        )
