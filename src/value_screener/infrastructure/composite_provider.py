from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.provider_protocol import FetchSnapshotProgressCallback

if TYPE_CHECKING:
    from value_screener.infrastructure.provider_protocol import FinancialDataProvider

logger = logging.getLogger(__name__)


class CompositeAShareDataProvider:
    """
    主备数据源：主源整批抛错时切换备源；不合并两源数值。
    单票失败由各自 Provider 内部记录为 SymbolFetchFailure，不触发整批切换。
    """

    def __init__(
        self,
        primary: FinancialDataProvider,
        secondary: FinancialDataProvider | None,
    ) -> None:
        self._primary = primary
        self._secondary = secondary

    @property
    def backend_name(self) -> str:
        if self._secondary is None:
            return f"composite({self._primary.backend_name})"
        return f"composite({self._primary.backend_name}|{self._secondary.backend_name})"

    def list_universe(self) -> list[str]:
        try:
            return self._primary.list_universe()
        except Exception as exc:  # noqa: BLE001
            logger.warning("primary list_universe failed: %s", exc)
            if self._secondary is None:
                raise
            return self._secondary.list_universe()

    def fetch_snapshots(
        self,
        symbols: list[str],
        *,
        on_progress: FetchSnapshotProgressCallback | None = None,
    ) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
        try:
            return _tag_snapshots(
                self._primary.fetch_snapshots(symbols, on_progress=on_progress),
                self._primary.backend_name,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("primary fetch_snapshots failed: %s", exc)
            if self._secondary is None:
                raise
            return _tag_snapshots(
                self._secondary.fetch_snapshots(symbols, on_progress=on_progress),
                self._secondary.backend_name,
            )


def _tag_snapshots(
    rows: list[StockFinancialSnapshot | SymbolFetchFailure],
    source: str,
) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
    out: list[StockFinancialSnapshot | SymbolFetchFailure] = []
    for row in rows:
        if isinstance(row, StockFinancialSnapshot):
            if not row.data_source:
                out.append(row.model_copy(update={"data_source": source}))
            else:
                out.append(row)
        else:
            out.append(row)
    return out
