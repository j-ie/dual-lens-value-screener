from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure

FetchSnapshotProgressCallback = Callable[[int, int, str], None]
"""拉取进度：(当前序号 1-based, 总数, 当前 ts_code)。"""


@runtime_checkable
class FinancialDataProvider(Protocol):
    """财务与市场快照数据源（TuShare / AkShare 等）。"""

    backend_name: str

    def list_universe(self) -> list[str]:
        """返回 ts_code 列表。"""

    def fetch_snapshots(
        self,
        symbols: list[str],
        *,
        on_progress: FetchSnapshotProgressCallback | None = None,
    ) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
        """按输入顺序返回成功快照或失败记录。"""
