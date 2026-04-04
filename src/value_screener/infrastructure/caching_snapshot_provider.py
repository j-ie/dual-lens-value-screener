from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from value_screener.domain.combined_ranking_params import snapshot_cache_enabled, snapshot_ttl_seconds
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.fetch_types import SymbolFetchFailure
from value_screener.infrastructure.financial_snapshot_repository import FinancialSnapshotRepository

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from value_screener.infrastructure.provider_protocol import (
        FetchSnapshotProgressCallback,
        FinancialDataProvider,
    )

logger = logging.getLogger(__name__)


class CachingSnapshotProvider:
    """
    在底层 Provider 之上做 DB 快照缓存：TTL 内命中则不再请求远端财报接口。
    """

    def __init__(
        self,
        inner: FinancialDataProvider,
        engine: Engine,
    ) -> None:
        self._inner = inner
        self._engine = engine
        self._repo = FinancialSnapshotRepository(engine)

    @property
    def backend_name(self) -> str:
        return f"snapshot-cache({self._inner.backend_name})"

    def list_universe(self) -> list[str]:
        return self._inner.list_universe()

    def fetch_snapshots(
        self,
        symbols: list[str],
        *,
        on_progress: FetchSnapshotProgressCallback | None = None,
    ) -> list[StockFinancialSnapshot | SymbolFetchFailure]:
        if not snapshot_cache_enabled():
            return self._inner.fetch_snapshots(symbols, on_progress=on_progress)

        ttl = snapshot_ttl_seconds()
        if ttl <= 0:
            return self._inner.fetch_snapshots(symbols, on_progress=on_progress)

        n = len(symbols)
        slots: list[StockFinancialSnapshot | SymbolFetchFailure | None] = [None] * n
        need_idx: list[int] = []
        need_syms: list[str] = []

        with self._engine.connect() as conn:
            for i, sym in enumerate(symbols):
                raw = self._repo.get_latest_valid_json(conn, sym, ttl_seconds=ttl)
                if raw is not None:
                    try:
                        slots[i] = StockFinancialSnapshot.model_validate(raw)
                    except Exception:
                        logger.debug("快照 JSON 反序列化失败，将重拉: %s", sym, exc_info=True)
                        need_idx.append(i)
                        need_syms.append(sym)
                else:
                    need_idx.append(i)
                    need_syms.append(sym)

        if not need_syms:
            return [s for s in slots if s is not None]  # type: ignore[list-item]

        fetched = self._inner.fetch_snapshots(need_syms, on_progress=on_progress)
        if len(fetched) != len(need_syms):
            logger.warning(
                "缓存包装：底层返回条数与请求不一致 %s != %s",
                len(fetched),
                len(need_syms),
            )

        with self._engine.begin() as conn:
            for j, item in enumerate(fetched):
                if j >= len(need_idx):
                    break
                idx = need_idx[j]
                slots[idx] = item
                if isinstance(item, StockFinancialSnapshot):
                    try:
                        self._repo.upsert_snapshot(conn, item)
                    except Exception:
                        logger.warning("写入财务快照失败: %s", item.symbol, exc_info=True)

        out: list[StockFinancialSnapshot | SymbolFetchFailure] = []
        for i in range(n):
            cell = slots[i]
            if cell is None:
                out.append(
                    SymbolFetchFailure(
                        symbol=symbols[i],
                        reason="缓存包装：底层未返回该位置结果",
                        source=self.backend_name,
                    )
                )
            else:
                out.append(cell)
        return out
