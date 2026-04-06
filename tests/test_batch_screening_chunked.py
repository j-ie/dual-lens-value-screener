"""分块批跑：进度基址与分块回调。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from value_screener.application.batch_screening_service import (
    BatchScreeningApplicationService,
    _chunk_symbols,
)
from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.domain.snapshot import StockFinancialSnapshot


class TestChunkSymbols(unittest.TestCase):
    def test_no_chunking(self) -> None:
        self.assertEqual(_chunk_symbols(["a", "b", "c"], None), [["a", "b", "c"]])
        self.assertEqual(_chunk_symbols(["a", "b", "c"], 0), [["a", "b", "c"]])

    def test_sized(self) -> None:
        self.assertEqual(_chunk_symbols(["a", "b", "c", "d"], 2), [["a", "b"], ["c", "d"]])


class TestBatchRunChunkCallback(unittest.TestCase):
    def test_invokes_callback_per_chunk(self) -> None:
        chunks_seen: list[list[str]] = []

        def list_uni() -> list[str]:
            return ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]

        def fetch_snaps(symbols: list[str], *, on_progress=None) -> list:
            del on_progress
            chunks_seen.append(list(symbols))
            out = []
            for s in symbols:
                out.append(
                    StockFinancialSnapshot(
                        symbol=s,
                        market_cap=1e9,
                        total_equity=1e8,
                        data_source="t",
                        trade_cal_date="20260101",
                    )
                )
            return out

        prov = MagicMock()
        prov.backend_name = "mock"
        prov.list_universe.side_effect = list_uni
        prov.fetch_snapshots.side_effect = fetch_snaps

        screening = ScreeningApplicationService()
        svc = BatchScreeningApplicationService(prov, screening)
        cb_chunks: list[int] = []

        def on_chunk(rows: list[dict], _snaps: list[StockFinancialSnapshot]) -> None:
            cb_chunks.append(len(rows))

        svc.run(
            None,
            None,
            chunk_size=2,
            on_chunk_screened=on_chunk,
        )
        self.assertEqual(chunks_seen, [["000001.SZ", "000002.SZ"], ["000003.SZ", "000004.SZ"]])
        self.assertEqual(cb_chunks, [2, 2])
        full = svc.run(None, None)
        self.assertEqual(len(full.snapshots_for_persist), len(full.results))


if __name__ == "__main__":
    unittest.main()
