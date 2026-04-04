"""TuShare 拉取：单 worker 与多 worker 结果顺序及内容一致（mock）。"""

import unittest
from unittest.mock import MagicMock, patch

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.tushare_provider import TushareAShareProvider


def _snap(ts_code: str, trade_date: str) -> StockFinancialSnapshot:
    return StockFinancialSnapshot(
        symbol=ts_code,
        market_cap=1e9,
        total_equity=1e8,
        data_source="tushare",
        trade_cal_date=trade_date,
    )


class TushareFetchConcurrencyTest(unittest.TestCase):
    def _provider(self, *, max_workers: int) -> TushareAShareProvider:
        with patch("tushare.set_token"), patch("tushare.pro_api", return_value=MagicMock()):
            return TushareAShareProvider(
                "test-token",
                request_sleep_seconds=0.0,
                max_workers=max_workers,
                max_retries=0,
                retry_backoff_seconds=0.0,
            )

    def test_parallel_preserves_symbol_order_and_matches_sequential(self) -> None:
        symbols = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
        mv = {s: 100.0 for s in symbols}

        def fetch_one(self, ts_code: str, trade_date: str, mv_wan: dict) -> StockFinancialSnapshot:
            del self, mv_wan
            return _snap(ts_code, trade_date)

        with patch.object(TushareAShareProvider, "_latest_open_trade_date", return_value="20260401"), patch.object(
            TushareAShareProvider, "_load_total_mv_map", return_value=mv
        ), patch.object(TushareAShareProvider, "_fetch_one", fetch_one):
            seq = self._provider(max_workers=1)
            par = self._provider(max_workers=4)
            a = seq.fetch_snapshots(symbols)
            b = par.fetch_snapshots(symbols)
        self.assertEqual(len(a), len(b))
        for x, y in zip(a, b, strict=True):
            self.assertIsInstance(x, StockFinancialSnapshot)
            self.assertIsInstance(y, StockFinancialSnapshot)
            self.assertEqual(x.symbol, y.symbol)
            self.assertEqual(x.market_cap, y.market_cap)

    def test_retry_then_success(self) -> None:
        symbols = ["000001.SZ"]
        mv = {"000001.SZ": 100.0}
        calls = {"n": 0}

        def fetch_one(self, ts_code: str, trade_date: str, mv_wan: dict) -> StockFinancialSnapshot:
            del self, mv_wan
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("transient")
            return _snap(ts_code, trade_date)

        with patch.object(TushareAShareProvider, "_latest_open_trade_date", return_value="20260401"), patch.object(
            TushareAShareProvider, "_load_total_mv_map", return_value=mv
        ), patch.object(TushareAShareProvider, "_fetch_one", fetch_one):
            p = TushareAShareProvider(
                "test-token",
                request_sleep_seconds=0.0,
                max_workers=1,
                max_retries=2,
                retry_backoff_seconds=0.0,
            )
            out = p.fetch_snapshots(symbols)
        self.assertEqual(calls["n"], 2)
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], StockFinancialSnapshot)


if __name__ == "__main__":
    unittest.main()
