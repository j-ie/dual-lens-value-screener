"""screening_result 混合存储：持久化行装配与 enrichment 投影。"""

from __future__ import annotations

import unittest

from value_screener.application.persist_screening_run import _rows_from_screening_results
from value_screener.application.result_enrichment import enrich_screening_result_row
from value_screener.application.screening_run_fact import build_hybrid_persist_fields
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.snapshot import StockFinancialSnapshot


class TestHybridPersist(unittest.TestCase):
    def test_build_hybrid_fields(self) -> None:
        snap = StockFinancialSnapshot(
            symbol="600519.SH",
            market_cap=1e12,
            net_income_ttm=5e10,
            total_equity=1e11,
            data_source="tushare",
            trade_cal_date="20260401",
            financials_end_date="20241231",
        )
        hy = build_hybrid_persist_fields(snap)
        self.assertEqual(hy["market_cap"], 1e12)
        self.assertAlmostEqual(hy["pe_ttm"] or 0, 20.0)
        self.assertEqual(hy["run_fact_json"]["symbol"], "600519.SH")

    def test_rows_from_screening_with_snaps(self) -> None:
        snap = StockFinancialSnapshot(
            symbol="000001.SZ",
            market_cap=100.0,
            net_income_ttm=10.0,
            total_equity=50.0,
            data_source="t",
            trade_cal_date="20260101",
            financials_end_date="20251231",
        )
        g = {"score": 50.0, "ncav": None, "market_cap_to_ncav": None, "current_ratio": None, "price_to_book": 2.0, "notes": {}}
        b = {"score": 60.0, "roe": 0.2, "debt_to_equity": 1.0, "ocf_to_net_income": 1.0, "notes": {}}
        item = {
            "symbol": "000001.SZ",
            "provenance": {"market_cap": 100.0, "data_source": "t"},
            "graham": g,
            "buffett": b,
        }
        ranking = CombinedRankingParams.from_env()
        rows = _rows_from_screening_results([item], ranking, (snap,))
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["pe_ttm"], 10.0)
        self.assertEqual(rows[0]["market_cap"], 100.0)
        self.assertEqual(rows[0]["run_fact_json"]["symbol"], "000001.SZ")

    def test_enrich_exposes_metrics(self) -> None:
        g = {"score": 50.0, "ncav": None, "market_cap_to_ncav": None, "current_ratio": None, "price_to_book": 2.0, "notes": {}}
        b = {"score": 60.0, "roe": 0.2, "debt_to_equity": 1.0, "ocf_to_net_income": 1.0, "notes": {}}
        row = {
            "symbol": "x",
            "graham_score": 50.0,
            "buffett_score": 60.0,
            "graham": g,
            "buffett": b,
            "provenance": {"market_cap": 200.0},
            "combined_score": 55.0,
            "coverage_ok": True,
            "ref_name": "",
            "ref_fullname": None,
            "ref_industry": "",
            "ref_area": "",
            "run_fact_json": {"net_income_ttm": 20.0},
            "market_cap": 200.0,
            "pe_ttm": 10.0,
        }
        out = enrich_screening_result_row(row)
        self.assertEqual(out["market_cap"], 200.0)
        self.assertEqual(out["pe_ttm"], 10.0)
        self.assertEqual(out["net_income_ttm"], 20.0)

    def test_enrich_dividend_from_provenance_when_run_fact_missing(self) -> None:
        g = {"score": 50.0, "ncav": None, "market_cap_to_ncav": None, "current_ratio": None, "price_to_book": 2.0, "notes": {}}
        b = {"score": 60.0, "roe": 0.2, "debt_to_equity": 1.0, "ocf_to_net_income": 1.0, "notes": {}}
        row = {
            "symbol": "x",
            "graham_score": 50.0,
            "buffett_score": 60.0,
            "graham": g,
            "buffett": b,
            "provenance": {"market_cap": 1e9, "dv_ttm": 3.25, "dv_ratio": 1.0},
            "combined_score": 55.0,
            "coverage_ok": True,
            "ref_name": "",
            "ref_fullname": None,
            "ref_industry": "",
            "ref_area": "",
            "run_fact_json": None,
            "market_cap": 1e9,
            "pe_ttm": None,
        }
        out = enrich_screening_result_row(row)
        self.assertAlmostEqual(out["dv_ttm"], 3.25)
        self.assertAlmostEqual(out["dv_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
