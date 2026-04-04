import unittest

from value_screener.domain.buffett import BuffettAssessor
from value_screener.domain.graham import GrahamAssessor
from value_screener.domain.scoring_params import BuffettScoringParams, GrahamScoringParams
from value_screener.domain.snapshot import StockFinancialSnapshot


class BuffettScoringTest(unittest.TestCase):
    def test_roe_only_linear_score(self) -> None:
        p = BuffettScoringParams.default()
        assessor = BuffettAssessor(params=p)
        snap = StockFinancialSnapshot(
            symbol="T",
            market_cap=1_000_000,
            total_equity=1_000_000,
            net_income_ttm=100_000,
        )
        out = assessor.assess(snap)
        self.assertAlmostEqual(out.roe or 0, 0.1, places=4)
        self.assertAlmostEqual(out.score, 50.0, places=1)


class GrahamScoringTest(unittest.TestCase):
    def test_price_to_book_only(self) -> None:
        assessor = GrahamAssessor(params=GrahamScoringParams.default())
        snap = StockFinancialSnapshot(
            symbol="T",
            market_cap=100_000_000,
            total_equity=100_000_000,
        )
        out = assessor.assess(snap)
        self.assertAlmostEqual(out.price_to_book or 0, 1.0, places=4)
        self.assertAlmostEqual(out.score, 80.0, places=1)

    def test_net_net_flag_when_mcap_below_ncav(self) -> None:
        assessor = GrahamAssessor(params=GrahamScoringParams.default())
        snap = StockFinancialSnapshot(
            symbol="T",
            market_cap=50_000_000,
            total_current_assets=200_000_000,
            total_liabilities=100_000_000,
            total_equity=100_000_000,
        )
        out = assessor.assess(snap)
        self.assertTrue(out.notes.get("net_net_tendency"))
        self.assertLess(out.market_cap_to_ncav or 2.0, 1.0)


if __name__ == "__main__":
    unittest.main()
