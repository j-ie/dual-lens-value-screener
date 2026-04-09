from __future__ import annotations

import unittest

from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.domain.dcf_sector_policy import DcfSectorKind
from value_screener.domain.investment_quality import (
    CompanyFinancials,
    InvestmentDecision,
    InvestmentQualityAnalyzer,
    resolve_worth_buy_decision,
)
from value_screener.domain.snapshot import StockFinancialSnapshot


class TestInvestmentQualityAnalyzer(unittest.TestCase):
    def setUp(self) -> None:
        self.analyzer = InvestmentQualityAnalyzer()

    def test_high_quality_company_can_be_buy_and_undervalued(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="A公司",
                revenue=(100.0, 120.0, 145.0),
                net_profit=(10.0, 12.0, 15.0),
                non_recurring_net_profit=(9.0, 11.0, 14.0),
                gross_margin=(35.0, 36.0, 37.0),
                net_margin=(10.0, 10.2, 10.5),
                expense_ratio=(20.0, 19.5, 19.0),
                operating_profit=(12.0, 14.0, 17.0),
                operating_cashflow=(11.0, 13.0, 16.0),
                free_cashflow=(5.0, 6.0, 8.0),
                cash=(30.0, 35.0, 40.0),
                short_debt=(10.0, 12.0, 12.0),
                interest_bearing_debt=(20.0, 22.0, 21.0),
                accounts_receivable=(15.0, 17.0, 18.0),
                inventory=(12.0, 13.0, 14.0),
                goodwill=(2.0, 2.0, 2.0),
                net_assets=(50.0, 58.0, 67.0),
                asset_liability_ratio=(45.0, 43.0, 42.0),
                roe=(14.0, 15.0, 16.0),
                roic=(10.0, 11.0, 12.0),
                pe=12.0,
            )
        )
        self.assertEqual(result.decision, InvestmentDecision.BUY)
        self.assertTrue(result.is_undervalued)
        self.assertEqual(result.decision_label_zh, "可买")

    def test_hard_risk_can_force_reject(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="风险公司",
                revenue=(100.0, 105.0, 110.0),
                net_profit=(10.0, 9.0, 8.0),
                non_recurring_net_profit=(9.0, 4.0, 3.0),
                operating_cashflow=(2.0, 1.0, -1.0),
                cash=(5.0, 4.0, 3.0),
                short_debt=(8.0, 9.0, 10.0),
                accounts_receivable=(10.0, 14.0, 20.0),
                inventory=(8.0, 12.0, 18.0),
                goodwill=(25.0, 26.0, 27.0),
                net_assets=(40.0, 40.0, 40.0),
                asset_liability_ratio=(70.0, 75.0, 80.0),
            )
        )
        self.assertEqual(result.decision, InvestmentDecision.REJECT)
        self.assertGreaterEqual(result.metadata.get("hard_risk_count", 0), 3)

    def test_financial_sector_uses_pb_policy(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="银行样本",
                sector_kind=DcfSectorKind.FINANCIAL,
                cash=(100.0,),
                short_debt=(10.0,),
                operating_cashflow=(5.0,),
                net_profit=(6.0,),
                non_recurring_net_profit=(6.0,),
                pb=0.8,
                pe=100.0,
            )
        )
        self.assertGreaterEqual(result.module_scores["valuation"], 1)

    def test_sparse_single_period_data_should_not_be_penalized_for_missing_history(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="单期样本",
                cash=(100.0,),
                short_debt=(10.0,),
                net_profit=(20.0,),
                operating_cashflow=(18.0,),
            )
        )
        self.assertGreaterEqual(result.module_scores["cashflow"], 0)
        self.assertEqual(result.module_scores["return_metrics"], 0)

    def test_worth_buy_decision_output_fields(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="A公司",
                revenue=(100.0, 120.0, 145.0),
                net_profit=(10.0, 12.0, 15.0),
                non_recurring_net_profit=(9.0, 11.0, 14.0),
                operating_cashflow=(11.0, 13.0, 16.0),
                cash=(30.0, 35.0, 40.0),
                short_debt=(10.0, 12.0, 12.0),
                pe=12.0,
            )
        )
        worth_buy = resolve_worth_buy_decision(result)
        self.assertIsInstance(worth_buy.is_worth_buy, bool)
        self.assertTrue(worth_buy.label_zh)
        self.assertIsInstance(worth_buy.reason_codes, tuple)
        self.assertIn("decision_buy", worth_buy.reason_codes)

    def test_worth_buy_reason_codes_include_blockers(self) -> None:
        result = self.analyzer.analyze(
            CompanyFinancials(
                name="观察样本",
                revenue=(100.0, 101.0),
                net_profit=(10.0, 10.0),
                non_recurring_net_profit=(10.0, 10.0),
                operating_cashflow=(4.0, 3.0),
                cash=(10.0,),
                short_debt=(9.0,),
                pe=30.0,
            )
        )
        worth_buy = resolve_worth_buy_decision(result)
        self.assertIn("decision_cautious", worth_buy.reason_codes)
        self.assertIn("not_undervalued", worth_buy.reason_codes)
        self.assertEqual(worth_buy.label_zh, "谨慎观察")


class TestScreeningServiceIntegration(unittest.TestCase):
    def test_screening_result_contains_investment_quality_block(self) -> None:
        service = ScreeningApplicationService()
        snap = StockFinancialSnapshot(
            symbol="000001.SZ",
            market_cap=1_000_000_000.0,
            total_current_assets=400_000_000.0,
            total_current_liabilities=200_000_000.0,
            total_equity=300_000_000.0,
            net_income_ttm=50_000_000.0,
            operating_cash_flow_ttm=60_000_000.0,
            revenue_ttm=500_000_000.0,
            interest_bearing_debt=120_000_000.0,
        )
        rows = service.screen([snap])
        self.assertEqual(len(rows), 1)
        iq = rows[0].get("investment_quality")
        self.assertIsInstance(iq, dict)
        self.assertIn("decision", iq)
        self.assertIn("decision_label_zh", iq)
        self.assertIn("module_scores", iq)
        self.assertIn("is_worth_buy", iq)
        self.assertIn("worth_buy_label_zh", iq)
        self.assertIn("worth_buy_reason_codes", iq)


if __name__ == "__main__":
    unittest.main()
