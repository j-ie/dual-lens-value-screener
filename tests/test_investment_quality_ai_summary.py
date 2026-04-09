from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from value_screener.application.investment_master_summary import (
    InvestmentMasterSummary,
    InvestmentMasterSummaryResult,
)
from value_screener.interfaces.main import app


def _fake_summary_result() -> InvestmentMasterSummaryResult:
    summary = InvestmentMasterSummary(
        conclusion="结论：继续跟踪，等待更高安全边际。",
        valuation_view="当前估值偏中性，需等待更好价格区间。",
        position_advice="观察仓",
        buy_trigger_zone={"ideal": "较当前价回撤 20% 以上", "acceptable": "较当前价回撤 10%~15%"},
        exit_conditions=["核心盈利能力持续恶化", "估值显著透支且业绩未兑现"],
        counter_arguments=["行业景气下行可能超预期"],
        watch_items=["经营现金流/净利润匹配度", "ROE 稳定性"],
        facts=["总分来自确定性规则计算", "输出包含风险提示字段"],
        judgments=["当前阶段更适合观察而非重仓"],
        confidence="medium",
        disclaimer="仅供个人投研复盘，不构成投资建议。",
    )
    return InvestmentMasterSummaryResult(
        summary=summary,
        prompt_version="investment-master-v1",
        model="fake-model",
    )


def test_ai_summary_single_mode_contract() -> None:
    payload = {
        "item": {
            "symbol": "600519.SH",
            "market_cap": 1000000000,
            "total_current_assets": 400000000,
            "total_current_liabilities": 200000000,
            "total_equity": 300000000,
            "net_income_ttm": 50000000,
            "operating_cash_flow_ttm": 60000000,
            "revenue_ttm": 500000000,
            "interest_bearing_debt": 120000000,
        },
        "industry": "一般工商业",
    }
    with patch(
        "value_screener.interfaces.investment_quality.summarize_investment_master",
        return_value=_fake_summary_result(),
    ):
        client = TestClient(app)
        resp = client.post("/api/v1/investment-quality/ai-summary", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["mode"] == "single"
    assert body["symbol"] == "600519.SH"
    ai = body["ai_summary"]
    assert ai["position_advice"]
    assert ai["buy_trigger_zone"]["ideal"]
    assert ai["buy_trigger_zone"]["acceptable"]
    assert len(ai["exit_conditions"]) > 0
    assert len(ai["counter_arguments"]) > 0
