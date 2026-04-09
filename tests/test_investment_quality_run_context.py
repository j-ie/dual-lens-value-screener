"""investment_quality_run_context：TuShare 补数与分项代理。"""

from __future__ import annotations

from value_screener.application.investment_quality_run_context import (
    _interest_bearing_debt_from_balance_row,
    _proxy_current_assets_from_bs_row,
    _proxy_current_liabilities_from_bs_row,
    sum_latest_n_first_key,
)


def test_proxy_current_assets_prefers_receiv_bill_over_notes() -> None:
    row = {
        "accounts_receiv_bill": 100.0,
        "notes_receiv": 50.0,
        "money_cap": 200.0,
    }
    assert _proxy_current_assets_from_bs_row(row) == 300.0


def test_proxy_current_assets_sums_notes_when_no_bill() -> None:
    row = {"notes_receiv": 10.0, "accounts_receiv": 20.0, "money_cap": 1.0}
    assert _proxy_current_assets_from_bs_row(row) == 31.0


def test_proxy_current_liabilities_prefers_accounts_pay() -> None:
    row = {"accounts_pay": 500.0, "notes_payable": 1.0, "st_borr": 100.0}
    assert _proxy_current_liabilities_from_bs_row(row) == 600.0


def test_proxy_current_liabilities_uses_payables_when_no_accounts_pay() -> None:
    row = {"payables": 80.0, "oth_payable": 20.0}
    assert _proxy_current_liabilities_from_bs_row(row) == 100.0


def test_sum_latest_n_first_key_prefers_total_revenue() -> None:
    rows = [
        {"total_revenue": 100.0, "revenue": 1.0},
        {"revenue": 50.0},
    ]
    assert sum_latest_n_first_key(rows, ("total_revenue", "revenue"), n=4) == 150.0


def test_interest_bearing_debt_sums_st_lt_from_payload() -> None:
    bal = {"payload": {"st_borr": 10.0, "lt_borr": 20.0}}
    assert _interest_bearing_debt_from_balance_row(bal) == 30.0


def test_interest_bearing_debt_prefers_tushare_borr_names() -> None:
    bal = {"st_borr": 5.0, "payload": {"st_borrow": 999.0}}
    assert _interest_bearing_debt_from_balance_row(bal) == 5.0
