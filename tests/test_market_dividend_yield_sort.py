from value_screener.infrastructure.tushare_market_dividend_yield_fetcher import (
    page_slice,
    sort_dividend_rows,
)


def test_sort_dv_ttm_desc_puts_null_last() -> None:
    rows = [
        {"ts_code": "A.SH", "dv_ttm": 1.0},
        {"ts_code": "B.SH", "dv_ttm": None},
        {"ts_code": "C.SH", "dv_ttm": 3.0},
    ]
    out = sort_dividend_rows(rows, sort="dv_ttm", order="desc")
    assert [r["ts_code"] for r in out] == ["C.SH", "A.SH", "B.SH"]


def test_page_slice() -> None:
    items = list(range(25))
    page, total = page_slice(items, page=2, page_size=10)
    assert total == 25
    assert page == list(range(10, 20))
