from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class StockFinancialSnapshot(BaseModel):
    """
    单标的财务与市场快照（字段尽量可选，便于多数据源映射）。
    金额与市值使用同一货币单位（如人民币元）。
    """

    symbol: str = Field(..., min_length=1, description="标的代码")
    market_cap: float = Field(..., gt=0, description="总市值")

    total_current_assets: float | None = Field(None, ge=0, description="流动资产合计")
    total_current_liabilities: float | None = Field(None, ge=0, description="流动负债合计")
    total_liabilities: float | None = Field(None, ge=0, description="负债合计")
    total_equity: float | None = Field(None, description="股东权益合计")

    net_income_ttm: float | None = Field(None, description="归属净利润 TTM")
    operating_cash_flow_ttm: float | None = Field(None, description="经营活动现金流 TTM")
    revenue_ttm: float | None = Field(None, ge=0, description="营业收入 TTM")

    interest_bearing_debt: float | None = Field(
        None,
        ge=0,
        description="有息负债合计（可选，缺失时用负债/权益粗代理）",
    )

    data_source: str | None = Field(None, description="快照主要数据来源标识")
    trade_cal_date: str | None = Field(None, description="行情/市值对应的交易日 YYYYMMDD")
    financials_end_date: str | None = Field(None, description="财报数据报告期截止日")

    dv_ratio: float | None = Field(
        None,
        description="股息率（%），年报口径；与 TuShare daily_basic.dv_ratio 一致",
    )
    dv_ttm: float | None = Field(
        None,
        description="股息率 TTM（%）；与 TuShare daily_basic.dv_ttm / 行情股息率列一致",
    )

    @field_validator("total_equity")
    @classmethod
    def equity_not_zero_if_set(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("total_equity must be positive when provided")
        return v
