from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from value_screener.application.market_dividend_yield_query import MarketDividendYieldQueryService
from value_screener.infrastructure.settings import AShareIngestionSettings

router = APIRouter(prefix="/api/v1", tags=["market"])


class DividendYieldRow(BaseModel):
    """单日 daily_basic 股息率维度。"""

    ts_code: str = Field(description="TuShare 证券代码，如 600519.SH")
    trade_date: str = Field(description="交易日 YYYYMMDD")
    close: float | None = Field(None, description="收盘价")
    dv_ratio: float | None = Field(None, description="股息率（%），年报口径")
    dv_ttm: float | None = Field(None, description="股息率 TTM（%）")


class AShareDividendYieldPageResponse(BaseModel):
    ok: bool
    error: str | None = None
    trade_date: str | None = None
    fetched_at: str | None = None
    total: int
    page: int
    page_size: int
    sort: Literal["dv_ratio", "dv_ttm"]
    order: Literal["asc", "desc"]
    items: list[DividendYieldRow]


@router.get(
    "/market/ashare-dividend-yields",
    response_model=AShareDividendYieldPageResponse,
    summary="全 A 股息率（TuShare daily_basic）",
)
def ashare_dividend_yields(
    trade_date: str | None = Query(
        None,
        min_length=8,
        max_length=8,
        description="交易日 YYYYMMDD；缺省为最近上交所开市日",
    ),
    sort: Literal["dv_ratio", "dv_ttm"] = Query(
        "dv_ttm",
        description="排序字段：dv_ratio 年报股息率%%，dv_ttm 滚动 TTM 股息率%%",
    ),
    order: Literal["asc", "desc"] = Query("desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=2000),
) -> AShareDividendYieldPageResponse:
    """
    一次 TuShare 调用取指定交易日全市场 `daily_basic`，在服务端排序后分页返回。

    需配置 `TUSHARE_TOKEN`；数据权限与积分以 TuShare 账号为准。缺失股息率的标的排在列表末尾。
    """

    settings = AShareIngestionSettings.from_env()
    svc = MarketDividendYieldQueryService(settings)
    payload = svc.load(
        trade_date=trade_date,
        sort=sort,
        order=order,
        page=page,
        page_size=page_size,
    )
    if not payload["ok"]:
        err = payload.get("error") or "服务不可用"
        code = 400 if err == "未配置 TUSHARE_TOKEN" else 503
        raise HTTPException(status_code=code, detail=err)
    return AShareDividendYieldPageResponse(
        ok=True,
        error=None,
        trade_date=payload["trade_date"],
        fetched_at=payload["fetched_at"],
        total=int(payload["total"]),
        page=int(payload["page"]),
        page_size=int(payload["page_size"]),
        sort=sort,
        order=order,
        items=[DividendYieldRow.model_validate(x) for x in payload["items"]],
    )
