from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

_REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(_REPO_ROOT / ".env", override=False)
from pydantic import BaseModel, Field

from value_screener.application.screening_service import ScreeningApplicationService
from value_screener.domain.combined_ranking_params import CombinedRankingParams
from value_screener.domain.triple_composite_params import TripleCompositeParams
from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.interfaces.ai_history import router as ai_history_router
from value_screener.interfaces.market import router as market_router
from value_screener.interfaces.reference import router as reference_router
from value_screener.interfaces.runs import router as runs_router


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    CombinedRankingParams.from_env()
    TripleCompositeParams.from_env()
    yield


app = FastAPI(
    title="dual-lens-value-screener",
    description="格雷厄姆 / 巴菲特双视角并列评估（初版）",
    version="0.1.0",
    lifespan=_lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(runs_router)
app.include_router(ai_history_router)
app.include_router(reference_router)
app.include_router(market_router)
_svc = ScreeningApplicationService()


class ScreenRequest(BaseModel):
    items: list[StockFinancialSnapshot] = Field(..., min_length=1)


class ScreenResponse(BaseModel):
    results: list[dict[str, Any]]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/screen", response_model=ScreenResponse)
def screen(req: ScreenRequest) -> ScreenResponse:
    rows = _svc.screen(req.items)
    return ScreenResponse(results=rows)


@app.get("/v1/examples")
def examples() -> dict[str, Any]:
    """无外部数据密钥时可复制到 POST /v1/screen 的示例体。"""
    return {
        "description": "示例：一只偏烟蒂、一只偏质量",
        "body": {
            "items": [
                {
                    "symbol": "DEMO-GRAHAM",
                    "market_cap": 80_000_000,
                    "total_current_assets": 200_000_000,
                    "total_current_liabilities": 60_000_000,
                    "total_liabilities": 120_000_000,
                    "total_equity": 150_000_000,
                    "net_income_ttm": 5_000_000,
                    "operating_cash_flow_ttm": 4_000_000,
                    "revenue_ttm": 100_000_000,
                    "interest_bearing_debt": 40_000_000,
                },
                {
                    "symbol": "DEMO-BUFFETT",
                    "market_cap": 5_000_000_000,
                    "total_current_assets": 2_000_000_000,
                    "total_current_liabilities": 800_000_000,
                    "total_liabilities": 1_200_000_000,
                    "total_equity": 3_000_000_000,
                    "net_income_ttm": 600_000_000,
                    "operating_cash_flow_ttm": 720_000_000,
                    "revenue_ttm": 4_000_000_000,
                    "interest_bearing_debt": 200_000_000,
                },
            ]
        },
    }
