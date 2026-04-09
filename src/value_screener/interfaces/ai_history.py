from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import distinct, select

from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.company_ai_analysis_repository import (
    CompanyAiAnalysisRepository,
    INDUSTRY_EMPTY_QUERY_VALUE,
)
from value_screener.infrastructure.mysql_collation import ts_code_equals
from value_screener.infrastructure.screening_schema import company_ai_analysis, security_reference

router = APIRouter(prefix="/api/v1", tags=["company-ai-history"])


class CompanyAiListItem(BaseModel):
    id: int
    ts_code: str
    analysis_date: str
    run_id: int | None = None
    ai_score: float
    ai_score_rationale: str | None = None
    opportunity_score: float | None = None
    opportunity_score_rationale: str | None = None
    summary_preview: str
    generated_at: str | None = None
    display_name: str = ""
    industry: str = ""
    dcf_ok: bool | None = None
    dcf_headline: str | None = None
    dcf: dict[str, object] | None = Field(
        default=None,
        description="仅当 include_dcf=1 时返回完整 DCF JSON",
    )


class CompanyAiListResponse(BaseModel):
    items: list[CompanyAiListItem]
    total: int
    page: int
    page_size: int
    sort: str
    order: str


class CompanyAiIndustriesResponse(BaseModel):
    industries: list[str]


class CompanyAiDetailResponse(BaseModel):
    """单条落库记录完整内容（供历史页展开行等）。"""

    id: int
    ts_code: str
    analysis_date: str
    run_id: int | None = None
    ai_score: float
    ai_score_rationale: str | None = None
    opportunity_score: float | None = None
    opportunity_score_rationale: str | None = None
    summary: str
    key_metrics_commentary: str
    risks: str
    alignment_with_scores: str
    narrative_markdown: str
    dcf_snapshot: dict[str, object] | None = None
    dcf_ok: bool | None = None
    dcf_headline: str | None = None
    context_hash: str = ""
    prompt_version: str = ""
    model: str = ""
    generated_at: str | None = None


def _parse_iso_date(name: str, raw: str | None) -> date | None:
    if raw is None or not str(raw).strip():
        return None
    s = str(raw).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{name} 须为 YYYY-MM-DD",
        ) from exc


@router.get("/company-ai-analyses", response_model=CompanyAiListResponse)
def list_company_ai_analyses(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort: Literal["analysis_date", "ai_score", "opportunity_score", "ts_code"] = "opportunity_score",
    order: Literal["asc", "desc"] = "desc",
    analysis_date_from: str | None = Query(None, description="含首日起，YYYY-MM-DD"),
    analysis_date_to: str | None = Query(None, description="含末日止，YYYY-MM-DD"),
    ai_score_min: float | None = Query(None, ge=0.0, le=100.0),
    industry: Annotated[list[str] | None, Query()] = None,
    include_dcf: bool = Query(False, description="为 true 时列表项含完整 dcf_json"),
    company_name: str | None = Query(
        None,
        max_length=128,
        description="公司名称/全称/ts_code 模糊匹配（子串）",
    ),
) -> CompanyAiListResponse:
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    d0 = _parse_iso_date("analysis_date_from", analysis_date_from)
    d1 = _parse_iso_date("analysis_date_to", analysis_date_to)
    ind_list = list(industry) if industry else []

    repo = CompanyAiAnalysisRepository(engine)
    with engine.connect() as conn:
        page_data = repo.page_list(
            conn,
            page=page,
            page_size=page_size,
            sort_key=sort,
            order=order,
            analysis_date_from=d0,
            analysis_date_to=d1,
            ai_score_min=ai_score_min,
            industries=ind_list if ind_list else None,
            include_dcf=include_dcf,
            company_name=company_name,
        )

    items = [CompanyAiListItem.model_validate(x) for x in page_data.items]
    return CompanyAiListResponse(
        items=items,
        total=page_data.total,
        page=page,
        page_size=page_size,
        sort=sort,
        order=order,
    )


@router.get("/company-ai-analyses/industries", response_model=CompanyAiIndustriesResponse)
def list_company_ai_industries() -> CompanyAiIndustriesResponse:
    """与 AI 历史行通过 ts_code 关联的参考表行业去重（供筛选下拉）。"""

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    stmt = (
        select(distinct(security_reference.c.industry))
        .select_from(
            company_ai_analysis.outerjoin(
                security_reference,
                ts_code_equals(company_ai_analysis.c.ts_code, security_reference.c.ts_code),
            )
        )
        .limit(500)
    )
    with engine.connect() as conn:
        raw = [row[0] for row in conn.execute(stmt)]
    out: set[str] = set()
    for v in raw:
        s = (v or "").strip()
        out.add(INDUSTRY_EMPTY_QUERY_VALUE if not s else s)
    return CompanyAiIndustriesResponse(industries=sorted(out))


@router.get("/company-ai-analyses/{analysis_id}", response_model=CompanyAiDetailResponse)
def get_company_ai_analysis_by_id(analysis_id: int) -> CompanyAiDetailResponse:
    if analysis_id < 1:
        raise HTTPException(status_code=422, detail="非法 analysis_id")

    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    repo = CompanyAiAnalysisRepository(engine)
    with engine.connect() as conn:
        row = repo.fetch_by_id(conn, analysis_id)
    if row is None:
        raise HTTPException(status_code=404, detail="记录不存在")
    return CompanyAiDetailResponse(
        id=analysis_id,
        ts_code=row["ts_code"],
        analysis_date=row["analysis_date"],
        run_id=row["run_id"],
        ai_score=row["ai_score"],
        ai_score_rationale=row.get("ai_score_rationale"),
        opportunity_score=row.get("opportunity_score"),
        opportunity_score_rationale=row.get("opportunity_score_rationale"),
        summary=row["summary"],
        key_metrics_commentary=row["key_metrics_commentary"],
        risks=row["risks"],
        alignment_with_scores=row["alignment_with_scores"],
        narrative_markdown=row["narrative_markdown"],
        dcf_snapshot=row.get("dcf_snapshot"),
        dcf_ok=row.get("dcf_ok"),
        dcf_headline=row.get("dcf_headline"),
        context_hash=row.get("context_hash") or "",
        prompt_version=row.get("prompt_version") or "",
        model=row.get("model") or "",
        generated_at=row.get("generated_at"),
    )
