"""company_ai_analysis 表：upsert 与历史列表分页。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from sqlalchemy import Select, and_, asc, delete, desc, func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from value_screener.infrastructure.company_name_search import (
    company_display_and_code_match_clause,
    normalized_company_search_term,
)
from value_screener.infrastructure.mysql_collation import ts_code_equals
from value_screener.infrastructure.screening_repository import (
    INDUSTRY_EMPTY_QUERY_VALUE,
    _industry_filter_or,
)
from value_screener.infrastructure.screening_schema import company_ai_analysis, security_reference

ListSortKey = Literal["analysis_date", "ai_score", "ts_code"]
ListOrderKey = Literal["asc", "desc"]


@dataclass(frozen=True, slots=True)
class CompanyAiListPage:
    items: list[dict[str, Any]]
    total: int


class CompanyAiAnalysisRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def delete_by_run_id(self, conn: Connection, run_id: int) -> None:
        """删除曾绑定到该 screening_run 的落库 AI 记录（与 ON DELETE SET NULL 相比，满足「随任务一并清理」）。"""

        conn.execute(delete(company_ai_analysis).where(company_ai_analysis.c.run_id == int(run_id)))

    def upsert(
        self,
        conn: Connection,
        *,
        ts_code: str,
        analysis_date: date,
        run_id: int | None,
        ai_score: float,
        ai_score_rationale: str | None,
        opportunity_score: float,
        opportunity_score_rationale: str | None,
        summary: str,
        key_metrics_commentary: str,
        risks: str,
        alignment_with_scores: str,
        narrative_markdown: str,
        context_hash: str,
        prompt_version: str,
        model: str,
        generated_at: datetime,
        now: datetime,
        dcf_json: dict[str, Any] | None = None,
        dcf_ok: bool | None = None,
        dcf_headline: str | None = None,
    ) -> None:
        vals = {
            "ts_code": str(ts_code).strip(),
            "analysis_date": analysis_date,
            "run_id": run_id,
            "ai_score": ai_score,
            "ai_score_rationale": ai_score_rationale,
            "opportunity_score": opportunity_score,
            "opportunity_score_rationale": opportunity_score_rationale,
            "summary": summary,
            "key_metrics_commentary": key_metrics_commentary,
            "risks": risks,
            "alignment_with_scores": alignment_with_scores,
            "narrative_markdown": narrative_markdown,
            "context_hash": context_hash,
            "prompt_version": prompt_version,
            "model": model,
            "generated_at": generated_at,
            "created_at": now,
            "updated_at": now,
            "dcf_json": dcf_json,
            "dcf_ok": dcf_ok,
            "dcf_headline": dcf_headline,
        }
        stmt = mysql_insert(company_ai_analysis).values(**vals)
        updatable = {
            "run_id": stmt.inserted.run_id,
            "ai_score": stmt.inserted.ai_score,
            "ai_score_rationale": stmt.inserted.ai_score_rationale,
            "opportunity_score": stmt.inserted.opportunity_score,
            "opportunity_score_rationale": stmt.inserted.opportunity_score_rationale,
            "summary": stmt.inserted.summary,
            "key_metrics_commentary": stmt.inserted.key_metrics_commentary,
            "risks": stmt.inserted.risks,
            "alignment_with_scores": stmt.inserted.alignment_with_scores,
            "narrative_markdown": stmt.inserted.narrative_markdown,
            "context_hash": stmt.inserted.context_hash,
            "prompt_version": stmt.inserted.prompt_version,
            "model": stmt.inserted.model,
            "generated_at": stmt.inserted.generated_at,
            "updated_at": stmt.inserted.updated_at,
            "dcf_json": stmt.inserted.dcf_json,
            "dcf_ok": stmt.inserted.dcf_ok,
            "dcf_headline": stmt.inserted.dcf_headline,
        }
        conn.execute(stmt.on_duplicate_key_update(**updatable))

    def fetch_by_id(self, conn: Connection, row_id: int) -> dict[str, Any] | None:
        stmt = select(company_ai_analysis).where(company_ai_analysis.c.id == int(row_id)).limit(1)
        r = conn.execute(stmt).mappings().first()
        if r is None:
            return None
        return _row_to_persisted_detail(dict(r))

    def fetch_latest_by_ts_code(self, conn: Connection, ts_code: str) -> dict[str, Any] | None:
        """按 analysis_date、id 取最新一条落库分析（含 dcf_json）。"""

        code = str(ts_code).strip()
        if not code:
            return None
        stmt = (
            select(company_ai_analysis)
            .where(company_ai_analysis.c.ts_code == code)
            .order_by(company_ai_analysis.c.analysis_date.desc(), company_ai_analysis.c.id.desc())
            .limit(1)
        )
        r = conn.execute(stmt).mappings().first()
        if r is None:
            return None
        return _row_to_persisted_detail(dict(r))

    def page_list(
        self,
        conn: Connection,
        *,
        page: int,
        page_size: int,
        sort_key: ListSortKey,
        order: ListOrderKey,
        analysis_date_from: date | None,
        analysis_date_to: date | None,
        ai_score_min: float | None,
        industries: list[str] | None,
        include_dcf: bool = False,
        company_name: str | None = None,
    ) -> CompanyAiListPage:
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 200:
            page_size = 20

        join_from = company_ai_analysis.outerjoin(
            security_reference,
            ts_code_equals(company_ai_analysis.c.ts_code, security_reference.c.ts_code),
        )
        where_parts: list[Any] = []
        if analysis_date_from is not None:
            where_parts.append(company_ai_analysis.c.analysis_date >= analysis_date_from)
        if analysis_date_to is not None:
            where_parts.append(company_ai_analysis.c.analysis_date <= analysis_date_to)
        if ai_score_min is not None:
            where_parts.append(company_ai_analysis.c.ai_score >= ai_score_min)
        inds = [i.strip() for i in (industries or []) if i and i.strip()]
        if inds:
            where_parts.append(_industry_filter_or(inds))

        cn_term = normalized_company_search_term(company_name)
        if cn_term:
            where_parts.append(
                company_display_and_code_match_clause(
                    ref_name_col=security_reference.c.name,
                    ref_fullname_col=security_reference.c.fullname,
                    ts_code_col=company_ai_analysis.c.ts_code,
                    term=cn_term,
                )
            )

        if sort_key == "ts_code":
            primary = desc(company_ai_analysis.c.ts_code) if order == "desc" else asc(company_ai_analysis.c.ts_code)
            order_by_list = [primary, company_ai_analysis.c.analysis_date.desc(), company_ai_analysis.c.id.desc()]
        elif sort_key == "ai_score":
            primary = desc(company_ai_analysis.c.ai_score) if order == "desc" else asc(company_ai_analysis.c.ai_score)
            order_by_list = [primary, company_ai_analysis.c.ts_code.asc(), company_ai_analysis.c.id.desc()]
        else:
            primary = (
                desc(company_ai_analysis.c.analysis_date)
                if order == "desc"
                else asc(company_ai_analysis.c.analysis_date)
            )
            order_by_list = [primary, company_ai_analysis.c.ts_code.asc(), company_ai_analysis.c.id.desc()]

        count_stmt = select(func.count()).select_from(join_from)
        cols: list[Any] = [
            company_ai_analysis.c.id,
            company_ai_analysis.c.ts_code,
            company_ai_analysis.c.analysis_date,
            company_ai_analysis.c.run_id,
            company_ai_analysis.c.ai_score,
            company_ai_analysis.c.ai_score_rationale,
            company_ai_analysis.c.opportunity_score,
            company_ai_analysis.c.opportunity_score_rationale,
            company_ai_analysis.c.summary,
            company_ai_analysis.c.generated_at,
            company_ai_analysis.c.dcf_ok,
            company_ai_analysis.c.dcf_headline,
            security_reference.c.name.label("ref_name"),
            security_reference.c.industry.label("ref_industry"),
        ]
        if include_dcf:
            cols.append(company_ai_analysis.c.dcf_json)
        data_stmt: Select[Any] = select(*cols).select_from(join_from)
        if where_parts:
            wc = and_(*where_parts)
            count_stmt = count_stmt.where(wc)
            data_stmt = data_stmt.where(wc)
        total = int(conn.execute(count_stmt).scalar_one())

        offset = (page - 1) * page_size
        data_stmt = data_stmt.order_by(*order_by_list).offset(offset).limit(page_size)
        items: list[dict[str, Any]] = []
        for r in conn.execute(data_stmt).mappings():
            items.append(_row_to_list_item(r, include_dcf=include_dcf))
        return CompanyAiListPage(items=items, total=total)


def _row_to_persisted_detail(r: dict[str, Any]) -> dict[str, Any]:
    """供详情接口返回的落库分析快照（结构化字段 + dcf）。"""

    sc = r["ai_score"]
    score_f = float(sc) if isinstance(sc, Decimal) else float(sc)
    ad = r["analysis_date"]
    ad_s = ad.isoformat() if hasattr(ad, "isoformat") else str(ad)
    dcf_j = r.get("dcf_json")
    dcf_obj = dict(dcf_j) if isinstance(dcf_j, dict) else None
    gen = r.get("generated_at")
    opp_raw = r.get("opportunity_score")
    opp_f: float | None
    if opp_raw is None:
        opp_f = None
    else:
        opp_f = round(float(opp_raw) if isinstance(opp_raw, Decimal) else float(opp_raw), 4)
    return {
        "ts_code": str(r["ts_code"]),
        "analysis_date": ad_s,
        "run_id": int(r["run_id"]) if r.get("run_id") is not None else None,
        "ai_score": round(score_f, 4),
        "ai_score_rationale": r.get("ai_score_rationale"),
        "opportunity_score": opp_f,
        "opportunity_score_rationale": r.get("opportunity_score_rationale"),
        "summary": str(r.get("summary") or ""),
        "key_metrics_commentary": str(r.get("key_metrics_commentary") or ""),
        "risks": str(r.get("risks") or ""),
        "alignment_with_scores": str(r.get("alignment_with_scores") or ""),
        "narrative_markdown": str(r.get("narrative_markdown") or ""),
        "dcf_snapshot": dcf_obj,
        "dcf_ok": r.get("dcf_ok"),
        "dcf_headline": r.get("dcf_headline"),
        "context_hash": str(r.get("context_hash") or ""),
        "prompt_version": str(r.get("prompt_version") or ""),
        "model": str(r.get("model") or ""),
        "generated_at": gen.isoformat() if gen is not None and hasattr(gen, "isoformat") else None,
    }


def _row_to_list_item(r: Any, *, include_dcf: bool = False) -> dict[str, Any]:
    sc = r["ai_score"]
    score_f = float(sc) if isinstance(sc, Decimal) else float(sc)
    ad = r["analysis_date"]
    ad_s = ad.isoformat() if hasattr(ad, "isoformat") else str(ad)
    summ = str(r.get("summary") or "")
    preview = summ if len(summ) <= 200 else summ[:197] + "..."
    opp_raw = r.get("opportunity_score")
    opp_display: float | None
    if opp_raw is None:
        opp_display = None
    else:
        opp_display = round(float(opp_raw) if isinstance(opp_raw, Decimal) else float(opp_raw), 4)
    out: dict[str, Any] = {
        "id": int(r["id"]),
        "ts_code": str(r["ts_code"]),
        "analysis_date": ad_s,
        "run_id": int(r["run_id"]) if r.get("run_id") is not None else None,
        "ai_score": round(score_f, 4),
        "ai_score_rationale": r.get("ai_score_rationale"),
        "opportunity_score": opp_display,
        "opportunity_score_rationale": r.get("opportunity_score_rationale"),
        "summary_preview": preview,
        "generated_at": r["generated_at"].isoformat() if r.get("generated_at") else None,
        "display_name": (r.get("ref_name") or "").strip(),
        "industry": (r.get("ref_industry") or "").strip(),
        "dcf_ok": r.get("dcf_ok"),
        "dcf_headline": (r.get("dcf_headline") or "").strip() or None,
    }
    if include_dcf:
        dj = r.get("dcf_json")
        out["dcf"] = dict(dj) if isinstance(dj, dict) else None
    return out


__all__ = [
    "CompanyAiAnalysisRepository",
    "CompanyAiListPage",
    "INDUSTRY_EMPTY_QUERY_VALUE",
    "ListOrderKey",
    "ListSortKey",
]
