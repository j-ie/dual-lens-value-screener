from __future__ import annotations

from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.engine import Connection, Engine

from value_screener.infrastructure.financial_statement_schema import fs_balance, fs_cashflow, fs_income
from value_screener.infrastructure.reference_repository import ReferenceMasterRepository


class AsOfFundamentalRepository:
    """按 as-of 日期重建财报可见快照（公告日优先，缺失时回退 end_date）。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._ref_repo = ReferenceMasterRepository(engine)

    def list_universe(self, conn: Connection, symbols: list[str] | None = None) -> list[str]:
        if symbols:
            return [str(x).strip() for x in symbols if str(x).strip()]
        return self._ref_repo.list_active_ts_codes(conn)

    @staticmethod
    def _visible(report_ann_date: str | None, end_date: str | None, as_of: str) -> bool:
        ann = (report_ann_date or "").strip()
        end = (end_date or "").strip()
        if ann and len(ann) == 8:
            return ann <= as_of
        if end and len(end) == 8:
            return end <= as_of
        return False

    def _latest_visible_row(
        self,
        conn: Connection,
        table: Any,
        ts_code: str,
        as_of: str,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(table)
            .where(table.c.ts_code == ts_code)
            .order_by(desc(table.c.ann_date), desc(table.c.end_date))
            .limit(max(1, int(limit) * 4))
        )
        rows = [dict(r) for r in conn.execute(stmt).mappings()]
        vis = [r for r in rows if self._visible(r.get("ann_date"), r.get("end_date"), as_of)]
        vis.sort(key=lambda r: (str(r.get("ann_date") or ""), str(r.get("end_date") or "")), reverse=True)
        return vis[:limit]

    def build_asof_fact_map(
        self,
        conn: Connection,
        as_of: str,
        symbols: list[str] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
        universe = self.list_universe(conn, symbols=symbols)
        out: dict[str, dict[str, Any]] = {}
        stat = {"total_symbols": len(universe), "no_visible_fs": 0, "ok_symbols": 0}
        for ts_code in universe:
            inc_rows = self._latest_visible_row(conn, fs_income, ts_code, as_of, limit=4)
            bal_rows = self._latest_visible_row(conn, fs_balance, ts_code, as_of, limit=1)
            cf_rows = self._latest_visible_row(conn, fs_cashflow, ts_code, as_of, limit=4)
            if not inc_rows or not bal_rows or not cf_rows:
                stat["no_visible_fs"] += 1
                continue
            bal0 = bal_rows[0]
            ni_ttm = sum(float(r.get("n_income_attr_p") or 0.0) for r in inc_rows)
            rev_ttm = sum(float(r.get("total_revenue") or 0.0) for r in inc_rows)
            ocf_ttm = sum(float(r.get("n_cashflow_act") or 0.0) for r in cf_rows)
            fact = {
                "total_current_assets": _num_or_none(bal0.get("total_cur_assets")),
                "total_current_liabilities": _num_or_none(bal0.get("total_cur_liab")),
                "total_liabilities": _num_or_none(bal0.get("total_liab")),
                "total_equity": _num_or_none(bal0.get("total_hldr_eqy_exc_min_int")),
                "net_income_ttm": ni_ttm if ni_ttm != 0 else None,
                "revenue_ttm": rev_ttm if rev_ttm != 0 else None,
                "operating_cash_flow_ttm": ocf_ttm if ocf_ttm != 0 else None,
                "ann_date_latest": str(inc_rows[0].get("ann_date") or ""),
                "end_date_latest": str(inc_rows[0].get("end_date") or ""),
            }
            out[ts_code] = fact
            stat["ok_symbols"] += 1
        return out, stat


def _num_or_none(v: Any) -> float | None:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x

