from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from value_screener.application.financial_statement_payload import (
    balance_scalars,
    cashflow_scalars,
    income_scalars,
    sanitize_financial_row,
)
from value_screener.infrastructure.financial_statement_schema import fs_balance, fs_cashflow, fs_income


class FinancialStatementRepository:
    """三大财报表幂等 upsert。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def upsert_income_rows(
        self,
        conn: Connection,
        rows: list[dict[str, Any]],
        *,
        data_source: str,
        fetched_at: datetime,
    ) -> int:
        n = 0
        for raw in rows:
            pl = sanitize_financial_row(raw)
            ex = income_scalars(raw)
            vals = {
                "ts_code": str(raw.get("ts_code") or "").strip(),
                "end_date": str(raw.get("end_date") or "").strip(),
                "ann_date": _str_or_none(raw.get("ann_date")),
                "f_ann_date": _str_or_none(raw.get("f_ann_date")),
                "report_type": _str_or_none(raw.get("report_type")),
                "comp_type": _str_or_none(raw.get("comp_type")),
                "payload": pl,
                "data_source": data_source,
                "fetched_at": fetched_at,
                **ex,
            }
            if not vals["ts_code"] or len(vals["end_date"]) != 8:
                continue
            stmt = mysql_insert(fs_income).values(**vals)
            upd = {c.name: getattr(stmt.inserted, c.name) for c in fs_income.columns if c.name != "id"}
            conn.execute(stmt.on_duplicate_key_update(**upd))
            n += 1
        return n

    def upsert_balance_rows(
        self,
        conn: Connection,
        rows: list[dict[str, Any]],
        *,
        data_source: str,
        fetched_at: datetime,
    ) -> int:
        n = 0
        for raw in rows:
            pl = sanitize_financial_row(raw)
            ex = balance_scalars(raw)
            vals = {
                "ts_code": str(raw.get("ts_code") or "").strip(),
                "end_date": str(raw.get("end_date") or "").strip(),
                "ann_date": _str_or_none(raw.get("ann_date")),
                "f_ann_date": _str_or_none(raw.get("f_ann_date")),
                "report_type": _str_or_none(raw.get("report_type")),
                "comp_type": _str_or_none(raw.get("comp_type")),
                "payload": pl,
                "data_source": data_source,
                "fetched_at": fetched_at,
                **ex,
            }
            if not vals["ts_code"] or len(vals["end_date"]) != 8:
                continue
            stmt = mysql_insert(fs_balance).values(**vals)
            upd = {c.name: getattr(stmt.inserted, c.name) for c in fs_balance.columns if c.name != "id"}
            conn.execute(stmt.on_duplicate_key_update(**upd))
            n += 1
        return n

    def upsert_cashflow_rows(
        self,
        conn: Connection,
        rows: list[dict[str, Any]],
        *,
        data_source: str,
        fetched_at: datetime,
    ) -> int:
        n = 0
        for raw in rows:
            pl = sanitize_financial_row(raw)
            ex = cashflow_scalars(raw)
            vals = {
                "ts_code": str(raw.get("ts_code") or "").strip(),
                "end_date": str(raw.get("end_date") or "").strip(),
                "ann_date": _str_or_none(raw.get("ann_date")),
                "f_ann_date": _str_or_none(raw.get("f_ann_date")),
                "report_type": _str_or_none(raw.get("report_type")),
                "comp_type": _str_or_none(raw.get("comp_type")),
                "payload": pl,
                "data_source": data_source,
                "fetched_at": fetched_at,
                **ex,
            }
            if not vals["ts_code"] or len(vals["end_date"]) != 8:
                continue
            stmt = mysql_insert(fs_cashflow).values(**vals)
            upd = {c.name: getattr(stmt.inserted, c.name) for c in fs_cashflow.columns if c.name != "id"}
            conn.execute(stmt.on_duplicate_key_update(**upd))
            n += 1
        return n

    def list_recent_income(
        self,
        conn: Connection,
        ts_code: str,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        code = str(ts_code).strip()
        if not code or limit < 1:
            return []
        stmt = (
            select(fs_income)
            .where(fs_income.c.ts_code == code)
            .order_by(desc(fs_income.c.end_date))
            .limit(limit)
        )
        return [_fin_row(m) for m in conn.execute(stmt).mappings()]

    def list_recent_balance(
        self,
        conn: Connection,
        ts_code: str,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        code = str(ts_code).strip()
        if not code or limit < 1:
            return []
        stmt = (
            select(fs_balance)
            .where(fs_balance.c.ts_code == code)
            .order_by(desc(fs_balance.c.end_date))
            .limit(limit)
        )
        return [_fin_row(m) for m in conn.execute(stmt).mappings()]

    def list_recent_cashflow(
        self,
        conn: Connection,
        ts_code: str,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        code = str(ts_code).strip()
        if not code or limit < 1:
            return []
        stmt = (
            select(fs_cashflow)
            .where(fs_cashflow.c.ts_code == code)
            .order_by(desc(fs_cashflow.c.end_date))
            .limit(limit)
        )
        return [_fin_row(m) for m in conn.execute(stmt).mappings()]


def _fin_row(m: Any) -> dict[str, Any]:
    return dict(m)


def _str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None
