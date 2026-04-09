from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from value_screener.infrastructure.screening_schema import security_reference

# 单次过大 INSERT 易触发 mysql-connector「Commands out of sync」或超过 max_allowed_packet；分块更稳。
_UPSERT_CHUNK_SIZE = 400

_STOCK_BASIC_COLUMNS = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
)


class ReferenceMasterRepository:
    """证券主数据：TuShare stock_basic 映射行 upsert 与按代码查询。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def upsert_stock_basic_rows(self, conn: Connection, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        now = datetime.now(timezone.utc)
        batch: list[dict[str, Any]] = []
        for raw in rows:
            rec: dict[str, Any] = {"synced_at": now}
            for col in _STOCK_BASIC_COLUMNS:
                v = raw.get(col)
                if v is not None and hasattr(v, "item"):
                    v = v.item()
                if v is None:
                    rec[col] = None
                elif col == "ts_code":
                    rec[col] = str(v).strip()
                else:
                    rec[col] = str(v).strip() if not isinstance(v, (int, float)) else str(v)
            if not rec.get("ts_code"):
                continue
            batch.append(rec)
        if not batch:
            return 0
        written = 0
        for start in range(0, len(batch), _UPSERT_CHUNK_SIZE):
            chunk = batch[start : start + _UPSERT_CHUNK_SIZE]
            stmt = mysql_insert(security_reference).values(chunk)
            update_map = {c: getattr(stmt.inserted, c) for c in _STOCK_BASIC_COLUMNS if c != "ts_code"}
            update_map["synced_at"] = stmt.inserted.synced_at
            stmt = stmt.on_duplicate_key_update(**update_map)
            result = conn.execute(stmt)
            result.close()
            written += len(chunk)
        return written

    def list_active_ts_codes(self, conn: Connection) -> list[str]:
        """已上市（list_status=L）证券 ts_code，按代码排序；表空则返回 []."""

        stmt = (
            select(security_reference.c.ts_code)
            .where(security_reference.c.list_status == "L")
            .order_by(security_reference.c.ts_code)
        )
        return [str(r[0]) for r in conn.execute(stmt)]

    def fetch_by_ts_codes(self, conn: Connection, ts_codes: Sequence[str]) -> dict[str, dict[str, Any]]:
        if not ts_codes:
            return {}
        stmt = select(security_reference).where(security_reference.c.ts_code.in_(list(ts_codes)))
        out: dict[str, dict[str, Any]] = {}
        for r in conn.execute(stmt).mappings():
            out[str(r["ts_code"])] = dict(r)
        return out

    def fetch_one_by_ts_code(self, conn: Connection, ts_code: str) -> dict[str, Any] | None:
        """按 ts_code 读取一行主数据；不存在返回 None。"""

        code = str(ts_code).strip()
        if not code:
            return None
        got = self.fetch_by_ts_codes(conn, [code])
        return got.get(code)

    def fetch_industry_map(self, conn: Connection, ts_codes: Sequence[str]) -> dict[str, str]:
        """批跑用：ts_code → industry（主数据无行业则为空串）。大单 IN 分块避免超长 SQL。"""

        codes = [str(c).strip() for c in ts_codes if c and str(c).strip()]
        if not codes:
            return {}
        out: dict[str, str] = {}
        chunk = 900
        for i in range(0, len(codes), chunk):
            part = codes[i : i + chunk]
            rows = self.fetch_by_ts_codes(conn, part)
            for ts, row in rows.items():
                out[ts] = str(row.get("industry") or "").strip()
        return out
