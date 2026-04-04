from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from value_screener.domain.snapshot import StockFinancialSnapshot
from value_screener.infrastructure.screening_schema import financial_snapshot


class FinancialSnapshotRepository:
    """财务快照：按 symbol + 报告期 upsert；按 TTL 取最新一行。"""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def get_latest_valid_json(
        self,
        conn: Connection,
        symbol: str,
        *,
        ttl_seconds: int,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        if ttl_seconds <= 0:
            return None
        t = now or datetime.now(timezone.utc)
        cutoff = t - timedelta(seconds=ttl_seconds)
        stmt = (
            select(financial_snapshot.c.snapshot_json, financial_snapshot.c.fetched_at)
            .where(
                financial_snapshot.c.symbol == symbol,
                financial_snapshot.c.fetched_at >= cutoff,
            )
            .order_by(financial_snapshot.c.fetched_at.desc())
            .limit(1)
        )
        row = conn.execute(stmt).first()
        if row is None:
            return None
        raw = row[0]
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            return json.loads(raw)
        return None

    def upsert_snapshot(
        self,
        conn: Connection,
        snap: StockFinancialSnapshot,
        *,
        content_hash: str | None = None,
    ) -> None:
        period = (snap.financials_end_date or "").strip()
        payload = snap.model_dump(mode="json")
        h = content_hash
        if h is None:
            h = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:32]
        stmt = mysql_insert(financial_snapshot).values(
            symbol=snap.symbol,
            financials_end_date=period,
            snapshot_json=payload,
            data_source=snap.data_source,
            fetched_at=datetime.now(timezone.utc),
            content_hash=h,
        )
        stmt = stmt.on_duplicate_key_update(
            snapshot_json=stmt.inserted.snapshot_json,
            data_source=stmt.inserted.data_source,
            fetched_at=stmt.inserted.fetched_at,
            content_hash=stmt.inserted.content_hash,
        )
        conn.execute(stmt)

    def count_for_symbol(self, conn: Connection, symbol: str) -> int:
        r = conn.execute(
            text("SELECT COUNT(*) FROM financial_snapshot WHERE symbol = :s"),
            {"s": symbol},
        ).scalar_one()
        return int(r)
