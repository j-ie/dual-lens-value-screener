"""screening_run / screening_result 表定义（SQLAlchemy Table，供仓储使用；索引见 Alembic 迁移）。"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
)

metadata = MetaData()

screening_run = Table(
    "screening_run",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("external_uuid", String(36), nullable=False, unique=True),
    Column("status", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    Column("universe_size", BigInteger, nullable=True),
    Column("snapshot_ok", BigInteger, nullable=True),
    Column("snapshot_failed", BigInteger, nullable=True),
    Column("provider_label", String(128), nullable=True),
    Column("meta_json", JSON, nullable=True),
)

screening_result = Table(
    "screening_result",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "run_id",
        BigInteger,
        ForeignKey("screening_run.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("symbol", String(32), nullable=False),
    Column("graham_score", Numeric(12, 4), nullable=False),
    Column("buffett_score", Numeric(12, 4), nullable=False),
    Column("graham_json", JSON, nullable=False),
    Column("buffett_json", JSON, nullable=False),
    Column("provenance_json", JSON, nullable=True),
    Column("combined_score", Numeric(12, 4), nullable=True),
    Column("coverage_ok", Boolean, nullable=False),
    Column("third_lens_score", Numeric(12, 4), nullable=True),
    Column("third_lens_json", JSON, nullable=True),
    Column("final_triple_score", Numeric(12, 4), nullable=True),
    UniqueConstraint("run_id", "symbol", name="uk_screening_result_run_symbol"),
)

financial_snapshot = Table(
    "financial_snapshot",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("symbol", String(32), nullable=False),
    Column("financials_end_date", String(16), nullable=False, server_default=""),
    Column("snapshot_json", JSON, nullable=False),
    Column("data_source", String(64), nullable=True),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("content_hash", String(64), nullable=True),
    UniqueConstraint("symbol", "financials_end_date", name="uk_financial_snapshot_sym_period"),
)

security_reference = Table(
    "security_reference",
    metadata,
    Column("ts_code", String(16), primary_key=True, nullable=False),
    Column("symbol", String(16), nullable=True),
    Column("name", String(64), nullable=True),
    Column("area", String(32), nullable=True),
    Column("industry", String(64), nullable=True),
    Column("fullname", String(256), nullable=True),
    Column("enname", String(256), nullable=True),
    Column("cnspell", String(32), nullable=True),
    Column("market", String(32), nullable=True),
    Column("exchange", String(16), nullable=True),
    Column("curr_type", String(16), nullable=True),
    Column("list_status", String(8), nullable=True),
    Column("list_date", String(16), nullable=True),
    Column("delist_date", String(16), nullable=True),
    Column("is_hs", String(8), nullable=True),
    Column("synced_at", DateTime(timezone=True), nullable=False),
)
