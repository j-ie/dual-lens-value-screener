"""financial_snapshot 表；screening_result 增加 combined_score、coverage_ok。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "004_fin_snap"
down_revision = "003_utf8mb4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "financial_snapshot",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("financials_end_date", sa.String(length=16), nullable=False, server_default=""),
        sa.Column("snapshot_json", mysql.JSON(), nullable=False),
        sa.Column("data_source", sa.String(length=64), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", "financials_end_date", name="uk_financial_snapshot_sym_period"),
    )
    op.execute(
        "CREATE INDEX idx_financial_snapshot_symbol_fetched ON financial_snapshot "
        "(symbol ASC, fetched_at DESC)"
    )

    op.add_column(
        "screening_result",
        sa.Column("combined_score", sa.Numeric(12, 4), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("coverage_ok", sa.Boolean(), nullable=False, server_default=sa.text("1")),
    )


def downgrade() -> None:
    op.drop_column("screening_result", "coverage_ok")
    op.drop_column("screening_result", "combined_score")
    op.execute("DROP INDEX idx_financial_snapshot_symbol_fetched ON financial_snapshot")
    op.drop_table("financial_snapshot")
