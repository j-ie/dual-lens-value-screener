"""initial screening_run and screening_result

Revision ID: 001_initial
Revises:
Create Date: 2026-04-04

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "screening_run",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("external_uuid", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("universe_size", sa.BigInteger(), nullable=True),
        sa.Column("snapshot_ok", sa.BigInteger(), nullable=True),
        sa.Column("snapshot_failed", sa.BigInteger(), nullable=True),
        sa.Column("provider_label", sa.String(length=128), nullable=True),
        sa.Column("meta_json", mysql.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("external_uuid"),
    )
    op.create_table(
        "screening_result",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("graham_score", sa.Numeric(12, 4), nullable=False),
        sa.Column("buffett_score", sa.Numeric(12, 4), nullable=False),
        sa.Column("graham_json", mysql.JSON(), nullable=False),
        sa.Column("buffett_json", mysql.JSON(), nullable=False),
        sa.Column("provenance_json", mysql.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["screening_run.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "symbol", name="uk_screening_result_run_symbol"),
    )
    op.execute(
        "CREATE INDEX idx_screening_result_run_buffett ON screening_result "
        "(run_id, buffett_score DESC, symbol ASC)"
    )
    op.execute(
        "CREATE INDEX idx_screening_result_run_graham ON screening_result "
        "(run_id, graham_score DESC, symbol ASC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX idx_screening_result_run_graham ON screening_result")
    op.execute("DROP INDEX idx_screening_result_run_buffett ON screening_result")
    op.drop_table("screening_result")
    op.drop_table("screening_run")
