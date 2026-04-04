"""security_reference for TuShare stock_basic cache

Revision ID: 002_security_ref
Revises: 001_initial
Create Date: 2026-04-04

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002_security_ref"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "security_reference",
        sa.Column("ts_code", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=16), nullable=True),
        sa.Column("name", sa.String(length=64), nullable=True),
        sa.Column("area", sa.String(length=32), nullable=True),
        sa.Column("industry", sa.String(length=64), nullable=True),
        sa.Column("fullname", sa.String(length=256), nullable=True),
        sa.Column("enname", sa.String(length=256), nullable=True),
        sa.Column("cnspell", sa.String(length=32), nullable=True),
        sa.Column("market", sa.String(length=32), nullable=True),
        sa.Column("exchange", sa.String(length=16), nullable=True),
        sa.Column("curr_type", sa.String(length=16), nullable=True),
        sa.Column("list_status", sa.String(length=8), nullable=True),
        sa.Column("list_date", sa.String(length=16), nullable=True),
        sa.Column("delist_date", sa.String(length=16), nullable=True),
        sa.Column("is_hs", sa.String(length=8), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("ts_code"),
    )


def downgrade() -> None:
    op.drop_table("security_reference")
