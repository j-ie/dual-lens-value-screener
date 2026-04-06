"""ingestion_job：按调度日可恢复财报同步任务游标。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "009_ingestion_job"
down_revision = "008_company_ai_dcf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_job",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("scheduled_date", sa.Date(), nullable=False),
        sa.Column("params_hash", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("cursor_ts_code", sa.String(length=16), nullable=True),
        sa.Column("universe_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "job_type",
            "scheduled_date",
            "params_hash",
            name="uk_ingestion_job_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("ingestion_job")
