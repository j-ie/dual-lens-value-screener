"""screening_result：价值质量结论落库（JSON + iq_decision 索引筛选）。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "012_iq_screen"
down_revision = "011_run_hybrid_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "screening_result",
        sa.Column("investment_quality_json", sa.JSON(), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("iq_decision", sa.String(length=32), nullable=True),
    )
    op.execute(
        "CREATE INDEX idx_screening_result_run_iq ON screening_result (run_id, iq_decision)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX idx_screening_result_run_iq ON screening_result")
    op.drop_column("screening_result", "iq_decision")
    op.drop_column("screening_result", "investment_quality_json")
