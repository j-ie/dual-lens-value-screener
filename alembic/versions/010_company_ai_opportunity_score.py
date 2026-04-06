"""company_ai_analysis：机会倾向分（黄金坑/安全边际叙事，与大模型第二标量分对齐）。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "010_company_ai_opportunity"
down_revision = "009_ingestion_job"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "company_ai_analysis",
        sa.Column("opportunity_score", sa.Numeric(8, 4), nullable=True),
    )
    op.add_column(
        "company_ai_analysis",
        sa.Column("opportunity_score_rationale", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("company_ai_analysis", "opportunity_score_rationale")
    op.drop_column("company_ai_analysis", "opportunity_score")
