"""company_ai_analysis：持久化 DCF 快照与列表摘要列。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "008_company_ai_dcf"
down_revision = "007_company_ai"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "company_ai_analysis",
        sa.Column("dcf_json", mysql.JSON(), nullable=True),
    )
    op.add_column(
        "company_ai_analysis",
        sa.Column("dcf_ok", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "company_ai_analysis",
        sa.Column("dcf_headline", sa.String(length=512), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("company_ai_analysis", "dcf_headline")
    op.drop_column("company_ai_analysis", "dcf_ok")
    op.drop_column("company_ai_analysis", "dcf_json")
