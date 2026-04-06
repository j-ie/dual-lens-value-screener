"""company_ai_analysis：AI 分析持久化（ts_code + analysis_date 唯一）。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "007_company_ai"
down_revision = "006_third_lens"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "company_ai_analysis",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ts_code", sa.String(length=32), nullable=False),
        sa.Column("analysis_date", sa.Date(), nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=True),
        sa.Column("ai_score", sa.Numeric(8, 4), nullable=False),
        sa.Column("ai_score_rationale", sa.String(length=512), nullable=True),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("key_metrics_commentary", sa.Text(), nullable=False),
        sa.Column("risks", sa.Text(), nullable=False),
        sa.Column("alignment_with_scores", sa.Text(), nullable=False),
        sa.Column("narrative_markdown", sa.Text(), nullable=False),
        sa.Column("context_hash", sa.String(length=64), nullable=False),
        sa.Column("prompt_version", sa.String(length=32), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["screening_run.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts_code", "analysis_date", name="uk_company_ai_analysis_ts_date"),
        mysql_charset="utf8mb4",
    )
    op.execute("CREATE INDEX idx_company_ai_ts ON company_ai_analysis (ts_code ASC)")
    op.execute("CREATE INDEX idx_company_ai_date ON company_ai_analysis (analysis_date DESC)")


def downgrade() -> None:
    op.execute("DROP INDEX idx_company_ai_date ON company_ai_analysis")
    op.execute("DROP INDEX idx_company_ai_ts ON company_ai_analysis")
    op.drop_table("company_ai_analysis")
