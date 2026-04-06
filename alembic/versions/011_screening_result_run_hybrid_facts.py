"""screening_result：混合存储 run_fact_json、market_cap、pe_ttm 及索引。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "011_run_hybrid_facts"
down_revision = "010_company_ai_opportunity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "screening_result",
        sa.Column("run_fact_json", mysql.JSON(), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("market_cap", sa.Numeric(24, 4), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("pe_ttm", sa.Numeric(24, 8), nullable=True),
    )
    op.execute(
        "CREATE INDEX idx_screening_result_run_pe_ttm ON screening_result (run_id, pe_ttm)"
    )
    op.execute(
        "CREATE INDEX idx_screening_result_run_mcap ON screening_result (run_id, market_cap)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX idx_screening_result_run_mcap ON screening_result")
    op.execute("DROP INDEX idx_screening_result_run_pe_ttm ON screening_result")
    op.drop_column("screening_result", "pe_ttm")
    op.drop_column("screening_result", "market_cap")
    op.drop_column("screening_result", "run_fact_json")
