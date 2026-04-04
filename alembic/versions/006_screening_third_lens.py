"""screening_result 第三套分与三元综合分。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "006_third_lens"
down_revision = "005_fs_stmt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "screening_result",
        sa.Column("third_lens_score", sa.Numeric(12, 4), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("third_lens_json", mysql.JSON(), nullable=True),
    )
    op.add_column(
        "screening_result",
        sa.Column("final_triple_score", sa.Numeric(12, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("screening_result", "final_triple_score")
    op.drop_column("screening_result", "third_lens_json")
    op.drop_column("screening_result", "third_lens_score")
