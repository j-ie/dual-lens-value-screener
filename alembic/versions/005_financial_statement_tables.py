"""三大财报表 fs_income / fs_balance / fs_cashflow（核心列 + payload JSON）。"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "005_fs_stmt"
down_revision = "004_fin_snap"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fs_income",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ts_code", sa.String(length=16), nullable=False),
        sa.Column("end_date", sa.String(length=8), nullable=False),
        sa.Column("ann_date", sa.String(length=8), nullable=True),
        sa.Column("f_ann_date", sa.String(length=8), nullable=True),
        sa.Column("report_type", sa.String(length=8), nullable=True),
        sa.Column("comp_type", sa.String(length=8), nullable=True),
        sa.Column("payload", mysql.JSON(), nullable=False),
        sa.Column("data_source", sa.String(length=32), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_revenue", sa.Numeric(24, 4), nullable=True),
        sa.Column("operate_profit", sa.Numeric(24, 4), nullable=True),
        sa.Column("total_profit", sa.Numeric(24, 4), nullable=True),
        sa.Column("n_income", sa.Numeric(24, 4), nullable=True),
        sa.Column("n_income_attr_p", sa.Numeric(24, 4), nullable=True),
        sa.Column("income_tax", sa.Numeric(24, 4), nullable=True),
        sa.Column("basic_eps", sa.Numeric(20, 6), nullable=True),
        sa.Column("diluted_eps", sa.Numeric(20, 6), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts_code", "end_date", name="uk_fs_income_ts_end"),
    )
    op.execute("CREATE INDEX idx_fs_income_ts ON fs_income (ts_code ASC, end_date DESC)")

    op.create_table(
        "fs_balance",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ts_code", sa.String(length=16), nullable=False),
        sa.Column("end_date", sa.String(length=8), nullable=False),
        sa.Column("ann_date", sa.String(length=8), nullable=True),
        sa.Column("f_ann_date", sa.String(length=8), nullable=True),
        sa.Column("report_type", sa.String(length=8), nullable=True),
        sa.Column("comp_type", sa.String(length=8), nullable=True),
        sa.Column("payload", mysql.JSON(), nullable=False),
        sa.Column("data_source", sa.String(length=32), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_assets", sa.Numeric(24, 4), nullable=True),
        sa.Column("total_liab", sa.Numeric(24, 4), nullable=True),
        sa.Column("total_cur_assets", sa.Numeric(24, 4), nullable=True),
        sa.Column("total_cur_liab", sa.Numeric(24, 4), nullable=True),
        sa.Column("money_cap", sa.Numeric(24, 4), nullable=True),
        sa.Column("inventories", sa.Numeric(24, 4), nullable=True),
        sa.Column("total_hldr_eqy_exc_min_int", sa.Numeric(24, 4), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts_code", "end_date", name="uk_fs_balance_ts_end"),
    )
    op.execute("CREATE INDEX idx_fs_balance_ts ON fs_balance (ts_code ASC, end_date DESC)")

    op.create_table(
        "fs_cashflow",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ts_code", sa.String(length=16), nullable=False),
        sa.Column("end_date", sa.String(length=8), nullable=False),
        sa.Column("ann_date", sa.String(length=8), nullable=True),
        sa.Column("f_ann_date", sa.String(length=8), nullable=True),
        sa.Column("report_type", sa.String(length=8), nullable=True),
        sa.Column("comp_type", sa.String(length=8), nullable=True),
        sa.Column("payload", mysql.JSON(), nullable=False),
        sa.Column("data_source", sa.String(length=32), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("n_cashflow_act", sa.Numeric(24, 4), nullable=True),
        sa.Column("n_cash_flows_inv_act", sa.Numeric(24, 4), nullable=True),
        sa.Column("n_cash_flows_fnc_act", sa.Numeric(24, 4), nullable=True),
        sa.Column("c_cash_equ_end_period", sa.Numeric(24, 4), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ts_code", "end_date", name="uk_fs_cashflow_ts_end"),
    )
    op.execute("CREATE INDEX idx_fs_cashflow_ts ON fs_cashflow (ts_code ASC, end_date DESC)")


def downgrade() -> None:
    op.execute("DROP INDEX idx_fs_cashflow_ts ON fs_cashflow")
    op.drop_table("fs_cashflow")
    op.execute("DROP INDEX idx_fs_balance_ts ON fs_balance")
    op.drop_table("fs_balance")
    op.execute("DROP INDEX idx_fs_income_ts ON fs_income")
    op.drop_table("fs_income")
