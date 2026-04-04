"""三大财报宽表（核心列 + payload JSON 全量字段）。每表独立 Column 定义，避免跨表复用。"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, MetaData, Numeric, String, Table, UniqueConstraint
from sqlalchemy.dialects.mysql import JSON

metadata = MetaData()


def _core_columns() -> list:
    return [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("ts_code", String(16), nullable=False),
        Column("end_date", String(8), nullable=False),
        Column("ann_date", String(8), nullable=True),
        Column("f_ann_date", String(8), nullable=True),
        Column("report_type", String(8), nullable=True),
        Column("comp_type", String(8), nullable=True),
        Column("payload", JSON, nullable=False),
        Column("data_source", String(32), nullable=False),
        Column("fetched_at", DateTime(timezone=True), nullable=False),
    ]


fs_income = Table(
    "fs_income",
    metadata,
    *_core_columns(),
    Column("total_revenue", Numeric(24, 4), nullable=True),
    Column("operate_profit", Numeric(24, 4), nullable=True),
    Column("total_profit", Numeric(24, 4), nullable=True),
    Column("n_income", Numeric(24, 4), nullable=True),
    Column("n_income_attr_p", Numeric(24, 4), nullable=True),
    Column("income_tax", Numeric(24, 4), nullable=True),
    Column("basic_eps", Numeric(20, 6), nullable=True),
    Column("diluted_eps", Numeric(20, 6), nullable=True),
    UniqueConstraint("ts_code", "end_date", name="uk_fs_income_ts_end"),
)

fs_balance = Table(
    "fs_balance",
    metadata,
    *_core_columns(),
    Column("total_assets", Numeric(24, 4), nullable=True),
    Column("total_liab", Numeric(24, 4), nullable=True),
    Column("total_cur_assets", Numeric(24, 4), nullable=True),
    Column("total_cur_liab", Numeric(24, 4), nullable=True),
    Column("money_cap", Numeric(24, 4), nullable=True),
    Column("inventories", Numeric(24, 4), nullable=True),
    Column("total_hldr_eqy_exc_min_int", Numeric(24, 4), nullable=True),
    UniqueConstraint("ts_code", "end_date", name="uk_fs_balance_ts_end"),
)

fs_cashflow = Table(
    "fs_cashflow",
    metadata,
    *_core_columns(),
    Column("n_cashflow_act", Numeric(24, 4), nullable=True),
    Column("n_cash_flows_inv_act", Numeric(24, 4), nullable=True),
    Column("n_cash_flows_fnc_act", Numeric(24, 4), nullable=True),
    Column("c_cash_equ_end_period", Numeric(24, 4), nullable=True),
    UniqueConstraint("ts_code", "end_date", name="uk_fs_cashflow_ts_end"),
)
