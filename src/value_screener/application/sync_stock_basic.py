from __future__ import annotations

import logging
import math
from typing import Any

from sqlalchemy.engine import Engine

from value_screener.infrastructure.reference_repository import ReferenceMasterRepository

logger = logging.getLogger(__name__)

_STOCK_BASIC_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,"
    "curr_type,list_status,list_date,delist_date,is_hs"
)


def _records_from_dataframe(df: Any) -> list[dict[str, Any]]:
    rows = df.to_dict("records")
    out: list[dict[str, Any]] = []
    for raw in rows:
        rec: dict[str, Any] = {}
        for k, v in raw.items():
            if v is None:
                rec[k] = None
            elif isinstance(v, float) and math.isnan(v):
                rec[k] = None
            else:
                rec[k] = v
        out.append(rec)
    return out


def sync_stock_basic_to_mysql(engine: Engine, token: str) -> int:
    """
    拉取 TuShare stock_basic（上市状态 L）并 upsert 至 security_reference。
    返回写入/更新的行数（近似为请求行数）。
    """

    if not token or not token.strip():
        raise ValueError("TUSHARE_TOKEN 不能为空")
    import tushare as ts

    ts.set_token(token.strip())
    pro = ts.pro_api()
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields=_STOCK_BASIC_FIELDS,
    )
    if df is None or df.empty:
        logger.warning("stock_basic 返回空")
        return 0
    records = _records_from_dataframe(df)
    repo = ReferenceMasterRepository(engine)
    with engine.begin() as conn:
        n = repo.upsert_stock_basic_rows(conn, records)
    logger.info("security_reference upsert 完成，约 %s 行", n)
    return n
