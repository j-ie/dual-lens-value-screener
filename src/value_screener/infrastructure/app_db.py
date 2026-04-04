from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _mysql_connect_args(url: str) -> dict[str, object]:
    """统一 utf8mb4，避免中文证券名写入报 1366 Incorrect string value。"""
    args: dict[str, object] = {"charset": "utf8mb4"}
    if "mysql+mysqlconnector" in url:
        # 未消费完结果集就发下一条会报 2014 Commands out of sync
        args["consume_results"] = True
    return args


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("未配置 DATABASE_URL，无法连接 MySQL")
    connect_args: dict[str, object] = {}
    if url.startswith("mysql+"):
        connect_args = _mysql_connect_args(url)
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        future=True,
        connect_args=connect_args,
    )
