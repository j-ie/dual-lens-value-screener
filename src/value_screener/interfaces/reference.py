from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException

from value_screener.application.sync_stock_basic import sync_stock_basic_to_mysql
from value_screener.infrastructure.app_db import get_engine
from value_screener.infrastructure.settings import AShareIngestionSettings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["reference"])


def _sync_api_enabled() -> bool:
    flag = os.environ.get("REFERENCE_SYNC_API_ENABLED", "").strip().lower()
    return flag in ("1", "true", "yes", "on")


@router.post("/reference/sync-stock-basic")
def sync_stock_basic() -> dict[str, object]:
    """将 TuShare stock_basic 同步至 security_reference（需 REFERENCE_SYNC_API_ENABLED=1）。"""

    if not _sync_api_enabled():
        raise HTTPException(status_code=403, detail="未开启 REFERENCE_SYNC_API_ENABLED")
    base = AShareIngestionSettings.from_env()
    token = (base.tushare_token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="未配置 TUSHARE_TOKEN")
    try:
        engine = get_engine()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    try:
        n = sync_stock_basic_to_mysql(engine, token)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sync-stock-basic failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"upserted_rows": n}
