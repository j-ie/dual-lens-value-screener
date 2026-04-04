from __future__ import annotations

import logging

from value_screener.domain.combined_ranking_params import snapshot_cache_enabled
from value_screener.infrastructure.caching_snapshot_provider import CachingSnapshotProvider
from value_screener.infrastructure.composite_provider import CompositeAShareDataProvider
from value_screener.infrastructure.settings import AShareIngestionSettings

logger = logging.getLogger(__name__)


def build_composite_provider(settings: AShareIngestionSettings) -> CompositeAShareDataProvider | CachingSnapshotProvider:
    """
    按配置组装主备 Provider。
    无 TuShare token 时仅 AkShare；有 token 时默认 TuShare 主、AkShare 备。
    """

    from value_screener.infrastructure.akshare_provider import AkShareAShareProvider
    from value_screener.infrastructure.tushare_provider import TushareAShareProvider

    ak = AkShareAShareProvider(request_sleep_seconds=settings.request_sleep_seconds)
    tu: TushareAShareProvider | None = None
    if settings.tushare_token:
        try:
            tu = TushareAShareProvider(
                token=settings.tushare_token,
                request_sleep_seconds=settings.request_sleep_seconds,
                max_workers=settings.tushare_max_workers,
                max_retries=settings.tushare_max_retries,
                retry_backoff_seconds=settings.tushare_retry_backoff_seconds,
            )
            logger.info(
                "TuShare 拉数并发: max_workers=%s sleep=%ss retries=%s",
                settings.tushare_max_workers,
                settings.request_sleep_seconds,
                settings.tushare_max_retries,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("TuShare 初始化失败，将仅使用 AkShare: %s", exc)

    primary_name = settings.primary_backend
    if tu is None:
        inner: CompositeAShareDataProvider = CompositeAShareDataProvider(ak, None)
    elif primary_name == "tushare":
        inner = CompositeAShareDataProvider(tu, ak)
    else:
        inner = CompositeAShareDataProvider(ak, tu)

    if not snapshot_cache_enabled():
        return inner
    try:
        from value_screener.infrastructure.app_db import get_engine

        engine = get_engine()
    except Exception:
        logger.info("未启用财务快照缓存：DATABASE_URL 不可用或引擎创建失败")
        return inner
    return CachingSnapshotProvider(inner, engine)
