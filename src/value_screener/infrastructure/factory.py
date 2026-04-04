from __future__ import annotations

import logging

from value_screener.infrastructure.composite_provider import CompositeAShareDataProvider
from value_screener.infrastructure.settings import AShareIngestionSettings

logger = logging.getLogger(__name__)


def build_composite_provider(settings: AShareIngestionSettings) -> CompositeAShareDataProvider:
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
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("TuShare 初始化失败，将仅使用 AkShare: %s", exc)

    primary_name = settings.primary_backend
    if tu is None:
        return CompositeAShareDataProvider(ak, None)
    if primary_name == "tushare":
        return CompositeAShareDataProvider(tu, ak)
    return CompositeAShareDataProvider(ak, tu)
