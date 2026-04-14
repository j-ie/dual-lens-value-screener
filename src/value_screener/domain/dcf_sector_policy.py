"""
DCF 行业分档：净债务与基期现金流的口径调节（领域策略）。

TuShare `stock_basic.industry` 经全量显式字典映射到 `DcfSectorKind`；未收录的非空行业回落到
GENERAL 并打日志。少量名称含「地产」但不在表中的标签仍归为 REAL_ESTATE（兼容历史变体）。
"""

from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger(__name__)

_LOGGED_UNKNOWN_INDUSTRY: set[str] = set()


class DcfSectorKind(str, Enum):
    """简化 DCF 的行业分档。"""

    GENERAL = "general"
    """一般工商业：优先有息负债科目合计 − 货币资金，否则负债合计 − 货币资金作净债务粗代理。"""

    FINANCIAL = "financial"
    """银行、保险、证券等：有息负债明细求和 − 货币资金；基期现金流优先用归母净利润年报。"""

    REAL_ESTATE = "real_estate"
    """房地产开发等：负债合计 − 合同负债（若可得）− 货币资金，削弱预收房款对杠杆的高估。"""

    CYCLICAL = "cyclical"
    """强周期行业：景气波动大；默认增长假设较保守，并附周期口径披露。"""


_TUSHARE_FINANCIAL: frozenset[str] = frozenset({"银行", "证券", "保险", "多元金融", "信托"})

_TUSHARE_REAL_ESTATE: frozenset[str] = frozenset(
    {
        "全国地产",
        "区域地产",
        "园区开发",
        "房产服务",
        "装修装饰",
        "建筑工程",
        "商品城",
    }
)

_TUSHARE_CYCLICAL: frozenset[str] = frozenset(
    {
        "黄金",
        "铝",
        "铜",
        "铅锌",
        "小金属",
        "普钢",
        "特种钢",
        "钢加工",
        "煤炭开采",
        "焦炭加工",
        "石油贸易",
        "石油开采",
        "石油加工",
        "化纤",
        "化工原料",
        "农药化肥",
        "塑料",
        "橡胶",
        "染料涂料",
        "水泥",
        "玻璃",
        "陶瓷",
        "矿物制品",
        "造纸",
        "其他建材",
        "船舶",
        "航空",
        "空运",
        "水运",
        "港口",
        "工程机械",
        "机床制造",
        "摩托车",
        "汽车整车",
        "汽车配件",
        "半导体",
        "纺织",
    }
)

_TUSHARE_GENERAL: frozenset[str] = frozenset(
    {
        "饲料",
        "食品",
        "铁路",
        "酒店餐饮",
        "通信设备",
        "运输设备",
        "轻工机械",
        "软饮料",
        "软件服务",
        "路桥",
        "超市连锁",
        "综合类",
        "纺织机械",
        "红黄酒",
        "种植业",
        "百货",
        "白酒",
        "电气设备",
        "电器连锁",
        "电器仪表",
        "电信运营",
        "生物制药",
        "环境保护",
        "火力发电",
        "渔业",
        "汽车服务",
        "水务",
        "水力发电",
        "林业",
        "机械基件",
        "机场",
        "服饰",
        "日用化工",
        "旅游服务",
        "旅游景点",
        "新型电力",
        "文教休闲",
        "批发业",
        "影视音像",
        "广告包装",
        "家用电器",
        "家居用品",
        "啤酒",
        "商贸代理",
        "医药商业",
        "医疗保健",
        "化工机械",
        "化学制药",
        "出版业",
        "农用机械",
        "农业综合",
        "其他商业",
        "公路",
        "公共交通",
        "元器件",
        "供气供热",
        "仓储物流",
        "互联网",
        "乳制品",
        "中成药",
        "专用机械",
        "IT设备",
    }
)

_DCF_BORDERLINE_INDUSTRY_LABELS: frozenset[str] = frozenset(
    {
        "纺织",
        "日用化工",
        "旅游服务",
        "酒店餐饮",
        "机床制造",
        "运输设备",
    }
)


def _build_tushare_industry_map() -> dict[str, DcfSectorKind]:
    m: dict[str, DcfSectorKind] = {}
    for x in _TUSHARE_FINANCIAL:
        m[x] = DcfSectorKind.FINANCIAL
    for x in _TUSHARE_REAL_ESTATE:
        m[x] = DcfSectorKind.REAL_ESTATE
    for x in _TUSHARE_CYCLICAL:
        m[x] = DcfSectorKind.CYCLICAL
    for x in _TUSHARE_GENERAL:
        m[x] = DcfSectorKind.GENERAL
    return m


_TUSHARE_INDUSTRY_TO_KIND: dict[str, DcfSectorKind] = _build_tushare_industry_map()


def is_dcf_borderline_industry(industry: str | None) -> bool:
    """边界行业：仅用于附加披露，不改变 `DcfSectorKind`。"""

    return (industry or "").strip() in _DCF_BORDERLINE_INDUSTRY_LABELS


def resolve_dcf_sector_kind_detailed(
    industry: str | None,
    *,
    ts_code: str | None = None,
) -> tuple[DcfSectorKind, bool]:
    """
    解析行业分档。

    :return: (分档, industry_explicit_map_hit)。第二项为 True 表示命中显式映射表键；
        空行业为 True（不视为未收录）；「地产」子串兼容为 False。
    """

    label = (industry or "").strip()
    if not label:
        return DcfSectorKind.GENERAL, True
    kind = _TUSHARE_INDUSTRY_TO_KIND.get(label)
    if kind is not None:
        return kind, True
    if "地产" in label:
        return DcfSectorKind.REAL_ESTATE, False
    if label not in _LOGGED_UNKNOWN_INDUSTRY:
        _LOGGED_UNKNOWN_INDUSTRY.add(label)
        extra: dict[str, str | None] = {"dcf_industry": label, "ts_code": ts_code}
        logger.info(
            "dcf industry not in explicit TuShare map; using GENERAL (ts_code=%s, industry=%s)",
            ts_code or "",
            label,
            extra=extra,
        )
    return DcfSectorKind.GENERAL, False


def resolve_dcf_sector_kind(industry: str | None, *, ts_code: str | None = None) -> DcfSectorKind:
    """将 TuShare `stock_basic.industry` 映射为 DCF 行业分档。"""

    return resolve_dcf_sector_kind_detailed(industry, ts_code=ts_code)[0]
