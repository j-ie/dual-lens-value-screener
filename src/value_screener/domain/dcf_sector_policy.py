"""
DCF 行业分档：净债务与基期现金流的口径调节（领域策略）。

非「是否允许算」，而是标识应采用哪类财务报表代理，避免金融业负债合计含存款、
地产股负债含大额合同负债等与工业 FCFF 假设严重偏离的情况。
"""

from __future__ import annotations

from enum import Enum


class DcfSectorKind(str, Enum):
    """简化 DCF 的行业分档。"""

    GENERAL = "general"
    """一般工商业：负债合计 − 货币资金作净债务粗代理。"""

    FINANCIAL = "financial"
    """银行、保险、证券等：有息负债明细求和 − 货币资金；基期现金流优先用归母净利润年报。"""

    REAL_ESTATE = "real_estate"
    """房地产开发等：负债合计 − 合同负债（若可得）− 货币资金，削弱预收房款对杠杆的高估。"""

    CYCLICAL = "cyclical"
    """强周期行业：景气波动大，估值阈值需采用周期口径，避免单年利润错判。"""


_FINANCIAL_INDUSTRY_LABELS: frozenset[str] = frozenset(
    {
        "银行",
        "保险",
        "证券",
        "多元金融",
        "信托",
    }
)

_REAL_ESTATE_INDUSTRY_LABELS: frozenset[str] = frozenset(
    {
        "全国地产",
        "区域地产",
        "房产服务",
    }
)

_CYCLICAL_INDUSTRY_LABELS: frozenset[str] = frozenset(
    {
        "航运",
        "港口",
        "航空",
        "机场",
        "煤炭",
        "钢铁",
        "有色金属",
        "石油开采",
        "石油加工",
        "化工原料",
        "化肥行业",
        "水泥建材",
        "工程机械",
        "船舶制造",
        "汽车整车",
        "养殖业",
        "饲料",
    }
)

_CYCLICAL_KEYWORDS: tuple[str, ...] = (
    "生猪",
    "猪",
    "航运",
    "航海",
    "油运",
    "干散货",
    "煤炭",
    "钢铁",
    "有色",
    "化工",
    "周期",
)


def resolve_dcf_sector_kind(industry: str | None) -> DcfSectorKind:
    """
    将 TuShare `stock_basic.industry`（与 enrichment 输出一致）映射为 DCF 行业分档。

    未命中集合时对「地产」类名称做子串匹配，以覆盖「商业地产」等变体。
    """

    label = (industry or "").strip()
    if label in _FINANCIAL_INDUSTRY_LABELS:
        return DcfSectorKind.FINANCIAL
    if label in _REAL_ESTATE_INDUSTRY_LABELS or "地产" in label:
        return DcfSectorKind.REAL_ESTATE
    if label in _CYCLICAL_INDUSTRY_LABELS:
        return DcfSectorKind.CYCLICAL
    if any(k in label for k in _CYCLICAL_KEYWORDS):
        return DcfSectorKind.CYCLICAL
    return DcfSectorKind.GENERAL
