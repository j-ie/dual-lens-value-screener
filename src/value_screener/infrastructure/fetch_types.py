from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SymbolFetchFailure:
    """单标的拉数失败记录（批处理不中断）。"""

    symbol: str
    reason: str
    source: str
