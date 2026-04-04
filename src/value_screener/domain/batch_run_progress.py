"""批跑进度在 screening_run.meta_json 中的键约定（与 API / 前端对齐）。"""

from __future__ import annotations

from typing import Any

PROGRESS_META_KEYS: tuple[str, ...] = (
    "progress_percent",
    "progress_current",
    "progress_total",
    "progress_phase",
    "progress_symbol",
)


def strip_progress_keys(meta: dict[str, Any] | None) -> dict[str, Any]:
    """完成或失败时从 meta 中移除进度字段。"""

    if not meta:
        return {}
    out = dict(meta)
    for k in PROGRESS_META_KEYS:
        out.pop(k, None)
    return out
