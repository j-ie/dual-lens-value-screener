"""AI 分析落库时 DCF 快照与列表摘要（纯函数，无 I/O）。"""

from __future__ import annotations

import json
from typing import Any


def dcf_snapshot_for_persistence(dcf: Any) -> tuple[dict[str, Any] | None, bool | None, str | None]:
    """
    从详情 API 同形的 dcf 块得到 (dcf_json, dcf_ok, dcf_headline)。

    dcf_json 为可 JSON 序列化的 dict；无法识别时返回 (None, None, None)。
    """

    if dcf is None:
        return None, None, None
    if not isinstance(dcf, dict):
        return None, None, None
    try:
        json.dumps(dcf, ensure_ascii=False)
    except (TypeError, ValueError):
        return None, None, None

    ok_raw = dcf.get("ok")
    if ok_raw is True:
        vals = dcf.get("values")
        vps = None
        if isinstance(vals, dict):
            vps = vals.get("value_per_share")
        if vps is not None:
            headline = f"每股内在价值（估算）约 {vps}"
        else:
            headline = "DCF 计算完成"
        return dcf, True, _truncate_headline(headline)

    if ok_raw is False:
        sr = dcf.get("skip_reason")
        sr_s = str(sr).strip() if sr is not None else ""
        msg = dcf.get("message")
        msg_s = str(msg).strip() if msg is not None else ""
        if sr_s and msg_s:
            headline = f"{sr_s}：{msg_s}"
        elif sr_s:
            headline = sr_s
        elif msg_s:
            headline = msg_s
        else:
            headline = "DCF 未完成"
        return dcf, False, _truncate_headline(headline)

    return dcf, None, None


def _truncate_headline(s: str, max_len: int = 500) -> str:
    t = s.strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 3] + "..."


__all__ = ["dcf_snapshot_for_persistence"]
