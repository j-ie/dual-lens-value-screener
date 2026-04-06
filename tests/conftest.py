"""pytest 全局夹具：避免 TuShare 批跑路径在单测里触发东财现货网络请求。"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session", autouse=True)
def _disable_em_dividend_supplement_for_tests() -> Generator[None, None, None]:
    """TuShare 拉数后会调东财补股息；单测 mock 了 daily_basic 仍会走补全，导致卡住。"""
    os.environ["VALUE_SCREENER_TUSHARE_SKIP_EM_DIVIDEND"] = "1"
    yield
