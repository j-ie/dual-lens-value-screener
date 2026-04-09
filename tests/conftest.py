"""pytest 全局夹具：单测中强制不拉东财补股息（与生产默认一致，且防止误设 USE_EM 时走网络）。"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session", autouse=True)
def _disable_em_dividend_supplement_for_tests() -> Generator[None, None, None]:
    """TuShare 拉数后会调东财补股息；单测 mock 了 daily_basic 仍会走补全，导致卡住。"""
    os.environ["VALUE_SCREENER_TUSHARE_SKIP_EM_DIVIDEND"] = "1"
    yield
