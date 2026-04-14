"""DCF 行业分档（领域策略）。"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import value_screener.domain.dcf_sector_policy as dcf_sector_policy
from value_screener.domain.dcf_sector_policy import (
    DcfSectorKind,
    _TUSHARE_INDUSTRY_TO_KIND,
    is_dcf_borderline_industry,
    resolve_dcf_sector_kind,
    resolve_dcf_sector_kind_detailed,
)


class DcfSectorPolicyTest(unittest.TestCase):
    def tearDown(self) -> None:
        dcf_sector_policy._LOGGED_UNKNOWN_INDUSTRY.clear()

    def test_explicit_financial(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("银行"), DcfSectorKind.FINANCIAL)
        self.assertEqual(resolve_dcf_sector_kind("  证券  "), DcfSectorKind.FINANCIAL)

    def test_explicit_real_estate(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("全国地产"), DcfSectorKind.REAL_ESTATE)

    def test_substring_real_estate_fallback(self) -> None:
        kind, hit = resolve_dcf_sector_kind_detailed("商业地产")
        self.assertEqual(kind, DcfSectorKind.REAL_ESTATE)
        self.assertFalse(hit)

    def test_explicit_cyclical(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("煤炭开采"), DcfSectorKind.CYCLICAL)
        self.assertEqual(resolve_dcf_sector_kind("半导体"), DcfSectorKind.CYCLICAL)

    def test_feed_general_not_cyclical(self) -> None:
        """全量表将饲料归为 GENERAL（与旧关键字规则不同）。"""
        self.assertEqual(resolve_dcf_sector_kind("饲料"), DcfSectorKind.GENERAL)

    def test_baijiu_general(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("白酒"), DcfSectorKind.GENERAL)

    def test_empty_and_none(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind(None), DcfSectorKind.GENERAL)
        self.assertEqual(resolve_dcf_sector_kind(""), DcfSectorKind.GENERAL)
        _k, hit = resolve_dcf_sector_kind_detailed(None)
        self.assertTrue(hit)

    def test_unknown_industry_logs_and_returns_general(self) -> None:
        with self.assertLogs("value_screener.domain.dcf_sector_policy", level="INFO") as cm:
            k = resolve_dcf_sector_kind("不存在的行业X", ts_code="000001.SZ")
        self.assertEqual(k, DcfSectorKind.GENERAL)
        self.assertTrue(any("不存在的行业X" in r for r in cm.output))

    def test_borderline_flag(self) -> None:
        self.assertTrue(is_dcf_borderline_industry("运输设备"))
        self.assertFalse(is_dcf_borderline_industry("白酒"))

    def test_tushare_map_size_and_unique_keys(self) -> None:
        self.assertEqual(len(_TUSHARE_INDUSTRY_TO_KIND), 111)
        self.assertEqual(len(_TUSHARE_INDUSTRY_TO_KIND), len(set(_TUSHARE_INDUSTRY_TO_KIND.keys())))

    def test_unknown_dedupes_log_per_process(self) -> None:
        with patch.object(dcf_sector_policy.logger, "info") as mock_info:
            resolve_dcf_sector_kind("全新行业Y")
            resolve_dcf_sector_kind("全新行业Y")
        self.assertEqual(mock_info.call_count, 1)


if __name__ == "__main__":
    unittest.main()
