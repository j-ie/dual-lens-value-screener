"""DCF 行业分档（领域策略）。"""

from __future__ import annotations

import unittest

from value_screener.domain.dcf_sector_policy import DcfSectorKind, resolve_dcf_sector_kind


class DcfSectorPolicyTest(unittest.TestCase):
    def test_financial_labels(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("银行"), DcfSectorKind.FINANCIAL)
        self.assertEqual(resolve_dcf_sector_kind("  证券  "), DcfSectorKind.FINANCIAL)

    def test_real_estate(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("全国地产"), DcfSectorKind.REAL_ESTATE)
        self.assertEqual(resolve_dcf_sector_kind("商业地产"), DcfSectorKind.REAL_ESTATE)

    def test_general(self) -> None:
        self.assertEqual(resolve_dcf_sector_kind("白酒"), DcfSectorKind.GENERAL)
        self.assertEqual(resolve_dcf_sector_kind(None), DcfSectorKind.GENERAL)
        self.assertEqual(resolve_dcf_sector_kind(""), DcfSectorKind.GENERAL)


if __name__ == "__main__":
    unittest.main()
