"""company_ai_dcf_snapshot 领域纯函数。"""

import unittest

from value_screener.domain.company_ai_dcf_snapshot import dcf_snapshot_for_persistence


class DcfSnapshotForPersistenceTest(unittest.TestCase):
    def test_none_returns_empty(self) -> None:
        j, ok, hl = dcf_snapshot_for_persistence(None)
        self.assertIsNone(j)
        self.assertIsNone(ok)
        self.assertIsNone(hl)

    def test_ok_true_with_value_per_share(self) -> None:
        dcf = {"ok": True, "values": {"value_per_share": 12.3456}}
        j, ok, hl = dcf_snapshot_for_persistence(dcf)
        self.assertIsNotNone(j)
        self.assertTrue(ok)
        self.assertIn("12.3456", hl or "")

    def test_ok_false_skip_reason(self) -> None:
        dcf = {"ok": False, "skip_reason": "disabled", "message": "DCF 未启用"}
        j, ok, hl = dcf_snapshot_for_persistence(dcf)
        self.assertIsNotNone(j)
        self.assertFalse(ok)
        self.assertIn("disabled", hl or "")


if __name__ == "__main__":
    unittest.main()
