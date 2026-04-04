from __future__ import annotations

import unittest

from value_screener.infrastructure.result_cache import industries_cache_fingerprint


class IndustriesFingerprintTests(unittest.TestCase):
    def test_empty_none(self) -> None:
        self.assertEqual(industries_cache_fingerprint([]), "")
        self.assertEqual(industries_cache_fingerprint(None), "")

    def test_order_invariant(self) -> None:
        a = industries_cache_fingerprint(["银行", "医药生物"])
        b = industries_cache_fingerprint(["医药生物", "银行"])
        self.assertEqual(a, b)
        self.assertTrue(a.startswith("ind:"))
