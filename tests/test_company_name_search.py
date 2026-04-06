"""公司名称检索辅助函数单测。"""

from __future__ import annotations

import unittest

from value_screener.infrastructure.company_name_search import (
    escape_sql_like_fragment,
    normalized_company_search_term,
)


class CompanyNameSearchTests(unittest.TestCase):
    def test_normalized_empty(self) -> None:
        self.assertIsNone(normalized_company_search_term(None))
        self.assertIsNone(normalized_company_search_term(""))
        self.assertIsNone(normalized_company_search_term("  \t "))

    def test_normalized_trim(self) -> None:
        self.assertEqual(normalized_company_search_term(" 茅台 "), "茅台")

    def test_escape_like_metacharacters(self) -> None:
        self.assertEqual(escape_sql_like_fragment("a%b_c\\d"), "a\\%b\\_c\\\\d")


if __name__ == "__main__":
    unittest.main()
