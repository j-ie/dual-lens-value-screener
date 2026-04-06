"""公司详情 DCF 查询参数与响应形态（Mock 服务层）。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from value_screener.interfaces.main import app


def _minimal_detail_payload(*, with_dcf: bool) -> dict:
    out: dict = {
        "run_id": 1,
        "ts_code": "600519.SH",
        "run": {"id": 1, "status": "success", "created_at": "2026-01-01", "finished_at": None},
        "run_snapshot": {"symbol": "600519.SH"},
        "reference": None,
        "financials": {"income": [], "balance": [], "cashflow": []},
        "live_quote": {"ok": False, "fetched_at": "t", "error": None, "data": None},
    }
    if with_dcf:
        out["dcf"] = {
            "ok": True,
            "skip_reason": None,
            "message": None,
            "warnings": [],
            "notes": [],
            "assumptions": {"wacc": 0.09},
            "values": {"value_per_share": 10.5},
        }
    return out


class CompanyDetailDcfRouteTests(unittest.TestCase):
    def test_include_dcf_false_response_has_null_dcf(self) -> None:
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch(
                "value_screener.interfaces.runs.CompanyDetailQueryService.load",
                return_value=_minimal_detail_payload(with_dcf=False),
            ) as load_mock:
                client = TestClient(app)
                r = client.get("/api/v1/runs/1/companies/600519.SH/detail")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertIsNone(body.get("dcf"))
        kw = load_mock.call_args.kwargs
        self.assertFalse(kw.get("include_dcf", False))

    def test_include_dcf_true_returns_dcf_block(self) -> None:
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            with patch(
                "value_screener.interfaces.runs.CompanyDetailQueryService.load",
                return_value=_minimal_detail_payload(with_dcf=True),
            ) as load_mock:
                client = TestClient(app)
                r = client.get(
                    "/api/v1/runs/1/companies/600519.SH/detail",
                    params={"include_dcf": "1"},
                )
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertIsNotNone(body.get("dcf"))
        self.assertTrue(body["dcf"]["ok"])
        self.assertEqual(body["dcf"]["values"]["value_per_share"], 10.5)
        self.assertTrue(load_mock.call_args.kwargs.get("include_dcf"))

    def test_dcf_wacc_out_of_range_422(self) -> None:
        with patch("value_screener.interfaces.runs.get_engine", return_value=MagicMock()):
            client = TestClient(app)
            r = client.get(
                "/api/v1/runs/1/companies/600519.SH/detail",
                params={"include_dcf": "1", "dcf_wacc": "0.01"},
            )
        self.assertEqual(r.status_code, 422)


if __name__ == "__main__":
    unittest.main()
