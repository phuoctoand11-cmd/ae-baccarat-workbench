import os
os.environ["AE_TESTING"] = "1"

import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from ae_baccarat_workbench.web.server import app, state, _extract_port_from_cdp_url


class WebServerApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_extract_port_from_cdp_url(self) -> None:
        self.assertEqual(_extract_port_from_cdp_url("http://localhost:9222"), 9222)
        self.assertEqual(_extract_port_from_cdp_url("http://127.0.0.1:9333"), 9333)
        self.assertEqual(_extract_port_from_cdp_url("http://127.0.0.1"), 9222)
        self.assertEqual(_extract_port_from_cdp_url("invalid-url"), 9222)

    def test_index_page(self) -> None:
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn("Run Length &ge; 58%", res.text)
        self.assertIn("Paper Theo Khung Giờ", res.text)

    def test_api_status_contains_expected_fields(self) -> None:
        res = self.client.get("/api/status")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("cdp_connected", data)
        self.assertIn("cdp_status_text", data)
        self.assertIn("target_url", data)
        self.assertIn("account_id", data)
        self.assertIn("ae_lobby_name", data)
        self.assertIn("daily_autobet_enabled", data)
        self.assertIn("today_pnl", data)
        self.assertIn("run_length_stake", data)
        self.assertIn("run_length_selected_windows", data)

    def test_api_autobet_audit_has_summary_attempts_and_events(self) -> None:
        res = self.client.get("/api/autobet/audit?limit=5")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("summary_24h", data)
        self.assertIn("attempts", data)
        self.assertIn("events", data)
        self.assertLessEqual(len(data["attempts"]), 5)

    def test_api_run_length_endpoints(self) -> None:
        res = self.client.get("/api/run_length")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("selected_windows", data)
        self.assertIn("stake", data)
        self.assertIn("today_rows", data)
        self.assertIn("summary", data)

        # Test config update
        payload = {"stake": 25.0, "windows": ["09:00-10:00", "14:00-15:00"]}
        res_cfg = self.client.post("/api/run_length/config", json=payload)
        self.assertEqual(res_cfg.status_code, 200)
        cfg_data = res_cfg.json()
        self.assertTrue(cfg_data.get("success"))
        self.assertEqual(cfg_data.get("stake"), 25.0)

        # Test history endpoint
        res_hist = self.client.get("/api/run_length/history")
        self.assertEqual(res_hist.status_code, 200)
        hist_data = res_hist.json()
        self.assertIn("rows", hist_data)
        self.assertIn("summary", hist_data)

    @patch("ae_baccarat_workbench.web.server.launch_chrome_cdp")
    def test_api_browser_launch(self, mock_launch: MagicMock) -> None:
        mock_launch.return_value = (True, "Khởi động Chrome thành công")
        res = self.client.post("/api/browser/launch")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data.get("success"))

    @patch("ae_baccarat_workbench.web.server.is_cdp_port_open")
    @patch("ae_baccarat_workbench.web.server.launch_chrome_cdp")
    @patch("ae_baccarat_workbench.web.server.run_browser_automation")
    def test_api_browser_login(
        self,
        mock_auto: MagicMock,
        mock_launch: MagicMock,
        mock_port_open: MagicMock,
    ) -> None:
        mock_port_open.return_value = True
        mock_auto.return_value = (True, "Vào sảnh thành công")

        payload = {
            "url": "https://www.vietdfvn.com/vn/live-dealer#redirect",
            "account_id": "testuser",
            "account_password": "testpassword",
            "lobby_name": "AE Sexy",
        }
        res = self.client.post("/api/browser/login", json=payload)
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data.get("success"))
        self.assertIn("Đang mở trình duyệt", data.get("message", ""))

    def tearDown(self) -> None:
        state.stop_monitor()

    @patch("ae_baccarat_workbench.web.server.state.start_monitor")
    @patch("ae_baccarat_workbench.web.server.state.stop_monitor")
    def test_api_cdp_reconnect(self, mock_stop: MagicMock, mock_start: MagicMock) -> None:
        res = self.client.post("/api/cdp/reconnect")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data.get("success"))
        mock_stop.assert_called_once()
        mock_start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
