import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ae_baccarat_workbench.app import _parse_cdp_port
from ae_baccarat_workbench.config import AppConfig, load_config, save_config
from ae_baccarat_workbench.monitor.browser_launcher import (
    DEFAULT_CDP_PORT,
    find_chrome_executable,
    get_default_user_data_dir,
    is_cdp_port_open,
)
from ae_baccarat_workbench.monitor.browser_navigator import (
    AE_SEXY_PATTERNS,
    CASINO_MENU_PATTERNS,
    build_lobby_patterns,
)


class BrowserAutomationTests(unittest.TestCase):
    def test_parse_cdp_port_extracts_correct_port(self) -> None:
        self.assertEqual(_parse_cdp_port("http://localhost:9222"), 9222)
        self.assertEqual(_parse_cdp_port("http://127.0.0.1:9333"), 9333)
        self.assertEqual(_parse_cdp_port("http://localhost"), DEFAULT_CDP_PORT)
        self.assertEqual(_parse_cdp_port("invalid-url"), DEFAULT_CDP_PORT)

    def test_casino_menu_patterns_match_vietnamese_and_english(self) -> None:
        samples = [
            "Live Casino",
            "LIVE CASINO",
            "Casino Trực Tuyến",
            "CASINO TRỰC TUYẾN",
            "Sòng bài",
            "SÒNG BÀI TRỰC TUYẾN",
            "Casino",
        ]
        for sample in samples:
            matched = any(p.search(sample) for p in CASINO_MENU_PATTERNS)
            self.assertTrue(matched, f"Expected match for: {sample}")

    def test_ae_sexy_patterns_match_variants(self) -> None:
        samples = [
            "AE Sexy",
            "AE SEXY",
            "AE Sexy Baccarat",
            "Sexy Casino",
            "Sexy Gaming",
            "SEXY GAMING",
            "AE Casino",
            "SEXY",
        ]
        for sample in samples:
            matched = any(p.search(sample) for p in AE_SEXY_PATTERNS)
            self.assertTrue(matched, f"Expected match for: {sample}")

    def test_is_cdp_port_open_returns_true_when_endpoint_responds(self) -> None:
        fake_response = MagicMock()
        fake_response.status = 200
        fake_response.read.return_value = json.dumps({"Browser": "Chrome/120.0"}).encode("utf-8")
        fake_response.__enter__.return_value = fake_response

        with patch("urllib.request.urlopen", return_value=fake_response):
            self.assertTrue(is_cdp_port_open(9222))

    def test_is_cdp_port_open_returns_false_on_connection_error(self) -> None:
        with patch("urllib.request.urlopen", side_effect=OSError("Connection refused")):
            self.assertFalse(is_cdp_port_open(9222))

    def test_find_chrome_executable_custom_path_verified(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".exe", delete=False) as f:
            temp_exe = Path(f.name)
        try:
            found = find_chrome_executable(str(temp_exe))
            self.assertEqual(found, temp_exe.resolve())
        finally:
            temp_exe.unlink(missing_ok=True)

    def test_get_default_user_data_dir_creates_and_returns_path(self) -> None:
        dir_path = get_default_user_data_dir()
        self.assertTrue(dir_path.exists())
        self.assertTrue(dir_path.is_dir())

    def test_config_saves_and_loads_browser_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg_path = Path(tmp_dir) / "config.test.json"
            cfg = AppConfig(
                target_url="https://casino-example.com",
                account_id="player123",
                account_password="secretpassword",
                remember_credentials=True,
                ae_lobby_name="Sexy Casino",
            )
            save_config(cfg, cfg_path)
            loaded = load_config(cfg_path)
            self.assertEqual(loaded.target_url, "https://casino-example.com")
            self.assertEqual(loaded.account_id, "player123")
            self.assertEqual(loaded.account_password, "secretpassword")
            self.assertEqual(loaded.ae_lobby_name, "Sexy Casino")
            self.assertTrue(loaded.remember_credentials)

    def test_config_does_not_save_password_when_remember_is_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg_path = Path(tmp_dir) / "config.test.json"
            cfg = AppConfig(
                target_url="https://casino-example.com",
                account_id="player123",
                account_password="secretpassword",
                remember_credentials=False,
            )
            save_config(cfg, cfg_path)
            loaded = load_config(cfg_path)
            self.assertEqual(loaded.account_password, "")
            self.assertFalse(loaded.remember_credentials)

    def test_config_saves_and_loads_selected_daily_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg_path = Path(tmp_dir) / "config.test.json"
            selected = ("06:00-07:00", "22:00-23:00")
            save_config(AppConfig(daily_selected_windows=selected), cfg_path)

            loaded = load_config(cfg_path)

            self.assertEqual(loaded.daily_selected_windows, selected)

    def test_config_saves_and_loads_run_length_tab_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg_path = Path(tmp_dir) / "config.test.json"
            selected = ("00:00-01:00", "23:00-24:00")
            save_config(
                AppConfig(
                    run_length_selected_windows=selected,
                    run_length_stake=25.0,
                ),
                cfg_path,
            )

            loaded = load_config(cfg_path)

            self.assertEqual(loaded.run_length_selected_windows, selected)
            self.assertEqual(loaded.run_length_stake, 25.0)

    def test_build_lobby_patterns_handles_custom_and_fallback_names(self) -> None:
        patterns = build_lobby_patterns("AE Sexy, Sexy Casino")
        self.assertTrue(any(p.search("AE Sexy") for p in patterns))
        self.assertTrue(any(p.search("sexy casino") for p in patterns))
        self.assertTrue(any(p.search("SEXY CASINO") for p in patterns))
        self.assertTrue(any(p.search("ae-sexy") for p in patterns))

    def test_build_lobby_patterns_fallback_defaults_when_empty(self) -> None:
        patterns = build_lobby_patterns("")
        self.assertTrue(any(p.search("AE Sexy") for p in patterns))
        self.assertTrue(any(p.search("Sexy Casino") for p in patterns))


if __name__ == "__main__":
    unittest.main()
