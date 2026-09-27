import os
os.environ["AE_TESTING"] = "1"

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient

from ae_baccarat_workbench.app import BANGKOK_TIMEZONE, BaccaratWorkbenchApp
from ae_baccarat_workbench.config import AppConfig, load_config, save_config
from ae_baccarat_workbench.models import BetSide, StrategyAction, StrategySignal
from ae_baccarat_workbench.web.server import app, state


class RunLengthAutoBetTests(unittest.TestCase):
    def test_config_field_and_serialization(self) -> None:
        cfg = AppConfig(run_length_autobet_enabled=True)
        self.assertTrue(cfg.run_length_autobet_enabled)
        # Test defaults
        cfg_default = AppConfig()
        self.assertFalse(cfg_default.run_length_autobet_enabled)

    def test_app_auto_arm_dispatches_when_enabled(self) -> None:
        fixed_now = datetime(2026, 3, 30, 23, 15, tzinfo=BANGKOK_TIMEZONE)
        candidate = StrategySignal(
            table_name="Baccarat C09",
            strategy_id="run_length",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.65,
            reason="ML pass",
            round_fingerprint="C09-02-15",
            features={"ml_probability_win": 0.65},
            created_at="2026-03-30T16:14:59+00:00",
        )
        app_mock = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app_mock.config = SimpleNamespace(run_length_stake=25.0, live_table_stale_seconds=0)
        app_mock.store = SimpleNamespace(
            run_length_hourly_slot_keys=Mock(return_value=set()),
            save_run_length_hourly_bet=Mock(return_value=99),
            run_length_hourly_slot_used=Mock(return_value=False),
            pending_run_length_hourly_row=Mock(return_value=None),
        )
        app_mock._selected_run_length_windows = Mock(return_value=("23:00-24:00",))
        app_mock._run_length_candidates = Mock(return_value=[candidate])
        app_mock._run_length_pending = None
        app_mock._run_length_slot_cache_date = ""
        app_mock._run_length_consumed_slots = set()
        app_mock._run_length_history_dirty = False
        app_mock._dispatch_live_autobet = Mock()
        app_mock.run_length_autobet_var = SimpleNamespace(get=Mock(return_value=True))
        app_mock._run_length_autobet_armed_today = set()
        app_mock.engine = SimpleNamespace(snapshots={})

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app_mock._auto_arm_run_length_hourly()

        app_mock.store.save_run_length_hourly_bet.assert_called_once()
        app_mock._dispatch_live_autobet.assert_called_once()
        orders, kwargs = app_mock._dispatch_live_autobet.call_args
        self.assertEqual(kwargs.get("source"), "run_length")
        self.assertEqual(len(orders[0]), 1)
        self.assertEqual(orders[0][0].table_name, "Baccarat C09")
        self.assertEqual(orders[0][0].side, "P")
        self.assertEqual(orders[0][0].stake, 25.0)

    def test_app_auto_arm_does_not_dispatch_when_disabled(self) -> None:
        fixed_now = datetime(2026, 3, 30, 23, 15, tzinfo=BANGKOK_TIMEZONE)
        candidate = StrategySignal(
            table_name="Baccarat C09",
            strategy_id="run_length",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.65,
            reason="ML pass",
            round_fingerprint="C09-02-15",
            features={"ml_probability_win": 0.65},
            created_at="2026-03-30T16:14:59+00:00",
        )
        app_mock = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app_mock.config = SimpleNamespace(run_length_stake=25.0, live_table_stale_seconds=0)
        app_mock.store = SimpleNamespace(
            run_length_hourly_slot_keys=Mock(return_value=set()),
            save_run_length_hourly_bet=Mock(return_value=99),
            run_length_hourly_slot_used=Mock(return_value=False),
            pending_run_length_hourly_row=Mock(return_value=None),
        )
        app_mock._selected_run_length_windows = Mock(return_value=("23:00-24:00",))
        app_mock._run_length_candidates = Mock(return_value=[candidate])
        app_mock._run_length_pending = None
        app_mock._run_length_slot_cache_date = ""
        app_mock._run_length_consumed_slots = set()
        app_mock._run_length_history_dirty = False
        app_mock._dispatch_live_autobet = Mock()
        app_mock.run_length_autobet_var = SimpleNamespace(get=Mock(return_value=False))
        app_mock._run_length_autobet_armed_today = set()

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app_mock._auto_arm_run_length_hourly()

        app_mock.store.save_run_length_hourly_bet.assert_called_once()
        app_mock._dispatch_live_autobet.assert_not_called()

    def test_web_endpoints_run_length_autobet(self) -> None:
        client = TestClient(app)

        # GET /api/status returns run_length_autobet_enabled
        res = client.get("/api/status")
        self.assertEqual(res.status_code, 200)
        self.assertIn("run_length_autobet_enabled", res.json())

        # POST /api/run_length/config with autobet
        res = client.post("/api/run_length/config", json={"autobet": True, "stake": 50.0})
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data.get("success"))
        self.assertTrue(data.get("autobet_enabled"))
        self.assertEqual(data.get("stake"), 50.0)
        self.assertTrue(state.run_length_autobet_enabled)

        # GET /api/run_length returns autobet_enabled
        res = client.get("/api/run_length")
        self.assertEqual(res.status_code, 200)
        self.assertTrue(res.json().get("autobet_enabled"))

        # Test POST /api/run_length/trigger
        candidate = StrategySignal(
            table_name="Baccarat C09",
            strategy_id="run_length",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.62,
            reason="ML pass",
            round_fingerprint="C09-03-20",
            features={"ml_probability_win": 0.62},
        )
        with patch.object(state, "_run_length_candidates", return_value=[candidate]):
            with patch.object(state, "dispatch_autobet") as mock_dispatch:
                res_trig = client.post("/api/run_length/trigger")
                self.assertEqual(res_trig.status_code, 200)
                trig_data = res_trig.json()
                self.assertTrue(trig_data.get("success"))
                self.assertIn("Baccarat C09", trig_data.get("message", ""))
                mock_dispatch.assert_called_once()
                dispatched_orders = mock_dispatch.call_args[0][0]
                self.assertEqual(len(dispatched_orders), 1)
                self.assertEqual(dispatched_orders[0].table_name, "Baccarat C09")
                self.assertEqual(dispatched_orders[0].side, "B")

        # Disable autobet
        client.post("/api/run_length/config", json={"autobet": False})
        self.assertFalse(state.run_length_autobet_enabled)


if __name__ == "__main__":
    unittest.main()
