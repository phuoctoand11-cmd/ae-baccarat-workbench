import os
os.environ["AE_TESTING"] = "1"

import math
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient

from ae_baccarat_workbench.app import (
    BANGKOK_TIMEZONE,
    BaccaratWorkbenchApp,
    _rank_ensemble_majority_candidates,
    _ensemble_majority_summary_label,
)
from ae_baccarat_workbench.config import AppConfig, load_config, save_config
from ae_baccarat_workbench.models import BetSide, Outcome, RoundEvent, StrategyAction, StrategySignal, TableSnapshot
from ae_baccarat_workbench.storage import WorkbenchStore
from ae_baccarat_workbench.web.server import app, state


class EnsembleMajorityAutoBetTests(unittest.TestCase):
    def test_config_field_and_serialization(self) -> None:
        cfg = AppConfig(
            ensemble_majority_autobet_enabled=True,
            ensemble_majority_stake=20.0,
            ensemble_majority_ml_min_probability=0.58,
            ensemble_majority_selected_windows=("12:00-13:00",),
        )
        self.assertTrue(cfg.ensemble_majority_autobet_enabled)
        self.assertEqual(cfg.ensemble_majority_stake, 20.0)
        self.assertEqual(cfg.ensemble_majority_ml_min_probability, 0.58)
        self.assertEqual(cfg.ensemble_majority_selected_windows, ("12:00-13:00",))

        # Test defaults
        cfg_default = AppConfig()
        self.assertFalse(cfg_default.ensemble_majority_autobet_enabled)
        self.assertEqual(cfg_default.ensemble_majority_stake, 10.0)
        self.assertEqual(cfg_default.ensemble_majority_ml_min_probability, 0.55)
        self.assertEqual(cfg_default.ensemble_majority_selected_windows, ())

    def test_rank_ensemble_majority_candidates(self) -> None:
        def make_snap(table_name: str) -> TableSnapshot:
            rnd = RoundEvent(
                table_name=table_name,
                outcome=Outcome.BANKER,
                shoe="shoe1",
                round_no=1,
            )
            return TableSnapshot(
                table_name=table_name,
                shoe="shoe1",
                rounds=(rnd,),
                last_seen="2026-09-18T10:00:05+00:00",
            )

        snap1 = make_snap("Baccarat C01")
        snap2 = make_snap("Baccarat C02")
        snap3 = make_snap("Baccarat C03")
        snap4 = make_snap("Baccarat C04")

        sig_valid = StrategySignal(
            table_name="Baccarat C01",
            strategy_id="ensemble_majority",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.66,
            reason="ensemble majority 2/3 agree",
            round_fingerprint=snap1.latest_fingerprint(),
            features={"ml_probability_win": 0.57, "strategy_confidence": 0.66},
            created_at="2026-09-18T10:00:00+00:00",
        )
        sig_low_ml = StrategySignal(
            table_name="Baccarat C02",
            strategy_id="ensemble_majority",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.66,
            reason="ensemble majority 2/3 agree",
            round_fingerprint=snap2.latest_fingerprint(),
            features={"ml_probability_win": 0.52, "strategy_confidence": 0.66},
            created_at="2026-09-18T10:00:00+00:00",
        )
        sig_wrong_strat = StrategySignal(
            table_name="Baccarat C03",
            strategy_id="run_length",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.66,
            reason="run length",
            round_fingerprint=snap3.latest_fingerprint(),
            features={"ml_probability_win": 0.60, "strategy_confidence": 0.66},
            created_at="2026-09-18T10:00:00+00:00",
        )
        sig_top = StrategySignal(
            table_name="Baccarat C04",
            strategy_id="ensemble_majority",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.66,
            reason="ensemble majority",
            round_fingerprint=snap4.latest_fingerprint(),
            features={"ml_probability_win": 0.62, "strategy_confidence": 0.66},
            created_at="2026-09-18T10:00:00+00:00",
        )

        snapshots = {
            "Baccarat C01": snap1,
            "Baccarat C02": snap2,
            "Baccarat C03": snap3,
            "Baccarat C04": snap4,
        }
        latest = {
            ("Baccarat C01", "ensemble_majority"): sig_valid,
            ("Baccarat C02", "ensemble_majority"): sig_low_ml,
            ("Baccarat C03", "run_length"): sig_wrong_strat,
            ("Baccarat C04", "ensemble_majority"): sig_top,
        }
        countdowns = {
            "Baccarat C01": 20.0,
            "Baccarat C02": 20.0,
            "Baccarat C03": 20.0,
            "Baccarat C04": 25.0,
        }

        # Default min_probability = 0.55
        res = _rank_ensemble_majority_candidates(
            latest,
            snapshots,
            countdowns,
            stale_seconds=0,
            min_probability=0.55,
        )
        self.assertEqual(len(res), 2)
        # C04 (62%) must rank above C01 (57%)
        self.assertEqual(res[0].table_name, "Baccarat C04")
        self.assertEqual(res[1].table_name, "Baccarat C01")

        # Threshold = 0.58: only C04 (62%) passes
        res_58 = _rank_ensemble_majority_candidates(
            latest,
            snapshots,
            countdowns,
            stale_seconds=0,
            min_probability=0.58,
        )
        self.assertEqual(len(res_58), 1)
        self.assertEqual(res_58[0].table_name, "Baccarat C04")

    def test_app_auto_arm_dispatches_when_enabled(self) -> None:
        fixed_now = datetime(2026, 9, 18, 14, 15, tzinfo=BANGKOK_TIMEZONE)
        candidate = StrategySignal(
            table_name="Baccarat C09",
            strategy_id="ensemble_majority",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.65,
            reason="ML pass",
            round_fingerprint="C09-02-15",
            features={"ml_probability_win": 0.65},
            created_at="2026-09-18T07:14:59+00:00",
        )
        app_mock = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app_mock.config = SimpleNamespace(
            ensemble_majority_stake=25.0,
            ensemble_majority_ml_min_probability=0.55,
            live_table_stale_seconds=0,
            money=SimpleNamespace(banker_commission=0.05),
        )
        app_mock.store = SimpleNamespace(
            ensemble_majority_hourly_slot_keys=Mock(return_value=set()),
            save_ensemble_majority_hourly_bet=Mock(return_value=101),
            ensemble_majority_hourly_slot_used=Mock(return_value=False),
            pending_ensemble_majority_hourly_row=Mock(return_value=None),
            settle_stale_ensemble_majority_hourly_bets=Mock(return_value=[]),
        )
        app_mock._selected_ensemble_majority_windows = Mock(return_value=("14:00-15:00",))
        app_mock._ensemble_majority_candidates = Mock(return_value=[candidate])
        app_mock._ensemble_majority_pending = None
        app_mock._ensemble_majority_slot_cache_date = ""
        app_mock._ensemble_majority_consumed_slots = set()
        app_mock._ensemble_majority_history_dirty = False
        app_mock._dispatch_live_autobet = Mock()
        app_mock.ensemble_majority_autobet_var = SimpleNamespace(get=Mock(return_value=True))
        app_mock.ensemble_majority_stake_var = SimpleNamespace(get=Mock(return_value="25.0"))
        app_mock._ensemble_majority_autobet_armed_today = set()
        app_mock.engine = SimpleNamespace(snapshots={})
        app_mock._daily_remaining_seconds = Mock(return_value={"Baccarat C09": 18.0})

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app_mock._auto_arm_ensemble_majority_hourly()

        app_mock.store.save_ensemble_majority_hourly_bet.assert_called_once()
        app_mock._dispatch_live_autobet.assert_called_once()
        orders, kwargs = app_mock._dispatch_live_autobet.call_args
        self.assertEqual(kwargs.get("source"), "ensemble_majority")
        self.assertEqual(len(orders[0]), 1)
        self.assertEqual(orders[0][0].table_name, "Baccarat C09")
        self.assertEqual(orders[0][0].side, "P")
        self.assertEqual(orders[0][0].stake, 25.0)

    def test_storage_ensemble_majority_hourly_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = Path(tmp_dir) / "test.sqlite"
            duckdb_path = Path(tmp_dir) / "test.duckdb"
            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=False)
            try:
                # 1. Save bet
                bet_id = store.save_ensemble_majority_hourly_bet(
                    session_date="2026-09-18",
                    session_window="14:00-15:00",
                    created_at="2026-09-18T07:15:00.000Z",
                    table_name="Baccarat C05",
                    side="B",
                    stake=20.0,
                    signal_fingerprint="C05|s1|10|h",
                    confidence=0.58,
                )
                self.assertIsNotNone(bet_id)
                self.assertTrue(store.ensemble_majority_hourly_slot_used("2026-09-18", "14:00-15:00"))

                # 2. Check pending
                pending = store.pending_ensemble_majority_hourly_row()
                self.assertIsNotNone(pending)
                self.assertEqual(pending["table_name"], "Baccarat C05")

                # 3. Settle bet
                settled = store.settle_ensemble_majority_hourly_bet(
                    bet_id=bet_id,
                    settled_at="2026-09-18T07:16:00.000Z",
                    outcome="B",
                    result="W",
                    pnl=19.0,
                )
                self.assertTrue(settled)
                self.assertIsNone(store.pending_ensemble_majority_hourly_row())

                # 4. Check summary
                summary = store.ensemble_majority_hourly_summary("2026-09-18", "14:00-15:00")
                self.assertEqual(summary["win_count"], 1)
                self.assertEqual(summary["loss_count"], 0)
                self.assertAlmostEqual(summary["total_pnl"], 19.0)

                # 5. Label formatting
                lbl = _ensemble_majority_summary_label(summary, 1)
                self.assertIn("W 1 - L 0", lbl)
                self.assertIn("P&L +19.00", lbl)
            finally:
                store.close()

    def test_web_endpoints_ensemble_majority(self) -> None:
        client = TestClient(app)

        # GET /api/status returns ensemble_majority fields
        res = client.get("/api/status")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("ensemble_majority_autobet_enabled", data)
        self.assertIn("ensemble_majority_ml_min_probability", data)
        self.assertIn("ensemble_majority_stake", data)

        # POST /api/ensemble_majority/config
        res = client.post(
            "/api/ensemble_majority/config",
            json={"autobet": True, "stake": 30.0, "min_probability": 0.58},
        )
        self.assertEqual(res.status_code, 200)
        cfg_data = res.json()
        self.assertTrue(cfg_data.get("success"))
        self.assertTrue(cfg_data.get("autobet_enabled"))
        self.assertEqual(cfg_data.get("stake"), 30.0)
        self.assertEqual(cfg_data.get("min_probability"), 0.58)
        self.assertTrue(state.ensemble_majority_autobet_enabled)
        self.assertEqual(state.ensemble_majority_ml_min_probability, 0.58)

        # GET /api/ensemble_majority
        res_em = client.get("/api/ensemble_majority")
        self.assertEqual(res_em.status_code, 200)
        em_data = res_em.json()
        self.assertTrue(em_data.get("autobet_enabled"))
        self.assertEqual(em_data.get("min_probability"), 0.58)
        self.assertEqual(em_data.get("stake"), 30.0)

        # POST /api/ensemble_majority/trigger
        candidate = StrategySignal(
            table_name="Baccarat C09",
            strategy_id="ensemble_majority",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.62,
            reason="ML pass",
            round_fingerprint="C09-03-20",
            features={"ml_probability_win": 0.62},
        )
        with patch.object(state, "_ensemble_majority_candidates", return_value=[candidate]):
            with patch.object(state, "dispatch_autobet") as mock_dispatch:
                res_trig = client.post("/api/ensemble_majority/trigger")
                self.assertEqual(res_trig.status_code, 200)
                trig_data = res_trig.json()
                self.assertTrue(trig_data.get("success"))
                self.assertIn("Baccarat C09", trig_data.get("message", ""))
                mock_dispatch.assert_called_once()
                dispatched_orders = mock_dispatch.call_args[0][0]
                self.assertEqual(len(dispatched_orders), 1)
                self.assertEqual(dispatched_orders[0].table_name, "Baccarat C09")
                self.assertEqual(mock_dispatch.call_args[1].get("source"), "ensemble_majority")


if __name__ == "__main__":
    unittest.main()
