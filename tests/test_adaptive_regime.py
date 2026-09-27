import os
os.environ["AE_TESTING"] = "1"

import math
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from ae_baccarat_workbench.app import (
    ADAPTIVE_REGIME_BANKER_MIN_ML,
    ADAPTIVE_REGIME_PLAYER_MIN_ML,
    ADAPTIVE_REGIME_STRATEGY_ID,
    BANGKOK_TIMEZONE,
    BaccaratWorkbenchApp,
    _adaptive_regime_summary_label,
    _rank_adaptive_regime_candidates,
)
from ae_baccarat_workbench.config import AppConfig
from ae_baccarat_workbench.ml_live import MlSignalFilter
from ae_baccarat_workbench.models import (
    BetSide,
    Outcome,
    RoundEvent,
    StrategyAction,
    StrategySignal,
    TableSnapshot,
)
from ae_baccarat_workbench.storage import WorkbenchStore
from ae_baccarat_workbench.strategies import (
    AdaptiveRegimeStrategy,
    StrategyContext,
    calculate_chop_rate,
    detect_anti_banker_3,
    detect_double_pair_pattern,
    detect_late_run_pattern,
    detect_ping_pong_pattern,
)
from ae_baccarat_workbench.web.server import app, state


class AdaptiveRegimeTests(unittest.TestCase):
    def test_calculate_chop_rate(self) -> None:
        # Chuỗi xen kẽ hoàn toàn: B, P, B, P, B -> 4 chuyển đổi trên 4 bước = 1.0 (100%)
        ping_pong = [Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER]
        self.assertAlmostEqual(calculate_chop_rate(ping_pong), 1.0)

        # Chuỗi bệt dài hoàn toàn: B, B, B, B, B -> 0 chuyển đổi = 0.0 (0%)
        long_streak = [Outcome.BANKER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]
        self.assertAlmostEqual(calculate_chop_rate(long_streak), 0.0)

        # Chuỗi ngắn (< 6 phần tử): kiểm tra đúng tỷ lệ
        mixed = [Outcome.BANKER, Outcome.PLAYER, Outcome.PLAYER, Outcome.BANKER]
        # Chuyển đổi: B->P (1), P->P (0), P->B (1) -> 2/3 = 0.6667
        self.assertAlmostEqual(calculate_chop_rate(mixed), 2 / 3, places=3)

    def test_detect_ping_pong_pattern(self) -> None:
        # B-P-B-P-B -> xen kẽ 4 lần, cây cuối là B -> kỳ vọng P
        bp = [Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER]
        self.assertEqual(detect_ping_pong_pattern(bp), BetSide.PLAYER)

        # P-B-P-B-P -> xen kẽ 4 lần, cây cuối là P -> kỳ vọng B
        pb = [Outcome.PLAYER, Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER, Outcome.PLAYER]
        self.assertEqual(detect_ping_pong_pattern(pb), BetSide.BANKER)

        # Không xen kẽ đủ (chỉ 2 cây):
        short = [Outcome.BANKER, Outcome.PLAYER]
        self.assertIsNone(detect_ping_pong_pattern(short))

        # Có cây trùng: B, P, B, B, P -> không phải ping pong thuần
        broken = [Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER, Outcome.BANKER, Outcome.PLAYER]
        self.assertIsNone(detect_ping_pong_pattern(broken))

    def test_detect_double_pair_pattern(self) -> None:
        # B-B-P-P-B (cặp B, cặp P, và 1 cây B mới xuất hiện) -> kỳ vọng B (để hoàn thiện cặp B thứ 2)
        bb_pp_b = [Outcome.BANKER, Outcome.BANKER, Outcome.PLAYER, Outcome.PLAYER, Outcome.BANKER]
        self.assertEqual(detect_double_pair_pattern(bb_pp_b), BetSide.BANKER)

        # B-B-P-P-B-B (đã đủ cặp B-B, P-P, B-B) -> kỳ vọng P (bắt đầu cặp P mới)
        bb_pp_bb = [
            Outcome.BANKER,
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.PLAYER,
            Outcome.BANKER,
            Outcome.BANKER,
        ]
        self.assertEqual(detect_double_pair_pattern(bb_pp_bb), BetSide.PLAYER)

    def test_detect_anti_banker_3(self) -> None:
        # B-B-B ở cuối sau P -> đúng 3 cây Cái -> kỳ vọng bẻ sang PLAYER ở cây 4
        p_bbb = [Outcome.PLAYER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]
        self.assertEqual(detect_anti_banker_3(p_bbb), BetSide.PLAYER)

        # P-P-P (3 cây Con) -> không kích hoạt anti-banker-3
        b_ppp = [Outcome.BANKER, Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER]
        self.assertIsNone(detect_anti_banker_3(b_ppp))

        # B-B-B-B (4 cây Cái) -> đã qua cây 4, không kích hoạt
        p_bbbb = [Outcome.PLAYER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]
        self.assertIsNone(detect_anti_banker_3(p_bbbb))

    def test_detect_late_run_pattern(self) -> None:
        # B-B-B-B (run = 4 cây Cái) -> bám tiếp Cái
        bbbb = [Outcome.BANKER, Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]
        side, run = detect_late_run_pattern(bbbb)
        self.assertEqual(side, BetSide.BANKER)
        self.assertEqual(run, 4)

        # P-P-P-P-P (run = 5 cây Con) -> bám tiếp Con
        ppppp = [Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER]
        side, run = detect_late_run_pattern(ppppp)
        self.assertEqual(side, BetSide.PLAYER)
        self.assertEqual(run, 5)

        # Run < 4 (chỉ 3 cây) -> None
        bbb = [Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]
        side, run = detect_late_run_pattern(bbb)
        self.assertIsNone(side)

    def test_adaptive_regime_strategy_evaluation(self) -> None:
        strat = AdaptiveRegimeStrategy()

        # Tạo context mẫu với snapshot có chuỗi ping pong (chop_rate cao)
        rounds = []
        for i in range(15):
            outcome = Outcome.BANKER if i % 2 == 0 else Outcome.PLAYER
            rounds.append(RoundEvent(table_name="Baccarat C01", outcome=outcome, shoe="s1", round_no=i + 1))
        snap_chop = TableSnapshot(table_name="Baccarat C01", shoe="s1", rounds=tuple(rounds))

        ctx = StrategyContext(table=snap_chop)
        sig = strat.evaluate(ctx)
        self.assertEqual(sig.action, StrategyAction.BET)
        self.assertEqual(sig.strategy_id, ADAPTIVE_REGIME_STRATEGY_ID)
        self.assertEqual(sig.features.get("regime_mode"), "chop")

    def test_dual_threshold_ml_filter(self) -> None:
        # Mock filter ML
        filter_ml = MlSignalFilter.__new__(MlSignalFilter)
        filter_ml.enabled = True
        filter_ml.ready = True
        filter_ml.threshold = 0.55
        filter_ml.error = None
        filter_ml.model_path = "xgboost.joblib"
        filter_ml._feature_cache = {}

        mock_snap = TableSnapshot(
            table_name="C01",
            shoe="s1",
            rounds=(RoundEvent(table_name="C01", outcome=Outcome.BANKER, shoe="s1", round_no=1),),
        )
        mock_store = SimpleNamespace()

        # Test Banker:
        sig_banker_pass = StrategySignal(
            table_name="C01",
            strategy_id="adaptive_regime",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.55,
            reason="test",
            round_fingerprint=mock_snap.latest_fingerprint(),
            created_at="2026-09-18T10:00:00Z",
        )
        # Xác suất 0.575 >= 0.570 -> PASS
        filter_ml._predict_probability = Mock(return_value=0.575)
        with patch("ae_baccarat_workbench.ml_live.build_live_feature_row", return_value={}):
            filtered_b_pass = filter_ml.apply(sig_banker_pass, mock_snap, mock_store, stake=1.0)
        self.assertTrue(filtered_b_pass.is_actionable)
        self.assertEqual(filtered_b_pass.action, StrategyAction.BET)

        # Xác suất 0.565 < 0.570 -> SKIP
        filter_ml._predict_probability = Mock(return_value=0.565)
        with patch("ae_baccarat_workbench.ml_live.build_live_feature_row", return_value={}):
            filtered_b_skip = filter_ml.apply(sig_banker_pass, mock_snap, mock_store, stake=1.0)
        self.assertFalse(filtered_b_skip.is_actionable)
        self.assertEqual(filtered_b_skip.action, StrategyAction.SKIP)

        # Test Player:
        sig_player_pass = StrategySignal(
            table_name="C02",
            strategy_id="adaptive_regime",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.55,
            reason="test",
            round_fingerprint=mock_snap.latest_fingerprint(),
            created_at="2026-09-18T10:00:00Z",
        )
        # Xác suất 0.530 >= 0.525 -> PASS
        filter_ml._predict_probability = Mock(return_value=0.530)
        with patch("ae_baccarat_workbench.ml_live.build_live_feature_row", return_value={}):
            filtered_p_pass = filter_ml.apply(sig_player_pass, mock_snap, mock_store, stake=1.0)
        self.assertTrue(filtered_p_pass.is_actionable)
        self.assertEqual(filtered_p_pass.action, StrategyAction.BET)

        # Xác suất 0.520 < 0.525 -> SKIP
        filter_ml._predict_probability = Mock(return_value=0.520)
        with patch("ae_baccarat_workbench.ml_live.build_live_feature_row", return_value={}):
            filtered_p_skip = filter_ml.apply(sig_player_pass, mock_snap, mock_store, stake=1.0)
        self.assertFalse(filtered_p_skip.is_actionable)
        self.assertEqual(filtered_p_skip.action, StrategyAction.SKIP)

    def test_rank_adaptive_regime_candidates_dual_margin(self) -> None:
        def make_snap(table_name: str) -> TableSnapshot:
            rnd = RoundEvent(table_name=table_name, outcome=Outcome.BANKER, shoe="shoe1", round_no=1)
            return TableSnapshot(
                table_name=table_name,
                shoe="shoe1",
                rounds=(rnd,),
                last_seen="2026-09-18T10:00:05+00:00",
            )

        snap1 = make_snap("Baccarat C01")
        snap2 = make_snap("Baccarat C02")

        # Banker: ML = 57.5% (ngưỡng 57.0% -> Margin = +0.5%)
        sig_banker = StrategySignal(
            table_name="Baccarat C01",
            strategy_id="adaptive_regime",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.55,
            reason="chop",
            round_fingerprint=snap1.latest_fingerprint(),
            features={"ml_probability_win": 0.575, "strategy_confidence": 0.55},
            created_at="2026-09-18T10:00:00+00:00",
        )

        # Player: ML = 54.5% (ngưỡng 52.5% -> Margin = +2.0%)
        sig_player = StrategySignal(
            table_name="Baccarat C02",
            strategy_id="adaptive_regime",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.55,
            reason="chop",
            round_fingerprint=snap2.latest_fingerprint(),
            features={"ml_probability_win": 0.545, "strategy_confidence": 0.55},
            created_at="2026-09-18T10:00:00+00:00",
        )

        snapshots = {"Baccarat C01": snap1, "Baccarat C02": snap2}
        latest = {
            ("Baccarat C01", "adaptive_regime"): sig_banker,
            ("Baccarat C02", "adaptive_regime"): sig_player,
        }
        countdowns = {"Baccarat C01": 20.0, "Baccarat C02": 20.0}

        candidates = _rank_adaptive_regime_candidates(
            latest_signals=latest,
            snapshots=snapshots,
            remaining_seconds_by_table=countdowns,
            stale_seconds=0,
        )

        self.assertEqual(len(candidates), 2)
        # Player có độ vượt chuẩn margin (+2.0%) cao hơn Banker (+0.5%), do đó Player C02 phải được xếp TRƯỚC!
        self.assertEqual(candidates[0].table_name, "Baccarat C02")
        self.assertEqual(candidates[0].side, BetSide.PLAYER)
        self.assertEqual(candidates[1].table_name, "Baccarat C01")
        self.assertEqual(candidates[1].side, BetSide.BANKER)

    def test_config_adaptive_regime_defaults(self) -> None:
        cfg = AppConfig()
        self.assertFalse(cfg.adaptive_regime_autobet_enabled)
        self.assertEqual(cfg.adaptive_regime_stake, 10.0)
        self.assertEqual(cfg.adaptive_regime_selected_windows, ())

        cfg_custom = AppConfig(
            adaptive_regime_autobet_enabled=True,
            adaptive_regime_stake=15.0,
            adaptive_regime_selected_windows=("18:00-19:00", "19:00-20:00"),
        )
        self.assertTrue(cfg_custom.adaptive_regime_autobet_enabled)
        self.assertEqual(cfg_custom.adaptive_regime_stake, 15.0)
        self.assertEqual(cfg_custom.adaptive_regime_selected_windows, ("18:00-19:00", "19:00-20:00"))

    def test_storage_adaptive_regime_hourly_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = Path(tmp_dir) / "test_ar.sqlite"
            duckdb_path = Path(tmp_dir) / "test_ar.duckdb"
            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=False)
            try:
                # 1. Lưu lệnh
                bet_id = store.save_adaptive_regime_hourly_bet(
                    session_date="2026-09-18",
                    session_window="19:00-20:00",
                    created_at="2026-09-18T12:05:00.000Z",
                    table_name="Baccarat C06",
                    side="P",
                    stake=10.0,
                    signal_fingerprint="C06|s2|15|hash",
                    confidence=0.545,
                )
                self.assertIsNotNone(bet_id)
                self.assertTrue(store.adaptive_regime_hourly_slot_used("2026-09-18", "19:00-20:00"))

                # 2. Check pending
                pending = store.pending_adaptive_regime_hourly_row()
                self.assertIsNotNone(pending)
                self.assertEqual(pending["table_name"], "Baccarat C06")
                self.assertEqual(pending["side"], "P")

                # 3. Settle lệnh
                settled = store.settle_adaptive_regime_hourly_bet(
                    bet_id=bet_id,
                    settled_at="2026-09-18T12:06:00.000Z",
                    outcome="P",
                    result="W",
                    pnl=10.0,
                )
                self.assertTrue(settled)
                self.assertIsNone(store.pending_adaptive_regime_hourly_row())

                # 4. Check summary
                summary = store.adaptive_regime_hourly_summary("2026-09-18", "19:00-20:00")
                self.assertEqual(summary["win_count"], 1)
                self.assertEqual(summary["loss_count"], 0)
                self.assertAlmostEqual(summary["total_pnl"], 10.0)

                # 5. Label formatting
                lbl = _adaptive_regime_summary_label(summary, 1)
                self.assertIn("W 1 - L 0 - T 0", lbl)
                self.assertIn("+10.00", lbl)
            finally:
                store.close()

    def test_app_auto_arm_adaptive_regime_dispatches(self) -> None:
        fixed_now = datetime(2026, 9, 18, 19, 15, tzinfo=BANGKOK_TIMEZONE)
        candidate = StrategySignal(
            table_name="Baccarat C07",
            strategy_id="adaptive_regime",
            action=StrategyAction.BET,
            side=BetSide.PLAYER,
            confidence=0.54,
            reason="ping_pong",
            round_fingerprint="C07-01-20",
            features={"ml_probability_win": 0.54, "road_pattern": "ping_pong"},
            created_at="2026-09-18T12:14:59+00:00",
        )

        app_mock = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app_mock.config = SimpleNamespace(
            adaptive_regime_stake=10.0,
            live_table_stale_seconds=0,
            money=SimpleNamespace(banker_commission=0.05),
        )
        app_mock.store = SimpleNamespace(
            adaptive_regime_hourly_slot_keys=Mock(return_value=set()),
            save_adaptive_regime_hourly_bet=Mock(return_value=202),
            adaptive_regime_hourly_slot_used=Mock(return_value=False),
            pending_adaptive_regime_hourly_row=Mock(return_value=None),
            settle_stale_adaptive_regime_hourly_bets=Mock(return_value=[]),
        )
        app_mock._selected_adaptive_regime_windows = Mock(return_value=("19:00-20:00",))
        app_mock._adaptive_regime_candidates = Mock(return_value=[candidate])
        app_mock._adaptive_regime_pending = None
        app_mock._adaptive_regime_slot_cache_date = ""
        app_mock._adaptive_regime_consumed_slots = set()
        app_mock._adaptive_regime_history_dirty = False
        app_mock._dispatch_live_autobet = Mock()
        app_mock.adaptive_regime_autobet_var = SimpleNamespace(get=Mock(return_value=True))
        app_mock.adaptive_regime_stake_var = SimpleNamespace(get=Mock(return_value="10.0"))
        app_mock._adaptive_regime_autobet_armed_today = set()
        app_mock.engine = SimpleNamespace(snapshots={})
        app_mock._daily_remaining_seconds = Mock(return_value={"Baccarat C07": 15.0})

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app_mock._auto_arm_adaptive_regime_hourly()

        app_mock.store.save_adaptive_regime_hourly_bet.assert_called_once()
        app_mock._dispatch_live_autobet.assert_called_once()
        orders, kwargs = app_mock._dispatch_live_autobet.call_args
        self.assertEqual(kwargs.get("source"), "adaptive_regime")
        self.assertEqual(len(orders[0]), 1)
        self.assertEqual(orders[0][0].table_name, "Baccarat C07")
        self.assertEqual(orders[0][0].side, "P")

    def test_web_api_adaptive_regime_endpoints(self) -> None:
        client = TestClient(app)

        # GET /api/adaptive_regime
        res_get = client.get("/api/adaptive_regime")
        self.assertEqual(res_get.status_code, 200)
        data = res_get.json()
        self.assertIn("banker_min_probability", data)
        self.assertIn("player_min_probability", data)
        self.assertEqual(data["banker_min_probability"], 0.57)
        self.assertEqual(data["player_min_probability"], 0.525)

        # POST /api/adaptive_regime/config
        res_post = client.post(
            "/api/adaptive_regime/config",
            json={"stake": 25.0, "windows": ["19:00-20:00"], "autobet": False},
        )
        self.assertEqual(res_post.status_code, 200)
        cfg_resp = res_post.json()
        self.assertTrue(cfg_resp["success"])
        self.assertEqual(cfg_resp["stake"], 25.0)
        self.assertEqual(cfg_resp["selected_windows"], ["19:00-20:00"])

        # GET /api/adaptive_regime/history
        res_hist = client.get("/api/adaptive_regime/history")
        self.assertEqual(res_hist.status_code, 200)
        hist_data = res_hist.json()
        self.assertIn("rows", hist_data)
        self.assertIn("summary", hist_data)


if __name__ == "__main__":
    unittest.main()
