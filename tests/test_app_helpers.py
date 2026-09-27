import queue
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ae_baccarat_workbench.app import (
    BaccaratWorkbenchApp,
    _active_daily_experiment_window,
    _auto_refresh_interval,
    _daily_experiment_result,
    _daily_history_summary_label,
    _daily_window_selection_summary,
    _elapsed_ms,
    _estimated_remaining_seconds,
    _filter_live_scores,
    _format_ms,
    _is_live_score,
    _latency_summary_label,
    _parse_auto_refresh_seconds,
    _parse_live_table_stale_seconds,
    _rank_daily_candidates,
    _rank_run_length_candidates,
    _run_length_summary_label,
    _signal_created_in_active_window,
    _signal_shoe_round,
    _snapshot_queue_key,
    _wl_streak,
)
from ae_baccarat_workbench.ae_decode import parse_manual_sequence
from ae_baccarat_workbench.models import BetSide, MoneyConfig, StrategyAction, StrategySignal


class AppHelperTests(unittest.TestCase):
    def test_refresh_views_reuses_one_table_score_snapshot(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        expected_scores = [SimpleNamespace(table_name="Baccarat C01", last_seen="")]
        app.engine = SimpleNamespace(table_scores=Mock(return_value=expected_scores))
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app._refresh_dashboard_tree = Mock()
        app._refresh_signal_tree = Mock()
        app._refresh_paper_tree = Mock()
        app._refresh_latency_tree = Mock()
        app._refresh_daily_tab = Mock()
        app._refresh_run_length_tab = Mock()

        app._refresh_views()

        app.engine.table_scores.assert_called_once_with()
        shared_scores = app._refresh_dashboard_tree.call_args.args[0]
        self.assertEqual(shared_scores, expected_scores)
        self.assertIs(shared_scores, app._refresh_daily_tab.call_args.args[0])
        app._refresh_run_length_tab.assert_called_once_with()

    def test_live_refresh_updates_only_the_visible_dashboard(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        expected_scores = [SimpleNamespace(table_name="Baccarat C01")]
        app.notebook = SimpleNamespace(select=lambda: "dashboard")
        app.dashboard_tab = "dashboard"
        app.signals_tab = "signals"
        app.daily_tab = "daily"
        app.run_length_tab = "run_length"
        app._refresh_dashboard_tree = Mock()
        app._refresh_signal_tree = Mock()
        app._refresh_paper_tree = Mock()
        app._refresh_daily_tab = Mock()
        app._refresh_run_length_tab = Mock()

        app._refresh_live_views(expected_scores)

        app._refresh_dashboard_tree.assert_called_once_with(expected_scores)
        app._refresh_signal_tree.assert_not_called()
        app._refresh_paper_tree.assert_not_called()
        app._refresh_daily_tab.assert_not_called()
        app._refresh_run_length_tab.assert_not_called()

    def test_live_refresh_updates_daily_with_the_same_score_snapshot(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        expected_scores = [SimpleNamespace(table_name="Baccarat C01")]
        app.notebook = SimpleNamespace(select=lambda: "daily")
        app.dashboard_tab = "dashboard"
        app.signals_tab = "signals"
        app.daily_tab = "daily"
        app.run_length_tab = "run_length"
        app._refresh_dashboard_tree = Mock()
        app._refresh_signal_tree = Mock()
        app._refresh_paper_tree = Mock()
        app._refresh_daily_tab = Mock()
        app._refresh_run_length_tab = Mock()

        app._refresh_live_views(expected_scores)

        app._refresh_daily_tab.assert_called_once_with(expected_scores)
        app._refresh_dashboard_tree.assert_not_called()
        app._refresh_signal_tree.assert_not_called()
        app._refresh_paper_tree.assert_not_called()
        app._refresh_run_length_tab.assert_not_called()

    def test_dashboard_live_road_uses_cache_and_updates_only_changed_row(self) -> None:
        class FakeTree:
            def __init__(self) -> None:
                self.rows = {}
                self.order = []
                self.insert_count = 0

            def exists(self, iid):
                return iid in self.rows

            def insert(self, _parent, _index, *, iid, values, tags):
                self.rows[iid] = {"values": values, "tags": tags}
                self.order.append(iid)
                self.insert_count += 1

            def item(self, iid, *, values, tags):
                self.rows[iid] = {"values": values, "tags": tags}

            def move(self, iid, _parent, index):
                self.order.remove(iid)
                self.order.insert(index, iid)

            def delete(self, iid):
                self.rows.pop(iid, None)
                if iid in self.order:
                    self.order.remove(iid)

        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.dashboard_tree = FakeTree()
        app.engine = SimpleNamespace(latest_signals={}, pending={})
        app._dashboard_wl_cache = {
            "Baccarat C01": {"history": "W L", "current": "L1"}
        }
        app._dashboard_totals_cache = {
            "settled_count": 2,
            "wins": 1,
            "losses": 1,
            "pushes": 0,
            "pnl": 0.0,
        }
        app._dashboard_stats_pending_tables = set()
        app._dashboard_row_values = {}
        app._request_dashboard_stats_refresh = Mock()
        score = SimpleNamespace(
            table_name="Baccarat C01",
            best_signal=None,
            display_signal=None,
            last_seen="2026-01-01T00:00:01+00:00",
            current_round_no=2,
            observed_rounds=2,
            road="B P",
            paper_pnl=0.0,
        )

        app._refresh_dashboard_tree([score])
        score.road = "B P B"
        score.current_round_no = 3
        score.observed_rounds = 3
        score.last_seen = "2026-01-01T00:00:02+00:00"
        app._refresh_dashboard_tree([score])

        values = app.dashboard_tree.rows["table:Baccarat C01"]["values"]
        self.assertEqual(values[3], "3")
        self.assertEqual(values[4], "B P B")
        self.assertEqual(app.dashboard_tree.insert_count, 2)  # table row + total row
        app._request_dashboard_stats_refresh.assert_not_called()

    def test_queue_refreshes_live_before_deferred_history(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        expected_scores = [SimpleNamespace(table_name="Baccarat C01", last_seen="")]
        app.queue = queue.Queue()
        app.root = SimpleNamespace(after=Mock())
        app.engine = SimpleNamespace(table_scores=Mock(return_value=expected_scores))
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app._pending_latency_samples = []
        app._last_live_views_refresh_monotonic = 0.0
        app._last_history_views_refresh_monotonic = 0.0
        app._live_views_dirty = True
        app._history_views_dirty = True
        app._refresh_live_views = Mock()
        app._refresh_history_views = Mock()
        app._finalize_latency_samples = Mock()

        with patch("ae_baccarat_workbench.app.time.perf_counter", return_value=100.0):
            app._process_queue()

        app._refresh_live_views.assert_called_once_with(expected_scores)
        app._refresh_history_views.assert_not_called()
        app._finalize_latency_samples.assert_called_once()

        with patch("ae_baccarat_workbench.app.time.perf_counter", return_value=100.0):
            app._process_queue()

        app._refresh_history_views.assert_called_once_with(expected_scores)

    def test_queue_keeps_latency_pending_until_live_view_is_refreshed(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.queue = queue.Queue()
        app.root = SimpleNamespace(after=Mock())
        app.engine = SimpleNamespace(table_scores=Mock())
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app._pending_latency_samples = [{"table_name": "Baccarat C01"}]
        app._last_live_views_refresh_monotonic = 100.0
        app._last_history_views_refresh_monotonic = 100.0
        app._live_views_dirty = True
        app._history_views_dirty = True
        app._refresh_live_views = Mock()
        app._refresh_history_views = Mock()
        app._finalize_latency_samples = Mock()

        with patch("ae_baccarat_workbench.app.time.perf_counter", return_value=100.1):
            app._process_queue()

        app._refresh_live_views.assert_not_called()
        app._refresh_history_views.assert_not_called()
        app._finalize_latency_samples.assert_not_called()
        self.assertEqual(app._pending_latency_samples, [{"table_name": "Baccarat C01"}])

    def test_wl_streak_counts_latest_non_push_result(self) -> None:
        self.assertEqual(_wl_streak(["W", "=", "W", "L"]), "W2")
        self.assertEqual(_wl_streak(["L", "L", "W"]), "L2")
        self.assertEqual(_wl_streak(["=", "="]), "-")

    def test_auto_refresh_interval_validation(self) -> None:
        self.assertIsNone(_auto_refresh_interval(False, "30"))
        self.assertEqual(_auto_refresh_interval(True, "300"), 300)
        self.assertEqual(_parse_auto_refresh_seconds("30.5"), 30)
        with self.assertRaises(ValueError):
            _auto_refresh_interval(True, "29")
        with self.assertRaises(ValueError):
            _parse_auto_refresh_seconds("abc")

    def test_live_table_stale_seconds_validation(self) -> None:
        self.assertEqual(_parse_live_table_stale_seconds("120.9"), 120)
        self.assertEqual(_parse_live_table_stale_seconds("0"), 0)
        with self.assertRaises(ValueError):
            _parse_live_table_stale_seconds("-1")

    def test_filter_live_scores_hides_stale_tables_but_allows_returning_updates(self) -> None:
        now = datetime(2026, 8, 30, 9, 20, 0, tzinfo=timezone.utc)
        fresh = SimpleNamespace(table_name="Baccarat C01", last_seen="2026-08-30T09:19:20+00:00")
        stale = SimpleNamespace(table_name="Table 1", last_seen="2026-08-30T09:10:00+00:00")
        returned = SimpleNamespace(table_name="Table 1", last_seen="2026-08-30T09:19:55+00:00")

        self.assertEqual([score.table_name for score in _filter_live_scores([fresh, stale], 120, now)], ["Baccarat C01"])
        self.assertTrue(_is_live_score(returned, 120, now))
        self.assertEqual(
            [score.table_name for score in _filter_live_scores([fresh, stale], 0, now)],
            ["Baccarat C01", "Table 1"],
        )

    def test_latency_helpers_format_non_negative_summary(self) -> None:
        self.assertEqual(_elapsed_ms(10.0, 10.125), 125.0)
        self.assertEqual(_elapsed_ms(10.0, 9.0), 0.0)
        self.assertEqual(_format_ms(12.345), "12.3")

        label = _latency_summary_label(
            [
                {"total_ms": 100, "queue_delay_ms": 30, "engine_ms": 20, "ui_delay_ms": 50},
                {"total_ms": 200, "queue_delay_ms": 60, "engine_ms": 40, "ui_delay_ms": 100},
                {"total_ms": 300, "queue_delay_ms": 90, "engine_ms": 60, "ui_delay_ms": 150},
            ]
        )
        self.assertIn("3 mau gan nhat", label)
        self.assertIn("total avg 200.0ms", label)
        self.assertIn("queue avg 60.0ms", label)

    def test_snapshot_queue_key_dedupes_only_identical_table_road(self) -> None:
        first = parse_manual_sequence("B P P", "Baccarat C01")
        same = parse_manual_sequence("B P P", "Baccarat C01")
        longer = parse_manual_sequence("B P P B", "Baccarat C01")

        self.assertEqual(_snapshot_queue_key(first), _snapshot_queue_key(same))
        self.assertNotEqual(_snapshot_queue_key(first), _snapshot_queue_key(longer))

    def test_daily_experiment_windows_cover_full_day_and_respect_selection(self) -> None:
        vietnam = timezone(timedelta(hours=7))
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 12, 0, tzinfo=vietnam)),
            "12:00-13:00",
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 13, 0, tzinfo=vietnam)),
            "13:00-14:00",
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 14, 59, tzinfo=vietnam)),
            "14:00-15:00",
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 19, 59, tzinfo=vietnam)),
            "19:00-20:00",
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 23, 59, tzinfo=vietnam)),
            "23:00-24:00",
        )

        selected = ("12:00-13:00", "18:00-19:00")
        self.assertEqual(
            _active_daily_experiment_window(
                datetime(2026, 9, 3, 12, 30, tzinfo=vietnam),
                selected,
            ),
            "12:00-13:00",
        )
        self.assertIsNone(
            _active_daily_experiment_window(
                datetime(2026, 9, 3, 13, 30, tzinfo=vietnam),
                selected,
            )
        )
        self.assertIsNone(
            _active_daily_experiment_window(
                datetime(2026, 9, 3, 12, 30, tzinfo=vietnam),
                (),
            )
        )

    def test_daily_window_selection_summary_is_compact(self) -> None:
        self.assertEqual(_daily_window_selection_summary(()), "Chưa chọn khung giờ")
        self.assertEqual(
            _daily_window_selection_summary(("12:00-13:00", "14:00-15:00")),
            "Đã chọn 2 khung: 12:00-13:00, 14:00-15:00",
        )

    def test_daily_experiment_result_uses_flat_stake_and_banker_commission(self) -> None:
        self.assertEqual(_daily_experiment_result("P", "P", 10, 0.05), ("W", 10.0))
        self.assertEqual(_daily_experiment_result("B", "B", 10, 0.05), ("W", 9.5))
        self.assertEqual(_daily_experiment_result("B", "P", 10, 0.05), ("L", -10))
        self.assertEqual(_daily_experiment_result("B", "T", 10, 0.05), ("T", 0.0))

    def test_daily_candidates_require_signal_fingerprint_for_current_round(self) -> None:
        current_snapshot = parse_manual_sequence("B P", "Baccarat C01")
        stale_snapshot = parse_manual_sequence("B P P", "Baccarat C02")
        current_signal = SimpleNamespace(
            is_actionable=True,
            features={"ml_probability_win": 0.61},
            confidence=0.61,
            round_fingerprint=current_snapshot.latest_fingerprint(),
        )
        stale_signal = SimpleNamespace(
            is_actionable=True,
            features={"ml_probability_win": 0.72},
            confidence=0.72,
            round_fingerprint=stale_snapshot.rounds[-2].fingerprint,
        )
        scores = [
            SimpleNamespace(table_name="Baccarat C01", best_signal=current_signal, score=0.61),
            SimpleNamespace(table_name="Baccarat C02", best_signal=stale_signal, score=0.72),
        ]

        ranked = _rank_daily_candidates(
            scores,
            {"Baccarat C01": current_snapshot, "Baccarat C02": stale_snapshot},
            {"Baccarat C01": 12.0, "Baccarat C02": 12.0},
        )

        self.assertEqual([score.table_name for score in ranked], ["Baccarat C01"])

    def test_daily_candidates_require_at_least_ten_seconds_remaining(self) -> None:
        snapshot = parse_manual_sequence("B P", "Baccarat C01")
        signal = SimpleNamespace(
            is_actionable=True,
            features={"ml_probability_win": 0.61},
            confidence=0.61,
            round_fingerprint=snapshot.latest_fingerprint(),
        )
        scores = [SimpleNamespace(table_name="Baccarat C01", best_signal=signal, score=0.61)]

        accepted = _rank_daily_candidates(
            scores,
            {"Baccarat C01": snapshot},
            {"Baccarat C01": 10.0},
        )
        rejected = _rank_daily_candidates(
            scores,
            {"Baccarat C01": snapshot},
            {"Baccarat C01": 9.999},
        )
        unknown = _rank_daily_candidates(scores, {"Baccarat C01": snapshot}, {})

        self.assertEqual([score.table_name for score in accepted], ["Baccarat C01"])
        self.assertEqual(rejected, [])
        self.assertEqual(unknown, [])

    def test_signal_must_be_created_after_active_window_opens(self) -> None:
        now = datetime(2026, 9, 15, 19, 0, 10, tzinfo=timezone(timedelta(hours=7)))
        before_window = SimpleNamespace(created_at="2026-09-15T11:59:59+00:00")
        at_window_start = SimpleNamespace(created_at="2026-09-15T12:00:00+00:00")
        fresh = SimpleNamespace(created_at="2026-09-15T12:00:09+00:00")

        self.assertFalse(
            _signal_created_in_active_window(before_window, now, "19:00-20:00")
        )
        self.assertTrue(
            _signal_created_in_active_window(at_window_start, now, "19:00-20:00")
        )
        self.assertTrue(_signal_created_in_active_window(fresh, now, "19:00-20:00"))

    def test_countdown_age_is_subtracted_and_stale_readings_are_dropped(self) -> None:
        readings = {
            "Baccarat C01": (8.0, 100.0),
            "Baccarat C02": (20.0, 98.5),
            "Baccarat C03": (20.0, 97.9),
        }

        remaining = _estimated_remaining_seconds(readings, now_monotonic=100.5)

        self.assertEqual(remaining["Baccarat C01"], 7.5)
        self.assertEqual(remaining["Baccarat C02"], 18.0)
        self.assertNotIn("Baccarat C03", remaining)

    def test_estimated_remaining_seconds_falls_back_to_recent_snapshot(self) -> None:
        snap = parse_manual_sequence("B P B", "Baccarat C16")
        snap = replace(snap, last_seen=datetime.now(timezone.utc).isoformat())
        remaining = _estimated_remaining_seconds(
            {},
            now_monotonic=100.5,
            snapshots={"Baccarat C16": snap},
        )
        self.assertIn("Baccarat C16", remaining)
        self.assertGreater(remaining["Baccarat C16"], 14.0)

    def test_run_length_candidates_are_independent_and_rank_highest_ml(self) -> None:
        first = parse_manual_sequence("B P B", "Baccarat C01")
        second = parse_manual_sequence("P B P", "Baccarat C02")
        third = parse_manual_sequence("B B P", "Baccarat C03")

        def signal(snapshot, strategy_id, probability, side=BetSide.BANKER):
            return StrategySignal(
                table_name=snapshot.table_name,
                strategy_id=strategy_id,
                action=StrategyAction.BET,
                side=side,
                confidence=probability,
                reason="ML pass",
                round_fingerprint=snapshot.latest_fingerprint(),
                features={
                    "ml_probability_win": probability,
                    "strategy_confidence": probability - 0.01,
                },
            )

        latest = {
            ("Baccarat C01", "run_length"): signal(first, "run_length", 0.58),
            ("Baccarat C02", "run_length"): signal(second, "run_length", 0.63),
            ("Baccarat C03", "ensemble_majority"): signal(
                third,
                "ensemble_majority",
                0.99,
            ),
        }
        ranked = _rank_run_length_candidates(
            latest,
            {
                first.table_name: first,
                second.table_name: second,
                third.table_name: third,
            },
            {
                first.table_name: 10.0,
                second.table_name: 12.0,
                third.table_name: 20.0,
            },
            stale_seconds=0,
        )

        self.assertEqual([item.table_name for item in ranked], ["Baccarat C02", "Baccarat C01"])
        self.assertTrue(all(item.strategy_id == "run_length" for item in ranked))

    def test_run_length_candidates_restore_side_from_a_higher_global_ml_threshold(self) -> None:
        snapshot = parse_manual_sequence("B P B", "Baccarat C04")
        globally_skipped = StrategySignal(
            table_name=snapshot.table_name,
            strategy_id="run_length",
            action=StrategyAction.SKIP,
            side=None,
            confidence=0.59,
            reason="ML skip: below the dashboard threshold",
            round_fingerprint=snapshot.latest_fingerprint(),
            features={
                "ml_probability_win": 0.59,
                "strategy_side": "P",
                "strategy_confidence": 0.67,
            },
        )

        ranked = _rank_run_length_candidates(
            {(snapshot.table_name, "run_length"): globally_skipped},
            {snapshot.table_name: snapshot},
            {snapshot.table_name: 10.0},
            stale_seconds=0,
        )

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0].side, BetSide.PLAYER)
        self.assertTrue(ranked[0].is_actionable)

    def test_run_length_candidates_reject_stale_fingerprint_low_ml_and_missing_rounds(self) -> None:
        now = datetime(2026, 9, 13, 5, 0, tzinfo=timezone.utc)
        stale_snapshot = replace(
            parse_manual_sequence("B P B", "Baccarat C05"),
            last_seen="2026-09-13T04:55:00+00:00",
        )
        missing_base = parse_manual_sequence("B P B", "Baccarat C06")
        missing_snapshot = replace(missing_base, rounds=(missing_base.rounds[0], missing_base.rounds[2]))
        current = parse_manual_sequence("P B P", "Baccarat C07")

        def signal(snapshot, probability, fingerprint=None):
            return StrategySignal(
                table_name=snapshot.table_name,
                strategy_id="run_length",
                action=StrategyAction.BET,
                side=BetSide.BANKER,
                confidence=probability,
                reason="ML pass",
                round_fingerprint=fingerprint or snapshot.latest_fingerprint(),
                features={"ml_probability_win": probability},
            )

        latest = {
            (stale_snapshot.table_name, "run_length"): signal(stale_snapshot, 0.80),
            (missing_snapshot.table_name, "run_length"): signal(missing_snapshot, 0.75),
            (current.table_name, "run_length"): signal(current, 0.5799),
            ("Baccarat C08", "run_length"): signal(
                replace(current, table_name="Baccarat C08"),
                0.90,
                current.rounds[-2].fingerprint,
            ),
        }
        snapshots = {
            stale_snapshot.table_name: stale_snapshot,
            missing_snapshot.table_name: missing_snapshot,
            current.table_name: current,
            "Baccarat C08": replace(current, table_name="Baccarat C08"),
        }

        ranked = _rank_run_length_candidates(
            latest,
            snapshots,
            {table_name: 12.0 for table_name in snapshots},
            stale_seconds=120,
            now=now,
        )

        self.assertEqual(ranked, [])

    def test_run_length_display_helpers_report_shoe_round_and_wl_rate(self) -> None:
        self.assertEqual(
            _signal_shoe_round("Baccarat C09|shoe-55|10|P"),
            ("shoe-55", "10"),
        )
        label = _run_length_summary_label(
            {
                "total_count": 5,
                "pending_count": 1,
                "win_count": 2,
                "loss_count": 1,
                "tie_count": 1,
                "total_pnl": 8.5,
            },
            displayed_count=5,
        )
        self.assertIn("W 2 - L 1 - T 1", label)
        self.assertIn("66.67%", label)
        self.assertIn("P&L +8.50", label)

    def test_daily_auto_arm_selects_only_highest_confidence_even_if_table_was_used(self) -> None:
        fixed_now = datetime(2026, 9, 3, 12, 5, tzinfo=timezone(timedelta(hours=7)))
        top_snapshot = parse_manual_sequence("B P", "Baccarat C01")
        other_snapshot = parse_manual_sequence("P B", "Baccarat C02")

        def signal(snapshot, probability, side):
            return SimpleNamespace(
                is_actionable=True,
                features={"ml_probability_win": probability},
                confidence=probability,
                round_fingerprint=snapshot.latest_fingerprint(),
                strategy_id="ensemble_majority",
                side=side,
                created_at="2026-09-03T05:04:59+00:00",
            )

        top_score = SimpleNamespace(
            table_name="Baccarat C01",
            best_signal=signal(top_snapshot, 0.72, BetSide.BANKER),
            score=0.72,
        )
        other_score = SimpleNamespace(
            table_name="Baccarat C02",
            best_signal=signal(other_snapshot, 0.65, BetSide.PLAYER),
            score=0.65,
        )

        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.store = SimpleNamespace(
            pending_daily_experiment_row=Mock(return_value=None),
            daily_experiment_rows=Mock(
                return_value=[
                    {
                        "table_name": "Baccarat C01",
                        "session_window": "12:00-13:00",
                        "status": "settled",
                    }
                ]
            ),
            save_daily_experiment_bet=Mock(return_value=True),
        )
        app.engine = SimpleNamespace(
            table_scores=Mock(return_value=[other_score, top_score]),
            snapshots={
                "Baccarat C01": top_snapshot,
                "Baccarat C02": other_snapshot,
            },
        )
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app.daily_stake_var = SimpleNamespace(get=lambda: "10")
        app.daily_status_var = SimpleNamespace(set=Mock())
        app.daily_autobet_var = SimpleNamespace(get=lambda: False)
        app._selected_daily_windows = Mock(return_value=("12:00-13:00",))
        app._autobet_armed_today = set()
        app._daily_history_dirty = False
        observed_monotonic = time.perf_counter()
        app._table_countdown_readings = {
            "Baccarat C01": (12.0, observed_monotonic),
            "Baccarat C02": (12.0, observed_monotonic),
        }
        app._dispatch_live_autobet = Mock()

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app._auto_arm_daily_experiment()

        app.store.save_daily_experiment_bet.assert_called_once()
        saved = app.store.save_daily_experiment_bet.call_args.kwargs
        self.assertEqual(saved["table_name"], "Baccarat C01")
        self.assertEqual(saved["confidence"], 0.72)
        app._dispatch_live_autobet.assert_not_called()

    def test_run_length_auto_arm_writes_only_the_dedicated_paper_ledger(self) -> None:
        fixed_now = datetime(2026, 9, 13, 23, 5, tzinfo=timezone(timedelta(hours=7)))
        snapshot = parse_manual_sequence("B P B", "Baccarat C09")
        candidate = StrategySignal(
            table_name=snapshot.table_name,
            strategy_id="run_length",
            action=StrategyAction.BET,
            side=BetSide.BANKER,
            confidence=0.64,
            reason="ML pass",
            round_fingerprint=snapshot.latest_fingerprint(),
            features={"ml_probability_win": 0.64},
            created_at="2026-09-13T16:04:59+00:00",
        )
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.config = SimpleNamespace(run_length_stake=25.0, live_table_stale_seconds=0)
        app.store = SimpleNamespace(
            run_length_hourly_slot_keys=Mock(return_value=set()),
            save_run_length_hourly_bet=Mock(return_value=42),
            run_length_hourly_slot_used=Mock(return_value=False),
            pending_run_length_hourly_row=Mock(return_value=None),
        )
        app._selected_run_length_windows = Mock(return_value=("23:00-24:00",))
        app._run_length_candidates = Mock(return_value=[candidate])
        app._run_length_pending = None
        app._run_length_slot_cache_date = ""
        app._run_length_consumed_slots = set()
        app._run_length_history_dirty = False
        app._dispatch_live_autobet = Mock()

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app._auto_arm_run_length_hourly()

        app.store.save_run_length_hourly_bet.assert_called_once()
        saved = app.store.save_run_length_hourly_bet.call_args.kwargs
        self.assertEqual(saved["session_window"], "23:00-24:00")
        self.assertEqual(saved["table_name"], "Baccarat C09")
        self.assertEqual(saved["stake"], 25.0)
        self.assertEqual(saved["confidence"], 0.64)
        self.assertEqual(app._run_length_pending["id"], 42)
        app._dispatch_live_autobet.assert_not_called()

    def test_hidden_run_length_tab_does_not_scan_ledger(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.run_length_tree = object()
        app._run_length_tab_visible = Mock(return_value=False)
        app.store = SimpleNamespace(
            run_length_hourly_rows=Mock(),
            run_length_hourly_summary=Mock(),
            run_length_hourly_slot_keys=Mock(),
        )

        app._refresh_run_length_tab()

        app.store.run_length_hourly_rows.assert_not_called()
        app.store.run_length_hourly_summary.assert_not_called()
        app.store.run_length_hourly_slot_keys.assert_not_called()

    def test_clean_run_length_history_tab_reuses_cached_filter_without_queries(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.run_length_history_date_var = SimpleNamespace(get=lambda: "Tất cả")
        app.run_length_history_window_var = SimpleNamespace(get=lambda: "Tất cả")
        app._run_length_history_dirty = False
        app._run_length_history_loaded_key = (None, None)
        app.store = SimpleNamespace(
            run_length_hourly_dates=Mock(),
            run_length_hourly_rows=Mock(),
            run_length_hourly_summary=Mock(),
        )

        app._refresh_run_length_history()

        app.store.run_length_hourly_dates.assert_not_called()
        app.store.run_length_hourly_rows.assert_not_called()
        app.store.run_length_hourly_summary.assert_not_called()

    def test_run_length_history_filters_date_and_hour_on_demand(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.run_length_history_date_var = SimpleNamespace(
            get=lambda: "2026-09-13",
            set=Mock(),
        )
        app.run_length_history_window_var = SimpleNamespace(get=lambda: "23:00-24:00")
        app.run_length_history_date_combo = SimpleNamespace(configure=Mock())
        app.run_length_history_summary_var = SimpleNamespace(set=Mock())
        app.run_length_history_tree = object()
        app._run_length_history_dirty = True
        app._run_length_history_loaded_key = None
        rows = [{"id": 1}]
        summary = {"total_count": 1, "pending_count": 1}
        app.store = SimpleNamespace(
            run_length_hourly_dates=Mock(return_value=["2026-09-13"]),
            run_length_hourly_rows=Mock(return_value=rows),
            run_length_hourly_summary=Mock(return_value=summary),
        )
        app._populate_run_length_tree = Mock()

        app._refresh_run_length_history(force=True)

        app.store.run_length_hourly_rows.assert_called_once_with(
            "2026-09-13",
            "23:00-24:00",
            limit=250,
        )
        app.store.run_length_hourly_summary.assert_called_once_with(
            "2026-09-13",
            "23:00-24:00",
        )
        app._populate_run_length_tree.assert_called_once_with(
            app.run_length_history_tree,
            rows,
        )
        self.assertFalse(app._run_length_history_dirty)
        self.assertEqual(
            app._run_length_history_loaded_key,
            ("2026-09-13", "23:00-24:00"),
        )

    def test_run_length_tie_settles_and_keeps_the_hour_consumed(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.config = SimpleNamespace(money=MoneyConfig())
        app._run_length_pending = {
            "id": 7,
            "session_date": "2026-09-13",
            "session_window": "23:00-24:00",
            "table_name": "Baccarat C09",
            "side": "B",
            "stake": 10.0,
            "signal_fingerprint": "Baccarat C09|shoe-1|10|P",
        }
        app._run_length_consumed_slots = {("2026-09-13", "23:00-24:00")}
        app._run_length_history_dirty = False
        app.store = SimpleNamespace(
            exact_next_round_after_fingerprint=Mock(
                return_value={"outcome": "T", "observed_at": "2026-09-13T16:05:30+00:00"}
            ),
            settle_run_length_hourly_bet=Mock(return_value=True),
            is_shoe_finished_after_signal=Mock(return_value=False),
        )

        app._settle_run_length_hourly("Baccarat C09")

        settled = app.store.settle_run_length_hourly_bet.call_args.kwargs
        self.assertEqual(settled["result"], "T")
        self.assertEqual(settled["pnl"], 0.0)
        self.assertIsNone(app._run_length_pending)
        self.assertIn(("2026-09-13", "23:00-24:00"), app._run_length_consumed_slots)
        self.assertTrue(app._run_length_history_dirty)

    def test_daily_history_summary_label_includes_wlt_pending_and_pnl(self) -> None:
        label = _daily_history_summary_label(
            {
                "total_count": 8,
                "pending_count": 1,
                "win_count": 4,
                "loss_count": 2,
                "tie_count": 1,
                "total_pnl": 15.5,
            },
            displayed_count=8,
        )
        self.assertEqual(
            label,
            "Hiển thị 8/8 lệnh | W 4 - L 2 - T 1 | Đang chờ 1 | P&L +15.50",
        )

    def test_daily_auto_arm_stops_when_window_has_won(self) -> None:
        fixed_now = datetime(2026, 9, 3, 12, 5, tzinfo=timezone(timedelta(hours=7)))
        top_snapshot = parse_manual_sequence("B P", "Baccarat C01")

        top_score = SimpleNamespace(
            table_name="Baccarat C01",
            best_signal=SimpleNamespace(
                is_actionable=True,
                features={"ml_probability_win": 0.72},
                confidence=0.72,
                round_fingerprint=top_snapshot.latest_fingerprint(),
                strategy_id="ensemble_majority",
                side=BetSide.BANKER,
                created_at="2026-09-03T05:04:59+00:00",
            ),
            score=0.72,
        )

        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.daily_stop_win_var = SimpleNamespace(get=lambda: True)
        app.store = SimpleNamespace(
            pending_daily_experiment_row=Mock(return_value=None),
            daily_experiment_rows=Mock(
                return_value=[
                    {
                        "table_name": "Baccarat C02",
                        "session_window": "12:00-13:00",
                        "status": "settled",
                        "result": "W",
                    }
                ]
            ),
            save_daily_experiment_bet=Mock(return_value=True),
        )
        app.engine = SimpleNamespace(
            table_scores=Mock(return_value=[top_score]),
            snapshots={"Baccarat C01": top_snapshot},
        )
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app.daily_stake_var = SimpleNamespace(get=lambda: "10")
        app.daily_status_var = SimpleNamespace(set=Mock())
        app.daily_autobet_var = SimpleNamespace(get=lambda: False)
        app._selected_daily_windows = Mock(return_value=("12:00-13:00",))
        app._autobet_armed_today = set()
        app._daily_history_dirty = False
        observed_monotonic = time.perf_counter()
        app._table_countdown_readings = {
            "Baccarat C01": (12.0, observed_monotonic),
        }
        app._dispatch_live_autobet = Mock()

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app._auto_arm_daily_experiment()

        # Should NOT save any order because window has won and stop_win is enabled
        app.store.save_daily_experiment_bet.assert_not_called()
        self.assertIn("Stop Win kích hoạt", app.daily_status_var.set.call_args[0][0])

    def test_daily_auto_arm_continues_when_window_has_lost(self) -> None:
        fixed_now = datetime(2026, 9, 3, 12, 5, tzinfo=timezone(timedelta(hours=7)))
        top_snapshot = parse_manual_sequence("B P", "Baccarat C01")

        top_score = SimpleNamespace(
            table_name="Baccarat C01",
            best_signal=SimpleNamespace(
                is_actionable=True,
                features={"ml_probability_win": 0.72},
                confidence=0.72,
                round_fingerprint=top_snapshot.latest_fingerprint(),
                strategy_id="ensemble_majority",
                side=BetSide.BANKER,
                created_at="2026-09-03T05:04:59+00:00",
            ),
            score=0.72,
        )

        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.daily_stop_win_var = SimpleNamespace(get=lambda: True)
        app.store = SimpleNamespace(
            pending_daily_experiment_row=Mock(return_value=None),
            daily_experiment_rows=Mock(
                return_value=[
                    {
                        "table_name": "Baccarat C02",
                        "session_window": "12:00-13:00",
                        "status": "settled",
                        "result": "L",
                    }
                ]
            ),
            save_daily_experiment_bet=Mock(return_value=True),
        )
        app.engine = SimpleNamespace(
            table_scores=Mock(return_value=[top_score]),
            snapshots={"Baccarat C01": top_snapshot},
        )
        app.config = SimpleNamespace(live_table_stale_seconds=0)
        app.daily_stake_var = SimpleNamespace(get=lambda: "10")
        app.daily_status_var = SimpleNamespace(set=Mock())
        app.daily_autobet_var = SimpleNamespace(get=lambda: False)
        app._selected_daily_windows = Mock(return_value=("12:00-13:00",))
        app._autobet_armed_today = set()
        app._daily_history_dirty = False
        observed_monotonic = time.perf_counter()
        app._table_countdown_readings = {
            "Baccarat C01": (12.0, observed_monotonic),
        }
        app._dispatch_live_autobet = Mock()

        with patch("ae_baccarat_workbench.app.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = fixed_now
            app._auto_arm_daily_experiment()

        # Should save order because order 1 was Lose
        app.store.save_daily_experiment_bet.assert_called_once()
        saved = app.store.save_daily_experiment_bet.call_args.kwargs
        self.assertEqual(saved["table_name"], "Baccarat C01")
        self.assertTrue(saved["stop_win_enabled"])

    def test_on_main_notebook_changed_refreshes_selected_tab(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app.dashboard_tab = "tab_dashboard"
        app.latency_tab = "tab_latency"
        app.signals_tab = "tab_signals"
        app.daily_tab = "tab_daily"
        app.run_length_tab = "tab_run_length"

        app._refresh_dashboard_tree = Mock()
        app._refresh_latency_tree = Mock()
        app._refresh_signal_tree = Mock()
        app._refresh_paper_tree = Mock()
        app._refresh_daily_tab = Mock()
        app._refresh_run_length_tab = Mock()

        # Dashboard tab selected
        app.notebook = SimpleNamespace(select=lambda: "tab_dashboard")
        app._on_main_notebook_changed()
        app._refresh_dashboard_tree.assert_called_once()

        # Latency tab selected
        app.notebook = SimpleNamespace(select=lambda: "tab_latency")
        app._on_main_notebook_changed()
        app._refresh_latency_tree.assert_called_once()

        # Signals tab selected
        app.notebook = SimpleNamespace(select=lambda: "tab_signals")
        app._on_main_notebook_changed()
        app._refresh_signal_tree.assert_called_once()
        app._refresh_paper_tree.assert_called_once()

    def test_initial_view_population_calls_all_views(self) -> None:
        app = BaccaratWorkbenchApp.__new__(BaccaratWorkbenchApp)
        app._refresh_latency_tree = Mock()
        app._refresh_dashboard_tree = Mock()
        app._refresh_signal_tree = Mock()
        app._refresh_paper_tree = Mock()
        app._refresh_daily_tab = Mock()
        app._refresh_run_length_tab = Mock()

        app._initial_view_population()

        app._refresh_latency_tree.assert_called_once()
        app._refresh_dashboard_tree.assert_called_once()
        app._refresh_signal_tree.assert_called_once()
        app._refresh_paper_tree.assert_called_once()
        app._refresh_daily_tab.assert_called_once()
        app._refresh_run_length_tab.assert_called_once()


if __name__ == "__main__":
    unittest.main()
