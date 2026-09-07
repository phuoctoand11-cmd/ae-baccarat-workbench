import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ae_baccarat_workbench.app import (
    _active_daily_experiment_window,
    _auto_refresh_interval,
    _daily_experiment_result,
    _daily_history_summary_label,
    _elapsed_ms,
    _filter_live_scores,
    _format_ms,
    _is_live_score,
    _latency_summary_label,
    _parse_auto_refresh_seconds,
    _parse_live_table_stale_seconds,
    _rank_daily_candidates,
    _snapshot_queue_key,
    _wl_streak,
)
from ae_baccarat_workbench.ae_decode import parse_manual_sequence


class AppHelperTests(unittest.TestCase):
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

    def test_daily_experiment_windows_use_local_clock_boundaries(self) -> None:
        vietnam = timezone(timedelta(hours=7))
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 12, 0, tzinfo=vietnam)),
            "12:00-13:00",
        )
        self.assertIsNone(
            _active_daily_experiment_window(datetime(2026, 9, 3, 13, 0, tzinfo=vietnam))
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 14, 59, tzinfo=vietnam)),
            "14:00-15:00",
        )
        self.assertEqual(
            _active_daily_experiment_window(datetime(2026, 9, 3, 19, 59, tzinfo=vietnam)),
            "18:00-20:00",
        )
        self.assertIsNone(
            _active_daily_experiment_window(datetime(2026, 9, 3, 20, 0, tzinfo=vietnam))
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
        )

        self.assertEqual([score.table_name for score in ranked], ["Baccarat C01"])

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


if __name__ == "__main__":
    unittest.main()
