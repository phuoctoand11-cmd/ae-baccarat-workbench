import sqlite3
import tempfile
import unittest
from pathlib import Path

try:
    import duckdb
except Exception:  # pragma: no cover - environment dependent
    duckdb = None

from ae_baccarat_workbench.models import BetSide, LatencySample, Outcome, PaperBet, RoundEvent
from ae_baccarat_workbench.storage import WorkbenchStore


class SqliteStorageTests(unittest.TestCase):
    def test_data_quality_exclusion_preserves_raw_rows_but_removes_them_from_ml_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                for index, (settled_at, outcome, delta) in enumerate(
                    [
                        ("2026-01-01T00:01:00+00:00", Outcome.BANKER, 9.5),
                        ("2026-01-01T00:11:00+00:00", Outcome.PLAYER, -10),
                    ],
                    start=1,
                ):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id="test_strategy",
                            side=BetSide.BANKER,
                            stake=10,
                            signal_fingerprint=f"signal-{index}",
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            reason="ML pass: win probability 60.0% >= threshold 55.0%",
                            created_at=settled_at,
                            settled_at=settled_at,
                        )
                    )

                exclusion_id = store.add_data_quality_exclusion(
                    started_at="2026-01-01T00:10:00+00:00",
                    ended_at="2026-01-01T00:20:00+00:00",
                    scope="all",
                    reason="decoder regression",
                    created_at="2026-01-01T00:21:00+00:00",
                )

                self.assertGreater(exclusion_id, 0)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM paper_bets").fetchone()[0], 2)
                self.assertEqual(len(store.data_quality_exclusion_rows()), 1)
                self.assertEqual(store.current_wl_streak("Baccarat C01"), "W1")
                self.assertEqual(
                    store.ml_pass_totals(),
                    {"settled_count": 1, "wins": 1, "losses": 0, "pushes": 0, "pnl": 9.5},
                )
            finally:
                store.close()

    def test_current_wl_streak_uses_full_persisted_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                for index, (side, outcome, delta) in enumerate(
                    [
                        (BetSide.BANKER, Outcome.BANKER, 9.5),
                        (BetSide.PLAYER, Outcome.TIE, 0),
                        (BetSide.PLAYER, Outcome.PLAYER, 10),
                        (BetSide.BANKER, Outcome.PLAYER, -10),
                        (BetSide.PLAYER, Outcome.BANKER, -10),
                    ],
                    start=1,
                ):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id="test_strategy",
                            side=side,
                            stake=10,
                            signal_fingerprint=f"signal-{index}",
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            created_at=f"2026-01-01T00:00:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:01:{index:02d}+00:00",
                        )
                    )

                self.assertEqual(store.current_wl_streak("Baccarat C01"), "L2")
                self.assertEqual(store.current_wl_streak("Baccarat C02"), "-")
            finally:
                store.close()

    def test_ml_pass_wl_summary_uses_only_ml_pass_history_by_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                rows = [
                    ("not ml", BetSide.PLAYER, Outcome.BANKER, -10),
                    ("ML pass: 56%", BetSide.PLAYER, Outcome.PLAYER, 10),
                    ("ML pass: 57%", BetSide.BANKER, Outcome.BANKER, 9.5),
                    ("ML skip: 42%", BetSide.BANKER, Outcome.PLAYER, -10),
                    ("ML pass: 58%", BetSide.PLAYER, Outcome.BANKER, -10),
                    ("ML skip: 43%", BetSide.PLAYER, Outcome.PLAYER, 10),
                    ("ML pass: 59%", BetSide.PLAYER, Outcome.BANKER, -10),
                    ("ML pass: 61%", BetSide.PLAYER, Outcome.BANKER, -10),
                    ("ML pass: 63%", BetSide.PLAYER, Outcome.TIE, 0),
                ]
                for index, (reason, side, outcome, delta) in enumerate(rows, start=1):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id="test_strategy",
                            side=side,
                            stake=10,
                            signal_fingerprint=f"signal-{index}",
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            reason=reason,
                            created_at=f"2026-01-01T00:00:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:01:{index:02d}+00:00",
                        )
                    )

                self.assertEqual(
                    store.ml_pass_wl_summary("Baccarat C01"),
                    {"history": "W W L L L", "current": "L3", "max_win": 2, "max_loss": 3},
                )
                self.assertEqual(
                    store.ml_pass_wl_summary("Baccarat C02"),
                    {"history": "-", "current": "-", "max_win": 0, "max_loss": 0},
                )

                store.save_paper_bet(
                    PaperBet(
                        table_name="Baccarat C02",
                        strategy_id="test_strategy",
                        side=BetSide.PLAYER,
                        stake=10,
                        signal_fingerprint="signal-c02",
                        status="settled",
                        outcome=Outcome.PLAYER,
                        pnl_delta=10,
                        pnl_after=0,
                        reason="ML pass: 64%",
                        created_at="2026-01-01T00:00:10+00:00",
                        settled_at="2026-01-01T00:01:10+00:00",
                    )
                )
                store.save_paper_bet(
                    PaperBet(
                        table_name="Baccarat C02",
                        strategy_id="test_strategy",
                        side=BetSide.BANKER,
                        stake=10,
                        signal_fingerprint="signal-pending",
                        status="pending",
                        outcome=None,
                        pnl_delta=0,
                        pnl_after=0,
                        reason="ML pass: 65%",
                        created_at="2026-01-01T00:00:11+00:00",
                    )
                )

                self.assertEqual(
                    store.ml_pass_totals(),
                    {"settled_count": 7, "wins": 3, "losses": 3, "pushes": 1, "pnl": -0.5},
                )
            finally:
                store.close()

    def test_latency_samples_are_saved_and_listed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                store.save_latency_sample(
                    LatencySample(
                        table_name="Baccarat C01",
                        source="cdp-ws",
                        current_round_no=12,
                        observed_rounds=12,
                        known_missing_rounds=0,
                        monitor_seen_at="2026-01-01T00:00:00.100+00:00",
                        app_received_at="2026-01-01T00:00:00.140+00:00",
                        engine_done_at="2026-01-01T00:00:00.170+00:00",
                        ui_refresh_at="2026-01-01T00:00:00.210+00:00",
                        queue_delay_ms=40.0,
                        engine_ms=30.0,
                        ui_delay_ms=40.0,
                        total_ms=110.0,
                        signal_count=6,
                        actionable_count=2,
                        pending_count=1,
                        created_at="2026-01-01T00:00:00.210+00:00",
                    )
                )

                rows = store.recent_latency_samples()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["table_name"], "Baccarat C01")
                self.assertEqual(rows[0]["source"], "cdp-ws")
                self.assertEqual(rows[0]["total_ms"], 110.0)
                self.assertEqual(rows[0]["actionable_count"], 2)
            finally:
                store.close()

    def test_ml_pass_summary_dedupes_same_prediction_by_highest_probability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                rows = [
                    ("same-round", "low", BetSide.BANKER, Outcome.BANKER, 9.5, "ML pass: win probability 56.0% >= threshold 55.0%"),
                    ("same-round", "high", BetSide.PLAYER, Outcome.BANKER, -10, "ML pass: win probability 72.0% >= threshold 55.0%"),
                    ("next-round", "high", BetSide.BANKER, Outcome.BANKER, 9.5, "ML pass: win probability 60.0% >= threshold 55.0%"),
                ]
                for index, (fingerprint, strategy, side, outcome, delta, reason) in enumerate(rows, start=1):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C09",
                            strategy_id=strategy,
                            side=side,
                            stake=10,
                            signal_fingerprint=fingerprint,
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            reason=reason,
                            created_at=f"2026-01-01T00:00:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:01:{index:02d}+00:00",
                        )
                    )

                self.assertEqual(
                    store.ml_pass_wl_summary("Baccarat C09"),
                    {"history": "L W", "current": "W1", "max_win": 1, "max_loss": 1},
                )
                self.assertEqual(
                    store.ml_pass_totals(),
                    {"settled_count": 2, "wins": 1, "losses": 1, "pushes": 0, "pnl": -0.5},
                )
            finally:
                store.close()

    def test_selected_ml_pass_rows_can_start_after_duplicate_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                rows = [
                    (
                        "old-duplicate",
                        "low",
                        BetSide.BANKER,
                        Outcome.BANKER,
                        9.5,
                        "ML pass: win probability 56.0% >= threshold 55.0%",
                    ),
                    (
                        "old-duplicate",
                        "high",
                        BetSide.PLAYER,
                        Outcome.BANKER,
                        -10,
                        "ML pass: win probability 72.0% >= threshold 55.0%",
                    ),
                    (
                        "old-single",
                        "single",
                        BetSide.PLAYER,
                        Outcome.PLAYER,
                        10,
                        "ML pass: win probability 61.0% >= threshold 55.0%",
                    ),
                    (
                        "new-single",
                        "single",
                        BetSide.BANKER,
                        Outcome.BANKER,
                        9.5,
                        "ML pass: win probability 62.0% >= threshold 55.0%",
                    ),
                ]
                for index, (fingerprint, strategy, side, outcome, delta, reason) in enumerate(rows, start=1):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C12",
                            strategy_id=strategy,
                            side=side,
                            stake=10,
                            signal_fingerprint=fingerprint,
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            reason=reason,
                            created_at=f"2026-01-01T00:00:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:01:{index:02d}+00:00",
                        )
                    )

                cutoff = store.ml_pass_duplicate_cutoff()
                self.assertEqual(cutoff["duplicate_groups"], 1)
                self.assertEqual(cutoff["cutoff"], "2026-01-01T00:01:02+00:00")

                selected_all = store.selected_ml_pass_rows()
                self.assertEqual([row["strategy_id"] for row in selected_all], ["high", "single", "single"])
                selected_after_cutoff = store.selected_ml_pass_rows(since=str(cutoff["cutoff"]))
                self.assertEqual([row["signal_fingerprint"] for row in selected_after_cutoff], ["old-single", "new-single"])
            finally:
                store.close()

    def test_daily_experiment_limits_each_window_and_settles_from_stored_rounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                signal_round = RoundEvent(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="shoe-1",
                    round_no=10,
                    outcome=Outcome.PLAYER,
                    source="cdp-ws",
                    observed_at="2026-09-03T05:00:00+00:00",
                )
                result_round = RoundEvent(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="shoe-1",
                    round_no=11,
                    outcome=Outcome.BANKER,
                    source="cdp-ws",
                    observed_at="2026-09-03T05:00:30+00:00",
                )
                store.upsert_rounds([signal_round])

                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:00:01+00:00",
                        table_name="Baccarat C09",
                        strategy_id="run_length",
                        side="B",
                        stake=10,
                        signal_fingerprint=signal_round.fingerprint,
                        confidence=0.61,
                    )
                )
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:00:02+00:00",
                        table_name="Baccarat C18",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c18-signal",
                        confidence=0.60,
                    )
                )
                self.assertFalse(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:00:03+00:00",
                        table_name="Baccarat C16",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c16-signal",
                        confidence=0.59,
                    )
                )
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="14:00-15:00",
                        created_at="2026-09-03T07:00:01+00:00",
                        table_name="Baccarat C21",
                        strategy_id="csss_sccc",
                        side="P",
                        stake=10,
                        signal_fingerprint="c21-signal",
                        confidence=0.62,
                    )
                )

                store.upsert_rounds([result_round])
                pending = store.pending_daily_experiment_row("Baccarat C09")
                self.assertIsNotNone(pending)
                next_round = store.next_round_after_fingerprint(
                    table_name="Baccarat C09",
                    signal_fingerprint=signal_round.fingerprint,
                )
                self.assertIsNotNone(next_round)
                self.assertEqual(next_round["fingerprint"], result_round.fingerprint)
                self.assertTrue(
                    store.settle_daily_experiment_bet(
                        bet_id=int(pending["id"]),
                        settled_at=str(next_round["observed_at"]),
                        outcome="B",
                        result="W",
                        pnl=9.5,
                    )
                )
                rows = store.daily_experiment_rows("2026-09-03", "12:00-13:00")
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["status"], "settled")
                self.assertEqual(rows[0]["pnl"], 9.5)

                self.assertEqual(store.daily_experiment_dates(), ["2026-09-03"])
                summary = store.daily_experiment_summary("2026-09-03")
                self.assertEqual(summary["total_count"], 3)
                self.assertEqual(summary["settled_count"], 1)
                self.assertEqual(summary["pending_count"], 2)
                self.assertEqual(summary["win_count"], 1)
                self.assertEqual(summary["loss_count"], 0)
                self.assertEqual(summary["tie_count"], 0)
                self.assertEqual(summary["total_pnl"], 9.5)

                window_summary = store.daily_experiment_summary(
                    "2026-09-03", "12:00-13:00"
                )
                self.assertEqual(window_summary["total_count"], 2)
                self.assertEqual(window_summary["pending_count"], 1)
            finally:
                store.close()

    def test_daily_experiment_rejects_signal_when_exact_next_round_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                signal_round = RoundEvent(
                    "Baccarat C22",
                    Outcome.BANKER,
                    table_id=1022,
                    shoe="2196",
                    round_no=6,
                    observed_at="2026-09-07T06:58:35+00:00",
                )
                next_round = RoundEvent(
                    "Baccarat C22",
                    Outcome.PLAYER,
                    table_id=1022,
                    shoe="2196",
                    round_no=7,
                    observed_at="2026-09-07T06:59:15+00:00",
                )
                later_non_contiguous_round = RoundEvent(
                    "Baccarat C22",
                    Outcome.BANKER,
                    table_id=1022,
                    shoe="2196",
                    round_no=9,
                    observed_at="2026-09-07T07:00:40+00:00",
                )
                store.upsert_rounds([signal_round, next_round, later_non_contiguous_round])

                self.assertEqual(
                    store.next_round_after_fingerprint(
                        table_name="Baccarat C22",
                        signal_fingerprint=signal_round.fingerprint,
                    )["fingerprint"],
                    next_round.fingerprint,
                )
                self.assertFalse(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-07",
                        session_window="14:00-15:00",
                        created_at="2026-09-07T07:00:01+00:00",
                        table_name="Baccarat C22",
                        strategy_id="ensemble_majority",
                        side="B",
                        stake=1000,
                        signal_fingerprint=signal_round.fingerprint,
                        confidence=0.55029,
                    )
                )
                self.assertEqual(store.daily_experiment_rows("2026-09-07"), [])
            finally:
                store.close()

    def test_daily_experiment_schema_migrates_existing_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "workbench.sqlite"
            conn = sqlite3.connect(sqlite_path)
            try:
                conn.execute(
                    """CREATE TABLE daily_experiment_bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_date TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    settled_at TEXT,
                    table_name TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    stake REAL NOT NULL,
                    signal_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    outcome TEXT,
                    result TEXT,
                    pnl REAL NOT NULL DEFAULT 0,
                    confidence REAL NOT NULL DEFAULT 0,
                    UNIQUE(session_date, table_name)
                    )"""
                )
                conn.commit()
            finally:
                conn.close()

            store = WorkbenchStore(sqlite_path, enable_duckdb=False)
            try:
                columns = {
                    row["name"]
                    for row in store.conn.execute("PRAGMA table_info(daily_experiment_bets)")
                }
                self.assertIn("session_window", columns)
            finally:
                store.close()


@unittest.skipIf(duckdb is None, "duckdb package is not installed")
class DuckDbMirrorTests(unittest.TestCase):
    def test_duckdb_backfills_sqlite_and_exposes_analytics_views(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "workbench.sqlite"
            duckdb_path = Path(tmp) / "analytics.duckdb"

            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=False)
            try:
                store.upsert_rounds(
                    [
                        RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="s1", round_no=1),
                        RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="s1", round_no=2),
                        RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="s1", round_no=3),
                        RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="s1", round_no=4),
                        RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="s1", round_no=5),
                    ]
                )
                for index, (side, outcome) in enumerate(
                    [
                        (BetSide.BANKER, Outcome.BANKER),
                        (BetSide.BANKER, Outcome.BANKER),
                        (BetSide.BANKER, Outcome.PLAYER),
                        (BetSide.PLAYER, Outcome.BANKER),
                        (BetSide.PLAYER, Outcome.BANKER),
                    ],
                    start=1,
                ):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id="test_strategy",
                            side=side,
                            stake=10,
                            signal_fingerprint=f"signal-{index}",
                            status="settled",
                            outcome=outcome,
                            pnl_delta=10 if side.outcome is outcome else -10,
                            pnl_after=0,
                            created_at=f"2026-01-01T00:00:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:01:{index:02d}+00:00",
                        )
                    )
                store.save_latency_sample(
                    LatencySample(
                        table_name="Baccarat C01",
                        source="cdp-xhr",
                        current_round_no=5,
                        observed_rounds=5,
                        known_missing_rounds=0,
                        monitor_seen_at="2026-01-01T00:02:00.000+00:00",
                        app_received_at="2026-01-01T00:02:00.030+00:00",
                        engine_done_at="2026-01-01T00:02:00.050+00:00",
                        ui_refresh_at="2026-01-01T00:02:00.090+00:00",
                        queue_delay_ms=30.0,
                        engine_ms=20.0,
                        ui_delay_ms=40.0,
                        total_ms=90.0,
                        signal_count=6,
                        actionable_count=1,
                        pending_count=1,
                        created_at="2026-01-01T00:02:00.090+00:00",
                    )
                )
            finally:
                store.close()

            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=True)
            store.close()

            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM rounds").fetchone()[0], 5)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM paper_bets").fetchone()[0], 5)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM latency_samples").fetchone()[0], 1)
                self.assertEqual(
                    con.execute("SELECT MAX(outcome_streak_len) FROM round_streaks WHERE outcome = 'P'").fetchone()[0],
                    3,
                )
                summary = con.execute(
                    """
                    SELECT observed_rounds, current_round_no, known_missing_rounds
                    FROM table_round_summary
                    WHERE table_name = 'Baccarat C01'
                    """
                ).fetchone()
                self.assertEqual(summary, (5, 5, 0))
                perf = con.execute(
                    """
                    SELECT wins, losses, decisions, max_win_streak, max_loss_streak
                    FROM strategy_performance
                    WHERE table_name = 'Baccarat C01' AND strategy_id = 'test_strategy'
                    """
                ).fetchone()
                self.assertEqual(perf, (2, 3, 5, 2, 3))
                self.assertEqual(con.execute("SELECT COUNT(*) FROM ml_rolling_features").fetchone()[0], 5)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
