import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
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

    def test_dashboard_ml_summary_reads_only_recent_current_shoe_without_max_streaks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                old_round = RoundEvent(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="10",
                    round_no=1,
                    outcome=Outcome.BANKER,
                    observed_at="2026-01-01T00:00:01+00:00",
                )
                current_rounds = [
                    RoundEvent(
                        table_name="Baccarat C01",
                        table_id=1001,
                        shoe="11",
                        round_no=index,
                        outcome=outcome,
                        observed_at=f"2026-01-01T00:01:{index:02d}+00:00",
                    )
                    for index, outcome in enumerate(
                        (Outcome.BANKER, Outcome.PLAYER, Outcome.TIE),
                        start=1,
                    )
                ]
                store.upsert_rounds([old_round, *current_rounds])

                bets = [
                    (old_round.fingerprint, "old", BetSide.BANKER, Outcome.BANKER, 9.5, 60.0),
                    (current_rounds[0].fingerprint, "low", BetSide.BANKER, Outcome.BANKER, 9.5, 56.0),
                    (current_rounds[0].fingerprint, "high", BetSide.PLAYER, Outcome.BANKER, -10.0, 72.0),
                    (current_rounds[1].fingerprint, "current", BetSide.PLAYER, Outcome.PLAYER, 10.0, 61.0),
                    (current_rounds[2].fingerprint, "push", BetSide.PLAYER, Outcome.TIE, 0.0, 63.0),
                ]
                for index, (fingerprint, strategy, side, outcome, delta, probability) in enumerate(bets, start=1):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id=strategy,
                            side=side,
                            stake=10,
                            signal_fingerprint=fingerprint,
                            status="settled",
                            outcome=outcome,
                            pnl_delta=delta,
                            pnl_after=0,
                            reason=(
                                f"ML pass: win probability {probability:.1f}% "
                                ">= threshold 55.0%"
                            ),
                            created_at=f"2026-01-01T00:02:{index:02d}+00:00",
                            settled_at=f"2026-01-01T00:03:{index:02d}+00:00",
                        )
                    )

                self.assertEqual(
                    store.ml_pass_recent_wl_summary("Baccarat C01"),
                    {"history": "L W", "current": "W1"},
                )
                dashboard = store.dashboard_ml_pass_snapshot({"Baccarat C01": "11"})
                self.assertEqual(
                    dashboard["summaries"],
                    {"Baccarat C01": {"history": "L W", "current": "W1"}},
                )
                self.assertNotIn("max_win", dashboard["summaries"]["Baccarat C01"])
                self.assertNotIn("max_loss", dashboard["summaries"]["Baccarat C01"])
            finally:
                store.close()

    def test_autobet_audit_is_persisted_asynchronously_with_terminal_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                base = {
                    "attempt_id": "attempt-001",
                    "attempt_created_at": "2026-09-16T01:00:00+00:00",
                    "order_id": "order-001",
                    "source": "run_length",
                    "session_window": "08:00-09:00",
                    "signal_fingerprint": "Baccarat C09|shoe-1|20|P",
                    "signal_created_at": "2026-09-16T00:59:59+00:00",
                    "table_name": "Baccarat C09",
                    "target_shoe": "shoe-1",
                    "target_round_no": 21,
                    "side": "PLAYER",
                    "stake": 50.0,
                }
                self.assertTrue(
                    store.enqueue_autobet_audit(
                        {
                            **base,
                            "occurred_at": "2026-09-16T01:00:00.100+00:00",
                            "stage": "DISPATCH_REQUESTED",
                            "status": "requested",
                            "reason_code": "DISPATCH_REQUESTED",
                            "message": "queued",
                            "countdown_seconds": 9.5,
                        }
                    )
                )
                self.assertTrue(
                    store.enqueue_autobet_audit(
                        {
                            **base,
                            "occurred_at": "2026-09-16T01:00:01.200+00:00",
                            "stage": "ORDER_SKIPPED",
                            "status": "skipped",
                            "reason_code": "COUNTDOWN_BELOW_4_AT_TABLE",
                            "message": "countdown 4s",
                            "countdown_seconds": 4,
                            "table_shoe": "shoe-1",
                            "table_round_no": 21,
                        }
                    )
                )
                self.assertTrue(store.flush_autobet_audit(timeout=2.0))

                attempts = store.autobet_attempt_rows()
                self.assertEqual(len(attempts), 1)
                self.assertEqual(attempts[0]["status"], "skipped")
                self.assertEqual(attempts[0]["reason_code"], "COUNTDOWN_BELOW_4_AT_TABLE")
                self.assertEqual(float(attempts[0]["countdown_seconds"]), 4.0)
                self.assertEqual(len(store.autobet_event_rows("attempt-001")), 2)
                self.assertEqual(
                    store.autobet_audit_summary()["status_counts"],
                    {"skipped": 1},
                )
            finally:
                store.close()

    def test_autobet_provider_ack_fields_are_persisted_without_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                event = {
                    "attempt_id": "attempt-ack-001",
                    "attempt_created_at": "2026-09-16T01:00:00+00:00",
                    "order_id": "order-ack-001",
                    "source": "daily",
                    "session_window": "19:00-20:00",
                    "signal_fingerprint": "Baccarat C03|26445|10|B",
                    "signal_created_at": "2026-09-16T00:59:59+00:00",
                    "table_name": "Baccarat C03",
                    "target_shoe": "26445",
                    "target_round_no": 11,
                    "side": "BANKER",
                    "stake": 500.0,
                    "occurred_at": "2026-09-16T01:00:01.200+00:00",
                    "stage": "PROVIDER_ACCEPTED",
                    "status": "provider_accepted",
                    "reason_code": "PROVIDER_ACCEPTED",
                    "message": "accepted",
                    "payload": {
                        "provider_bet_id": "BET-123",
                        "provider_status": "accepted",
                        "provider_error_code": "",
                        "provider_source": "websocket",
                        "provider_endpoint": "provider.example/bet/place",
                        "observed_fields": ["betId", "status", "amount"],
                    },
                }
                self.assertTrue(store.enqueue_autobet_audit(event))
                self.assertTrue(store.flush_autobet_audit(timeout=2.0))

                row = store.autobet_attempt_rows()[0]
                self.assertEqual(row["status"], "provider_accepted")
                self.assertEqual(row["provider_bet_id"], "BET-123")
                self.assertEqual(row["provider_status"], "accepted")
                self.assertEqual(row["provider_source"], "websocket")
                self.assertIsNotNone(row["provider_ack_at"])
                self.assertNotIn("cookie", row["payload_json"].lower())
                self.assertNotIn("token", row["payload_json"].lower())
                self.assertNotIn("password", row["payload_json"].lower())
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

    def test_daily_experiment_runs_sequentially_and_allows_same_table_again(self) -> None:
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
                second_result_round = RoundEvent(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="shoe-1",
                    round_no=12,
                    outcome=Outcome.PLAYER,
                    source="cdp-ws",
                    observed_at="2026-09-03T05:01:00+00:00",
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
                self.assertFalse(
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

                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:00:31+00:00",
                        table_name="Baccarat C09",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint=result_round.fingerprint,
                        confidence=0.62,
                    )
                )
                self.assertFalse(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:00:32+00:00",
                        table_name="Baccarat C21",
                        strategy_id="csss_sccc",
                        side="P",
                        stake=10,
                        signal_fingerprint="c21-signal",
                        confidence=0.63,
                    )
                )

                store.upsert_rounds([second_result_round])
                pending = store.pending_daily_experiment_row("Baccarat C09")
                self.assertIsNotNone(pending)
                next_round = store.next_round_after_fingerprint(
                    table_name="Baccarat C09",
                    signal_fingerprint=result_round.fingerprint,
                )
                self.assertIsNotNone(next_round)
                self.assertEqual(next_round["fingerprint"], second_result_round.fingerprint)
                self.assertTrue(
                    store.settle_daily_experiment_bet(
                        bet_id=int(pending["id"]),
                        settled_at=str(next_round["observed_at"]),
                        outcome="P",
                        result="W",
                        pnl=10,
                    )
                )
                self.assertFalse(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:01:01+00:00",
                        table_name="Baccarat C16",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c16-signal",
                        confidence=0.58,
                    )
                )
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="14:00-15:00",
                        created_at="2026-09-03T07:00:01+00:00",
                        table_name="Baccarat C09",
                        strategy_id="csss_sccc",
                        side="P",
                        stake=10,
                        signal_fingerprint="c09-afternoon-signal",
                        confidence=0.64,
                    )
                )

                rows = store.daily_experiment_rows("2026-09-03", "12:00-13:00")
                self.assertEqual(len(rows), 2)
                self.assertEqual([row["table_name"] for row in rows], ["Baccarat C09", "Baccarat C09"])
                self.assertEqual([row["status"] for row in rows], ["settled", "settled"])
                self.assertEqual(rows[0]["pnl"], 9.5)

                self.assertEqual(store.daily_experiment_dates(), ["2026-09-03"])
                summary = store.daily_experiment_summary("2026-09-03")
                self.assertEqual(summary["total_count"], 3)
                self.assertEqual(summary["settled_count"], 2)
                self.assertEqual(summary["pending_count"], 1)
                self.assertEqual(summary["win_count"], 2)
                self.assertEqual(summary["loss_count"], 0)
                self.assertEqual(summary["tie_count"], 0)
                self.assertEqual(summary["total_pnl"], 19.5)

                window_summary = store.daily_experiment_summary(
                    "2026-09-03", "12:00-13:00"
                )
                self.assertEqual(window_summary["total_count"], 2)
                self.assertEqual(window_summary["pending_count"], 0)
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

    def test_run_length_hourly_ledger_restores_pending_and_consumes_tie_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "workbench.sqlite"
            signal_round = RoundEvent(
                "Baccarat C09",
                Outcome.PLAYER,
                table_id=1009,
                shoe="shoe-58",
                round_no=10,
                observed_at="2026-09-13T16:00:00+00:00",
            )
            tie_round = RoundEvent(
                "Baccarat C09",
                Outcome.TIE,
                table_id=1009,
                shoe="shoe-58",
                round_no=11,
                observed_at="2026-09-13T16:00:30+00:00",
            )
            win_round = RoundEvent(
                "Baccarat C09",
                Outcome.PLAYER,
                table_id=1009,
                shoe="shoe-58",
                round_no=12,
                observed_at="2026-09-13T17:00:30+00:00",
            )

            store = WorkbenchStore(db_path, enable_duckdb=False)
            try:
                store.upsert_rounds([signal_round])
                bet_id = store.save_run_length_hourly_bet(
                    session_date="2026-09-13",
                    session_window="23:00-24:00",
                    created_at="2026-09-13T16:00:01+00:00",
                    table_name="Baccarat C09",
                    side="B",
                    stake=10,
                    signal_fingerprint=signal_round.fingerprint,
                    confidence=0.58,
                )
                self.assertIsNotNone(bet_id)
                self.assertIsNone(
                    store.save_run_length_hourly_bet(
                        session_date="2026-09-13",
                        session_window="23:00-24:00",
                        created_at="2026-09-13T16:00:02+00:00",
                        table_name="Baccarat C18",
                        side="P",
                        stake=10,
                        signal_fingerprint="other-signal",
                        confidence=0.80,
                    )
                )
            finally:
                store.close()

            store = WorkbenchStore(db_path, enable_duckdb=False)
            try:
                pending = store.pending_run_length_hourly_row()
                self.assertIsNotNone(pending)
                self.assertEqual(int(pending["id"]), bet_id)

                store.upsert_rounds([tie_round])
                exact_next = store.exact_next_round_after_fingerprint(
                    table_name="Baccarat C09",
                    signal_fingerprint=signal_round.fingerprint,
                )
                self.assertIsNotNone(exact_next)
                self.assertEqual(exact_next["outcome"], "T")
                self.assertTrue(
                    store.settle_run_length_hourly_bet(
                        bet_id=int(pending["id"]),
                        settled_at=str(exact_next["observed_at"]),
                        outcome="T",
                        result="T",
                        pnl=0,
                    )
                )

                self.assertIsNone(
                    store.save_run_length_hourly_bet(
                        session_date="2026-09-13",
                        session_window="23:00-24:00",
                        created_at="2026-09-13T16:00:31+00:00",
                        table_name="Baccarat C09",
                        side="P",
                        stake=10,
                        signal_fingerprint=tie_round.fingerprint,
                        confidence=0.70,
                    )
                )

                second_id = store.save_run_length_hourly_bet(
                    session_date="2026-09-14",
                    session_window="00:00-01:00",
                    created_at="2026-09-13T17:00:01+00:00",
                    table_name="Baccarat C09",
                    side="P",
                    stake=20,
                    signal_fingerprint=tie_round.fingerprint,
                    confidence=0.64,
                )
                self.assertIsNotNone(second_id)
                store.upsert_rounds([win_round])
                self.assertTrue(
                    store.settle_run_length_hourly_bet(
                        bet_id=int(second_id),
                        settled_at=win_round.observed_at,
                        outcome="P",
                        result="W",
                        pnl=20,
                    )
                )

                rows = store.run_length_hourly_rows()
                self.assertEqual(len(rows), 2)
                self.assertEqual([row["result"] for row in rows], ["W", "T"])
                summary = store.run_length_hourly_summary()
                self.assertEqual(summary["win_count"], 1)
                self.assertEqual(summary["loss_count"], 0)
                self.assertEqual(summary["tie_count"], 1)
                self.assertEqual(summary["total_pnl"], 20)
                self.assertEqual(
                    store.run_length_hourly_dates(),
                    ["2026-09-14", "2026-09-13"],
                )
                filtered_rows = store.run_length_hourly_rows(
                    "2026-09-13",
                    "23:00-24:00",
                )
                self.assertEqual(len(filtered_rows), 1)
                self.assertEqual(filtered_rows[0]["result"], "T")
                filtered_summary = store.run_length_hourly_summary(
                    "2026-09-14",
                    "00:00-01:00",
                )
                self.assertEqual(filtered_summary["total_count"], 1)
                self.assertEqual(filtered_summary["win_count"], 1)
                self.assertEqual(
                    store.run_length_hourly_slot_keys("2026-09-13"),
                    {("2026-09-13", "23:00-24:00")},
                )
            finally:
                store.close()

    def test_stable_pair_ledger_arms_once_and_settles_exact_next_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                signal_round = RoundEvent(
                    "Baccarat C09",
                    Outcome.PLAYER,
                    table_id=1009,
                    shoe="shoe-55",
                    round_no=10,
                    observed_at="2026-09-08T05:00:00+00:00",
                )
                result_round = RoundEvent(
                    "Baccarat C09",
                    Outcome.BANKER,
                    table_id=1009,
                    shoe="shoe-55",
                    round_no=11,
                    observed_at="2026-09-08T05:00:30+00:00",
                )
                store.upsert_rounds([signal_round])
                pending_id = store.save_stable_pair_bet(
                    created_at="2026-09-08T05:00:01+00:00",
                    table_name="Baccarat C09",
                    strategy_id="shoe_profile",
                    side="B",
                    stake=1,
                    signal_fingerprint=signal_round.fingerprint,
                    confidence=0.58,
                )
                self.assertIsNotNone(pending_id)
                self.assertIsNone(
                    store.save_stable_pair_bet(
                        created_at="2026-09-08T05:00:02+00:00",
                        table_name="Baccarat C09",
                        strategy_id="shoe_profile",
                        side="B",
                        stake=1,
                        signal_fingerprint="Baccarat C09|shoe-55|10|duplicate",
                        confidence=0.58,
                    )
                )
                self.assertEqual(len(store.pending_stable_pair_rows()), 1)

                store.upsert_rounds([result_round])
                next_round = store.next_round_after_fingerprint(
                    table_name="Baccarat C09",
                    signal_fingerprint=signal_round.fingerprint,
                )
                self.assertIsNotNone(next_round)
                self.assertTrue(
                    store.settle_stable_pair_bet(
                        bet_id=int(pending_id),
                        settled_at=str(next_round["observed_at"]),
                        outcome=str(next_round["outcome"]),
                        result="W",
                        pnl=0.95,
                    )
                )
                self.assertEqual(store.pending_stable_pair_rows(), [])
                rows = store.stable_pair_rows()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["status"], "settled")
                self.assertEqual(rows[0]["pnl"], 0.95)
                self.assertEqual(
                    store.stable_pair_summary(),
                    {
                        "total_count": 1,
                        "settled_count": 1,
                        "pending_count": 0,
                        "win_count": 1,
                        "loss_count": 0,
                        "tie_count": 0,
                        "total_pnl": 0.95,
                    },
                )
            finally:
                store.close()

    def test_stable_pair_rejects_signal_if_exact_next_round_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                signal_round = RoundEvent(
                    "Baccarat C05",
                    Outcome.BANKER,
                    table_id=1005,
                    shoe="shoe-60",
                    round_no=20,
                )
                next_round = RoundEvent(
                    "Baccarat C05",
                    Outcome.PLAYER,
                    table_id=1005,
                    shoe="shoe-60",
                    round_no=21,
                )
                store.upsert_rounds([signal_round, next_round])

                self.assertIsNone(
                    store.save_stable_pair_bet(
                        created_at="2026-09-08T06:00:00+00:00",
                        table_name="Baccarat C05",
                        strategy_id="ensemble_majority",
                        side="P",
                        stake=1,
                        signal_fingerprint=signal_round.fingerprint,
                        confidence=0.57,
                    )
                )
                self.assertEqual(store.stable_pair_rows(), [])
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
                conn.execute(
                    """INSERT INTO daily_experiment_bets
                    (session_date, created_at, settled_at, table_name, strategy_id, side,
                     stake, signal_fingerprint, status, outcome, result, pnl, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "2026-09-03",
                        "2026-09-03T05:00:00+00:00",
                        "2026-09-03T05:00:30+00:00",
                        "Baccarat C09",
                        "run_length",
                        "B",
                        10,
                        "legacy-signal",
                        "settled",
                        "B",
                        "W",
                        9.5,
                        0.61,
                    ),
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
                schema = store.conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='daily_experiment_bets'"
                ).fetchone()["sql"]
                self.assertNotIn("UNIQUE(session_date, table_name)", schema)
                self.assertEqual(len(store.daily_experiment_rows("2026-09-03")), 1)
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-03",
                        session_window="12:00-13:00",
                        created_at="2026-09-03T05:01:00+00:00",
                        table_name="Baccarat C09",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="new-signal-same-table",
                        confidence=0.62,
                    )
                )
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

    def test_next_round_after_fingerprint_handles_skipped_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                # Insert round 16 and round 18 (round 17 skipped)
                store.upsert_rounds(
                    [
                        RoundEvent(
                            table_name="Baccarat C07",
                            outcome=Outcome.BANKER,
                            table_id=1007,
                            shoe="24845",
                            round_no=16,
                            observed_at="2026-09-10T06:37:01+00:00",
                        ),
                        RoundEvent(
                            table_name="Baccarat C07",
                            outcome=Outcome.TIE,
                            table_id=1007,
                            shoe="24845",
                            round_no=18,
                            observed_at="2026-09-10T06:37:13+00:00",
                        ),
                    ]
                )
                # Next round for round 16 should find round 18
                self.assertIsNone(
                    store.exact_next_round_after_fingerprint(
                        table_name="Baccarat C07",
                        signal_fingerprint="Baccarat C07|24845|16|B",
                    )
                )
                next_row = store.next_round_after_fingerprint(
                    table_name="Baccarat C07",
                    signal_fingerprint="Baccarat C07|24845|16|B",
                )
                self.assertIsNotNone(next_row)
                self.assertEqual(int(next_row["round_no"]), 18)
                self.assertEqual(str(next_row["outcome"]), "T")
            finally:
                store.close()

    def test_is_shoe_finished_after_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                store.upsert_rounds(
                    [
                        RoundEvent(
                            table_name="Baccarat C07",
                            outcome=Outcome.BANKER,
                            table_id=1007,
                            shoe="24845",
                            round_no=65,
                            observed_at="2026-09-10T06:37:01+00:00",
                        )
                    ]
                )
                self.assertFalse(
                    store.is_shoe_finished_after_signal(
                        table_name="Baccarat C07",
                        signal_fingerprint="Baccarat C07|24845|65|B",
                    )
                )
                # Now a new shoe starts
                store.upsert_rounds(
                    [
                        RoundEvent(
                            table_name="Baccarat C07",
                            outcome=Outcome.PLAYER,
                            table_id=1007,
                            shoe="24846",
                            round_no=1,
                            observed_at="2026-09-10T06:40:00+00:00",
                        )
                    ]
                )
                self.assertTrue(
                    store.is_shoe_finished_after_signal(
                        table_name="Baccarat C07",
                        signal_fingerprint="Baccarat C07|24845|65|B",
                    )
                )
            finally:
                store.close()

    def test_daily_experiment_stop_win_in_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                # 1. Order 1 wins in 12:00-13:00
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-14",
                        session_window="12:00-13:00",
                        created_at="2026-09-14T05:00:00+00:00",
                        table_name="Baccarat C01",
                        strategy_id="run_length",
                        side="B",
                        stake=10,
                        signal_fingerprint="c01-sig1",
                        confidence=0.60,
                        stop_win_enabled=True,
                    )
                )
                pending = store.pending_daily_experiment_row("Baccarat C01")
                self.assertIsNotNone(pending)
                self.assertTrue(
                    store.settle_daily_experiment_bet(
                        bet_id=int(pending["id"]),
                        settled_at="2026-09-14T05:00:30+00:00",
                        outcome="B",
                        result="W",
                        pnl=9.5,
                    )
                )
                self.assertTrue(store.daily_window_has_won("2026-09-14", "12:00-13:00"))

                # With stop_win_enabled=True, order 2 must be rejected
                self.assertFalse(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-14",
                        session_window="12:00-13:00",
                        created_at="2026-09-14T05:05:00+00:00",
                        table_name="Baccarat C02",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c02-sig2",
                        confidence=0.62,
                        stop_win_enabled=True,
                    )
                )

                # With stop_win_enabled=False, order 2 is allowed
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-14",
                        session_window="12:00-13:00",
                        created_at="2026-09-14T05:05:00+00:00",
                        table_name="Baccarat C02",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c02-sig2",
                        confidence=0.62,
                        stop_win_enabled=False,
                    )
                )

                # Settle order 2 so there is no pending order blocking new bets
                pending2 = store.pending_daily_experiment_row("Baccarat C02")
                self.assertIsNotNone(pending2)
                self.assertTrue(
                    store.settle_daily_experiment_bet(
                        bet_id=int(pending2["id"]),
                        settled_at="2026-09-14T05:05:30+00:00",
                        outcome="P",
                        result="W",
                        pnl=10.0,
                    )
                )

                # 2. Window 14:00-15:00 where Order 1 loses
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-14",
                        session_window="14:00-15:00",
                        created_at="2026-09-14T07:00:00+00:00",
                        table_name="Baccarat C03",
                        strategy_id="run_length",
                        side="B",
                        stake=10,
                        signal_fingerprint="c03-sig1",
                        confidence=0.60,
                        stop_win_enabled=True,
                    )
                )
                pending_lose = store.pending_daily_experiment_row("Baccarat C03")
                self.assertIsNotNone(pending_lose)
                self.assertTrue(
                    store.settle_daily_experiment_bet(
                        bet_id=int(pending_lose["id"]),
                        settled_at="2026-09-14T07:00:30+00:00",
                        outcome="P",
                        result="L",
                        pnl=-10.0,
                    )
                )
                self.assertFalse(store.daily_window_has_won("2026-09-14", "14:00-15:00"))

                # Because order 1 lost, order 2 is allowed even when stop_win_enabled=True
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-14",
                        session_window="14:00-15:00",
                        created_at="2026-09-14T07:05:00+00:00",
                        table_name="Baccarat C04",
                        strategy_id="run_length",
                        side="P",
                        stake=10,
                        signal_fingerprint="c04-sig2",
                        confidence=0.65,
                        stop_win_enabled=True,
                    )
                )
            finally:
                store.close()

    def test_settle_stale_daily_experiment_bets_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = WorkbenchStore(Path(tmp_dir) / "workbench.sqlite")
            try:
                store.save_daily_experiment_bet(
                    session_date="2026-09-15",
                    session_window="11:00-12:00",
                    created_at="2026-09-15T04:00:00+00:00",
                    table_name="Baccarat C19",
                    strategy_id="ensemble_majority",
                    side="B",
                    stake=500.0,
                    signal_fingerprint="Baccarat C19|3662|30|P",
                    confidence=0.60,
                )
                self.assertIsNotNone(store.pending_daily_experiment_row())

                dt_60s = datetime.fromisoformat("2026-09-15T04:01:00+00:00")
                settled = store.settle_stale_daily_experiment_bets(max_age_seconds=180.0, now_dt=dt_60s)
                self.assertEqual(settled, [])
                self.assertIsNotNone(store.pending_daily_experiment_row())

                dt_240s = datetime.fromisoformat("2026-09-15T04:04:00+00:00")
                settled = store.settle_stale_daily_experiment_bets(max_age_seconds=180.0, now_dt=dt_240s)
                self.assertEqual(len(settled), 1)
                self.assertIsNone(store.pending_daily_experiment_row())

                row = store.daily_experiment_rows("2026-09-15", "11:00-12:00")[0]
                self.assertEqual(row["status"], "settled")
                self.assertEqual(row["outcome"], "TIMEOUT")
                self.assertEqual(row["result"], "T")
                self.assertEqual(row["pnl"], 0.0)
            finally:
                store.close()

    def test_settle_stale_daily_experiment_bets_with_result_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = WorkbenchStore(Path(tmp_dir) / "workbench.sqlite")
            try:
                signal_round = RoundEvent(
                    table_name="Baccarat C19",
                    shoe="3662",
                    round_no=30,
                    outcome=Outcome.PLAYER,
                    observed_at="2026-09-15T03:59:54+00:00",
                )
                store.upsert_rounds([signal_round])
                store.save_daily_experiment_bet(
                    session_date="2026-09-15",
                    session_window="11:00-12:00",
                    created_at="2026-09-15T04:00:00+00:00",
                    table_name="Baccarat C19",
                    strategy_id="ensemble_majority",
                    side="B",
                    stake=100.0,
                    signal_fingerprint=signal_round.fingerprint,
                    confidence=0.60,
                )
                next_round = RoundEvent(
                    table_name="Baccarat C19",
                    shoe="3662",
                    round_no=31,
                    outcome=Outcome.BANKER,
                    observed_at="2026-09-15T04:00:40+00:00",
                )
                store.upsert_rounds([next_round])

                dt_now = datetime.fromisoformat("2026-09-15T04:01:00+00:00")
                settled = store.settle_stale_daily_experiment_bets(now_dt=dt_now)
                self.assertEqual(len(settled), 1)
                row = store.daily_experiment_rows("2026-09-15", "11:00-12:00")[0]
                self.assertEqual(row["status"], "settled")
                self.assertEqual(row["outcome"], "B")
                self.assertEqual(row["result"], "W")
                self.assertAlmostEqual(row["pnl"], 95.0)
            finally:
                store.close()

    def test_settle_stale_run_length_hourly_bets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = WorkbenchStore(Path(tmp_dir) / "workbench.sqlite")
            try:
                bet_id = store.save_run_length_hourly_bet(
                    session_date="2026-09-15",
                    session_window="11:00-12:00",
                    created_at="2026-09-15T04:00:00+00:00",
                    table_name="Baccarat C08",
                    side="P",
                    stake=50.0,
                    signal_fingerprint="c08-rl-sig",
                    confidence=0.62,
                )
                self.assertIsNotNone(bet_id)
                self.assertIsNotNone(store.pending_run_length_hourly_row())

                dt_timeout = datetime.fromisoformat("2026-09-15T04:05:00+00:00")
                settled = store.settle_stale_run_length_hourly_bets(max_age_seconds=180.0, now_dt=dt_timeout)
                self.assertEqual(settled, [bet_id])
                self.assertIsNone(store.pending_run_length_hourly_row())

                row = store.run_length_hourly_rows("2026-09-15", "11:00-12:00")[0]
                self.assertEqual(row["status"], "settled")
                self.assertEqual(row["outcome"], "TIMEOUT")
                self.assertEqual(row["result"], "T")
                self.assertEqual(row["pnl"], 0.0)
            finally:
                store.close()

    def test_save_daily_experiment_bet_auto_settles_stale_pending_bet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = WorkbenchStore(Path(tmp_dir) / "workbench.sqlite")
            try:
                # First bet created at 04:00:00
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-15",
                        session_window="11:00-12:00",
                        created_at="2026-09-15T04:00:00+00:00",
                        table_name="Baccarat C01",
                        strategy_id="ensemble",
                        side="B",
                        stake=100.0,
                        signal_fingerprint="c01-old",
                        confidence=0.60,
                    )
                )
                self.assertIsNotNone(store.pending_daily_experiment_row())

                # 300s later, second bet attempted without prior settlement
                # Should auto-settle the stale bet as TIMEOUT and save the new bet successfully!
                self.assertTrue(
                    store.save_daily_experiment_bet(
                        session_date="2026-09-15",
                        session_window="11:00-12:00",
                        created_at="2026-09-15T04:05:00+00:00",
                        table_name="Baccarat C02",
                        strategy_id="ensemble",
                        side="P",
                        stake=100.0,
                        signal_fingerprint="c02-new",
                        confidence=0.65,
                    )
                )
                rows = store.daily_experiment_rows("2026-09-15", "11:00-12:00")
                self.assertEqual(len(rows), 2)
                # First bet is settled as TIMEOUT
                old_bet = next(r for r in rows if r["table_name"] == "Baccarat C01")
                self.assertEqual(old_bet["status"], "settled")
                self.assertEqual(old_bet["outcome"], "TIMEOUT")
                # Second bet is pending
                new_bet = next(r for r in rows if r["table_name"] == "Baccarat C02")
                self.assertEqual(new_bet["status"], "pending")
            finally:
                store.close()

    def test_save_run_length_hourly_bet_auto_settles_stale_pending_bet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = WorkbenchStore(Path(tmp_dir) / "workbench.sqlite")
            try:
                # First bet created at 04:00:00
                bet1 = store.save_run_length_hourly_bet(
                    session_date="2026-09-15",
                    session_window="11:00-12:00",
                    created_at="2026-09-15T04:00:00+00:00",
                    table_name="Baccarat C01",
                    side="B",
                    stake=50.0,
                    signal_fingerprint="c01-rl-old",
                    confidence=0.60,
                )
                self.assertIsNotNone(bet1)

                # 300s later, second bet attempted on new window
                bet2 = store.save_run_length_hourly_bet(
                    session_date="2026-09-15",
                    session_window="12:00-13:00",
                    created_at="2026-09-15T04:05:00+00:00",
                    table_name="Baccarat C02",
                    side="P",
                    stake=50.0,
                    signal_fingerprint="c02-rl-new",
                    confidence=0.62,
                )
                self.assertIsNotNone(bet2)
                row1 = store.run_length_hourly_rows("2026-09-15", "11:00-12:00")[0]
                self.assertEqual(row1["status"], "settled")
                self.assertEqual(row1["outcome"], "TIMEOUT")
                row2 = store.run_length_hourly_rows("2026-09-15", "12:00-13:00")[0]
                self.assertEqual(row2["status"], "pending")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
