from dataclasses import replace
import tempfile
import unittest
from pathlib import Path

from ae_baccarat_workbench.ae_decode import parse_manual_sequence
from ae_baccarat_workbench.engine import WorkbenchEngine
from ae_baccarat_workbench.models import BetSide, MoneyConfig, Outcome, RoundEvent, StrategyAction, StrategySignal, TableSnapshot
from ae_baccarat_workbench.storage import WorkbenchStore


class EngineTests(unittest.TestCase):
    def test_ingest_arms_and_settles_paper_bets_on_next_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                first_signals = engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C01"))
                self.assertTrue(any(signal.is_actionable for signal in first_signals))
                self.assertGreater(len(engine.pending), 0)

                engine.ingest(parse_manual_sequence("B P P P P P", "Baccarat C01"))

                self.assertGreater(len(engine.paper_log), 0)
                self.assertTrue(all(bet.status == "settled" for bet in engine.paper_log))
                self.assertGreater(sum(bet.pnl_delta for bet in engine.paper_log), 0)
                self.assertEqual(len(store.recent_paper_bets()), len(engine.paper_log))
            finally:
                store.close()

    def test_money_config_update_applies_to_existing_managers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(0,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C01"))
                self.assertGreater(len(engine.pending), 0)
                self.assertTrue(all(bet.stake == 0 for bet in engine.pending.values()))

                engine.update_money_config(
                    MoneyConfig(
                        stake_chain=(10,),
                        progression_mode="flat",
                        stop_loss=500,
                        take_profit=500,
                        group_take_profit=500,
                        group_stop_loss=500,
                    )
                )
                engine.ingest(parse_manual_sequence("B P P P P P", "Baccarat C01"))

                self.assertGreater(len(engine.paper_log), 0)
                self.assertTrue(all(bet.stake == 10 for bet in engine.pending.values()))
            finally:
                store.close()

    def test_late_shoe_cutoff_skips_new_paper_bets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
                stop_signals_after_round=65,
            )
            try:
                outcomes = [Outcome.BANKER, Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER, Outcome.PLAYER]
                snapshot = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="s1",
                    rounds=tuple(
                        RoundEvent(
                            "Baccarat C01",
                            outcome,
                            table_id=1001,
                            shoe="s1",
                            round_no=65 + index,
                        )
                        for index, outcome in enumerate(outcomes)
                    ),
                )

                signals = engine.ingest(snapshot)

                self.assertTrue(any("gan cuoi shoe" in signal.reason for signal in signals))
                self.assertFalse(any(signal.is_actionable for signal in signals))
                self.assertEqual(len(engine.pending), 0)
            finally:
                store.close()

    def test_ml_filter_can_skip_new_paper_bets(self) -> None:
        class RejectingMlFilter:
            def __init__(self) -> None:
                self.calls = 0

            def apply(self, signal, snapshot, store, *, stake):
                self.calls += 1
                features = dict(signal.features)
                features["ml_probability_win"] = 0.40
                return replace(
                    signal,
                    action=StrategyAction.SKIP,
                    side=None,
                    confidence=0.40,
                    reason="ML skip test",
                    features=features,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            ml_filter = RejectingMlFilter()
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
                ml_filter=ml_filter,
            )
            try:
                signals = engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C01"))

                self.assertGreater(ml_filter.calls, 0)
                self.assertFalse(any(signal.is_actionable for signal in signals))
                self.assertEqual(len(engine.pending), 0)
                scores = engine.table_scores()
                self.assertEqual(len(scores), 1)
                self.assertIsNone(scores[0].best_signal)
                self.assertIsNotNone(scores[0].display_signal)
                self.assertTrue(scores[0].display_signal.is_actionable)
                saved = list(
                    store.conn.execute(
                        "SELECT action, side, reason, features_json FROM signals WHERE reason = 'ML skip test'"
                    )
                )
                self.assertGreater(len(saved), 0)
                self.assertTrue(all(row["action"] == "skip" for row in saved))
                self.assertTrue(all(row["side"] is None for row in saved))
            finally:
                store.close()

    def test_paper_trading_arms_only_highest_ml_pass_per_round(self) -> None:
        class FixedStrategy:
            name = "Fixed"

            def __init__(self, strategy_id: str, side: BetSide, confidence: float) -> None:
                self.strategy_id = strategy_id
                self.side = side
                self.confidence = confidence

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=self.side,
                    confidence=self.confidence,
                    reason=f"base {self.strategy_id}",
                    round_fingerprint=context.round_fingerprint,
                )

        class ScoredMlFilter:
            def __init__(self) -> None:
                self.probabilities = {"lower_ml": 0.56, "higher_ml": 0.72}

            def apply(self, signal, snapshot, store, *, stake):
                probability = self.probabilities[signal.strategy_id]
                features = dict(signal.features)
                features["ml_probability_win"] = probability
                return replace(
                    signal,
                    confidence=probability,
                    reason=f"ML pass: win probability {probability:.1%} >= threshold 55.0%; {signal.reason}",
                    features=features,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[
                    FixedStrategy("lower_ml", BetSide.BANKER, 0.90),
                    FixedStrategy("higher_ml", BetSide.PLAYER, 0.51),
                ],
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
                ml_filter=ScoredMlFilter(),
            )
            try:
                signals = engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C01"))

                self.assertEqual(len(signals), 2)
                self.assertEqual(len(engine.pending), 1)
                pending = next(iter(engine.pending.values()))
                self.assertEqual(pending.strategy_id, "higher_ml")
                self.assertEqual(pending.side, BetSide.PLAYER)
                self.assertEqual(
                    store.conn.execute("SELECT COUNT(1) FROM signals").fetchone()[0],
                    2,
                )

                engine.ingest(parse_manual_sequence("B P P P P B", "Baccarat C01"))

                self.assertEqual(len(engine.paper_log), 1)
                self.assertEqual(engine.paper_log[0].strategy_id, "higher_ml")
            finally:
                store.close()

    def test_table_scores_use_stable_table_name_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C11"))
                engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C02"))
                engine.ingest(parse_manual_sequence("B P P P P", "Baccarat C01"))

                self.assertEqual(
                    [score.table_name for score in engine.table_scores()],
                    ["Baccarat C01", "Baccarat C02", "Baccarat C11"],
                )
            finally:
                store.close()

    def test_duplicate_latest_snapshot_without_new_round_does_not_resave_signals(self) -> None:
        class FixedStrategy:
            strategy_id = "fixed"
            name = "Fixed"

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.70,
                    reason="fixed test",
                    round_fingerprint=context.round_fingerprint,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[FixedStrategy()],
                min_confidence=0.50,
                paper_trading_enabled=False,
            )
            try:
                first = engine.ingest(parse_manual_sequence("B P P", "Baccarat C01"))
                duplicate = engine.ingest(parse_manual_sequence("B P P", "Baccarat C01"))

                self.assertEqual(len(first), 1)
                self.assertEqual(duplicate, [])
                self.assertEqual(store.conn.execute("SELECT COUNT(1) FROM signals").fetchone()[0], 1)
            finally:
                store.close()

    def test_pending_settles_against_immediate_next_round_in_multi_round_snapshot(self) -> None:
        class FixedStrategy:
            strategy_id = "fixed"
            name = "Fixed"

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.70,
                    reason="fixed test",
                    round_fingerprint=context.round_fingerprint,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[FixedStrategy()],
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                round_one = RoundEvent(
                    "Baccarat C01",
                    Outcome.BANKER,
                    table_id=1001,
                    shoe="1",
                    round_no=1,
                )
                round_two = RoundEvent(
                    "Baccarat C01",
                    Outcome.PLAYER,
                    table_id=1001,
                    shoe="1",
                    round_no=2,
                )
                round_three = RoundEvent(
                    "Baccarat C01",
                    Outcome.BANKER,
                    table_id=1001,
                    shoe="1",
                    round_no=3,
                )

                engine.ingest(
                    TableSnapshot(
                        table_name="Baccarat C01",
                        table_id=1001,
                        shoe="1",
                        rounds=(round_one,),
                    )
                )
                self.assertEqual(len(engine.pending), 1)

                engine.ingest(
                    TableSnapshot(
                        table_name="Baccarat C01",
                        table_id=1001,
                        shoe="1",
                        rounds=(round_one, round_two, round_three),
                    )
                )

                self.assertEqual(len(engine.paper_log), 1)
                settled = engine.paper_log[0]
                self.assertEqual(settled.outcome, Outcome.PLAYER)
                self.assertEqual(settled.pnl_delta, -10)
            finally:
                store.close()

    def test_incomplete_shoe_blocks_signals_and_paper_arming(self) -> None:
        class FixedStrategy:
            strategy_id = "fixed"
            name = "Fixed"

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.70,
                    reason="fixed test",
                    round_fingerprint=context.round_fingerprint,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[FixedStrategy()],
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                snapshot = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="1",
                    rounds=(
                        RoundEvent(
                            "Baccarat C01",
                            Outcome.BANKER,
                            table_id=1001,
                            shoe="1",
                            round_no=8,
                        ),
                    ),
                )

                signals = engine.ingest(snapshot)

                self.assertEqual(len(signals), 1)
                self.assertFalse(signals[0].is_actionable)
                self.assertIn("shoe thieu 7 van", signals[0].reason)
                self.assertEqual(signals[0].features["data_quality_gate"], "incomplete_shoe")
                self.assertEqual(len(engine.pending), 0)
            finally:
                store.close()

    def test_pending_is_not_settled_across_a_missing_round(self) -> None:
        class FixedStrategy:
            strategy_id = "fixed"
            name = "Fixed"

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.70,
                    reason="fixed test",
                    round_fingerprint=context.round_fingerprint,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[FixedStrategy()],
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                first = RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="1", round_no=1)
                third = RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="1", round_no=3)
                engine.ingest(TableSnapshot("Baccarat C01", (first,), table_id=1001, shoe="1"))
                self.assertEqual(len(engine.pending), 1)

                engine.ingest(TableSnapshot("Baccarat C01", (first, third), table_id=1001, shoe="1"))

                self.assertEqual(len(engine.paper_log), 0)
                self.assertEqual(len(engine.pending), 1)
            finally:
                store.close()

    def test_stale_same_shoe_lower_round_snapshot_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(store, paper_trading_enabled=False)
            try:
                current = TableSnapshot(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="24860",
                    source="cdp-ws",
                    rounds=(
                        RoundEvent("Baccarat C09", Outcome.BANKER, table_id=1009, shoe="24860", round_no=35),
                    ),
                )
                stale = TableSnapshot(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="24860",
                    source="cdp-xhr",
                    rounds=(
                        RoundEvent("Baccarat C09", Outcome.PLAYER, table_id=1009, shoe="24860", round_no=34),
                    ),
                )

                engine.ingest(current)
                signals = engine.ingest(stale)

                self.assertEqual(signals, [])
                self.assertEqual(engine.snapshots["Baccarat C09"].current_round_no, 35)
                self.assertEqual(engine.table_scores()[0].road, "B")
            finally:
                store.close()

    def test_new_round_without_signal_generation_invalidates_previous_signal(self) -> None:
        class FixedStrategy:
            strategy_id = "fixed"
            name = "Fixed"

            def evaluate(self, context) -> StrategySignal:
                return StrategySignal(
                    table_name=context.table.table_name,
                    strategy_id=self.strategy_id,
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.70,
                    reason="fixed test",
                    round_fingerprint=context.round_fingerprint,
                )

        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                strategies=[FixedStrategy()],
                min_confidence=0.50,
                paper_trading_enabled=False,
            )
            try:
                first = RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="1", round_no=1)
                second = RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="1", round_no=2)

                engine.ingest(TableSnapshot("Baccarat C01", (first,), table_id=1001, shoe="1"))
                self.assertIsNotNone(engine.table_scores()[0].best_signal)

                signals = engine.ingest(
                    TableSnapshot("Baccarat C01", (first, second), table_id=1001, shoe="1"),
                    generate_signals=False,
                )

                self.assertEqual(signals, [])
                self.assertFalse(any(key[0] == "Baccarat C01" for key in engine.latest_signals))
                self.assertFalse(any(key[0] == "Baccarat C01" for key in engine.latest_raw_signals))
                self.assertIsNone(engine.table_scores()[0].best_signal)
                self.assertIsNone(engine.table_scores()[0].display_signal)
            finally:
                store.close()

    def test_merge_keeps_one_result_per_shoe_round_when_sources_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(store, paper_trading_enabled=False)
            try:
                first = TableSnapshot(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="24860",
                    source="cdp-ws",
                    rounds=(
                        RoundEvent("Baccarat C09", Outcome.BANKER, table_id=1009, shoe="24860", round_no=1),
                    ),
                )
                next_snapshot = TableSnapshot(
                    table_name="Baccarat C09",
                    table_id=1009,
                    shoe="24860",
                    source="cdp-xhr",
                    rounds=(
                        RoundEvent("Baccarat C09", Outcome.PLAYER, table_id=1009, shoe="24860", round_no=1),
                        RoundEvent("Baccarat C09", Outcome.PLAYER, table_id=1009, shoe="24860", round_no=2),
                    ),
                )

                engine.ingest(first)
                engine.ingest(next_snapshot)

                self.assertEqual(engine.table_scores()[0].road, "B P")
                self.assertEqual(store.conn.execute("SELECT COUNT(1) FROM rounds").fetchone()[0], 2)
            finally:
                store.close()

    def test_new_shoe_resets_visible_table_road_without_deleting_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            engine = WorkbenchEngine(
                store,
                money_config=MoneyConfig(
                    stake_chain=(10,),
                    progression_mode="flat",
                    stop_loss=500,
                    take_profit=500,
                    group_take_profit=500,
                    group_stop_loss=500,
                ),
                min_confidence=0.50,
                paper_trading_enabled=True,
            )
            try:
                old_shoe = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="1",
                    rounds=(
                        RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="1", round_no=1),
                        RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="1", round_no=2),
                        RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="1", round_no=3),
                    ),
                )
                new_shoe = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="2",
                    rounds=(
                        RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="2", round_no=1),
                    ),
                )

                engine.ingest(old_shoe)
                engine.ingest(new_shoe)

                self.assertEqual(engine.table_scores()[0].road, "B")
                self.assertEqual(engine.snapshots["Baccarat C01"].current_round_no, 1)
                self.assertEqual(store.conn.execute("SELECT COUNT(1) FROM rounds").fetchone()[0], 4)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
