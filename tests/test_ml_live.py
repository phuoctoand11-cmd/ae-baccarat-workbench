import tempfile
import unittest
from pathlib import Path

from ae_baccarat_workbench.ml_live import MlSignalFilter, build_live_feature_row
from ae_baccarat_workbench.models import (
    BetSide,
    Outcome,
    PaperBet,
    RoundEvent,
    StrategyAction,
    StrategySignal,
    TableSnapshot,
)
from ae_baccarat_workbench.storage import WorkbenchStore


class MlLiveFeatureTests(unittest.TestCase):
    def test_enabled_filter_skips_signal_when_model_is_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                snapshot = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="s1",
                    rounds=(
                        RoundEvent(
                            "Baccarat C01",
                            Outcome.BANKER,
                            table_id=1001,
                            shoe="s1",
                            round_no=1,
                        ),
                    ),
                )
                signal = StrategySignal(
                    table_name="Baccarat C01",
                    strategy_id="sequence_follow",
                    action=StrategyAction.BET,
                    side=BetSide.BANKER,
                    confidence=0.60,
                    reason="test",
                    round_fingerprint=snapshot.latest_fingerprint(),
                )
                ml_filter = MlSignalFilter(Path(tmp) / "missing.joblib", enabled=True)

                filtered = ml_filter.apply(signal, snapshot, store, stake=10)

                self.assertFalse(filtered.is_actionable)
                self.assertEqual(filtered.action, StrategyAction.SKIP)
                self.assertIsNone(filtered.side)
                self.assertIn("not ready", filtered.reason)
                self.assertEqual(filtered.features["strategy_side"], "B")
            finally:
                store.close()

    def test_live_features_only_count_pre_signal_settled_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WorkbenchStore(Path(tmp) / "workbench.sqlite", enable_duckdb=False)
            try:
                rounds = (
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.BANKER,
                        table_id=1001,
                        shoe="s1",
                        round_no=1,
                        source="test",
                        observed_at="2026-01-01T00:00:01+00:00",
                    ),
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.PLAYER,
                        table_id=1001,
                        shoe="s1",
                        round_no=2,
                        source="test",
                        observed_at="2026-01-01T00:00:02+00:00",
                    ),
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.PLAYER,
                        table_id=1001,
                        shoe="s1",
                        round_no=3,
                        source="test",
                        observed_at="2026-01-01T00:00:03+00:00",
                    ),
                )
                snapshot = TableSnapshot(
                    table_name="Baccarat C01",
                    table_id=1001,
                    shoe="s1",
                    rounds=rounds,
                    source="test",
                )
                store.upsert_rounds(rounds)
                store.save_paper_bet(
                    PaperBet(
                        table_name="Baccarat C01",
                        strategy_id="sequence_follow",
                        side=BetSide.BANKER,
                        stake=10,
                        signal_fingerprint=rounds[0].fingerprint,
                        status="settled",
                        outcome=Outcome.BANKER,
                        pnl_delta=9.5,
                        pnl_after=9.5,
                        reason="pre signal",
                        created_at="2026-01-01T00:01:00+00:00",
                        settled_at="2026-01-01T00:01:10+00:00",
                    )
                )
                store.save_paper_bet(
                    PaperBet(
                        table_name="Baccarat C01",
                        strategy_id="sequence_follow",
                        side=BetSide.PLAYER,
                        stake=10,
                        signal_fingerprint=rounds[1].fingerprint,
                        status="settled",
                        outcome=Outcome.BANKER,
                        pnl_delta=-10,
                        pnl_after=-0.5,
                        reason="post signal",
                        created_at="2026-01-01T00:02:00+00:00",
                        settled_at="2026-01-01T00:03:00+00:00",
                    )
                )
                signal = StrategySignal(
                    table_name="Baccarat C01",
                    strategy_id="sequence_follow",
                    action=StrategyAction.BET,
                    side=BetSide.PLAYER,
                    confidence=0.60,
                    reason="test",
                    round_fingerprint=rounds[-1].fingerprint,
                    created_at="2026-01-01T00:02:30+00:00",
                )

                row = build_live_feature_row(signal, snapshot, store, stake=10)

                self.assertEqual(row["shoe_observed_rounds_to_signal"], 3)
                self.assertEqual(row["table_seen_rounds_to_signal"], 3)
                self.assertEqual(row["rolling_strategy_settled_bets_to_signal"], 1)
                self.assertEqual(row["rolling_strategy_wins_to_signal"], 1)
                self.assertEqual(row["rolling_strategy_losses_to_signal"], 0)
                self.assertEqual(row["prev_wl_result"], "win")
                self.assertEqual(row["prev_wl_streak_len"], 1)
                self.assertEqual(row["rolling_strategy_recent_10_win_rate"], 1.0)
                self.assertEqual(row["rolling_strategy_pnl_to_signal"], 9.5)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
