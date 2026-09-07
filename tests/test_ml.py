import tempfile
import unittest
from pathlib import Path

try:
    import duckdb  # noqa: F401
except Exception:  # pragma: no cover - environment dependent
    duckdb = None

try:
    import pandas  # noqa: F401
except Exception:  # pragma: no cover - environment dependent
    pandas = None

try:
    import sklearn  # noqa: F401
except Exception:  # pragma: no cover - environment dependent
    sklearn = None

try:
    import xgboost  # noqa: F401
except Exception:  # pragma: no cover - environment dependent
    xgboost = None

from ae_baccarat_workbench.ml import FEATURE_COLUMNS, evaluate_model, feature_query, load_training_frame, train_feature_frame
from ae_baccarat_workbench.models import BetSide, Outcome, PaperBet, RoundEvent
from ae_baccarat_workbench.storage import WorkbenchStore


class MlFeatureQueryTests(unittest.TestCase):
    def test_feature_query_uses_required_duckdb_views(self) -> None:
        sql = feature_query()
        for view_name in (
            "round_streaks",
            "paper_bet_results",
            "paper_wl_streaks",
            "strategy_performance",
            "table_round_summary",
        ):
            self.assertIn(view_name, sql)

    def test_model_feature_columns_do_not_include_settled_result_leakage(self) -> None:
        leaked_columns = {"settled_outcome", "wl_result", "pnl_delta", "pnl_after", "is_win", "is_loss", "is_push"}
        self.assertTrue(leaked_columns.isdisjoint(FEATURE_COLUMNS))


@unittest.skipIf(duckdb is None or pandas is None, "duckdb and pandas packages are required")
class MlFeatureFrameTests(unittest.TestCase):
    def test_load_training_frame_from_duckdb_views(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "workbench.sqlite"
            duckdb_path = Path(tmp) / "analytics.duckdb"
            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=True)
            try:
                rounds = [
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.BANKER,
                        table_id=1001,
                        shoe="s1",
                        round_no=1,
                        observed_at="2026-01-01T00:00:01+00:00",
                    ),
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.BANKER,
                        table_id=1001,
                        shoe="s1",
                        round_no=2,
                        observed_at="2026-01-01T00:00:02+00:00",
                    ),
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.PLAYER,
                        table_id=1001,
                        shoe="s1",
                        round_no=3,
                        observed_at="2026-01-01T00:02:30+00:00",
                    ),
                    RoundEvent(
                        "Baccarat C01",
                        Outcome.PLAYER,
                        table_id=1001,
                        shoe="s1",
                        round_no=4,
                        observed_at="2026-01-01T00:03:30+00:00",
                    ),
                ]
                rounds[0] = RoundEvent(
                    "Baccarat C01",
                    Outcome.BANKER,
                    table_id=1001,
                    shoe="s1",
                    round_no=1,
                    observed_at="2026-01-01T00:00:01+00:00",
                )
                rounds[1] = RoundEvent(
                    "Baccarat C01",
                    Outcome.BANKER,
                    table_id=1001,
                    shoe="s1",
                    round_no=2,
                    observed_at="2026-01-01T00:01:30+00:00",
                )
                store.upsert_rounds(rounds)
                for index, (signal_round, side, settled_outcome, pnl, created_at, settled_at) in enumerate(
                    [
                        (
                            rounds[0],
                            BetSide.BANKER,
                            Outcome.BANKER,
                            9.5,
                            "2026-01-01T00:00:10+00:00",
                            "2026-01-01T00:01:30+00:00",
                        ),
                        (
                            rounds[1],
                            BetSide.BANKER,
                            Outcome.PLAYER,
                            -10,
                            "2026-01-01T00:01:40+00:00",
                            "2026-01-01T00:02:30+00:00",
                        ),
                        (
                            rounds[2],
                            BetSide.PLAYER,
                            Outcome.PLAYER,
                            10,
                            "2026-01-01T00:02:40+00:00",
                            "2026-01-01T00:03:30+00:00",
                        ),
                    ],
                    start=1,
                ):
                    store.save_paper_bet(
                        PaperBet(
                            table_name="Baccarat C01",
                            strategy_id="test_strategy",
                            side=side,
                            stake=10,
                            signal_fingerprint=signal_round.fingerprint,
                            status="settled",
                            outcome=settled_outcome,
                            pnl_delta=pnl,
                            pnl_after=pnl,
                            created_at=created_at,
                            settled_at=settled_at,
                        )
                    )
            finally:
                store.close()

            frame = load_training_frame(duckdb_path)

            self.assertEqual(len(frame), 3)
            self.assertEqual(frame["target_win"].tolist(), [1, 0, 1])
            self.assertEqual(frame.loc[0, "prev_wl_result"], "none")
            self.assertEqual(frame.loc[1, "prev_wl_result"], "win")
            self.assertEqual(frame.loc[1, "signal_outcome_streak_len"], 2)
            self.assertEqual(frame.loc[0, "rolling_strategy_decisions_to_signal"], 0)
            self.assertEqual(frame.loc[1, "rolling_strategy_decisions_to_signal"], 1)
            self.assertEqual(frame.loc[2, "rolling_strategy_decisions_to_signal"], 2)
            self.assertEqual(frame.loc[2, "rolling_strategy_wins_to_signal"], 1)
            self.assertEqual(frame.loc[2, "rolling_strategy_losses_to_signal"], 1)
            self.assertEqual(frame.loc[0, "table_seen_rounds_to_signal"], 1)
            self.assertIn("shoe_banker_ratio_to_signal", frame.columns)

    def test_late_observed_older_round_is_not_counted_as_pre_signal_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sqlite_path = Path(tmp) / "workbench.sqlite"
            duckdb_path = Path(tmp) / "analytics.duckdb"
            store = WorkbenchStore(sqlite_path, duckdb_path, enable_duckdb=True)
            try:
                late_old_round = RoundEvent(
                    "Baccarat C01",
                    Outcome.BANKER,
                    table_id=1001,
                    shoe="s1",
                    round_no=1,
                    observed_at="2026-01-01T00:05:00+00:00",
                )
                signal_round = RoundEvent(
                    "Baccarat C01",
                    Outcome.PLAYER,
                    table_id=1001,
                    shoe="s1",
                    round_no=2,
                    observed_at="2026-01-01T00:00:10+00:00",
                )
                store.upsert_rounds([late_old_round, signal_round])
                store.save_paper_bet(
                    PaperBet(
                        table_name="Baccarat C01",
                        strategy_id="test_strategy",
                        side=BetSide.PLAYER,
                        stake=10,
                        signal_fingerprint=signal_round.fingerprint,
                        status="settled",
                        outcome=Outcome.PLAYER,
                        pnl_delta=10,
                        pnl_after=10,
                        created_at="2026-01-01T00:00:20+00:00",
                        settled_at="2026-01-01T00:01:00+00:00",
                    )
                )
            finally:
                store.close()

            frame = load_training_frame(duckdb_path)

            self.assertEqual(len(frame), 1)
            self.assertEqual(frame.loc[0, "shoe_observed_rounds_to_signal"], 1)
            self.assertEqual(frame.loc[0, "shoe_banker_rounds_to_signal"], 0)
            self.assertEqual(frame.loc[0, "shoe_player_rounds_to_signal"], 1)


@unittest.skipIf(pandas is None or sklearn is None, "pandas and scikit-learn packages are required")
class MlTrainingTests(unittest.TestCase):
    def test_train_feature_frame_writes_baseline_artifacts(self) -> None:
        rows = []
        for index in range(12):
            rows.append(
                {
                    "created_at": f"2026-01-01T00:{index:02d}:00+00:00",
                    "table_name": "Baccarat C01" if index < 6 else "Baccarat C02",
                    "strategy_id": "test_strategy",
                    "side": "B" if index % 2 == 0 else "P",
                    "signal_round_outcome": "B" if index % 3 == 0 else "P",
                    "prev_wl_result": "win" if index % 2 == 0 else "loss",
                    "target_win": index % 2,
                    "stake": 10,
                    "side_is_banker": 1 if index % 2 == 0 else 0,
                    "side_is_player": 0 if index % 2 == 0 else 1,
                    "signal_round_no": index + 1,
                    "signal_seq_no": index + 1,
                    "signal_outcome_streak_len": (index % 4) + 1,
                    "table_observed_rounds": 72,
                    "table_current_round_no": 72,
                    "table_known_missing_rounds": 0,
                    "table_banker_rounds": 34,
                    "table_player_rounds": 34,
                    "table_tie_rounds": 4,
                    "table_banker_ratio": 34 / 72,
                    "table_player_ratio": 34 / 72,
                    "table_tie_ratio": 4 / 72,
                    "prev_bet_win": 1 if index % 2 == 0 else 0,
                    "prev_bet_loss": 0 if index % 2 == 0 else 1,
                    "prev_wl_streak_len": (index % 3) + 1,
                    "strategy_settled_bets": 12,
                    "strategy_wins": 6,
                    "strategy_losses": 6,
                    "strategy_pushes": 0,
                    "strategy_decisions": 12,
                    "strategy_win_rate_ex_push": 0.5,
                    "strategy_pnl": 0,
                    "strategy_max_win_streak": 2,
                    "strategy_max_loss_streak": 2,
                }
            )
        frame = pandas.DataFrame(rows)

        with tempfile.TemporaryDirectory() as tmp:
            run = train_feature_frame(
                frame,
                output_dir=Path(tmp),
                min_rows=10,
                test_size=0.25,
                include_xgboost=True,
            )

            model_names = {model.name for model in run.models}
            self.assertIn("sklearn_logistic", model_names)
            if xgboost is not None:
                self.assertIn("xgboost", model_names)
                self.assertTrue((Path(tmp) / "xgboost.joblib").exists())
            self.assertTrue((Path(tmp) / "training_features.csv").exists())
            self.assertTrue((Path(tmp) / "training_metrics.json").exists())
            self.assertTrue((Path(tmp) / "sklearn_logistic.joblib").exists())

            evaluation = evaluate_model(
                Path(tmp) / "training_features.csv",
                Path(tmp),
                model_name="sklearn_logistic",
                test_size=0.25,
                decision_threshold=0.50,
                thresholds=(0.50, 0.60),
                min_group_rows=1,
            )

            self.assertTrue((Path(tmp) / "evaluation_predictions_sklearn_logistic.csv").exists())
            self.assertTrue((Path(tmp) / "evaluation_thresholds_sklearn_logistic.csv").exists())
            self.assertTrue((Path(tmp) / "evaluation_by_strategy_sklearn_logistic.csv").exists())
            self.assertTrue((Path(tmp) / "evaluation_by_table_sklearn_logistic.csv").exists())
            self.assertTrue((Path(tmp) / "evaluation_report_sklearn_logistic.md").exists())
            self.assertEqual(evaluation.model_name, "sklearn_logistic")
            self.assertEqual(evaluation.test_rows, 3)


if __name__ == "__main__":
    unittest.main()
