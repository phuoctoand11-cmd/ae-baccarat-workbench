from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .storage import rolling_feature_select_sql


TARGET_COLUMN = "target_win"
DEFAULT_DUCKDB_PATH = Path("data") / "analytics.duckdb"
DEFAULT_OUTPUT_DIR = Path("data") / "ml"
DEFAULT_THRESHOLDS: tuple[float, ...] = (0.50, 0.52, 0.55, 0.58, 0.60, 0.62, 0.65, 0.70)

CATEGORICAL_FEATURES: tuple[str, ...] = (
    "table_name",
    "strategy_id",
    "side",
    "signal_round_outcome",
    "prev_wl_result",
)

NUMERIC_FEATURES: tuple[str, ...] = (
    "signal_confidence",
    "stake",
    "side_is_banker",
    "side_is_player",
    "signal_round_no",
    "signal_seq_no",
    "signal_outcome_streak_len",
    "shoe_observed_rounds_to_signal",
    "shoe_current_round_no_to_signal",
    "shoe_known_missing_rounds_to_signal",
    "shoe_banker_rounds_to_signal",
    "shoe_player_rounds_to_signal",
    "shoe_tie_rounds_to_signal",
    "shoe_banker_ratio_to_signal",
    "shoe_player_ratio_to_signal",
    "shoe_tie_ratio_to_signal",
    "shoe_last_6_rounds",
    "shoe_last_6_banker_ratio",
    "shoe_last_6_player_ratio",
    "shoe_last_6_tie_ratio",
    "shoe_last_12_rounds",
    "shoe_last_12_banker_ratio",
    "shoe_last_12_player_ratio",
    "shoe_last_12_tie_ratio",
    "table_seen_rounds_to_signal",
    "table_seen_banker_ratio_to_signal",
    "table_seen_player_ratio_to_signal",
    "table_seen_tie_ratio_to_signal",
    "prev_bet_win",
    "prev_bet_loss",
    "prev_wl_streak_len",
    "rolling_strategy_settled_bets_to_signal",
    "rolling_strategy_wins_to_signal",
    "rolling_strategy_losses_to_signal",
    "rolling_strategy_pushes_to_signal",
    "rolling_strategy_decisions_to_signal",
    "rolling_strategy_win_rate_to_signal",
    "rolling_strategy_pnl_to_signal",
    "rolling_strategy_max_win_streak_to_signal",
    "rolling_strategy_max_loss_streak_to_signal",
    "rolling_strategy_recent_10_decisions",
    "rolling_strategy_recent_10_win_rate",
    "rolling_strategy_recent_10_pnl",
    "rolling_table_settled_bets_to_signal",
    "rolling_table_win_rate_to_signal",
    "rolling_table_pnl_to_signal",
    "rolling_global_strategy_settled_bets_to_signal",
    "rolling_global_strategy_win_rate_to_signal",
    "rolling_global_strategy_pnl_to_signal",
    "rolling_global_strategy_recent_30_decisions",
    "rolling_global_strategy_recent_30_win_rate",
)

FEATURE_COLUMNS: tuple[str, ...] = NUMERIC_FEATURES + CATEGORICAL_FEATURES


class MissingMlDependency(RuntimeError):
    pass


@dataclass(frozen=True)
class ModelMetrics:
    name: str
    train_rows: int
    test_rows: int
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float | None
    model_path: str | None = None


@dataclass(frozen=True)
class TrainingRun:
    created_at: str
    duckdb_path: str
    output_dir: str
    rows: int
    target_column: str
    numeric_features: tuple[str, ...]
    categorical_features: tuple[str, ...]
    models: list[ModelMetrics]
    skipped_models: list[str]
    feature_csv_path: str
    metrics_path: str


@dataclass(frozen=True)
class EvaluationRun:
    created_at: str
    model_name: str
    model_path: str
    feature_csv_path: str
    output_dir: str
    total_rows: int
    train_rows: int
    test_rows: int
    base_win_rate: float
    decision_threshold: float
    predictions_path: str
    threshold_report_path: str
    strategy_report_path: str
    table_report_path: str
    summary_path: str
    markdown_path: str


def feature_query() -> str:
    return rolling_feature_select_sql()


def load_training_frame(duckdb_path: str | Path = DEFAULT_DUCKDB_PATH):
    duckdb = _import_duckdb()
    path = Path(duckdb_path)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {path}")
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot open DuckDB analytics file: {path}. Close the desktop app first if it is holding the DuckDB lock."
        ) from exc
    try:
        return con.execute(feature_query()).fetchdf()
    finally:
        con.close()


def train_models(
    duckdb_path: str | Path = DEFAULT_DUCKDB_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    min_rows: int = 30,
    test_size: float = 0.25,
    random_state: int = 42,
    include_xgboost: bool = True,
) -> TrainingRun:
    frame = load_training_frame(duckdb_path)
    return train_feature_frame(
        frame,
        duckdb_path=duckdb_path,
        output_dir=output_dir,
        min_rows=min_rows,
        test_size=test_size,
        random_state=random_state,
        include_xgboost=include_xgboost,
    )


def train_feature_frame(
    frame: Any,
    *,
    duckdb_path: str | Path = DEFAULT_DUCKDB_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    min_rows: int = 30,
    test_size: float = 0.25,
    random_state: int = 42,
    include_xgboost: bool = True,
) -> TrainingRun:
    deps = _import_training_deps()
    pandas = deps["pandas"]
    joblib = deps["joblib"]
    ColumnTransformer = deps["ColumnTransformer"]
    LogisticRegression = deps["LogisticRegression"]
    accuracy_score = deps["accuracy_score"]
    f1_score = deps["f1_score"]
    precision_score = deps["precision_score"]
    recall_score = deps["recall_score"]
    roc_auc_score = deps["roc_auc_score"]
    Pipeline = deps["Pipeline"]
    SimpleImputer = deps["SimpleImputer"]
    StandardScaler = deps["StandardScaler"]
    OneHotEncoder = deps["OneHotEncoder"]

    frame = _prepare_training_frame(frame, pandas)

    if len(frame) < min_rows:
        raise ValueError(f"Need at least {min_rows} settled win/loss rows, found {len(frame)}.")
    if frame[TARGET_COLUMN].nunique() < 2:
        raise ValueError("Training target has only one class. Collect both win and loss rows before training.")
    if not 0 < test_size < 1:
        raise ValueError("test_size must be greater than 0 and less than 1.")

    train, test = _chronological_split(frame, test_size)
    if train[TARGET_COLUMN].nunique() < 2:
        raise ValueError("Training split has only one class. Collect more rows or lower --test-size.")

    X_train = train[list(FEATURE_COLUMNS)]
    y_train = train[TARGET_COLUMN]
    X_test = test[list(FEATURE_COLUMNS)]
    y_test = test[TARGET_COLUMN]

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    feature_csv_path = output_path / "training_features.csv"
    metrics_path = output_path / "training_metrics.json"
    frame.to_csv(feature_csv_path, index=False)

    models: list[ModelMetrics] = []
    skipped_models: list[str] = []

    sklearn_model = Pipeline(
        steps=[
            ("preprocess", _preprocessor(ColumnTransformer, Pipeline, SimpleImputer, StandardScaler, OneHotEncoder)),
            (
                "model",
                LogisticRegression(max_iter=1000, class_weight="balanced", random_state=random_state),
            ),
        ]
    )
    sklearn_model.fit(X_train, y_train)
    sklearn_model_path = output_path / "sklearn_logistic.joblib"
    joblib.dump(sklearn_model, sklearn_model_path)
    models.append(
        _score_model(
            "sklearn_logistic",
            sklearn_model,
            X_test,
            y_test,
            len(y_train),
            str(sklearn_model_path),
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
            roc_auc_score,
        )
    )

    if include_xgboost:
        try:
            from xgboost import XGBClassifier  # type: ignore

            xgboost_model = Pipeline(
                steps=[
                    ("preprocess", _preprocessor(ColumnTransformer, Pipeline, SimpleImputer, StandardScaler, OneHotEncoder)),
                    (
                        "model",
                        XGBClassifier(
                            n_estimators=200,
                            max_depth=3,
                            learning_rate=0.05,
                            subsample=0.9,
                            colsample_bytree=0.9,
                            eval_metric="logloss",
                            random_state=random_state,
                            n_jobs=2,
                        ),
                    ),
                ]
            )
            xgboost_model.fit(X_train, y_train)
            xgboost_model_path = output_path / "xgboost.joblib"
            joblib.dump(xgboost_model, xgboost_model_path)
            models.append(
                _score_model(
                    "xgboost",
                    xgboost_model,
                    X_test,
                    y_test,
                    len(y_train),
                    str(xgboost_model_path),
                    accuracy_score,
                    precision_score,
                    recall_score,
                    f1_score,
                    roc_auc_score,
                )
            )
        except Exception as exc:
            skipped_models.append(f"xgboost: {exc}")

    run = TrainingRun(
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        duckdb_path=str(Path(duckdb_path)),
        output_dir=str(output_path),
        rows=len(frame),
        target_column=TARGET_COLUMN,
        numeric_features=NUMERIC_FEATURES,
        categorical_features=CATEGORICAL_FEATURES,
        models=models,
        skipped_models=skipped_models,
        feature_csv_path=str(feature_csv_path),
        metrics_path=str(metrics_path),
    )
    metrics_path.write_text(_to_json(run), encoding="utf-8")
    return run


def export_features(
    duckdb_path: str | Path = DEFAULT_DUCKDB_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> Path:
    frame = load_training_frame(duckdb_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    feature_csv_path = output_path / "training_features.csv"
    frame.to_csv(feature_csv_path, index=False)
    return feature_csv_path


def evaluate_model(
    feature_csv_path: str | Path = DEFAULT_OUTPUT_DIR / "training_features.csv",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    model_name: str = "auto",
    model_path: str | Path | None = None,
    test_size: float = 0.25,
    decision_threshold: float = 0.55,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
    min_group_rows: int = 30,
) -> EvaluationRun:
    deps = _import_training_deps()
    pandas = deps["pandas"]
    joblib = deps["joblib"]

    features_path = Path(feature_csv_path)
    if not features_path.exists():
        raise FileNotFoundError(f"Feature CSV not found: {features_path}")
    if not 0 < decision_threshold <= 1:
        raise ValueError("decision_threshold must be greater than 0 and less than or equal to 1.")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    resolved_model_name, resolved_model_path = _resolve_model_path(output_path, model_name, model_path)
    if not resolved_model_path.exists():
        raise FileNotFoundError(f"Model file not found: {resolved_model_path}")

    frame = pandas.read_csv(features_path, low_memory=False)
    frame = _prepare_training_frame(frame, pandas)
    if len(frame) < 2:
        raise ValueError(f"Need at least 2 feature rows to evaluate, found {len(frame)}.")
    train, test = _chronological_split(frame, test_size)
    if test.empty:
        raise ValueError("Evaluation test split is empty. Lower --test-size or collect more data.")

    model = joblib.load(resolved_model_path)
    probabilities = _predict_win_probabilities(model, test[list(FEATURE_COLUMNS)])
    predictions = test.copy()
    predictions["ml_model"] = resolved_model_name
    predictions["ml_probability_win"] = probabilities
    predictions["ml_prediction_win"] = (predictions["ml_probability_win"] >= 0.5).astype(int)
    if "pnl_delta" not in predictions.columns:
        predictions["pnl_delta"] = 0
    predictions["pnl_delta"] = pandas.to_numeric(predictions["pnl_delta"], errors="coerce").fillna(0)

    base_win_rate = round(float(predictions[TARGET_COLUMN].mean()), 6)
    threshold_df = _build_threshold_report(pandas, predictions, thresholds, base_win_rate)
    strategy_df = _build_group_report(
        pandas,
        predictions,
        "strategy_id",
        decision_threshold,
        min_group_rows,
    )
    table_df = _build_group_report(
        pandas,
        predictions,
        "table_name",
        decision_threshold,
        min_group_rows,
    )

    suffix = resolved_model_name
    predictions_path = output_path / f"evaluation_predictions_{suffix}.csv"
    threshold_report_path = output_path / f"evaluation_thresholds_{suffix}.csv"
    strategy_report_path = output_path / f"evaluation_by_strategy_{suffix}.csv"
    table_report_path = output_path / f"evaluation_by_table_{suffix}.csv"
    summary_path = output_path / f"evaluation_summary_{suffix}.json"
    markdown_path = output_path / f"evaluation_report_{suffix}.md"

    predictions.to_csv(predictions_path, index=False)
    threshold_df.to_csv(threshold_report_path, index=False)
    strategy_df.to_csv(strategy_report_path, index=False)
    table_df.to_csv(table_report_path, index=False)

    run = EvaluationRun(
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        model_name=resolved_model_name,
        model_path=str(resolved_model_path),
        feature_csv_path=str(features_path),
        output_dir=str(output_path),
        total_rows=len(frame),
        train_rows=len(train),
        test_rows=len(test),
        base_win_rate=base_win_rate,
        decision_threshold=decision_threshold,
        predictions_path=str(predictions_path),
        threshold_report_path=str(threshold_report_path),
        strategy_report_path=str(strategy_report_path),
        table_report_path=str(table_report_path),
        summary_path=str(summary_path),
        markdown_path=str(markdown_path),
    )
    summary_path.write_text(_to_json(run), encoding="utf-8")
    markdown_path.write_text(
        _evaluation_markdown(run, threshold_df, strategy_df, table_df, min_group_rows),
        encoding="utf-8",
    )
    return run


def _prepare_training_frame(frame: Any, pandas: Any) -> Any:
    frame = pandas.DataFrame(frame).copy()
    for column in FEATURE_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    if TARGET_COLUMN not in frame.columns:
        raise ValueError(f"Missing target column: {TARGET_COLUMN}")
    frame = frame.dropna(subset=[TARGET_COLUMN])
    frame[TARGET_COLUMN] = pandas.to_numeric(frame[TARGET_COLUMN], errors="coerce")
    frame = frame.dropna(subset=[TARGET_COLUMN])
    frame[TARGET_COLUMN] = frame[TARGET_COLUMN].astype(int)
    for column in NUMERIC_FEATURES:
        frame[column] = pandas.to_numeric(frame[column], errors="coerce").fillna(0)
    for column in CATEGORICAL_FEATURES:
        frame[column] = frame[column].fillna("unknown").astype(str)
    sort_columns = [column for column in ("created_at", "table_name", "strategy_id") if column in frame.columns]
    if sort_columns:
        frame = frame.sort_values(sort_columns, kind="stable")
    return frame.reset_index(drop=True)


def _chronological_split(frame: Any, test_size: float) -> tuple[Any, Any]:
    if not 0 < test_size < 1:
        raise ValueError("test_size must be greater than 0 and less than 1.")
    split_at = int(len(frame) * (1 - test_size))
    split_at = min(max(split_at, 1), len(frame) - 1)
    return frame.iloc[:split_at].copy(), frame.iloc[split_at:].copy()


def _resolve_model_path(
    output_path: Path,
    model_name: str,
    model_path: str | Path | None,
) -> tuple[str, Path]:
    if model_path is not None:
        path = Path(model_path)
        name = model_name if model_name != "auto" else path.stem
        return name, path
    if model_name == "auto":
        for candidate_name in ("xgboost", "sklearn_logistic"):
            candidate_path = output_path / f"{candidate_name}.joblib"
            if candidate_path.exists():
                return candidate_name, candidate_path
        return "xgboost", output_path / "xgboost.joblib"
    if model_name not in {"xgboost", "sklearn_logistic"}:
        raise ValueError("model_name must be 'auto', 'xgboost', or 'sklearn_logistic'.")
    return model_name, output_path / f"{model_name}.joblib"


def _predict_win_probabilities(model: Any, X_test: Any) -> Any:
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X_test)
        return probabilities[:, 1]
    return model.predict(X_test)


def _build_threshold_report(pandas: Any, predictions: Any, thresholds: tuple[float, ...], base_win_rate: float) -> Any:
    rows: list[dict[str, Any]] = []
    total_rows = len(predictions)
    for threshold in thresholds:
        selected = predictions[predictions["ml_probability_win"] >= threshold]
        kept_rows = len(selected)
        wins = int(selected[TARGET_COLUMN].sum()) if kept_rows else 0
        losses = kept_rows - wins
        win_rate = _safe_rate(wins, kept_rows)
        pnl = round(float(selected["pnl_delta"].sum()), 6) if kept_rows else 0.0
        avg_probability = (
            round(float(selected["ml_probability_win"].mean()), 6)
            if kept_rows
            else None
        )
        rows.append(
            {
                "threshold": threshold,
                "kept_rows": kept_rows,
                "coverage": _safe_rate(kept_rows, total_rows),
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "lift_vs_base_win_rate": (
                    round(float(win_rate - base_win_rate), 6) if win_rate is not None else None
                ),
                "avg_probability": avg_probability,
                "pnl": pnl,
                "avg_pnl": _safe_rate(pnl, kept_rows),
            }
        )
    return pandas.DataFrame(rows)


def _build_group_report(
    pandas: Any,
    predictions: Any,
    group_column: str,
    decision_threshold: float,
    min_group_rows: int,
) -> Any:
    rows: list[dict[str, Any]] = []
    selected_all = predictions[predictions["ml_probability_win"] >= decision_threshold]
    group_values = sorted(str(value) for value in predictions[group_column].dropna().unique())
    for value in group_values:
        group_all = predictions[predictions[group_column].astype(str) == value]
        selected = selected_all[selected_all[group_column].astype(str) == value]
        selected_rows = len(selected)
        wins = int(selected[TARGET_COLUMN].sum()) if selected_rows else 0
        losses = selected_rows - wins
        rows.append(
            {
                group_column: value,
                "test_rows": len(group_all),
                "selected_rows": selected_rows,
                "coverage": _safe_rate(selected_rows, len(group_all)),
                "wins": wins,
                "losses": losses,
                "win_rate": _safe_rate(wins, selected_rows),
                "base_win_rate": _safe_rate(int(group_all[TARGET_COLUMN].sum()), len(group_all)),
                "avg_probability": (
                    round(float(selected["ml_probability_win"].mean()), 6)
                    if selected_rows
                    else None
                ),
                "pnl": round(float(selected["pnl_delta"].sum()), 6) if selected_rows else 0.0,
                "avg_pnl": _safe_rate(float(selected["pnl_delta"].sum()), selected_rows),
                "enough_sample": selected_rows >= min_group_rows,
            }
        )
    report = pandas.DataFrame(rows)
    if report.empty:
        return report
    return report.sort_values(["enough_sample", "win_rate", "selected_rows"], ascending=[False, False, False])


def _safe_rate(numerator: float | int, denominator: float | int) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 6)


def _preprocessor(
    ColumnTransformer: Any,
    Pipeline: Any,
    SimpleImputer: Any,
    StandardScaler: Any,
    OneHotEncoder: Any,
) -> Any:
    return ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
                        ("scaler", StandardScaler()),
                    ]
                ),
                list(NUMERIC_FEATURES),
            ),
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
                        ("onehot", _one_hot_encoder(OneHotEncoder)),
                    ]
                ),
                list(CATEGORICAL_FEATURES),
            ),
        ]
    )


def _one_hot_encoder(OneHotEncoder: Any) -> Any:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    except TypeError:  # pragma: no cover - older scikit-learn compatibility
        return OneHotEncoder(handle_unknown="ignore", sparse=True)


def _score_model(
    name: str,
    model: Any,
    X_test: Any,
    y_test: Any,
    train_rows: int,
    model_path: str,
    accuracy_score: Any,
    precision_score: Any,
    recall_score: Any,
    f1_score: Any,
    roc_auc_score: Any,
) -> ModelMetrics:
    predicted = model.predict(X_test)
    roc_auc: float | None = None
    if len(set(y_test.tolist())) > 1 and hasattr(model, "predict_proba"):
        try:
            probabilities = model.predict_proba(X_test)[:, 1]
            roc_auc = float(roc_auc_score(y_test, probabilities))
        except Exception:
            roc_auc = None
    return ModelMetrics(
        name=name,
        train_rows=train_rows,
        test_rows=len(y_test),
        accuracy=round(float(accuracy_score(y_test, predicted)), 6),
        precision=round(float(precision_score(y_test, predicted, zero_division=0)), 6),
        recall=round(float(recall_score(y_test, predicted, zero_division=0)), 6),
        f1=round(float(f1_score(y_test, predicted, zero_division=0)), 6),
        roc_auc=round(roc_auc, 6) if roc_auc is not None else None,
        model_path=model_path,
    )


def _import_duckdb() -> Any:
    try:
        import duckdb  # type: ignore

        return duckdb
    except Exception as exc:
        raise MissingMlDependency('Missing duckdb. Install with: pip install -e ".[ml]"') from exc


def _import_training_deps() -> dict[str, Any]:
    try:
        import joblib  # type: ignore
        import pandas  # type: ignore
        from sklearn.compose import ColumnTransformer  # type: ignore
        from sklearn.impute import SimpleImputer  # type: ignore
        from sklearn.linear_model import LogisticRegression  # type: ignore
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score  # type: ignore
        from sklearn.pipeline import Pipeline  # type: ignore
        from sklearn.preprocessing import OneHotEncoder, StandardScaler  # type: ignore
    except Exception as exc:
        raise MissingMlDependency('Missing ML dependencies. Install with: pip install -e ".[ml]"') from exc

    return {
        "joblib": joblib,
        "pandas": pandas,
        "ColumnTransformer": ColumnTransformer,
        "LogisticRegression": LogisticRegression,
        "accuracy_score": accuracy_score,
        "precision_score": precision_score,
        "recall_score": recall_score,
        "f1_score": f1_score,
        "roc_auc_score": roc_auc_score,
        "Pipeline": Pipeline,
        "SimpleImputer": SimpleImputer,
        "StandardScaler": StandardScaler,
        "OneHotEncoder": OneHotEncoder,
    }


def _to_json(run: TrainingRun) -> str:
    payload = asdict(run)
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def _print_run(run: TrainingRun) -> None:
    print(f"Feature rows: {run.rows}")
    print(f"Feature CSV: {run.feature_csv_path}")
    print(f"Metrics JSON: {run.metrics_path}")
    for model in run.models:
        print(
            f"{model.name}: accuracy={model.accuracy:.3f} "
            f"precision={model.precision:.3f} recall={model.recall:.3f} f1={model.f1:.3f} "
            f"roc_auc={model.roc_auc if model.roc_auc is not None else 'n/a'}"
        )
    for skipped in run.skipped_models:
        print(f"Skipped {skipped}")


def _evaluation_markdown(
    run: EvaluationRun,
    threshold_df: Any,
    strategy_df: Any,
    table_df: Any,
    min_group_rows: int,
) -> str:
    lines = [
        "# ML Evaluation Report",
        "",
        f"- Created at: `{run.created_at}`",
        f"- Model: `{run.model_name}`",
        f"- Feature rows: `{run.total_rows}`",
        f"- Train rows: `{run.train_rows}`",
        f"- Test rows: `{run.test_rows}`",
        f"- Base test win rate: `{_format_percent(run.base_win_rate)}`",
        f"- Decision threshold for group reports: `{_format_percent(run.decision_threshold)}`",
        "",
        "## Threshold Report",
        "",
        _markdown_table(
            threshold_df,
            [
                "threshold",
                "kept_rows",
                "coverage",
                "win_rate",
                "lift_vs_base_win_rate",
                "avg_probability",
                "pnl",
            ],
            limit=20,
        ),
        "",
        "## By Strategy",
        "",
        f"Rows marked `enough_sample=True` have at least `{min_group_rows}` selected rows.",
        "",
        _markdown_table(
            strategy_df,
            [
                "strategy_id",
                "test_rows",
                "selected_rows",
                "coverage",
                "base_win_rate",
                "win_rate",
                "avg_probability",
                "pnl",
                "enough_sample",
            ],
            limit=30,
        ),
        "",
        "## By Table",
        "",
        _markdown_table(
            table_df,
            [
                "table_name",
                "test_rows",
                "selected_rows",
                "coverage",
                "base_win_rate",
                "win_rate",
                "avg_probability",
                "pnl",
                "enough_sample",
            ],
            limit=50,
        ),
        "",
        "## Interpretation Rule",
        "",
        "Use the model as a filter only when a threshold keeps enough rows and its observed win rate is clearly above the base test win rate.",
    ]
    return "\n".join(lines) + "\n"


def _markdown_table(frame: Any, columns: list[str], *, limit: int) -> str:
    if frame is None or frame.empty:
        return "_No rows._"
    available = [column for column in columns if column in frame.columns]
    rows = frame[available].head(limit).to_dict("records")
    header = "| " + " | ".join(available) + " |"
    divider = "| " + " | ".join("---" for _ in available) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(_markdown_value(row.get(column)) for column in available) + " |")
    return "\n".join([header, divider, *body])


def _markdown_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _print_evaluation(run: EvaluationRun) -> None:
    print(f"Model: {run.model_name}")
    print(f"Feature rows: {run.total_rows}")
    print(f"Train rows: {run.train_rows}")
    print(f"Test rows: {run.test_rows}")
    print(f"Base test win rate: {_format_percent(run.base_win_rate)}")
    print(f"Decision threshold: {_format_percent(run.decision_threshold)}")
    print(f"Predictions CSV: {run.predictions_path}")
    print(f"Threshold report CSV: {run.threshold_report_path}")
    print(f"Strategy report CSV: {run.strategy_report_path}")
    print(f"Table report CSV: {run.table_report_path}")
    print(f"Markdown report: {run.markdown_path}")


def _parse_thresholds(raw: str) -> tuple[float, ...]:
    values: list[float] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        value = float(text)
        if not 0 < value <= 1:
            raise ValueError("thresholds must be greater than 0 and less than or equal to 1.")
        values.append(value)
    if not values:
        raise ValueError("At least one threshold is required.")
    return tuple(values)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train offline ML models from the DuckDB analytics mirror.")
    parser.add_argument("--duckdb", default=str(DEFAULT_DUCKDB_PATH), help="Path to analytics.duckdb")
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for features, metrics, models")
    parser.add_argument("--features", default=str(DEFAULT_OUTPUT_DIR / "training_features.csv"), help="Path to exported training_features.csv")
    parser.add_argument("--min-rows", type=int, default=30, help="Minimum settled win/loss paper bets required")
    parser.add_argument("--test-size", type=float, default=0.25, help="Chronological test split ratio")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--no-xgboost", action="store_true", help="Train only the scikit-learn baseline")
    parser.add_argument("--features-only", action="store_true", help="Export the DuckDB feature frame without training")
    parser.add_argument("--evaluate", action="store_true", help="Evaluate a trained model and write threshold/group reports")
    parser.add_argument("--model", default="auto", choices=("auto", "xgboost", "sklearn_logistic"), help="Model artifact to evaluate")
    parser.add_argument("--model-path", default=None, help="Explicit model .joblib path for evaluation")
    parser.add_argument("--decision-threshold", type=float, default=0.55, help="Probability threshold for strategy/table reports")
    parser.add_argument("--thresholds", default=",".join(f"{value:.2f}" for value in DEFAULT_THRESHOLDS), help="Comma-separated probability thresholds")
    parser.add_argument("--min-group-rows", type=int, default=30, help="Minimum selected rows for strategy/table sample flag")
    args = parser.parse_args(argv)

    try:
        if args.evaluate:
            run = evaluate_model(
                args.features,
                args.out,
                model_name=args.model,
                model_path=args.model_path,
                test_size=args.test_size,
                decision_threshold=args.decision_threshold,
                thresholds=_parse_thresholds(args.thresholds),
                min_group_rows=args.min_group_rows,
            )
            _print_evaluation(run)
            return 0
        if args.features_only:
            path = export_features(args.duckdb, args.out)
            print(f"Feature CSV: {path}")
            return 0
        run = train_models(
            args.duckdb,
            args.out,
            min_rows=args.min_rows,
            test_size=args.test_size,
            random_state=args.random_state,
            include_xgboost=not args.no_xgboost,
        )
        _print_run(run)
        return 0
    except (MissingMlDependency, FileNotFoundError, RuntimeError, ValueError) as exc:
        parser.exit(2, f"ML training failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
