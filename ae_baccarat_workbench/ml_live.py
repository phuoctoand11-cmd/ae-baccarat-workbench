from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .ml import CATEGORICAL_FEATURES, FEATURE_COLUMNS, NUMERIC_FEATURES
from .models import Outcome, RoundEvent, StrategyAction, StrategySignal, TableSnapshot
from .storage import WorkbenchStore


LIVE_FEATURE_ROUND_WINDOW = 120
LIVE_FEATURE_PAPER_WINDOW = 120


@dataclass(frozen=True)
class MlFilterStatus:
    enabled: bool
    ready: bool
    model_path: str
    threshold: float
    error: str | None = None


class MlSignalFilter:
    def __init__(self, model_path: str | Path, *, threshold: float = 0.55, enabled: bool = True) -> None:
        self.model_path = Path(model_path)
        self.threshold = max(0.0, min(1.0, float(threshold)))
        self.enabled = bool(enabled)
        self.ready = False
        self.error: str | None = None
        self.model: Any = None
        self.pandas: Any = None
        self._feature_cache: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        if not self.enabled:
            self.error = "disabled by config"
            return
        if not self.model_path.exists():
            self.error = f"model file not found: {self.model_path}"
            return
        try:
            import joblib  # type: ignore
            import pandas  # type: ignore

            self.pandas = pandas
            self.model = joblib.load(self.model_path)
            self.ready = True
        except Exception as exc:
            self.error = str(exc)

    @property
    def status(self) -> MlFilterStatus:
        return MlFilterStatus(
            enabled=self.enabled,
            ready=self.ready,
            model_path=str(self.model_path),
            threshold=self.threshold,
            error=self.error,
        )

    def status_message(self) -> str:
        if not self.enabled:
            return "ML filter disabled by config."
        if self.ready:
            return f"ML filter enabled: {self.model_path} threshold={self.threshold:.1%}"
        return f"ML filter NOT ready: {self.error or 'unknown error'}"

    def apply(
        self,
        signal: StrategySignal,
        snapshot: TableSnapshot,
        store: WorkbenchStore,
        *,
        stake: float,
    ) -> StrategySignal:
        if not signal.is_actionable:
            return signal
        if not self.enabled:
            return signal
        if not self.ready:
            features = dict(signal.features)
            features.update(
                {
                    "ml_filter_error": self.error or "model not ready",
                    "ml_filter_model": str(self.model_path),
                    "ml_filter_threshold": self.threshold,
                    "strategy_side": signal.side.value if signal.side else None,
                    "strategy_confidence": signal.confidence,
                }
            )
            return replace(
                signal,
                action=StrategyAction.SKIP,
                side=None,
                confidence=0.0,
                reason=f"ML filter not ready, skip signal: {self.error or 'model not ready'}",
                features=features,
            )
        try:
            cache_key = (
                signal.table_name,
                signal.strategy_id,
                signal.round_fingerprint,
                str(snapshot.shoe),
            )
            row = self._feature_cache.get(cache_key)
            if row is None:
                row = build_live_feature_row(signal, snapshot, store, stake=stake)
                if len(self._feature_cache) >= 5000:
                    self._feature_cache.clear()
                self._feature_cache[cache_key] = dict(row)
            else:
                row = dict(row)
            probability = self._predict_probability(row)
        except Exception as exc:
            features = dict(signal.features)
            features.update(
                {
                    "ml_filter_error": str(exc),
                    "ml_filter_model": str(self.model_path),
                    "ml_filter_threshold": self.threshold,
                    "strategy_side": signal.side.value if signal.side else None,
                    "strategy_confidence": signal.confidence,
                }
            )
            return replace(
                signal,
                action=StrategyAction.SKIP,
                side=None,
                confidence=0.0,
                reason=f"ML filter error, skip signal: {exc}",
                features=features,
            )

        features = dict(signal.features)
        features.update(
            {
                "ml_probability_win": round(probability, 6),
                "ml_filter_threshold": self.threshold,
                "ml_filter_model": str(self.model_path),
                "strategy_side": signal.side.value if signal.side else None,
                "strategy_confidence": signal.confidence,
            }
        )
        if probability < self.threshold:
            return replace(
                signal,
                action=StrategyAction.SKIP,
                side=None,
                confidence=probability,
                reason=(
                    f"ML skip: win probability {probability:.1%} < "
                    f"threshold {self.threshold:.1%}; base signal {signal.side.vi_label if signal.side else '-'} "
                    f"{signal.confidence:.1%} from {signal.strategy_id}"
                ),
                features=features,
            )
        return replace(
            signal,
            confidence=probability,
            reason=f"ML pass: win probability {probability:.1%} >= threshold {self.threshold:.1%}; {signal.reason}",
            features=features,
        )

    def _predict_probability(self, row: dict[str, Any]) -> float:
        if self.pandas is None or self.model is None:
            raise RuntimeError("ML model is not loaded")
        frame = self.pandas.DataFrame([row])
        for column in NUMERIC_FEATURES:
            if column not in frame.columns:
                frame[column] = 0
            frame[column] = self.pandas.to_numeric(frame[column], errors="coerce").fillna(0)
        for column in CATEGORICAL_FEATURES:
            if column not in frame.columns:
                frame[column] = "unknown"
            frame[column] = frame[column].fillna("unknown").astype(str)
        probabilities = self.model.predict_proba(frame[list(FEATURE_COLUMNS)])
        return max(0.0, min(1.0, float(probabilities[0][1])))


def build_live_feature_row(
    signal: StrategySignal,
    snapshot: TableSnapshot,
    store: WorkbenchStore,
    *,
    stake: float,
) -> dict[str, Any]:
    latest = _signal_round(snapshot, signal.round_fingerprint)
    signal_round_no = latest.round_no if latest and latest.round_no is not None else snapshot.current_round_no
    signal_shoe = latest.shoe if latest else snapshot.shoe
    shoe_rounds = _known_shoe_rounds(snapshot, latest, signal.created_at)
    table_rounds = _table_round_rows_before(store, signal.table_name, signal.created_at)
    strategy_rows = _paper_rows_before(
        store,
        signal.created_at,
        table_name=signal.table_name,
        strategy_id=signal.strategy_id,
    )
    table_bet_rows = _paper_rows_before(store, signal.created_at, table_name=signal.table_name)
    global_strategy_rows = _paper_rows_before(store, signal.created_at, strategy_id=signal.strategy_id)

    shoe_counts = _round_counts(shoe_rounds)
    table_counts = _stored_round_counts(table_rounds)
    strategy_stats = _paper_stats(strategy_rows)
    strategy_streaks = _streak_stats(strategy_rows)
    strategy_recent = _recent_decision_stats(strategy_rows, 10)
    table_stats = _paper_stats(table_bet_rows)
    global_strategy_stats = _paper_stats(global_strategy_rows)
    global_recent = _recent_decision_stats(global_strategy_rows, 30)

    shoe_observed = len(shoe_rounds)
    shoe_current = max(
        [r.round_no for r in shoe_rounds if r.round_no is not None],
        default=shoe_observed,
    )
    table_seen = table_counts["total"]
    latest_outcome = latest.outcome.value if latest else "unknown"

    row: dict[str, Any] = {
        "created_at": signal.created_at,
        "table_name": signal.table_name,
        "strategy_id": signal.strategy_id,
        "side": signal.side.value if signal.side else "unknown",
        "signal_fingerprint": signal.round_fingerprint,
        "signal_table_id": latest.table_id if latest else snapshot.table_id,
        "signal_shoe": str(signal_shoe) if signal_shoe is not None else None,
        "target_win": 0,
        "stake": float(stake),
        "side_is_banker": 1 if signal.side and signal.side.value == "B" else 0,
        "side_is_player": 1 if signal.side and signal.side.value == "P" else 0,
        "signal_confidence": signal.confidence,
        "signal_round_outcome": latest_outcome,
        "signal_round_no": signal_round_no or 0,
        "signal_seq_no": _signal_seq_no(shoe_rounds, latest),
        "signal_outcome_streak_len": _outcome_streak_len(shoe_rounds, latest),
        "shoe_observed_rounds_to_signal": shoe_observed,
        "shoe_current_round_no_to_signal": shoe_current,
        "shoe_known_missing_rounds_to_signal": max(0, int(shoe_current) - shoe_observed),
        "shoe_banker_rounds_to_signal": shoe_counts["B"],
        "shoe_player_rounds_to_signal": shoe_counts["P"],
        "shoe_tie_rounds_to_signal": shoe_counts["T"],
        "shoe_banker_ratio_to_signal": _ratio(shoe_counts["B"], shoe_observed),
        "shoe_player_ratio_to_signal": _ratio(shoe_counts["P"], shoe_observed),
        "shoe_tie_ratio_to_signal": _ratio(shoe_counts["T"], shoe_observed),
        "table_seen_rounds_to_signal": table_seen,
        "table_seen_banker_ratio_to_signal": _ratio(table_counts["B"], table_seen),
        "table_seen_player_ratio_to_signal": _ratio(table_counts["P"], table_seen),
        "table_seen_tie_ratio_to_signal": _ratio(table_counts["T"], table_seen),
        "prev_wl_result": strategy_streaks["prev_wl_result"],
        "prev_bet_win": strategy_streaks["prev_bet_win"],
        "prev_bet_loss": strategy_streaks["prev_bet_loss"],
        "prev_wl_streak_len": strategy_streaks["prev_wl_streak_len"],
        "rolling_strategy_settled_bets_to_signal": strategy_stats["settled"],
        "rolling_strategy_wins_to_signal": strategy_stats["wins"],
        "rolling_strategy_losses_to_signal": strategy_stats["losses"],
        "rolling_strategy_pushes_to_signal": strategy_stats["pushes"],
        "rolling_strategy_decisions_to_signal": strategy_stats["decisions"],
        "rolling_strategy_win_rate_to_signal": strategy_stats["win_rate"],
        "rolling_strategy_pnl_to_signal": strategy_stats["pnl"],
        "rolling_strategy_max_win_streak_to_signal": strategy_streaks["max_win_streak"],
        "rolling_strategy_max_loss_streak_to_signal": strategy_streaks["max_loss_streak"],
        "rolling_strategy_recent_10_decisions": strategy_recent["decisions"],
        "rolling_strategy_recent_10_win_rate": strategy_recent["win_rate"],
        "rolling_strategy_recent_10_pnl": strategy_recent["pnl"],
        "rolling_table_settled_bets_to_signal": table_stats["settled"],
        "rolling_table_win_rate_to_signal": table_stats["win_rate"],
        "rolling_table_pnl_to_signal": table_stats["pnl"],
        "rolling_global_strategy_settled_bets_to_signal": global_strategy_stats["settled"],
        "rolling_global_strategy_win_rate_to_signal": global_strategy_stats["win_rate"],
        "rolling_global_strategy_pnl_to_signal": global_strategy_stats["pnl"],
        "rolling_global_strategy_recent_30_decisions": global_recent["decisions"],
        "rolling_global_strategy_recent_30_win_rate": global_recent["win_rate"],
    }
    _add_recent_round_features(row, shoe_rounds, 6)
    _add_recent_round_features(row, shoe_rounds, 12)
    return row


def _signal_round(snapshot: TableSnapshot, fingerprint: str) -> RoundEvent | None:
    for event in snapshot.rounds:
        if event.fingerprint == fingerprint:
            return event
    return snapshot.latest_round


def _known_shoe_rounds(snapshot: TableSnapshot, latest: RoundEvent | None, signal_created_at: str) -> list[RoundEvent]:
    if latest is None:
        return []
    rounds = [
        event
        for event in snapshot.rounds
        if _same_shoe(event.shoe, latest.shoe)
        and _round_not_after(event, latest)
        and event.observed_at <= signal_created_at
    ]
    rounds.sort(key=lambda event: (event.round_no if event.round_no is not None else 2147483647, event.observed_at, event.fingerprint))
    if latest not in rounds:
        rounds.append(latest)
        rounds.sort(
            key=lambda event: (
                event.round_no if event.round_no is not None else 2147483647,
                event.observed_at,
                event.fingerprint,
            )
        )
    return rounds


def _same_shoe(left: object, right: object) -> bool:
    return str(left) == str(right)


def _round_not_after(event: RoundEvent, latest: RoundEvent) -> bool:
    if event.round_no is None or latest.round_no is None:
        return event.observed_at <= latest.observed_at
    return event.round_no <= latest.round_no


def _signal_seq_no(shoe_rounds: list[RoundEvent], latest: RoundEvent | None) -> int:
    if latest is None:
        return 0
    for index, event in enumerate(shoe_rounds, start=1):
        if event.fingerprint == latest.fingerprint:
            return index
    return len(shoe_rounds)


def _outcome_streak_len(shoe_rounds: list[RoundEvent], latest: RoundEvent | None) -> int:
    if latest is None:
        return 0
    count = 0
    for event in reversed(shoe_rounds):
        if event.outcome is latest.outcome:
            count += 1
        else:
            break
    return count


def _round_counts(rounds: list[RoundEvent]) -> dict[str, int]:
    return {
        "B": sum(1 for event in rounds if event.outcome is Outcome.BANKER),
        "P": sum(1 for event in rounds if event.outcome is Outcome.PLAYER),
        "T": sum(1 for event in rounds if event.outcome is Outcome.TIE),
    }


def _add_recent_round_features(row: dict[str, Any], shoe_rounds: list[RoundEvent], window: int) -> None:
    recent = shoe_rounds[-window:]
    counts = _round_counts(recent)
    total = len(recent)
    prefix = f"shoe_last_{window}"
    row[f"{prefix}_rounds"] = total
    row[f"{prefix}_banker_ratio"] = _ratio(counts["B"], total)
    row[f"{prefix}_player_ratio"] = _ratio(counts["P"], total)
    row[f"{prefix}_tie_ratio"] = _ratio(counts["T"], total)


def _table_round_rows_before(store: WorkbenchStore, table_name: str, before: str) -> list[Any]:
    rows = list(
        store.conn.execute(
            """
            SELECT outcome
            FROM rounds AS r
            WHERE table_name = ? AND observed_at <= ?
              AND NOT EXISTS (
                SELECT 1
                FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'rounds')
                  AND r.observed_at >= dq.started_at
                  AND r.observed_at <= dq.ended_at
              )
            ORDER BY observed_at DESC, COALESCE(round_no, 2147483647) DESC, fingerprint DESC
            LIMIT ?
            """,
            (table_name, before, LIVE_FEATURE_ROUND_WINDOW),
        )
    )
    rows.reverse()
    return rows


def _stored_round_counts(rows: list[Any]) -> dict[str, int]:
    return {
        "total": len(rows),
        "B": sum(1 for row in rows if row["outcome"] == "B"),
        "P": sum(1 for row in rows if row["outcome"] == "P"),
        "T": sum(1 for row in rows if row["outcome"] == "T"),
    }


def _paper_rows_before(
    store: WorkbenchStore,
    before: str,
    *,
    table_name: str | None = None,
    strategy_id: str | None = None,
) -> list[Any]:
    clauses = [
        "status = 'settled'",
        "COALESCE(settled_at, created_at) < ?",
        """NOT EXISTS (
            SELECT 1
            FROM data_quality_exclusions AS dq
            WHERE dq.scope IN ('all', 'paper_bets')
              AND COALESCE(paper_bets.settled_at, paper_bets.created_at) >= dq.started_at
              AND COALESCE(paper_bets.settled_at, paper_bets.created_at) <= dq.ended_at
        )""",
    ]
    params: list[Any] = [before]
    if table_name is not None:
        clauses.append("table_name = ?")
        params.append(table_name)
    if strategy_id is not None:
        clauses.append("strategy_id = ?")
        params.append(strategy_id)
    sql = f"""
        SELECT created_at, settled_at, table_name, strategy_id, side, outcome, pnl_delta, signal_fingerprint
        FROM paper_bets
        WHERE {' AND '.join(clauses)}
        ORDER BY COALESCE(settled_at, created_at) DESC, created_at DESC, signal_fingerprint DESC
        LIMIT {LIVE_FEATURE_PAPER_WINDOW}
    """
    rows = list(store.conn.execute(sql, params))
    rows.reverse()
    return rows


def _paper_stats(rows: list[Any]) -> dict[str, float | int]:
    decisions = [_paper_decision(row) for row in rows]
    wins = sum(1 for value in decisions if value == "win")
    losses = sum(1 for value in decisions if value == "loss")
    pushes = sum(1 for value in decisions if value == "push")
    decision_count = wins + losses
    pnl = sum(float(row["pnl_delta"] or 0) for row in rows)
    return {
        "settled": len(rows),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "decisions": decision_count,
        "win_rate": _ratio(wins, decision_count),
        "pnl": pnl,
    }


def _streak_stats(rows: list[Any]) -> dict[str, int | str]:
    current = ""
    current_len = 0
    max_win = 0
    max_loss = 0
    last_result = "none"
    last_len = 0
    for row in rows:
        result = _paper_decision(row)
        if result not in {"win", "loss"}:
            continue
        if result != current:
            current = result
            current_len = 1
        else:
            current_len += 1
        if result == "win":
            max_win = max(max_win, current_len)
        else:
            max_loss = max(max_loss, current_len)
        last_result = result
        last_len = current_len
    return {
        "prev_wl_result": last_result,
        "prev_bet_win": 1 if last_result == "win" else 0,
        "prev_bet_loss": 1 if last_result == "loss" else 0,
        "prev_wl_streak_len": last_len,
        "max_win_streak": max_win,
        "max_loss_streak": max_loss,
    }


def _recent_decision_stats(rows: list[Any], limit: int) -> dict[str, float | int]:
    decisions = [(row, _paper_decision(row)) for row in rows]
    decisions = [(row, result) for row, result in decisions if result in {"win", "loss"}][-limit:]
    wins = sum(1 for _, result in decisions if result == "win")
    pnl = sum(float(row["pnl_delta"] or 0) for row, _ in decisions)
    return {
        "decisions": len(decisions),
        "win_rate": _ratio(wins, len(decisions)),
        "pnl": pnl,
    }


def _paper_decision(row: Any) -> str:
    outcome = row["outcome"]
    side = row["side"]
    if outcome is None:
        return "pending"
    if outcome == "T":
        return "push"
    return "win" if side == outcome else "loss"


def _ratio(numerator: float | int, denominator: float | int) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)
