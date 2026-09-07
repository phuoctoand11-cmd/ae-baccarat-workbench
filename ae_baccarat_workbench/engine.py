from __future__ import annotations

import re
from dataclasses import replace
from typing import Any, Iterable

from .money import MoneyManager
from .models import BetSide, MoneyConfig, PaperBet, RoundEvent, StrategyAction, StrategySignal, TableScore, TableSnapshot
from .storage import WorkbenchStore
from .strategies import BetStrategy, StrategyContext, default_strategies


class WorkbenchEngine:
    def __init__(
        self,
        store: WorkbenchStore,
        *,
        strategies: list[BetStrategy] | None = None,
        money_config: MoneyConfig | None = None,
        min_confidence: float = 0.50,
        paper_trading_enabled: bool = True,
        expected_shoe_rounds: int = 72,
        stop_signals_after_round: int = 65,
        ml_filter: Any | None = None,
    ) -> None:
        self.store = store
        self.strategies = strategies or default_strategies()
        self.money_config = money_config or MoneyConfig()
        self.min_confidence = min_confidence
        self.paper_trading_enabled = paper_trading_enabled
        self.expected_shoe_rounds = expected_shoe_rounds
        self.stop_signals_after_round = stop_signals_after_round
        self.ml_filter = ml_filter
        self.snapshots: dict[str, TableSnapshot] = {}
        self.money: dict[tuple[str, str], MoneyManager] = {}
        self.pending: dict[tuple[str, str], PaperBet] = {}
        self.latest_signals: dict[tuple[str, str], StrategySignal] = {}
        self.latest_raw_signals: dict[tuple[str, str], StrategySignal] = {}
        self.paper_log: list[PaperBet] = []

    def ingest(self, snapshot: TableSnapshot, *, generate_signals: bool = True) -> list[StrategySignal]:
        if not snapshot.rounds:
            return []
        if self._snapshot_is_stale(snapshot):
            return []
        previous = self.snapshots.get(snapshot.table_name)
        reset_table = self._snapshot_resets_table(snapshot)
        merged = snapshot if reset_table else self._merge_snapshot(snapshot)
        self.snapshots[merged.table_name] = merged
        if previous is None or reset_table:
            rounds_to_store = list(merged.rounds)
        else:
            rounds_to_store = _new_round_events(previous, merged)
        self.store.upsert_rounds(rounds_to_store)
        latest_changed = previous is None or reset_table or _latest_round_identity(previous) != _latest_round_identity(merged)
        if previous is not None and not reset_table and not rounds_to_store and not latest_changed:
            return []
        if reset_table:
            self._reset_table_cycle(merged.table_name)
        else:
            self._settle_pending(merged)
        if not latest_changed:
            return []
        if not generate_signals:
            self._clear_latest_signals_for_table(merged.table_name)
            return []

        context = StrategyContext(merged)
        signals: list[StrategySignal] = []
        for strategy in self.strategies:
            raw_signal = self._apply_shoe_cutoff(strategy.evaluate(context), merged)
            raw_signal = self._apply_snapshot_quality_gate(raw_signal, merged)
            self.latest_raw_signals[(merged.table_name, raw_signal.strategy_id)] = raw_signal
            signal = self._apply_ml_filter(raw_signal, merged)
            self.latest_signals[(merged.table_name, signal.strategy_id)] = signal
            self.store.save_signal(signal)
            signals.append(signal)
        if self.paper_trading_enabled:
            self._arm_best_paper_signal(signals)
        return signals

    def ingest_many(self, snapshots: Iterable[TableSnapshot]) -> list[StrategySignal]:
        out: list[StrategySignal] = []
        for snapshot in snapshots:
            out.extend(self.ingest(snapshot))
        return out

    def table_scores(self) -> list[TableScore]:
        scores: list[TableScore] = []
        for table_name in sorted(self.snapshots, key=_table_sort_key):
            snapshot = self.snapshots[table_name]
            latest_fingerprint = snapshot.latest_fingerprint()
            approved_signals = [
                s
                for (t, _), s in self.latest_signals.items()
                if t == table_name and s.is_actionable and s.round_fingerprint == latest_fingerprint
            ]
            display_signals = [
                s
                for (t, _), s in self.latest_raw_signals.items()
                if t == table_name and s.is_actionable and s.round_fingerprint == latest_fingerprint
            ]
            best = max(approved_signals, key=lambda s: s.confidence, default=None)
            display = max(display_signals, key=lambda s: _display_signal_score(s, self.latest_signals), default=best)
            score = (best.confidence if best else 0.0) + min(0.12, snapshot.total_rounds / 600)
            pnl = sum(m.state.pnl for (t, _), m in self.money.items() if t == table_name)
            scores.append(
                TableScore(
                    table_name=table_name,
                    best_signal=best,
                    display_signal=display,
                    total_rounds=snapshot.total_rounds,
                    observed_rounds=snapshot.observed_rounds,
                    current_round_no=snapshot.current_round_no,
                    known_missing_rounds=snapshot.known_missing_rounds,
                    road=snapshot.current_shoe_road(),
                    score=round(score, 4),
                    paper_pnl=round(pnl, 2),
                    last_seen=snapshot.last_seen,
                )
            )
        return scores

    def signal_rows(self, limit: int = 80) -> list[StrategySignal]:
        return sorted(self.latest_signals.values(), key=lambda s: s.created_at, reverse=True)[:limit]

    def update_money_config(self, money_config: MoneyConfig) -> None:
        self.money_config = money_config
        for manager in self.money.values():
            manager.config = money_config

    def _merge_snapshot(self, incoming: TableSnapshot) -> TableSnapshot:
        current = self.snapshots.get(incoming.table_name)
        if current is None:
            return incoming
        seen = {_round_identity(event) for event in current.rounds}
        rounds = list(current.rounds)
        for event in incoming.rounds:
            identity = _round_identity(event)
            if identity not in seen:
                rounds.append(event)
                seen.add(identity)
        rounds.sort(key=_round_sort_key)
        return replace(incoming, rounds=tuple(rounds))

    def _snapshot_resets_table(self, incoming: TableSnapshot) -> bool:
        current = self.snapshots.get(incoming.table_name)
        if current is None:
            return False
        current_latest = current.latest_round
        incoming_latest = incoming.latest_round
        if current_latest is None or incoming_latest is None:
            return False
        if current_latest.shoe is not None and incoming_latest.shoe is not None:
            if str(current_latest.shoe) == str(incoming_latest.shoe):
                return False
            current_shoe_no = _shoe_number(current_latest.shoe)
            incoming_shoe_no = _shoe_number(incoming_latest.shoe)
            if current_shoe_no is not None and incoming_shoe_no is not None:
                return incoming_shoe_no > current_shoe_no
            return True
        current_round = current.current_round_no
        incoming_round = incoming.current_round_no
        return incoming_round <= 5 and current_round - incoming_round >= 10

    def _snapshot_is_stale(self, incoming: TableSnapshot) -> bool:
        current = self.snapshots.get(incoming.table_name)
        if current is None:
            return False
        current_latest = current.latest_round
        incoming_latest = incoming.latest_round
        if current_latest is None or incoming_latest is None:
            return False
        if current_latest.shoe is not None and incoming_latest.shoe is not None:
            if str(current_latest.shoe) == str(incoming_latest.shoe):
                return incoming.current_round_no < current.current_round_no
            current_shoe_no = _shoe_number(current_latest.shoe)
            incoming_shoe_no = _shoe_number(incoming_latest.shoe)
            if current_shoe_no is not None and incoming_shoe_no is not None:
                return incoming_shoe_no < current_shoe_no
        return False

    def _clear_pending_for_table(self, table_name: str) -> None:
        for key in [key for key in self.pending if key[0] == table_name]:
            del self.pending[key]

    def _reset_table_cycle(self, table_name: str) -> None:
        """Start a new prediction/money cycle while preserving stored history."""
        self._clear_pending_for_table(table_name)
        for key in [key for key in self.money if key[0] == table_name]:
            del self.money[key]
        self._clear_latest_signals_for_table(table_name)

    def _clear_latest_signals_for_table(self, table_name: str) -> None:
        for mapping in (self.latest_signals, self.latest_raw_signals):
            for key in [key for key in mapping if key[0] == table_name]:
                del mapping[key]

    def _settle_pending(self, snapshot: TableSnapshot) -> None:
        for key, bet in list(self.pending.items()):
            table_name, strategy_id = key
            if table_name != snapshot.table_name:
                continue
            result = _settlement_round_for_bet(snapshot, bet)
            if result is None:
                continue
            manager = self._money_manager(table_name, strategy_id)
            pnl_delta = manager.apply_result(bet.side, result.outcome, bet.stake)
            settled = replace(
                bet,
                status="settled",
                outcome=result.outcome,
                pnl_delta=round(pnl_delta, 2),
                pnl_after=round(manager.state.pnl, 2),
                settled_at=result.observed_at,
            )
            self.paper_log.insert(0, settled)
            self.store.save_paper_bet(settled)
            del self.pending[key]
            if manager.state.closed:
                manager.reset_group()

    def _arm_paper_if_allowed(self, signal: StrategySignal) -> None:
        if not self._is_paper_candidate(signal):
            return
        if any(
            bet.table_name == signal.table_name and bet.signal_fingerprint == signal.round_fingerprint
            for bet in self.pending.values()
        ):
            return
        key = (signal.table_name, signal.strategy_id)
        if key in self.pending:
            return
        manager = self._money_manager(signal.table_name, signal.strategy_id)
        quote = manager.quote()
        if quote.reason.startswith("Dừng"):
            return
        bet = PaperBet(
            table_name=signal.table_name,
            strategy_id=signal.strategy_id,
            side=signal.side,
            stake=quote.stake,
            signal_fingerprint=signal.round_fingerprint,
            reason=signal.reason,
        )
        self.pending[key] = bet

    def _arm_best_paper_signal(self, signals: list[StrategySignal]) -> None:
        candidates = sorted(
            (signal for signal in signals if self._is_paper_candidate(signal)),
            key=_paper_signal_rank,
            reverse=True,
        )
        # Prefer the strongest observed family: shoe profile confirmed by a
        # second independent pattern. Custom strategies still use the normal
        # ranking when this pair is unavailable.
        primary = next((s for s in candidates if s.strategy_id == "shoe_profile"), None)
        if primary is not None:
            confirmations = {
                s.strategy_id
                for s in candidates
                if s.strategy_id in {"run_length", "ensemble_majority"} and s.side is primary.side
            }
            if confirmations:
                candidates = [primary] + [s for s in candidates if s is not primary]
        for signal in candidates:
            before_count = len(self.pending)
            self._arm_paper_if_allowed(signal)
            if len(self.pending) > before_count:
                return

    def _is_paper_candidate(self, signal: StrategySignal) -> bool:
        if signal.action is not StrategyAction.BET or signal.side is None:
            return False
        if signal.confidence < self.min_confidence:
            return False
        if any(bet.table_name == signal.table_name for bet in self.pending.values()):
            return False
        return True

    def _money_manager(self, table_name: str, strategy_id: str) -> MoneyManager:
        key = (table_name, strategy_id)
        if key not in self.money:
            self.money[key] = MoneyManager(self.money_config)
        return self.money[key]

    def _apply_shoe_cutoff(self, signal: StrategySignal, snapshot: TableSnapshot) -> StrategySignal:
        if not signal.is_actionable:
            return signal
        if self.stop_signals_after_round <= 0:
            return signal
        current_round = snapshot.current_round_no
        if current_round < self.stop_signals_after_round:
            return signal
        features = dict(signal.features)
        features.update(
            {
                "current_round_no": current_round,
                "observed_rounds": snapshot.observed_rounds,
                "known_missing_rounds": snapshot.known_missing_rounds,
                "expected_shoe_rounds": self.expected_shoe_rounds,
                "stop_signals_after_round": self.stop_signals_after_round,
            }
        )
        return replace(
            signal,
            action=StrategyAction.SKIP,
            side=None,
            confidence=0.0,
            reason=(
                f"Dung signal vi gan cuoi shoe: van hien tai {current_round}, "
                f"nguong dung {self.stop_signals_after_round}"
            ),
            features=features,
        )

    def _apply_snapshot_quality_gate(self, signal: StrategySignal, snapshot: TableSnapshot) -> StrategySignal:
        if not signal.is_actionable or snapshot.known_missing_rounds <= 0:
            return signal
        features = dict(signal.features)
        features.update(
            {
                "current_round_no": snapshot.current_round_no,
                "observed_rounds": snapshot.observed_rounds,
                "known_missing_rounds": snapshot.known_missing_rounds,
                "data_quality_gate": "incomplete_shoe",
            }
        )
        return replace(
            signal,
            action=StrategyAction.SKIP,
            side=None,
            confidence=0.0,
            reason=(
                f"Dung signal vi shoe thieu {snapshot.known_missing_rounds} van: "
                f"van hien tai {snapshot.current_round_no}, da quan sat {snapshot.observed_rounds}"
            ),
            features=features,
        )

    def _apply_ml_filter(self, signal: StrategySignal, snapshot: TableSnapshot) -> StrategySignal:
        if self.ml_filter is None or not signal.is_actionable:
            return signal
        if signal.confidence < self.min_confidence:
            return signal
        manager = self._money_manager(signal.table_name, signal.strategy_id)
        quote = manager.quote()
        return self.ml_filter.apply(signal, snapshot, self.store, stake=quote.stake)


def format_signal(signal: StrategySignal | None) -> str:
    if signal is None or not signal.is_actionable or signal.side is None:
        return "Chưa có"
    return f"{signal.side.vi_label} {signal.confidence:.1%} - {signal.strategy_id}"


def signal_side_label(side: BetSide | None) -> str:
    return side.vi_label if side else "-"


def _display_signal_score(
    signal: StrategySignal,
    filtered_signals: dict[tuple[str, str], StrategySignal],
) -> float:
    filtered = filtered_signals.get((signal.table_name, signal.strategy_id))
    probability = filtered.features.get("ml_probability_win") if filtered else None
    if isinstance(probability, (int, float)):
        return float(probability)
    return signal.confidence


def _paper_signal_rank(signal: StrategySignal) -> tuple[float, float, str]:
    probability = signal.features.get("ml_probability_win")
    if isinstance(probability, (int, float)):
        ml_score = float(probability)
    else:
        ml_score = signal.confidence
    return (ml_score, signal.confidence, signal.strategy_id)


def _table_sort_key(table_name: str) -> tuple[tuple[int, object], ...]:
    parts: list[tuple[int, object]] = []
    for part in re.split(r"(\d+)", table_name):
        if not part:
            continue
        if part.isdigit():
            parts.append((1, int(part)))
        else:
            parts.append((0, part.casefold()))
    return tuple(parts)


def _new_round_events(previous: TableSnapshot, current: TableSnapshot) -> list[RoundEvent]:
    previous_identities = {_round_identity(event) for event in previous.rounds}
    return [event for event in current.rounds if _round_identity(event) not in previous_identities]


def _latest_round_identity(snapshot: TableSnapshot) -> tuple[str, str, object] | None:
    latest = snapshot.latest_round
    if latest is None:
        return None
    return _round_identity(latest)


def _settlement_round_for_bet(snapshot: TableSnapshot, bet: PaperBet) -> RoundEvent | None:
    rounds = list(snapshot.current_shoe_rounds)
    for index, event in enumerate(rounds):
        if event.fingerprint == bet.signal_fingerprint:
            next_index = index + 1
            if next_index >= len(rounds):
                return None
            next_round = rounds[next_index]
            if event.round_no is not None and next_round.round_no is not None:
                if next_round.round_no != event.round_no + 1:
                    return None
            return next_round
    return None


def _round_identity(event: object) -> tuple[str, str, object]:
    round_no = getattr(event, "round_no", None)
    if round_no is not None:
        shoe = getattr(event, "shoe", None)
        return ("round", str(shoe) if shoe is not None else "", round_no)
    return ("fingerprint", "", getattr(event, "fingerprint", ""))


def _round_sort_key(event: object) -> tuple[str, int, str]:
    shoe = getattr(event, "shoe", None)
    round_no = getattr(event, "round_no", None)
    observed_at = getattr(event, "observed_at", "")
    return (str(shoe or ""), int(round_no or 0), str(observed_at))


def _shoe_number(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None
