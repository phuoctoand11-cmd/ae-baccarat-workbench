from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


MlMode = str


@dataclass(frozen=True)
class MlDecision:
    id: int
    table_name: str
    created_at: str
    settled_at: str
    strategy_id: str
    side: str
    outcome: str
    pnl_delta: float
    signal_fingerprint: str

    @property
    def order_at(self) -> str:
        return self.settled_at or self.created_at

    @property
    def ml_result(self) -> str:
        if self.pnl_delta > 0:
            return "W"
        if self.pnl_delta < 0:
            return "L"
        return "P"


@dataclass
class X3Cycle:
    cycle_id: int
    table_name: str
    mode: MlMode
    trigger: str
    trigger_streak: int
    start_at: str
    start_row_id: int
    bets: int = 0
    pushes: int = 0
    misses_before_win: int = 0
    pnl: float = 0.0
    current_stake: float = 10.0
    max_stake: float = 10.0
    max_capital_at_risk: float = 10.0
    max_drawdown: float = 0.0
    closed: bool = False
    end_at: str | None = None
    end_row_id: int | None = None
    win_pnl: float = 0.0


@dataclass(frozen=True)
class X3Summary:
    cycles: int
    closed: int
    open: int
    pnl: float
    avg_pnl_closed: float
    total_bets_closed: int
    pushes_closed: int
    max_misses_before_win: int
    max_single_stake: float
    max_capital_at_risk: float
    max_cycle_drawdown: float


@dataclass(frozen=True)
class X3ParallelRisk:
    at: str
    active_cycles: int
    next_stake_sum: float
    risk_plus_next_stake: float
    unrealized_pnl: float
    tables: tuple[str, ...]


@dataclass(frozen=True)
class X3SimulationReport:
    cutoff: str | None
    duplicate_groups_before_cutoff: int
    rows_used: int
    first_at: str | None
    last_at: str | None
    table_count: int
    cycles: tuple[X3Cycle, ...]
    open_cycles: tuple[X3Cycle, ...]
    summary_all: X3Summary
    summary_reverse: X3Summary
    summary_follow: X3Summary
    max_parallel_risk: X3ParallelRisk


def run_x3_simulation(
    rows: Iterable[Any],
    *,
    cutoff: str | None = None,
    duplicate_groups_before_cutoff: int = 0,
    base_stake: float = 10.0,
    multiplier: float = 3.0,
    w_trigger: int = 8,
    l_trigger: int = 6,
    banker_commission: float = 0.05,
) -> X3SimulationReport:
    decisions = [_decision_from_row(row) for row in rows]
    decisions = [decision for decision in decisions if decision.side in {"B", "P"} and decision.outcome in {"B", "P", "T"}]
    decisions.sort(key=lambda decision: (decision.order_at, decision.created_at, decision.id))

    states: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"streak_value": "", "streak_count": 0, "active": None}
    )
    closed_cycles: list[X3Cycle] = []
    next_cycle_id = 1
    max_parallel = X3ParallelRisk("", 0, 0.0, 0.0, 0.0, ())

    for decision in decisions:
        state = states[decision.table_name]
        active = state["active"]
        if active is None:
            trigger_value = str(state["streak_value"])
            trigger_count = int(state["streak_count"])
            if trigger_value == "W" and trigger_count >= w_trigger:
                active = X3Cycle(
                    cycle_id=next_cycle_id,
                    table_name=decision.table_name,
                    mode="reverse",
                    trigger=f"W{trigger_count}",
                    trigger_streak=trigger_count,
                    start_at=decision.order_at,
                    start_row_id=decision.id,
                    current_stake=base_stake,
                    max_stake=base_stake,
                    max_capital_at_risk=base_stake,
                )
                next_cycle_id += 1
                state["active"] = active
            elif trigger_value == "L" and trigger_count >= l_trigger:
                active = X3Cycle(
                    cycle_id=next_cycle_id,
                    table_name=decision.table_name,
                    mode="follow",
                    trigger=f"L{trigger_count}",
                    trigger_streak=trigger_count,
                    start_at=decision.order_at,
                    start_row_id=decision.id,
                    current_stake=base_stake,
                    max_stake=base_stake,
                    max_capital_at_risk=base_stake,
                )
                next_cycle_id += 1
                state["active"] = active

        if active is not None:
            _apply_cycle_decision(
                active,
                decision,
                multiplier=multiplier,
                banker_commission=banker_commission,
            )
            if active.closed:
                closed_cycles.append(active)
                state["active"] = None

        _update_streak(state, decision.ml_result)
        max_parallel = _max_parallel_risk(states, decision.order_at, max_parallel)

    open_cycles = tuple(
        state["active"]
        for state in states.values()
        if isinstance(state.get("active"), X3Cycle) and not state["active"].closed
    )
    cycles = tuple(closed_cycles)
    table_names = {decision.table_name for decision in decisions}
    return X3SimulationReport(
        cutoff=cutoff,
        duplicate_groups_before_cutoff=duplicate_groups_before_cutoff,
        rows_used=len(decisions),
        first_at=decisions[0].order_at if decisions else None,
        last_at=decisions[-1].order_at if decisions else None,
        table_count=len(table_names),
        cycles=cycles,
        open_cycles=open_cycles,
        summary_all=_summarize((*cycles, *open_cycles)),
        summary_reverse=_summarize([cycle for cycle in (*cycles, *open_cycles) if cycle.mode == "reverse"]),
        summary_follow=_summarize([cycle for cycle in (*cycles, *open_cycles) if cycle.mode == "follow"]),
        max_parallel_risk=max_parallel,
    )


def _decision_from_row(row: Any) -> MlDecision:
    return MlDecision(
        id=int(_row_value(row, "id") or 0),
        table_name=str(_row_value(row, "table_name") or ""),
        created_at=str(_row_value(row, "created_at") or ""),
        settled_at=str(_row_value(row, "settled_at") or _row_value(row, "created_at") or ""),
        strategy_id=str(_row_value(row, "strategy_id") or ""),
        side=str(_row_value(row, "side") or ""),
        outcome=str(_row_value(row, "outcome") or ""),
        pnl_delta=float(_row_value(row, "pnl_delta") or 0.0),
        signal_fingerprint=str(_row_value(row, "signal_fingerprint") or ""),
    )


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, None)


def _apply_cycle_decision(
    cycle: X3Cycle,
    decision: MlDecision,
    *,
    multiplier: float,
    banker_commission: float,
) -> None:
    bet_side = _opposite_side(decision.side) if cycle.mode == "reverse" else decision.side
    stake = cycle.current_stake
    delta = _bet_pnl(bet_side, decision.outcome, stake, banker_commission)
    cycle.bets += 1
    if decision.outcome == "T":
        cycle.pushes += 1
        return
    cycle.pnl = round(cycle.pnl + delta, 10)
    cycle.max_drawdown = min(cycle.max_drawdown, cycle.pnl)
    if delta > 0:
        cycle.closed = True
        cycle.end_at = decision.order_at
        cycle.end_row_id = decision.id
        cycle.win_pnl = delta
        return
    cycle.misses_before_win += 1
    cycle.current_stake = round(cycle.current_stake * multiplier, 10)
    cycle.max_stake = max(cycle.max_stake, cycle.current_stake)
    cycle.max_capital_at_risk += cycle.current_stake


def _update_streak(state: dict[str, Any], result: str) -> None:
    if result not in {"W", "L"}:
        return
    if result == state["streak_value"]:
        state["streak_count"] = int(state["streak_count"]) + 1
    else:
        state["streak_value"] = result
        state["streak_count"] = 1


def _opposite_side(side: str) -> str:
    return "P" if side == "B" else "B"


def _bet_pnl(bet_side: str, outcome: str, stake: float, banker_commission: float) -> float:
    if stake <= 0 or outcome == "T":
        return 0.0
    if bet_side != outcome:
        return -stake
    if bet_side == "B":
        return stake * (1.0 - banker_commission)
    return stake


def _summarize(cycles: Iterable[X3Cycle]) -> X3Summary:
    values = list(cycles)
    closed = [cycle for cycle in values if cycle.closed]
    open_cycles = [cycle for cycle in values if not cycle.closed]
    pnl = sum(cycle.pnl for cycle in closed)
    return X3Summary(
        cycles=len(values),
        closed=len(closed),
        open=len(open_cycles),
        pnl=round(pnl, 2),
        avg_pnl_closed=round(pnl / len(closed), 2) if closed else 0.0,
        total_bets_closed=sum(cycle.bets for cycle in closed),
        pushes_closed=sum(cycle.pushes for cycle in closed),
        max_misses_before_win=max((cycle.misses_before_win for cycle in closed), default=0),
        max_single_stake=round(max((cycle.max_stake for cycle in values), default=0.0), 2),
        max_capital_at_risk=round(max((cycle.max_capital_at_risk for cycle in values), default=0.0), 2),
        max_cycle_drawdown=round(min((cycle.max_drawdown for cycle in values), default=0.0), 2),
    )


def _max_parallel_risk(
    states: dict[str, dict[str, Any]],
    at: str,
    current_max: X3ParallelRisk,
) -> X3ParallelRisk:
    active_cycles = [
        state["active"]
        for state in states.values()
        if isinstance(state.get("active"), X3Cycle) and not state["active"].closed
    ]
    if not active_cycles:
        return current_max
    next_stake_sum = sum(cycle.current_stake for cycle in active_cycles)
    unrealized = sum(cycle.pnl for cycle in active_cycles)
    risk_plus_next = sum(max(0.0, -cycle.pnl) + cycle.current_stake for cycle in active_cycles)
    candidate = X3ParallelRisk(
        at=at,
        active_cycles=len(active_cycles),
        next_stake_sum=round(next_stake_sum, 2),
        risk_plus_next_stake=round(risk_plus_next, 2),
        unrealized_pnl=round(unrealized, 2),
        tables=tuple(
            f"{cycle.table_name} {cycle.mode} {cycle.trigger} pnl={cycle.pnl:.2f} next={cycle.current_stake:.2f}"
            for cycle in active_cycles
        ),
    )
    if (
        candidate.risk_plus_next_stake,
        candidate.next_stake_sum,
        candidate.active_cycles,
    ) > (
        current_max.risk_plus_next_stake,
        current_max.next_stake_sum,
        current_max.active_cycles,
    ):
        return candidate
    return current_max
