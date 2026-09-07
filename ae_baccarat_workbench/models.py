from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_now_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Outcome(str, Enum):
    BANKER = "B"
    PLAYER = "P"
    TIE = "T"

    @property
    def vi_label(self) -> str:
        return {
            Outcome.BANKER: "Cai",
            Outcome.PLAYER: "Con",
            Outcome.TIE: "Hoa",
        }[self]


class BetSide(str, Enum):
    BANKER = "B"
    PLAYER = "P"

    @property
    def outcome(self) -> Outcome:
        return Outcome.BANKER if self is BetSide.BANKER else Outcome.PLAYER

    @property
    def vi_label(self) -> str:
        return "Cai" if self is BetSide.BANKER else "Con"


class StrategyAction(str, Enum):
    BET = "bet"
    SKIP = "skip"


class ExecutionMode(str, Enum):
    NONE = "none"
    VIRTUAL = "virtual"
    ALERT = "alert"


@dataclass(frozen=True)
class RoundEvent:
    table_name: str
    outcome: Outcome
    table_id: int | None = None
    shoe: str | int | None = None
    round_no: int | None = None
    source: str = "manual"
    observed_at: str = field(default_factory=utc_now_iso)

    @property
    def fingerprint(self) -> str:
        table_part = self.table_name or f"id:{self.table_id or 'unknown'}"
        shoe_part = self.shoe if self.shoe is not None else "shoe?"
        round_part = self.round_no if self.round_no is not None else self.observed_at
        return f"{table_part}|{shoe_part}|{round_part}|{self.outcome.value}"


@dataclass(frozen=True)
class TableSnapshot:
    table_name: str
    rounds: tuple[RoundEvent, ...]
    table_id: int | None = None
    shoe: str | int | None = None
    source: str = "manual"
    last_seen: str = field(default_factory=utc_now_iso)

    @property
    def latest_round(self) -> RoundEvent | None:
        return self.rounds[-1] if self.rounds else None

    @property
    def total_rounds(self) -> int:
        return self.current_round_no

    @property
    def observed_rounds(self) -> int:
        return len(self.rounds)

    @property
    def current_round_no(self) -> int:
        round_numbers = [r.round_no for r in self.rounds if r.round_no is not None]
        return max(round_numbers, default=len(self.rounds))

    @property
    def known_missing_rounds(self) -> int:
        return max(0, self.current_round_no - self.observed_rounds)

    def outcomes(self, *, skip_tie: bool = False) -> list[Outcome]:
        values = [r.outcome for r in self.rounds]
        if skip_tie:
            return [v for v in values if v is not Outcome.TIE]
        return values

    def compact_road(self, limit: int = 18) -> str:
        values = [r.outcome.value for r in self.rounds[-limit:]]
        return " ".join(values)

    @property
    def current_shoe_rounds(self) -> tuple[RoundEvent, ...]:
        latest = self.latest_round
        if latest is None:
            return ()
        if latest.shoe is None:
            rounds = list(self.rounds)
        else:
            rounds = [event for event in self.rounds if str(event.shoe) == str(latest.shoe)]
        rounds.sort(key=lambda event: (event.round_no if event.round_no is not None else 2147483647, event.observed_at))
        return tuple(rounds)

    def current_shoe_road(self, limit: int | None = None) -> str:
        rounds = self.current_shoe_rounds
        if limit is not None:
            rounds = rounds[-limit:]
        return " ".join(event.outcome.value for event in rounds)

    def shoe_position_ratio(self, expected_rounds: int = 72) -> float:
        if expected_rounds <= 0:
            return 0.0
        return min(1.0, self.current_round_no / expected_rounds)

    def latest_fingerprint(self) -> str:
        latest = self.latest_round
        return latest.fingerprint if latest else f"{self.table_name}|empty"


@dataclass(frozen=True)
class StrategySignal:
    table_name: str
    strategy_id: str
    action: StrategyAction
    side: BetSide | None
    confidence: float
    reason: str
    round_fingerprint: str
    features: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now_iso)

    @property
    def is_actionable(self) -> bool:
        return self.action is StrategyAction.BET and self.side is not None


@dataclass(frozen=True)
class MoneyConfig:
    stake_chain: tuple[float, ...] = (0, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200)
    progression_mode: str = "loss_up_win_reset"
    stop_loss: float = 500.0
    take_profit: float = 5000.0
    group_take_profit: float = 80.0
    group_stop_loss: float = 200.0
    banker_commission: float = 0.05


@dataclass
class MoneyState:
    index: int = 0
    pnl: float = 0.0
    group_pnl: float = 0.0
    loss_count: int = 0
    closed: bool = False
    close_reason: str = ""


@dataclass(frozen=True)
class MoneyQuote:
    stake: float
    index: int
    mode: str
    reason: str


@dataclass(frozen=True)
class PaperBet:
    table_name: str
    strategy_id: str
    side: BetSide
    stake: float
    signal_fingerprint: str
    status: str = "pending"
    outcome: Outcome | None = None
    pnl_delta: float = 0.0
    pnl_after: float = 0.0
    reason: str = ""
    created_at: str = field(default_factory=utc_now_iso)
    settled_at: str | None = None


@dataclass(frozen=True)
class LatencySample:
    table_name: str
    source: str
    current_round_no: int
    observed_rounds: int
    known_missing_rounds: int
    monitor_seen_at: str
    app_received_at: str
    engine_done_at: str
    ui_refresh_at: str
    queue_delay_ms: float
    engine_ms: float
    ui_delay_ms: float
    total_ms: float
    signal_count: int
    actionable_count: int
    pending_count: int
    created_at: str = field(default_factory=utc_now_iso_ms)


@dataclass(frozen=True)
class TableScore:
    table_name: str
    best_signal: StrategySignal | None
    display_signal: StrategySignal | None
    total_rounds: int
    observed_rounds: int
    current_round_no: int
    known_missing_rounds: int
    road: str
    score: float
    paper_pnl: float
    last_seen: str
