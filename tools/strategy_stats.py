from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


DB_PATH = Path("data/workbench.sqlite")


@dataclass
class StrategyStats:
    strategy_id: str
    settled: int = 0
    wins: int = 0
    losses: int = 0
    pushes: int = 0
    pnl: float = 0.0
    tables: set[str] | None = None
    max_win_streak: int = 0
    max_loss_streak: int = 0
    current_streak: str = "-"

    def __post_init__(self) -> None:
        if self.tables is None:
            self.tables = set()

    @property
    def decisions(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float | None:
        return self.wins / self.decisions if self.decisions else None

    @property
    def avg_pnl(self) -> float:
        return self.pnl / self.settled if self.settled else 0.0

    @property
    def wilson_lower_95(self) -> float | None:
        if not self.decisions:
            return None
        z = 1.96
        n = self.decisions
        p = self.win_rate or 0.0
        denom = 1 + z * z / n
        centre = p + z * z / (2 * n)
        margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
        return (centre - margin) / denom


def _load_rows(ml_pass_only: bool) -> list[sqlite3.Row]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    where = "WHERE status = 'settled'"
    if ml_pass_only:
        where += " AND reason LIKE 'ML pass:%'"
    rows = list(
        con.execute(
            f"""
            SELECT table_name, strategy_id, pnl_delta, settled_at, created_at, id
            FROM paper_bets
            {where}
            ORDER BY COALESCE(settled_at, created_at), created_at, id
            """
        )
    )
    con.close()
    return rows


def _summarize(rows: list[sqlite3.Row], recent_per_strategy: int | None = None) -> list[StrategyStats]:
    if recent_per_strategy is not None and recent_per_strategy > 0:
        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in reversed(rows):
            values = grouped[str(row["strategy_id"])]
            if len(values) < recent_per_strategy:
                values.append(row)
        rows = []
        for values in grouped.values():
            rows.extend(reversed(values))
        rows.sort(key=lambda row: (str(row["settled_at"] or row["created_at"]), str(row["created_at"]), int(row["id"])))

    stats: dict[str, StrategyStats] = {}
    values_by_strategy: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        strategy_id = str(row["strategy_id"])
        stat = stats.setdefault(strategy_id, StrategyStats(strategy_id))
        stat.settled += 1
        stat.pnl += float(row["pnl_delta"] or 0)
        assert stat.tables is not None
        stat.tables.add(str(row["table_name"]))
        delta = float(row["pnl_delta"] or 0)
        if delta > 0:
            stat.wins += 1
            values_by_strategy[strategy_id].append("W")
        elif delta < 0:
            stat.losses += 1
            values_by_strategy[strategy_id].append("L")
        else:
            stat.pushes += 1
    for strategy_id, values in values_by_strategy.items():
        stat = stats[strategy_id]
        run_value = ""
        run_count = 0
        for value in values:
            if value != run_value:
                run_value = value
                run_count = 1
            else:
                run_count += 1
            if value == "W":
                stat.max_win_streak = max(stat.max_win_streak, run_count)
            else:
                stat.max_loss_streak = max(stat.max_loss_streak, run_count)
        if values:
            current = values[-1]
            count = 0
            for value in reversed(values):
                if value != current:
                    break
                count += 1
            stat.current_streak = f"{current}{count}"
    return sorted(
        stats.values(),
        key=lambda item: (
            item.wilson_lower_95 or 0,
            item.win_rate or 0,
            item.pnl,
            item.decisions,
        ),
        reverse=True,
    )


def _print_report(label: str, stats: list[StrategyStats]) -> None:
    print(label)
    print(
        "strategy_id,settled,decisions,wins,losses,pushes,win_rate_ex_push,pnl,avg_pnl,"
        "tables,max_w,max_l,current,wilson_lower_95"
    )
    for stat in stats:
        tables = len(stat.tables or ())
        win_rate = "" if stat.win_rate is None else f"{stat.win_rate:.4f}"
        lower = "" if stat.wilson_lower_95 is None else f"{stat.wilson_lower_95:.4f}"
        print(
            f"{stat.strategy_id},{stat.settled},{stat.decisions},{stat.wins},{stat.losses},"
            f"{stat.pushes},{win_rate},{stat.pnl:.2f},{stat.avg_pnl:.4f},{tables},"
            f"{stat.max_win_streak},{stat.max_loss_streak},{stat.current_streak},{lower}"
        )
    print()


def main() -> int:
    if not DB_PATH.exists():
        raise SystemExit(f"Missing database: {DB_PATH}")
    all_rows = _load_rows(ml_pass_only=False)
    ml_rows = _load_rows(ml_pass_only=True)
    _print_report("ALL_SETTLED", _summarize(all_rows))
    _print_report("ML_PASS_ONLY", _summarize(ml_rows))
    _print_report("RECENT_200_PER_STRATEGY", _summarize(all_rows, recent_per_strategy=200))
    _print_report("RECENT_200_ML_PASS_PER_STRATEGY", _summarize(ml_rows, recent_per_strategy=200))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
