from __future__ import annotations

import contextlib
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Iterable

from .models import LatencySample, PaperBet, RoundEvent, StrategySignal

logger = logging.getLogger(__name__)


def rolling_feature_select_sql() -> str:
    return """
    WITH signal_rows AS (
      SELECT
        created_at AS signal_recorded_at,
        table_name,
        strategy_id,
        action AS signal_action,
        side AS signal_side,
        confidence AS signal_confidence,
        round_fingerprint
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY table_name, strategy_id, round_fingerprint
            ORDER BY created_at DESC
          ) AS rn
        FROM signals AS raw_signals
        WHERE NOT EXISTS (
          SELECT 1
          FROM data_quality_exclusions AS dq
          WHERE dq.scope IN ('all', 'signals')
            AND raw_signals.created_at >= dq.started_at
            AND raw_signals.created_at <= dq.ended_at
        )
      ) ranked_signals
      WHERE rn = 1
    ),
    legacy_table_keys AS (
      SELECT table_name, MAX(table_id) AS table_id
      FROM table_round_summary
      GROUP BY table_name
    ),
    legacy_strategy_keys AS (
      SELECT DISTINCT table_name, strategy_id
      FROM strategy_performance
    ),
    base AS (
      SELECT
        b.created_at,
        b.settled_at,
        b.table_name,
        b.strategy_id,
        b.side,
        b.stake,
        b.signal_fingerprint,
        b.outcome AS settled_outcome,
        b.wl_result,
        b.pnl_delta,
        b.pnl_after,
        CASE WHEN b.wl_result = 'win' THEN 1 ELSE 0 END AS target_win,
        CASE WHEN b.side = 'B' THEN 1 ELSE 0 END AS side_is_banker,
        CASE WHEN b.side = 'P' THEN 1 ELSE 0 END AS side_is_player,
        COALESCE(s.signal_confidence, 0) AS signal_confidence,
        s.signal_recorded_at
      FROM paper_bet_results b
      LEFT JOIN signal_rows s
        ON s.table_name = b.table_name
       AND s.strategy_id = b.strategy_id
       AND s.round_fingerprint = b.signal_fingerprint
      LEFT JOIN legacy_strategy_keys lsk
        ON lsk.table_name = b.table_name
       AND lsk.strategy_id = b.strategy_id
      WHERE b.status = 'settled'
        AND b.wl_result IN ('win', 'loss')
    ),
    anchored AS (
      SELECT
        b.*,
        COALESCE(r.table_id, ltk.table_id) AS signal_table_id,
        r.shoe AS signal_shoe,
        r.round_no AS signal_round_no,
        r.outcome AS signal_round_outcome,
        r.seq_no AS signal_seq_no,
        r.outcome_streak_len AS signal_outcome_streak_len
      FROM base b
      LEFT JOIN round_streaks r
        ON r.fingerprint = b.signal_fingerprint
      LEFT JOIN legacy_table_keys ltk
        ON ltk.table_name = b.table_name
    )
    SELECT
      b.created_at,
      b.settled_at,
      b.table_name,
      b.strategy_id,
      b.side,
      b.signal_fingerprint,
      b.signal_recorded_at,
      b.signal_table_id,
      b.signal_shoe,
      b.settled_outcome,
      b.wl_result,
      b.pnl_delta,
      b.pnl_after,
      b.target_win,
      b.stake,
      b.side_is_banker,
      b.side_is_player,
      b.signal_confidence,
      COALESCE(b.signal_round_outcome, 'unknown') AS signal_round_outcome,
      COALESCE(b.signal_round_no, 0) AS signal_round_no,
      COALESCE(b.signal_seq_no, 0) AS signal_seq_no,
      COALESCE(b.signal_outcome_streak_len, 0) AS signal_outcome_streak_len,
      COALESCE(shoe.shoe_observed_rounds_to_signal, 0) AS shoe_observed_rounds_to_signal,
      COALESCE(shoe.shoe_current_round_no_to_signal, 0) AS shoe_current_round_no_to_signal,
      GREATEST(
        COALESCE(shoe.shoe_current_round_no_to_signal, 0) - COALESCE(shoe.shoe_observed_rounds_to_signal, 0),
        0
      ) AS shoe_known_missing_rounds_to_signal,
      COALESCE(shoe.shoe_banker_rounds_to_signal, 0) AS shoe_banker_rounds_to_signal,
      COALESCE(shoe.shoe_player_rounds_to_signal, 0) AS shoe_player_rounds_to_signal,
      COALESCE(shoe.shoe_tie_rounds_to_signal, 0) AS shoe_tie_rounds_to_signal,
      COALESCE(
        CAST(shoe.shoe_banker_rounds_to_signal AS DOUBLE) /
          NULLIF(shoe.shoe_observed_rounds_to_signal, 0),
        0
      ) AS shoe_banker_ratio_to_signal,
      COALESCE(
        CAST(shoe.shoe_player_rounds_to_signal AS DOUBLE) /
          NULLIF(shoe.shoe_observed_rounds_to_signal, 0),
        0
      ) AS shoe_player_ratio_to_signal,
      COALESCE(
        CAST(shoe.shoe_tie_rounds_to_signal AS DOUBLE) /
          NULLIF(shoe.shoe_observed_rounds_to_signal, 0),
        0
      ) AS shoe_tie_ratio_to_signal,
      COALESCE(last6.last_6_rounds, 0) AS shoe_last_6_rounds,
      COALESCE(
        CAST(last6.last_6_banker_rounds AS DOUBLE) / NULLIF(last6.last_6_rounds, 0),
        0
      ) AS shoe_last_6_banker_ratio,
      COALESCE(
        CAST(last6.last_6_player_rounds AS DOUBLE) / NULLIF(last6.last_6_rounds, 0),
        0
      ) AS shoe_last_6_player_ratio,
      COALESCE(
        CAST(last6.last_6_tie_rounds AS DOUBLE) / NULLIF(last6.last_6_rounds, 0),
        0
      ) AS shoe_last_6_tie_ratio,
      COALESCE(last12.last_12_rounds, 0) AS shoe_last_12_rounds,
      COALESCE(
        CAST(last12.last_12_banker_rounds AS DOUBLE) / NULLIF(last12.last_12_rounds, 0),
        0
      ) AS shoe_last_12_banker_ratio,
      COALESCE(
        CAST(last12.last_12_player_rounds AS DOUBLE) / NULLIF(last12.last_12_rounds, 0),
        0
      ) AS shoe_last_12_player_ratio,
      COALESCE(
        CAST(last12.last_12_tie_rounds AS DOUBLE) / NULLIF(last12.last_12_rounds, 0),
        0
      ) AS shoe_last_12_tie_ratio,
      COALESCE(table_seen.table_seen_rounds_to_signal, 0) AS table_seen_rounds_to_signal,
      COALESCE(
        CAST(table_seen.table_seen_banker_rounds_to_signal AS DOUBLE) /
          NULLIF(table_seen.table_seen_rounds_to_signal, 0),
        0
      ) AS table_seen_banker_ratio_to_signal,
      COALESCE(
        CAST(table_seen.table_seen_player_rounds_to_signal AS DOUBLE) /
          NULLIF(table_seen.table_seen_rounds_to_signal, 0),
        0
      ) AS table_seen_player_ratio_to_signal,
      COALESCE(
        CAST(table_seen.table_seen_tie_rounds_to_signal AS DOUBLE) /
          NULLIF(table_seen.table_seen_rounds_to_signal, 0),
        0
      ) AS table_seen_tie_ratio_to_signal,
      COALESCE(prev_strategy.prev_wl_result, 'none') AS prev_wl_result,
      COALESCE(prev_strategy.prev_bet_win, 0) AS prev_bet_win,
      COALESCE(prev_strategy.prev_bet_loss, 0) AS prev_bet_loss,
      COALESCE(prev_strategy.prev_wl_streak_len, 0) AS prev_wl_streak_len,
      COALESCE(strategy_roll.rolling_strategy_settled_bets_to_signal, 0) AS rolling_strategy_settled_bets_to_signal,
      COALESCE(strategy_roll.rolling_strategy_wins_to_signal, 0) AS rolling_strategy_wins_to_signal,
      COALESCE(strategy_roll.rolling_strategy_losses_to_signal, 0) AS rolling_strategy_losses_to_signal,
      COALESCE(strategy_roll.rolling_strategy_pushes_to_signal, 0) AS rolling_strategy_pushes_to_signal,
      COALESCE(strategy_roll.rolling_strategy_decisions_to_signal, 0) AS rolling_strategy_decisions_to_signal,
      COALESCE(strategy_roll.rolling_strategy_win_rate_to_signal, 0) AS rolling_strategy_win_rate_to_signal,
      COALESCE(strategy_roll.rolling_strategy_pnl_to_signal, 0) AS rolling_strategy_pnl_to_signal,
      COALESCE(strategy_streak.rolling_strategy_max_win_streak_to_signal, 0) AS rolling_strategy_max_win_streak_to_signal,
      COALESCE(strategy_streak.rolling_strategy_max_loss_streak_to_signal, 0) AS rolling_strategy_max_loss_streak_to_signal,
      COALESCE(strategy_recent.rolling_strategy_recent_10_decisions, 0) AS rolling_strategy_recent_10_decisions,
      COALESCE(strategy_recent.rolling_strategy_recent_10_win_rate, 0) AS rolling_strategy_recent_10_win_rate,
      COALESCE(strategy_recent.rolling_strategy_recent_10_pnl, 0) AS rolling_strategy_recent_10_pnl,
      COALESCE(table_roll.rolling_table_settled_bets_to_signal, 0) AS rolling_table_settled_bets_to_signal,
      COALESCE(table_roll.rolling_table_win_rate_to_signal, 0) AS rolling_table_win_rate_to_signal,
      COALESCE(table_roll.rolling_table_pnl_to_signal, 0) AS rolling_table_pnl_to_signal,
      COALESCE(global_strategy.rolling_global_strategy_settled_bets_to_signal, 0)
        AS rolling_global_strategy_settled_bets_to_signal,
      COALESCE(global_strategy.rolling_global_strategy_win_rate_to_signal, 0)
        AS rolling_global_strategy_win_rate_to_signal,
      COALESCE(global_strategy.rolling_global_strategy_pnl_to_signal, 0)
        AS rolling_global_strategy_pnl_to_signal,
      COALESCE(global_recent.rolling_global_strategy_recent_30_decisions, 0)
        AS rolling_global_strategy_recent_30_decisions,
      COALESCE(global_recent.rolling_global_strategy_recent_30_win_rate, 0)
        AS rolling_global_strategy_recent_30_win_rate
    FROM anchored b
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS shoe_observed_rounds_to_signal,
        MAX(COALESCE(rr.round_no, rr.seq_no, 0)) AS shoe_current_round_no_to_signal,
        SUM(CASE WHEN rr.outcome = 'B' THEN 1 ELSE 0 END) AS shoe_banker_rounds_to_signal,
        SUM(CASE WHEN rr.outcome = 'P' THEN 1 ELSE 0 END) AS shoe_player_rounds_to_signal,
        SUM(CASE WHEN rr.outcome = 'T' THEN 1 ELSE 0 END) AS shoe_tie_rounds_to_signal
      FROM round_streaks rr
      WHERE rr.table_name = b.table_name
        AND ((rr.shoe = b.signal_shoe) OR (rr.shoe IS NULL AND b.signal_shoe IS NULL))
        AND rr.seq_no <= COALESCE(b.signal_seq_no, 0)
        AND rr.observed_at <= b.created_at
    ) shoe ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS last_6_rounds,
        SUM(CASE WHEN rr.outcome = 'B' THEN 1 ELSE 0 END) AS last_6_banker_rounds,
        SUM(CASE WHEN rr.outcome = 'P' THEN 1 ELSE 0 END) AS last_6_player_rounds,
        SUM(CASE WHEN rr.outcome = 'T' THEN 1 ELSE 0 END) AS last_6_tie_rounds
      FROM round_streaks rr
      WHERE rr.table_name = b.table_name
        AND ((rr.shoe = b.signal_shoe) OR (rr.shoe IS NULL AND b.signal_shoe IS NULL))
        AND rr.seq_no <= COALESCE(b.signal_seq_no, 0)
        AND rr.seq_no > COALESCE(b.signal_seq_no, 0) - 6
        AND rr.observed_at <= b.created_at
    ) last6 ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS last_12_rounds,
        SUM(CASE WHEN rr.outcome = 'B' THEN 1 ELSE 0 END) AS last_12_banker_rounds,
        SUM(CASE WHEN rr.outcome = 'P' THEN 1 ELSE 0 END) AS last_12_player_rounds,
        SUM(CASE WHEN rr.outcome = 'T' THEN 1 ELSE 0 END) AS last_12_tie_rounds
      FROM round_streaks rr
      WHERE rr.table_name = b.table_name
        AND ((rr.shoe = b.signal_shoe) OR (rr.shoe IS NULL AND b.signal_shoe IS NULL))
        AND rr.seq_no <= COALESCE(b.signal_seq_no, 0)
        AND rr.seq_no > COALESCE(b.signal_seq_no, 0) - 12
        AND rr.observed_at <= b.created_at
    ) last12 ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS table_seen_rounds_to_signal,
        SUM(CASE WHEN rr.outcome = 'B' THEN 1 ELSE 0 END) AS table_seen_banker_rounds_to_signal,
        SUM(CASE WHEN rr.outcome = 'P' THEN 1 ELSE 0 END) AS table_seen_player_rounds_to_signal,
        SUM(CASE WHEN rr.outcome = 'T' THEN 1 ELSE 0 END) AS table_seen_tie_rounds_to_signal
      FROM round_streaks rr
      WHERE rr.table_name = b.table_name
        AND rr.observed_at <= b.created_at
    ) table_seen ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        pw.wl_result AS prev_wl_result,
        CASE WHEN pw.wl_result = 'win' THEN 1 ELSE 0 END AS prev_bet_win,
        CASE WHEN pw.wl_result = 'loss' THEN 1 ELSE 0 END AS prev_bet_loss,
        pw.wl_streak_len AS prev_wl_streak_len
      FROM paper_wl_streaks pw
      WHERE pw.table_name = b.table_name
        AND pw.strategy_id = b.strategy_id
        AND COALESCE(pw.settled_at, pw.created_at) < b.created_at
      ORDER BY COALESCE(pw.settled_at, pw.created_at) DESC, pw.created_at DESC, pw.signal_fingerprint DESC
      LIMIT 1
    ) prev_strategy ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS rolling_strategy_settled_bets_to_signal,
        SUM(p2.is_win) AS rolling_strategy_wins_to_signal,
        SUM(p2.is_loss) AS rolling_strategy_losses_to_signal,
        SUM(p2.is_push) AS rolling_strategy_pushes_to_signal,
        SUM(CASE WHEN p2.wl_result IN ('win', 'loss') THEN 1 ELSE 0 END) AS rolling_strategy_decisions_to_signal,
        COALESCE(
          CAST(SUM(p2.is_win) AS DOUBLE) /
            NULLIF(SUM(CASE WHEN p2.wl_result IN ('win', 'loss') THEN 1 ELSE 0 END), 0),
          0
        ) AS rolling_strategy_win_rate_to_signal,
        COALESCE(SUM(p2.pnl_delta), 0) AS rolling_strategy_pnl_to_signal
      FROM paper_bet_results p2
      WHERE p2.table_name = b.table_name
        AND p2.strategy_id = b.strategy_id
        AND p2.status = 'settled'
        AND COALESCE(p2.settled_at, p2.created_at) < b.created_at
    ) strategy_roll ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        MAX(CASE WHEN pw.wl_result = 'win' THEN pw.wl_streak_len ELSE 0 END)
          AS rolling_strategy_max_win_streak_to_signal,
        MAX(CASE WHEN pw.wl_result = 'loss' THEN pw.wl_streak_len ELSE 0 END)
          AS rolling_strategy_max_loss_streak_to_signal
      FROM paper_wl_streaks pw
      WHERE pw.table_name = b.table_name
        AND pw.strategy_id = b.strategy_id
        AND COALESCE(pw.settled_at, pw.created_at) < b.created_at
    ) strategy_streak ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS rolling_strategy_recent_10_decisions,
        COALESCE(CAST(SUM(recent.is_win) AS DOUBLE) / NULLIF(COUNT(*), 0), 0)
          AS rolling_strategy_recent_10_win_rate,
        COALESCE(SUM(recent.pnl_delta), 0) AS rolling_strategy_recent_10_pnl
      FROM (
        SELECT p2.is_win, p2.pnl_delta
        FROM paper_bet_results p2
        WHERE p2.table_name = b.table_name
          AND p2.strategy_id = b.strategy_id
          AND p2.status = 'settled'
          AND p2.wl_result IN ('win', 'loss')
          AND COALESCE(p2.settled_at, p2.created_at) < b.created_at
        ORDER BY COALESCE(p2.settled_at, p2.created_at) DESC, p2.created_at DESC, p2.signal_fingerprint DESC
        LIMIT 10
      ) recent
    ) strategy_recent ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS rolling_table_settled_bets_to_signal,
        COALESCE(
          CAST(SUM(p2.is_win) AS DOUBLE) /
            NULLIF(SUM(CASE WHEN p2.wl_result IN ('win', 'loss') THEN 1 ELSE 0 END), 0),
          0
        ) AS rolling_table_win_rate_to_signal,
        COALESCE(SUM(p2.pnl_delta), 0) AS rolling_table_pnl_to_signal
      FROM paper_bet_results p2
      WHERE p2.table_name = b.table_name
        AND p2.status = 'settled'
        AND COALESCE(p2.settled_at, p2.created_at) < b.created_at
    ) table_roll ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS rolling_global_strategy_settled_bets_to_signal,
        COALESCE(
          CAST(SUM(p2.is_win) AS DOUBLE) /
            NULLIF(SUM(CASE WHEN p2.wl_result IN ('win', 'loss') THEN 1 ELSE 0 END), 0),
          0
        ) AS rolling_global_strategy_win_rate_to_signal,
        COALESCE(SUM(p2.pnl_delta), 0) AS rolling_global_strategy_pnl_to_signal
      FROM paper_bet_results p2
      WHERE p2.strategy_id = b.strategy_id
        AND p2.status = 'settled'
        AND COALESCE(p2.settled_at, p2.created_at) < b.created_at
    ) global_strategy ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) AS rolling_global_strategy_recent_30_decisions,
        COALESCE(CAST(SUM(recent.is_win) AS DOUBLE) / NULLIF(COUNT(*), 0), 0)
          AS rolling_global_strategy_recent_30_win_rate
      FROM (
        SELECT p2.is_win
        FROM paper_bet_results p2
        WHERE p2.strategy_id = b.strategy_id
          AND p2.status = 'settled'
          AND p2.wl_result IN ('win', 'loss')
          AND COALESCE(p2.settled_at, p2.created_at) < b.created_at
        ORDER BY COALESCE(p2.settled_at, p2.created_at) DESC, p2.created_at DESC, p2.signal_fingerprint DESC
        LIMIT 30
      ) recent
    ) global_recent ON TRUE
    ORDER BY b.created_at, b.table_name, b.strategy_id, b.signal_fingerprint
    """


def rolling_feature_view_sql() -> str:
    return f"CREATE OR REPLACE VIEW ml_rolling_features AS {rolling_feature_select_sql()}"


class WorkbenchStore:
    def __init__(self, sqlite_path: Path, duckdb_path: Path | None = None, *, enable_duckdb: bool = True) -> None:
        self.sqlite_path = sqlite_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self.duck = DuckDbMirror(duckdb_path) if enable_duckdb and duckdb_path else None
        self.init_schema()

    def close(self) -> None:
        self.conn.close()
        if self.duck:
            self.duck.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS tables (
              table_name TEXT PRIMARY KEY,
              table_id INTEGER,
              last_seen TEXT NOT NULL,
              round_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS rounds (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              fingerprint TEXT NOT NULL UNIQUE,
              table_name TEXT NOT NULL,
              table_id INTEGER,
              shoe TEXT,
              round_no INTEGER,
              outcome TEXT NOT NULL,
              source TEXT NOT NULL,
              observed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              table_name TEXT NOT NULL,
              strategy_id TEXT NOT NULL,
              action TEXT NOT NULL,
              side TEXT,
              confidence REAL NOT NULL,
              reason TEXT NOT NULL,
              features_json TEXT NOT NULL,
              round_fingerprint TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_bets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              settled_at TEXT,
              table_name TEXT NOT NULL,
              strategy_id TEXT NOT NULL,
              side TEXT NOT NULL,
              stake REAL NOT NULL,
              signal_fingerprint TEXT NOT NULL,
              status TEXT NOT NULL,
              outcome TEXT,
              pnl_delta REAL NOT NULL,
              pnl_after REAL NOT NULL,
              reason TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_experiment_bets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_date TEXT NOT NULL,
              session_window TEXT NOT NULL DEFAULT '12:00-13:00',
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
            );

            CREATE TABLE IF NOT EXISTS latency_samples (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              table_name TEXT NOT NULL,
              source TEXT NOT NULL,
              current_round_no INTEGER NOT NULL,
              observed_rounds INTEGER NOT NULL,
              known_missing_rounds INTEGER NOT NULL,
              monitor_seen_at TEXT NOT NULL,
              app_received_at TEXT NOT NULL,
              engine_done_at TEXT NOT NULL,
              ui_refresh_at TEXT NOT NULL,
              queue_delay_ms REAL NOT NULL,
              engine_ms REAL NOT NULL,
              ui_delay_ms REAL NOT NULL,
              total_ms REAL NOT NULL,
              signal_count INTEGER NOT NULL,
              actionable_count INTEGER NOT NULL,
              pending_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS data_quality_exclusions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at TEXT NOT NULL,
              ended_at TEXT NOT NULL,
              scope TEXT NOT NULL DEFAULT 'all',
              reason TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_rounds_table ON rounds(table_name, shoe, round_no);
            CREATE INDEX IF NOT EXISTS idx_signals_table_time ON signals(table_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_paper_table_time ON paper_bets(table_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_paper_ml_pass_table_time
              ON paper_bets(table_name, status, reason, settled_at, created_at, id);
            CREATE INDEX IF NOT EXISTS idx_latency_table_time
              ON latency_samples(table_name, created_at, id);
            CREATE INDEX IF NOT EXISTS idx_data_quality_exclusions_time
              ON data_quality_exclusions(started_at, ended_at, scope);
            """
        )
        daily_columns = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(daily_experiment_bets)").fetchall()
        }
        if "session_window" not in daily_columns:
            self.conn.execute(
                "ALTER TABLE daily_experiment_bets "
                "ADD COLUMN session_window TEXT NOT NULL DEFAULT '12:00-13:00'"
            )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_experiment_session "
            "ON daily_experiment_bets(session_date, session_window, status, id)"
        )
        self.conn.commit()
        if self.duck:
            self.duck.init_schema()
            self.duck.sync_from_sqlite(self.conn)

    def upsert_rounds(self, rounds: Iterable[RoundEvent]) -> None:
        rows = list(rounds)
        if not rows:
            return
        inserted_rows: list[RoundEvent] = []
        table_updates: dict[str, tuple[int | None, str]] = {}
        with self.conn:
            for event in rows:
                cursor = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO rounds
                    (fingerprint, table_name, table_id, shoe, round_no, outcome, source, observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.fingerprint,
                        event.table_name,
                        event.table_id,
                        str(event.shoe) if event.shoe is not None else None,
                        event.round_no,
                        event.outcome.value,
                        event.source,
                        event.observed_at,
                    ),
                )
                if cursor.rowcount:
                    inserted_rows.append(event)
                current_table_id, current_last_seen = table_updates.get(event.table_name, (None, ""))
                table_id = event.table_id if event.table_id is not None else current_table_id
                last_seen = max(current_last_seen, event.observed_at)
                table_updates[event.table_name] = (table_id, last_seen)
            for table_name, (table_id, last_seen) in table_updates.items():
                self.conn.execute(
                    """
                    INSERT INTO tables(table_name, table_id, last_seen, round_count)
                    VALUES (?, ?, ?, (
                      SELECT COUNT(*) FROM rounds WHERE rounds.table_name = ?
                    ))
                    ON CONFLICT(table_name) DO UPDATE SET
                      table_id = COALESCE(excluded.table_id, tables.table_id),
                      last_seen = CASE
                        WHEN excluded.last_seen > tables.last_seen THEN excluded.last_seen
                        ELSE tables.last_seen
                      END,
                      round_count = excluded.round_count
                    """,
                    (table_name, table_id, last_seen, table_name),
                )
        if self.duck:
            self.duck.append_rounds(inserted_rows)

    def save_signal(self, signal: StrategySignal) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO signals
                (created_at, table_name, strategy_id, action, side, confidence, reason, features_json, round_fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.created_at,
                    signal.table_name,
                    signal.strategy_id,
                    signal.action.value,
                    signal.side.value if signal.side else None,
                    signal.confidence,
                    signal.reason,
                    json.dumps(signal.features, ensure_ascii=False, sort_keys=True),
                    signal.round_fingerprint,
                ),
            )
        if self.duck:
            self.duck.append_signal(signal)

    def save_paper_bet(self, bet: PaperBet) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO paper_bets
                (created_at, settled_at, table_name, strategy_id, side, stake, signal_fingerprint,
                 status, outcome, pnl_delta, pnl_after, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bet.created_at,
                    bet.settled_at,
                    bet.table_name,
                    bet.strategy_id,
                    bet.side.value,
                    bet.stake,
                    bet.signal_fingerprint,
                    bet.status,
                    bet.outcome.value if bet.outcome else None,
                    bet.pnl_delta,
                    bet.pnl_after,
                    bet.reason,
                ),
            )
        if self.duck:
            self.duck.append_paper_bet(bet)

    def save_daily_experiment_bet(
        self,
        *,
        session_date: str,
        session_window: str,
        created_at: str,
        table_name: str,
        strategy_id: str,
        side: str,
        stake: float,
        signal_fingerprint: str,
        confidence: float,
        max_per_window: int = 2,
    ) -> bool:
        with self.conn:
            if self.next_round_after_fingerprint(
                table_name=table_name,
                signal_fingerprint=signal_fingerprint,
            ) is not None:
                return False
            row = self.conn.execute(
                """SELECT COUNT(*) AS row_count
                FROM daily_experiment_bets
                WHERE session_date=? AND session_window=?""",
                (session_date, session_window),
            ).fetchone()
            if int(row["row_count"] or 0) >= max_per_window:
                return False
            cursor = self.conn.execute(
                """INSERT OR IGNORE INTO daily_experiment_bets
                (session_date, session_window, created_at, table_name, strategy_id, side, stake,
                 signal_fingerprint, status, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (
                    session_date,
                    session_window,
                    created_at,
                    table_name,
                    strategy_id,
                    side,
                    stake,
                    signal_fingerprint,
                    confidence,
                ),
            )
            return cursor.rowcount > 0

    def settle_daily_experiment_bet(
        self,
        *,
        bet_id: int,
        settled_at: str,
        outcome: str,
        result: str,
        pnl: float,
    ) -> bool:
        with self.conn:
            cursor = self.conn.execute(
                """UPDATE daily_experiment_bets SET settled_at=?, status='settled', outcome=?, result=?, pnl=?
                WHERE id=? AND status='pending'""",
                (settled_at, outcome, result, pnl, bet_id),
            )
            return cursor.rowcount > 0

    def daily_experiment_rows(
        self,
        session_date: str | None = None,
        session_window: str | None = None,
        *,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        filters: list[str] = [
            """NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'daily_experiment_bets')
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) >= dq.started_at
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) <= dq.ended_at
            )"""
        ]
        params: list[object] = []
        if session_date:
            filters.append("session_date = ?")
            params.append(session_date)
        if session_window:
            filters.append("session_window = ?")
            params.append(session_window)
        where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
        order_clause = " ORDER BY id" if session_date else " ORDER BY id DESC"
        limit_clause = " LIMIT ?" if limit is not None else ""
        if limit is not None:
            params.append(max(0, int(limit)))
        return self.conn.execute(
            f"SELECT * FROM daily_experiment_bets{where_clause}{order_clause}{limit_clause}",
            tuple(params),
        ).fetchall()

    def daily_experiment_dates(self, *, limit: int = 366) -> list[str]:
        rows = self.conn.execute(
            """SELECT session_date
            FROM daily_experiment_bets
            WHERE NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'daily_experiment_bets')
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) >= dq.started_at
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) <= dq.ended_at
            )
            GROUP BY session_date
            ORDER BY session_date DESC
            LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall()
        return [str(row["session_date"]) for row in rows]

    def daily_experiment_summary(
        self,
        session_date: str | None = None,
        session_window: str | None = None,
    ) -> dict[str, int | float]:
        filters: list[str] = [
            """NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'daily_experiment_bets')
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) >= dq.started_at
                AND COALESCE(daily_experiment_bets.settled_at, daily_experiment_bets.created_at) <= dq.ended_at
            )"""
        ]
        params: list[object] = []
        if session_date:
            filters.append("session_date = ?")
            params.append(session_date)
        if session_window:
            filters.append("session_window = ?")
            params.append(session_window)
        row = self.conn.execute(
            f"""SELECT
              COUNT(*) AS total_count,
              SUM(CASE WHEN status = 'settled' THEN 1 ELSE 0 END) AS settled_count,
              SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
              SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS win_count,
              SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS loss_count,
              SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS tie_count,
              COALESCE(SUM(CASE WHEN status = 'settled' THEN pnl ELSE 0 END), 0) AS total_pnl
            FROM daily_experiment_bets
            WHERE {' AND '.join(filters)}""",
            tuple(params),
        ).fetchone()
        return {
            "total_count": int(row["total_count"] or 0),
            "settled_count": int(row["settled_count"] or 0),
            "pending_count": int(row["pending_count"] or 0),
            "win_count": int(row["win_count"] or 0),
            "loss_count": int(row["loss_count"] or 0),
            "tie_count": int(row["tie_count"] or 0),
            "total_pnl": float(row["total_pnl"] or 0),
        }

    def pending_daily_experiment_row(self, table_name: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT * FROM daily_experiment_bets
            WHERE table_name=? AND status='pending'
              AND NOT EXISTS (
                SELECT 1
                FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'daily_experiment_bets')
                  AND daily_experiment_bets.created_at >= dq.started_at
                  AND daily_experiment_bets.created_at <= dq.ended_at
              )
            ORDER BY id DESC LIMIT 1""",
            (table_name,),
        ).fetchone()

    def next_round_after_fingerprint(
        self,
        *,
        table_name: str,
        signal_fingerprint: str,
    ) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT next_round.*
            FROM rounds AS signal_round
            JOIN rounds AS next_round
              ON next_round.table_name = signal_round.table_name
             AND next_round.shoe IS signal_round.shoe
             AND next_round.round_no = signal_round.round_no + 1
            WHERE signal_round.table_name = ?
              AND signal_round.fingerprint = ?
              AND NOT EXISTS (
                SELECT 1 FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'rounds')
                  AND signal_round.observed_at >= dq.started_at
                  AND signal_round.observed_at <= dq.ended_at
              )
              AND NOT EXISTS (
                SELECT 1 FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'rounds')
                  AND next_round.observed_at >= dq.started_at
                  AND next_round.observed_at <= dq.ended_at
              )
            ORDER BY next_round.observed_at, next_round.id
            LIMIT 1""",
            (table_name, signal_fingerprint),
        ).fetchone()

    def save_latency_sample(self, sample: LatencySample) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO latency_samples
                (created_at, table_name, source, current_round_no, observed_rounds, known_missing_rounds,
                 monitor_seen_at, app_received_at, engine_done_at, ui_refresh_at,
                 queue_delay_ms, engine_ms, ui_delay_ms, total_ms,
                 signal_count, actionable_count, pending_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample.created_at,
                    sample.table_name,
                    sample.source,
                    sample.current_round_no,
                    sample.observed_rounds,
                    sample.known_missing_rounds,
                    sample.monitor_seen_at,
                    sample.app_received_at,
                    sample.engine_done_at,
                    sample.ui_refresh_at,
                    sample.queue_delay_ms,
                    sample.engine_ms,
                    sample.ui_delay_ms,
                    sample.total_ms,
                    sample.signal_count,
                    sample.actionable_count,
                    sample.pending_count,
                ),
            )
        if self.duck:
            self.duck.append_latency_sample(sample)

    def recent_latency_samples(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM latency_samples
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def add_data_quality_exclusion(
        self,
        *,
        started_at: str,
        ended_at: str,
        scope: str,
        reason: str,
        created_at: str,
    ) -> int:
        if not started_at or not ended_at or ended_at < started_at:
            raise ValueError("Invalid data quality exclusion interval.")
        if scope not in {"all", "rounds", "signals", "paper_bets", "daily_experiment_bets"}:
            raise ValueError(f"Unsupported data quality exclusion scope: {scope}")
        with self.conn:
            cursor = self.conn.execute(
                """INSERT INTO data_quality_exclusions
                (started_at, ended_at, scope, reason, created_at)
                VALUES (?, ?, ?, ?, ?)""",
                (started_at, ended_at, scope, reason, created_at),
            )
        if self.duck:
            self.duck.append_data_quality_exclusion(
                int(cursor.lastrowid),
                started_at,
                ended_at,
                scope,
                reason,
                created_at,
            )
        return int(cursor.lastrowid)

    def data_quality_exclusion_rows(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM data_quality_exclusions ORDER BY id"))

    def list_tables(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT table_name, table_id, last_seen, round_count FROM tables ORDER BY last_seen DESC"
            )
        )

    def recent_paper_bets(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT * FROM paper_bets
                ORDER BY COALESCE(settled_at, created_at) DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def current_wl_streak(self, table_name: str) -> str:
        rows = self.conn.execute(
            """
            SELECT pnl_delta
            FROM paper_bets
            WHERE table_name = ? AND status = 'settled'
              AND NOT EXISTS (
                SELECT 1
                FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'paper_bets')
                  AND COALESCE(paper_bets.settled_at, paper_bets.created_at) >= dq.started_at
                  AND COALESCE(paper_bets.settled_at, paper_bets.created_at) <= dq.ended_at
              )
            ORDER BY COALESCE(settled_at, created_at) DESC, created_at DESC, id DESC
            """,
            (table_name,),
        )
        current = ""
        count = 0
        for row in rows:
            delta = float(row["pnl_delta"] or 0)
            if delta > 0:
                value = "W"
            elif delta < 0:
                value = "L"
            else:
                continue
            if not current:
                current = value
            if value != current:
                break
            count += 1
        return f"{current}{count}" if current else "-"

    def ml_pass_wl_summary(self, table_name: str, history_limit: int = 30) -> dict[str, int | str]:
        all_rows = self._selected_ml_pass_rows(table_name)
        current_rows = self._selected_ml_pass_rows(table_name, current_shoe_only=True)

        def _wl_values(rows: Iterable[sqlite3.Row]) -> list[str]:
            values: list[str] = []
            for row in rows:
                delta = float(row["pnl_delta"] or 0)
                if delta > 0:
                    values.append("W")
                elif delta < 0:
                    values.append("L")
            return values

        all_values = _wl_values(all_rows)
        values = _wl_values(current_rows)
        if not values:
            values = []
        # Longest W/L streaks are lifetime table metrics; current is shoe-local.
        max_win = 0
        max_loss = 0
        run_value = ""
        run_count = 0
        for value in all_values:
            if value != run_value:
                run_value = value
                run_count = 1
            else:
                run_count += 1
            if value == "W":
                max_win = max(max_win, run_count)
            else:
                max_loss = max(max_loss, run_count)
        if not values:
            return {"history": "-", "current": "-", "max_win": max_win, "max_loss": max_loss}

        current = values[-1]
        current_count = 0
        for value in reversed(values):
            if value != current:
                break
            current_count += 1
        if history_limit > 0:
            history_values = values[-history_limit:]
        else:
            history_values = values
        return {
            "history": " ".join(history_values),
            "current": f"{current}{current_count}",
            "max_win": max_win,
            "max_loss": max_loss,
        }

    def ml_pass_totals(self) -> dict[str, float | int]:
        rows = self._selected_ml_pass_rows()
        return {
            "settled_count": len(rows),
            "wins": sum(1 for row in rows if float(row["pnl_delta"] or 0) > 0),
            "losses": sum(1 for row in rows if float(row["pnl_delta"] or 0) < 0),
            "pushes": sum(1 for row in rows if float(row["pnl_delta"] or 0) == 0),
            "pnl": round(sum(float(row["pnl_delta"] or 0) for row in rows), 2),
        }

    def selected_ml_pass_rows(self, *, since: str | None = None) -> list[sqlite3.Row]:
        return self._selected_ml_pass_rows(since=since)

    def ml_pass_duplicate_cutoff(self) -> dict[str, int | str | None]:
        row = self.conn.execute(
            """
            WITH grouped AS (
              SELECT
                table_name,
                signal_fingerprint,
                COUNT(*) AS row_count,
                MAX(COALESCE(settled_at, created_at)) AS last_at
              FROM paper_bets
              WHERE status = 'settled'
                AND reason LIKE 'ML pass:%'
                AND NOT EXISTS (
                  SELECT 1
                  FROM data_quality_exclusions AS dq
                  WHERE dq.scope IN ('all', 'paper_bets')
                    AND COALESCE(paper_bets.settled_at, paper_bets.created_at) >= dq.started_at
                    AND COALESCE(paper_bets.settled_at, paper_bets.created_at) <= dq.ended_at
                )
              GROUP BY table_name, signal_fingerprint
              HAVING COUNT(*) > 1
            )
            SELECT
              MAX(last_at) AS cutoff,
              COUNT(*) AS duplicate_groups
            FROM grouped
            """
        ).fetchone()
        return {
            "cutoff": row["cutoff"] if row else None,
            "duplicate_groups": int(row["duplicate_groups"] or 0) if row else 0,
        }

    def _selected_ml_pass_rows(
        self,
        table_name: str | None = None,
        *,
        since: str | None = None,
        current_shoe_only: bool = False,
    ) -> list[sqlite3.Row]:
        filters = [
            "status = 'settled'",
            "reason LIKE 'ML pass:%'",
            """NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'paper_bets')
                AND COALESCE(paper_bets.settled_at, paper_bets.created_at) >= dq.started_at
                AND COALESCE(paper_bets.settled_at, paper_bets.created_at) <= dq.ended_at
            )""",
        ]
        params: list[object] = []
        if table_name is not None:
            filters.append("table_name = ?")
            params.append(table_name)
        if since:
            filters.append("COALESCE(settled_at, created_at) > ?")
            params.append(since)
        where_clause = " AND ".join(filters)
        rows = self.conn.execute(
            f"""
            SELECT
              id,
              created_at,
              settled_at,
              table_name,
              strategy_id,
              side,
              stake,
              signal_fingerprint,
              outcome,
              pnl_delta,
              reason
            FROM paper_bets
            WHERE {where_clause}
            ORDER BY COALESCE(settled_at, created_at), created_at, id
            """,
            tuple(params),
        ).fetchall()
        # A table's displayed W/L cycle belongs to the current shoe only.
        # Keep historical rows in storage, but exclude rows anchored to older shoes.
        if table_name is not None and current_shoe_only:
            current_shoe_row = self.conn.execute(
                """
                SELECT shoe
                FROM rounds AS r
                WHERE table_name = ? AND shoe IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM data_quality_exclusions AS dq
                    WHERE dq.scope IN ('all', 'rounds')
                      AND r.observed_at >= dq.started_at
                      AND r.observed_at <= dq.ended_at
                  )
                ORDER BY id DESC
                LIMIT 1
                """,
                (table_name,),
            ).fetchone()
            if current_shoe_row is not None and current_shoe_row[0] is not None:
                shoe_by_fingerprint = {
                    str(row[0]): str(row[1])
                    for row in self.conn.execute(
                        """SELECT fingerprint, shoe FROM rounds AS r
                        WHERE table_name = ? AND shoe IS NOT NULL
                          AND NOT EXISTS (
                            SELECT 1 FROM data_quality_exclusions AS dq
                            WHERE dq.scope IN ('all', 'rounds')
                              AND r.observed_at >= dq.started_at
                              AND r.observed_at <= dq.ended_at
                          )""",
                        (table_name,),
                    ).fetchall()
                }
                current_shoe = str(current_shoe_row[0])
                rows = [
                    row
                    for row in rows
                    if shoe_by_fingerprint.get(str(row["signal_fingerprint"])) == current_shoe
                ]
        return _select_best_ml_pass_rows(rows)

    def analytics_status(self) -> str:
        if self.duck is None:
            return "DuckDB analytics mirror disabled by config."
        if self.duck.enabled:
            return f"DuckDB analytics mirror enabled: {self.duck.path}"
        return f"DuckDB analytics mirror NOT enabled: {self.duck.error or 'unknown error'}"


def _select_best_ml_pass_rows(rows: Iterable[sqlite3.Row]) -> list[sqlite3.Row]:
    selected: dict[tuple[str, str], sqlite3.Row] = {}
    for row in rows:
        key = (str(row["table_name"]), str(row["signal_fingerprint"]))
        current = selected.get(key)
        if current is None or _ml_pass_row_rank(row) > _ml_pass_row_rank(current):
            selected[key] = row
    return sorted(
        selected.values(),
        key=lambda row: (str(row["settled_at"] or row["created_at"]), str(row["created_at"]), int(row["id"] or 0)),
    )


def _ml_pass_row_rank(row: sqlite3.Row) -> tuple[float, str, str, int]:
    return (
        _ml_probability_from_reason(str(row["reason"] or "")),
        str(row["settled_at"] or row["created_at"]),
        str(row["created_at"]),
        int(row["id"] or 0),
    )


def _ml_probability_from_reason(reason: str) -> float:
    match = re.search(r"win probability\s+([0-9]+(?:\.[0-9]+)?)%", reason)
    if not match:
        return 0.0
    return float(match.group(1)) / 100.0


class DuckDbMirror:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.enabled = False
        self.conn = None
        self.error: str | None = None
        try:
            import duckdb  # type: ignore

            self.conn = duckdb.connect(str(path))
            self.enabled = True
        except Exception as exc:
            self.conn = None
            self.enabled = False
            self.error = str(exc)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()

    def init_schema(self) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tables (
              table_name VARCHAR PRIMARY KEY,
              table_id INTEGER,
              last_seen VARCHAR,
              round_count INTEGER
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rounds (
              fingerprint VARCHAR PRIMARY KEY,
              table_name VARCHAR,
              table_id INTEGER,
              shoe VARCHAR,
              round_no INTEGER,
              outcome VARCHAR,
              source VARCHAR,
              observed_at VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
              created_at VARCHAR,
              table_name VARCHAR,
              strategy_id VARCHAR,
              action VARCHAR,
              side VARCHAR,
              confidence DOUBLE,
              reason VARCHAR,
              features_json VARCHAR,
              round_fingerprint VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_bets (
              created_at VARCHAR,
              settled_at VARCHAR,
              table_name VARCHAR,
              strategy_id VARCHAR,
              side VARCHAR,
              stake DOUBLE,
              signal_fingerprint VARCHAR,
              status VARCHAR,
              outcome VARCHAR,
              pnl_delta DOUBLE,
              pnl_after DOUBLE,
              reason VARCHAR
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS latency_samples (
              created_at VARCHAR,
              table_name VARCHAR,
              source VARCHAR,
              current_round_no INTEGER,
              observed_rounds INTEGER,
              known_missing_rounds INTEGER,
              monitor_seen_at VARCHAR,
              app_received_at VARCHAR,
              engine_done_at VARCHAR,
              ui_refresh_at VARCHAR,
              queue_delay_ms DOUBLE,
              engine_ms DOUBLE,
              ui_delay_ms DOUBLE,
              total_ms DOUBLE,
              signal_count INTEGER,
              actionable_count INTEGER,
              pending_count INTEGER
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_quality_exclusions (
              id INTEGER PRIMARY KEY,
              started_at VARCHAR,
              ended_at VARCHAR,
              scope VARCHAR,
              reason VARCHAR,
              created_at VARCHAR
            )
            """
        )
        self.init_views()

    def init_views(self) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            """
            CREATE OR REPLACE VIEW paper_bet_results AS
            SELECT
              *,
              CASE
                WHEN status <> 'settled' OR outcome IS NULL THEN 'pending'
                WHEN outcome = 'T' THEN 'push'
                WHEN side = outcome THEN 'win'
                ELSE 'loss'
              END AS wl_result,
              CASE WHEN status = 'settled' AND outcome <> 'T' AND side = outcome THEN 1 ELSE 0 END AS is_win,
              CASE WHEN status = 'settled' AND outcome <> 'T' AND side <> outcome THEN 1 ELSE 0 END AS is_loss,
              CASE WHEN status = 'settled' AND outcome = 'T' THEN 1 ELSE 0 END AS is_push,
              CASE
                WHEN pnl_delta > 0 THEN 'profit'
                WHEN pnl_delta < 0 THEN 'loss'
                ELSE 'flat'
              END AS pnl_result
            FROM paper_bets AS pb
            WHERE NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'paper_bets')
                AND COALESCE(pb.settled_at, pb.created_at) >= dq.started_at
                AND COALESCE(pb.settled_at, pb.created_at) <= dq.ended_at
            )
            """
        )
        self.conn.execute(
            """
            CREATE OR REPLACE VIEW paper_wl_streaks AS
            WITH settled AS (
              SELECT *
              FROM paper_bet_results
              WHERE wl_result IN ('win', 'loss')
            ),
            ordered AS (
              SELECT
                *,
                ROW_NUMBER() OVER (
                  PARTITION BY table_name, strategy_id
                  ORDER BY COALESCE(settled_at, created_at), created_at, signal_fingerprint
                ) AS seq_no,
                LAG(wl_result) OVER (
                  PARTITION BY table_name, strategy_id
                  ORDER BY COALESCE(settled_at, created_at), created_at, signal_fingerprint
                ) AS prev_wl_result
              FROM settled
            ),
            marked AS (
              SELECT
                *,
                CASE WHEN prev_wl_result IS NULL OR prev_wl_result <> wl_result THEN 1 ELSE 0 END AS is_new_streak
              FROM ordered
            ),
            grouped AS (
              SELECT
                *,
                SUM(is_new_streak) OVER (
                  PARTITION BY table_name, strategy_id
                  ORDER BY seq_no
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS streak_group
              FROM marked
            )
            SELECT
              created_at,
              settled_at,
              table_name,
              strategy_id,
              side,
              stake,
              signal_fingerprint,
              outcome,
              pnl_delta,
              pnl_after,
              reason,
              wl_result,
              seq_no,
              ROW_NUMBER() OVER (
                PARTITION BY table_name, strategy_id, streak_group
                ORDER BY seq_no
              ) AS wl_streak_len
            FROM grouped
            """
        )
        self.conn.execute(
            """
            CREATE OR REPLACE VIEW round_streaks AS
            WITH ordered AS (
              SELECT
                *,
                ROW_NUMBER() OVER (
                  PARTITION BY table_name, shoe
                  ORDER BY COALESCE(round_no, 2147483647), observed_at, fingerprint
                ) AS seq_no,
                LAG(outcome) OVER (
                  PARTITION BY table_name, shoe
                  ORDER BY COALESCE(round_no, 2147483647), observed_at, fingerprint
                ) AS prev_outcome
              FROM rounds AS r
              WHERE NOT EXISTS (
                SELECT 1
                FROM data_quality_exclusions AS dq
                WHERE dq.scope IN ('all', 'rounds')
                  AND r.observed_at >= dq.started_at
                  AND r.observed_at <= dq.ended_at
              )
            ),
            marked AS (
              SELECT
                *,
                CASE WHEN prev_outcome IS NULL OR prev_outcome <> outcome THEN 1 ELSE 0 END AS is_new_streak
              FROM ordered
            ),
            grouped AS (
              SELECT
                *,
                SUM(is_new_streak) OVER (
                  PARTITION BY table_name, shoe
                  ORDER BY seq_no
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS streak_group
              FROM marked
            )
            SELECT
              fingerprint,
              table_name,
              table_id,
              shoe,
              round_no,
              outcome,
              source,
              observed_at,
              seq_no,
              ROW_NUMBER() OVER (
                PARTITION BY table_name, shoe, streak_group
                ORDER BY seq_no
              ) AS outcome_streak_len
            FROM grouped
            """
        )
        self.conn.execute(
            """
            CREATE OR REPLACE VIEW table_round_summary AS
            SELECT
              table_name,
              table_id,
              COUNT(*) AS total_rounds,
              COUNT(*) AS observed_rounds,
              MAX(round_no) AS current_round_no,
              GREATEST(COALESCE(MAX(round_no), COUNT(*)) - COUNT(*), 0) AS known_missing_rounds,
              SUM(CASE WHEN outcome = 'B' THEN 1 ELSE 0 END) AS banker_rounds,
              SUM(CASE WHEN outcome = 'P' THEN 1 ELSE 0 END) AS player_rounds,
              SUM(CASE WHEN outcome = 'T' THEN 1 ELSE 0 END) AS tie_rounds,
              MIN(observed_at) AS first_seen,
              MAX(observed_at) AS last_seen
            FROM rounds AS r
            WHERE NOT EXISTS (
              SELECT 1
              FROM data_quality_exclusions AS dq
              WHERE dq.scope IN ('all', 'rounds')
                AND r.observed_at >= dq.started_at
                AND r.observed_at <= dq.ended_at
            )
            GROUP BY table_name, table_id
            """
        )
        self.conn.execute(
            """
            CREATE OR REPLACE VIEW strategy_performance AS
            WITH perf AS (
              SELECT
                table_name,
                strategy_id,
                COUNT(*) AS settled_bets,
                SUM(is_win) AS wins,
                SUM(is_loss) AS losses,
                SUM(is_push) AS pushes,
                SUM(CASE WHEN wl_result IN ('win', 'loss') THEN 1 ELSE 0 END) AS decisions,
                CASE
                  WHEN SUM(CASE WHEN wl_result IN ('win', 'loss') THEN 1 ELSE 0 END) = 0 THEN NULL
                  ELSE CAST(SUM(is_win) AS DOUBLE) /
                    SUM(CASE WHEN wl_result IN ('win', 'loss') THEN 1 ELSE 0 END)
                END AS win_rate_ex_push,
                SUM(pnl_delta) AS pnl,
                MIN(created_at) AS first_signal_at,
                MAX(COALESCE(settled_at, created_at)) AS last_settled_at
              FROM paper_bet_results
              WHERE status = 'settled'
              GROUP BY table_name, strategy_id
            ),
            streaks AS (
              SELECT
                table_name,
                strategy_id,
                MAX(CASE WHEN wl_result = 'win' THEN wl_streak_len ELSE 0 END) AS max_win_streak,
                MAX(CASE WHEN wl_result = 'loss' THEN wl_streak_len ELSE 0 END) AS max_loss_streak
              FROM paper_wl_streaks
              GROUP BY table_name, strategy_id
            )
            SELECT
              perf.*,
              COALESCE(streaks.max_win_streak, 0) AS max_win_streak,
              COALESCE(streaks.max_loss_streak, 0) AS max_loss_streak
            FROM perf
            LEFT JOIN streaks USING (table_name, strategy_id)
            """
        )
        self.conn.execute(rolling_feature_view_sql())

    def sync_from_sqlite(self, sqlite_conn: sqlite3.Connection) -> None:
        if not self.enabled or self.conn is None:
            return
        if self._counts_match(sqlite_conn):
            return
        try:
            self.conn.execute("BEGIN TRANSACTION")
            for table in (
                "data_quality_exclusions",
                "latency_samples",
                "paper_bets",
                "signals",
                "rounds",
                "tables",
            ):
                self.conn.execute(f"DELETE FROM {table}")

            self._executemany(
                "INSERT INTO tables VALUES (?, ?, ?, ?)",
                [tuple(row) for row in sqlite_conn.execute("SELECT table_name, table_id, last_seen, round_count FROM tables")],
            )
            self._executemany(
                "INSERT INTO rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(row)
                    for row in sqlite_conn.execute(
                        """
                        SELECT fingerprint, table_name, table_id, shoe, round_no, outcome, source, observed_at
                        FROM rounds
                        ORDER BY observed_at, table_name, shoe, round_no
                        """
                    )
                ],
            )
            self._executemany(
                "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(row)
                    for row in sqlite_conn.execute(
                        """
                        SELECT created_at, table_name, strategy_id, action, side, confidence, reason,
                               features_json, round_fingerprint
                        FROM signals
                        ORDER BY created_at
                        """
                    )
                ],
            )
            self._executemany(
                "INSERT INTO paper_bets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(row)
                    for row in sqlite_conn.execute(
                        """
                        SELECT created_at, settled_at, table_name, strategy_id, side, stake, signal_fingerprint,
                               status, outcome, pnl_delta, pnl_after, reason
                        FROM paper_bets
                        ORDER BY COALESCE(settled_at, created_at), created_at
                        """
                    )
                ],
            )
            self._executemany(
                "INSERT INTO latency_samples VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(row)
                    for row in sqlite_conn.execute(
                        """
                        SELECT created_at, table_name, source, current_round_no, observed_rounds,
                               known_missing_rounds, monitor_seen_at, app_received_at, engine_done_at,
                               ui_refresh_at, queue_delay_ms, engine_ms, ui_delay_ms, total_ms,
                               signal_count, actionable_count, pending_count
                        FROM latency_samples
                        ORDER BY id
                        """
                    )
                ],
            )
            self._executemany(
                "INSERT INTO data_quality_exclusions VALUES (?, ?, ?, ?, ?, ?)",
                [
                    tuple(row)
                    for row in sqlite_conn.execute(
                        """SELECT id, started_at, ended_at, scope, reason, created_at
                        FROM data_quality_exclusions ORDER BY id"""
                    )
                ],
            )
            self.conn.execute("COMMIT")
        except Exception as exc:
            with contextlib.suppress(Exception):
                self.conn.execute("ROLLBACK")
            logger.debug("DuckDB sync from SQLite failed: %s", exc)

    def append_rounds(self, rounds: list[RoundEvent]) -> None:
        if not self.enabled or self.conn is None or not rounds:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event.fingerprint,
                    event.table_name,
                    event.table_id,
                    str(event.shoe) if event.shoe is not None else None,
                    event.round_no,
                    event.outcome.value,
                    event.source,
                    event.observed_at,
                )
                for event in rounds
            ],
        )

    def append_signal(self, signal: StrategySignal) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                signal.created_at,
                signal.table_name,
                signal.strategy_id,
                signal.action.value,
                signal.side.value if signal.side else None,
                signal.confidence,
                signal.reason,
                json.dumps(signal.features, ensure_ascii=False, sort_keys=True),
                signal.round_fingerprint,
            ),
        )

    def append_paper_bet(self, bet: PaperBet) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            "INSERT INTO paper_bets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                bet.created_at,
                bet.settled_at,
                bet.table_name,
                bet.strategy_id,
                bet.side.value,
                bet.stake,
                bet.signal_fingerprint,
                bet.status,
                bet.outcome.value if bet.outcome else None,
                bet.pnl_delta,
                bet.pnl_after,
                bet.reason,
            ),
        )

    def append_latency_sample(self, sample: LatencySample) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            "INSERT INTO latency_samples VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sample.created_at,
                sample.table_name,
                sample.source,
                sample.current_round_no,
                sample.observed_rounds,
                sample.known_missing_rounds,
                sample.monitor_seen_at,
                sample.app_received_at,
                sample.engine_done_at,
                sample.ui_refresh_at,
                sample.queue_delay_ms,
                sample.engine_ms,
                sample.ui_delay_ms,
                sample.total_ms,
                sample.signal_count,
                sample.actionable_count,
                sample.pending_count,
            ),
        )

    def append_data_quality_exclusion(
        self,
        exclusion_id: int,
        started_at: str,
        ended_at: str,
        scope: str,
        reason: str,
        created_at: str,
    ) -> None:
        if not self.enabled or self.conn is None:
            return
        self.conn.execute(
            "INSERT OR REPLACE INTO data_quality_exclusions VALUES (?, ?, ?, ?, ?, ?)",
            (exclusion_id, started_at, ended_at, scope, reason, created_at),
        )

    def _counts_match(self, sqlite_conn: sqlite3.Connection) -> bool:
        if not self.enabled or self.conn is None:
            return True
        for table in (
            "tables",
            "rounds",
            "signals",
            "paper_bets",
            "latency_samples",
            "data_quality_exclusions",
        ):
            try:
                sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                duck_count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                return False
            if sqlite_count != duck_count:
                return False
        return True

    def _executemany(self, sql: str, rows: list[tuple]) -> None:
        if self.conn is not None and rows:
            self.conn.executemany(sql, rows)
