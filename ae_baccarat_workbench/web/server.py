from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.requests import Request

from urllib.parse import urlparse

from ..app import (
    BANGKOK_TIMEZONE,
    DAILY_CANDIDATE_MIN_REMAINING_SECONDS,
    DAILY_EXPERIMENT_MAX_PER_WINDOW,
    DAILY_EXPERIMENT_WINDOW_LABELS,
    DAILY_EXPERIMENT_WINDOWS,
    ENSEMBLE_MAJORITY_HISTORY_ROW_LIMIT,
    ENSEMBLE_MAJORITY_ML_MIN_PROBABILITY,
    ENSEMBLE_MAJORITY_STRATEGY_ID,
    ADAPTIVE_REGIME_BANKER_MIN_ML,
    ADAPTIVE_REGIME_HISTORY_ROW_LIMIT,
    ADAPTIVE_REGIME_PLAYER_MIN_ML,
    ADAPTIVE_REGIME_STRATEGY_ID,
    EXCLUDED_TABLE_NAMES,
    RUN_LENGTH_HISTORY_ROW_LIMIT,
    RUN_LENGTH_ML_MIN_PROBABILITY,
    RUN_LENGTH_STRATEGY_ID,
    _active_daily_experiment_window,
    _adaptive_regime_summary_label,
    _daily_experiment_result,
    _daily_side_label,
    _ensemble_majority_summary_label,
    _estimated_remaining_seconds,
    _filter_live_scores,
    _rank_adaptive_regime_candidates,
    _rank_daily_candidates,
    _rank_ensemble_majority_candidates,
    _rank_run_length_candidates,
    _signal_created_in_active_window,
    _signal_shoe_round,
)
from ..config import AppConfig, load_config, save_config
from ..engine import WorkbenchEngine
from ..ml_live import MlSignalFilter
from ..models import BetSide, TableSnapshot, utc_now_iso_ms
from ..monitor.auto_bettor import (
    BetOrder,
    BetResult,
    LiveAutoBettor,
    autobet_audit_event,
    extract_target_round_and_shoe,
    normalize_bet_side,
    prepare_bet_orders,
)
from ..monitor.browser_launcher import is_cdp_port_open, launch_chrome_cdp
from ..monitor.browser_navigator import run_browser_automation
from ..monitor.cdp import AeSexyCdpMonitor
from ..storage import WorkbenchStore

logger = logging.getLogger("ae_workbench.web")
WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

import sys
if not TEMPLATES_DIR.exists() and getattr(sys, "frozen", False):
    for candidate in [
        Path(getattr(sys, "_MEIPASS", "")).resolve() / "ae_baccarat_workbench" / "web" / "templates",
        Path(sys.executable).resolve().parent / "_internal" / "ae_baccarat_workbench" / "web" / "templates",
        Path(sys.executable).resolve().parent / "ae_baccarat_workbench" / "web" / "templates",
    ]:
        if candidate.exists():
            TEMPLATES_DIR = candidate
            break



def _extract_port_from_cdp_url(cdp_url: str, default: int = 9222) -> int:
    try:
        parsed = urlparse(cdp_url)
        return parsed.port or default
    except Exception:
        return default


class DailyConfigPayload(BaseModel):
    stake: float | None = None
    windows: list[str] | None = None
    autobet: bool | None = None
    stop_win: bool | None = None


class RunLengthConfigPayload(BaseModel):
    stake: float | None = None
    windows: list[str] | None = None
    autobet: bool | None = None


class EnsembleMajorityConfigPayload(BaseModel):
    stake: float | None = None
    windows: list[str] | None = None
    autobet: bool | None = None
    min_probability: float | None = None


class AdaptiveRegimeConfigPayload(BaseModel):
    stake: float | None = None
    windows: list[str] | None = None
    autobet: bool | None = None


class LoginPayload(BaseModel):
    url: str | None = None
    account_id: str | None = None
    account_password: str | None = None
    lobby_name: str | None = None


class WebState:
    def __init__(self) -> None:
        self.config: AppConfig = load_config()
        self.ws_clients: set[WebSocket] = set()
        self.logs: list[dict[str, Any]] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self.selected_windows: set[str] = set(self.config.daily_selected_windows)
        self.daily_autobet_enabled: bool = bool(self.config.daily_autobet_enabled)
        self.daily_stake: float = float(getattr(self.config, "daily_stake", 10.0))
        self.daily_stop_win_enabled: bool = bool(getattr(self.config, "daily_stop_win_enabled", True))
        self.run_length_selected_windows: set[str] = set(self.config.run_length_selected_windows)
        self.run_length_stake: float = float(getattr(self.config, "run_length_stake", 10.0))
        self.run_length_autobet_enabled: bool = bool(getattr(self.config, "run_length_autobet_enabled", False))
        self._run_length_autobet_armed_today: set[str] = set()
        self._run_length_slot_cache_date: str = ""
        self._run_length_consumed_slots: set[tuple[str, str]] = set()

        self.ensemble_majority_selected_windows: set[str] = set(getattr(self.config, "ensemble_majority_selected_windows", ()))
        self.ensemble_majority_stake: float = float(getattr(self.config, "ensemble_majority_stake", 10.0))
        self.ensemble_majority_autobet_enabled: bool = bool(getattr(self.config, "ensemble_majority_autobet_enabled", False))
        self.ensemble_majority_ml_min_probability: float = float(getattr(self.config, "ensemble_majority_ml_min_probability", 0.55))
        self._ensemble_majority_autobet_armed_today: set[str] = set()
        self._ensemble_majority_slot_cache_date: str = ""
        self._ensemble_majority_consumed_slots: set[tuple[str, str]] = set()

        self.adaptive_regime_selected_windows: set[str] = set(getattr(self.config, "adaptive_regime_selected_windows", ()))
        self.adaptive_regime_stake: float = float(getattr(self.config, "adaptive_regime_stake", 10.0))
        self.adaptive_regime_autobet_enabled: bool = bool(getattr(self.config, "adaptive_regime_autobet_enabled", False))
        self._adaptive_regime_autobet_armed_today: set[str] = set()
        self._adaptive_regime_slot_cache_date: str = ""
        self._adaptive_regime_consumed_slots: set[tuple[str, str]] = set()

        self.armed_orders_today: set[str] = set()
        self.table_countdown_readings: dict[str, tuple[float, float]] = {}
        self.cdp_status_text: str = "Chưa kết nối CDP"
        self.cdp_connected: bool = False
        self.monitor: AeSexyCdpMonitor | None = None
        self.monitor_task: asyncio.Task[None] | None = None
        self.monitor_thread: threading.Thread | None = None
        self._monitor_running: bool = False

        self.store: WorkbenchStore = WorkbenchStore(
            self.config.sqlite_abs_path,
            enable_duckdb=False,
        )
        self._run_length_pending = self.store.pending_run_length_hourly_row()
        self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
        self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
        self.ml_filter: MlSignalFilter = MlSignalFilter(
            model_path=self.config.ml_model_abs_path,
            threshold=self.config.ml_decision_threshold,
            enabled=self.config.ml_filter_enabled,
        )
        self.engine: WorkbenchEngine = WorkbenchEngine(
            self.store,
            money_config=self.config.money,
            min_confidence=self.config.min_confidence,
            paper_trading_enabled=self.config.paper_trading_enabled,
            expected_shoe_rounds=self.config.expected_shoe_rounds,
            stop_signals_after_round=self.config.stop_signals_after_round,
            ml_filter=self.ml_filter,
        )

        # Pre-seed engine with latest shoe snapshots from SQLite so table histories
        # and strategy patterns are ready immediately without waiting for fresh shoes.
        try:
            initial_snapshots = self.store.load_latest_snapshots()
            for snap in initial_snapshots:
                self.engine.ingest(snap, generate_signals=False)
            if initial_snapshots:
                self.log(f"Đã nạp trước dữ liệu {len(initial_snapshots)} bàn từ cơ sở dữ liệu.", "info")
        except Exception as exc:
            logger.warning("Could not pre-seed snapshots: %s", exc)

        self.auto_bettor: LiveAutoBettor = LiveAutoBettor(cdp_url=self.config.cdp_url)

    def log(self, message: str, level: str = "info") -> None:
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "message": message,
            "level": level,
        }
        self.logs.append(entry)
        if len(self.logs) > 200:
            self.logs = self.logs[-200:]
        self.dispatch_broadcast({"type": "log", "data": entry})

    def dispatch_broadcast(self, message: dict[str, Any]) -> None:
        if not self._loop or self._loop.is_closed() or not self.ws_clients:
            return
        try:
            running_loop = asyncio.get_running_loop()
            if running_loop is self._loop:
                self._loop.create_task(self.broadcast(message))
            else:
                asyncio.run_coroutine_threadsafe(self.broadcast(message), self._loop)
        except RuntimeError:
            asyncio.run_coroutine_threadsafe(self.broadcast(message), self._loop)

    async def broadcast(self, message: dict[str, Any]) -> None:
        dead_clients: list[WebSocket] = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_json(message)
            except Exception:
                dead_clients.append(ws)
        for dead in dead_clients:
            self.ws_clients.discard(dead)

    def on_snapshot(self, snapshot: TableSnapshot) -> None:
        try:
            signals = self.engine.ingest(snapshot, generate_signals=True)
        except Exception as exc:
            self.log(f"Lỗi xử lý snapshot {snapshot.table_name}: {exc}", "error")
            return

        self._settle_daily_experiment(snapshot.table_name)
        self._settle_run_length_hourly(snapshot.table_name)
        self._settle_ensemble_majority_hourly(snapshot.table_name)
        self._settle_adaptive_regime_hourly(snapshot.table_name)
        self._auto_arm_daily_experiment()
        self._auto_arm_run_length_hourly()
        self._auto_arm_ensemble_majority_hourly()
        self._auto_arm_adaptive_regime_hourly()

        self.dispatch_broadcast({
            "type": "table_snapshot",
            "table_name": snapshot.table_name,
            "round_no": snapshot.current_round_no,
            "total_tables": len(self.engine.table_scores()),
        })

    def on_countdowns(self, countdowns: dict[str, float]) -> None:
        previous_eligible = {
            table_name
            for table_name, seconds in self.remaining_seconds().items()
            if seconds >= DAILY_CANDIDATE_MIN_REMAINING_SECONDS
        }
        observed_monotonic = time.perf_counter()
        for table_name, seconds in countdowns.items():
            self.table_countdown_readings[str(table_name)] = (float(seconds), observed_monotonic)
        # Prune very stale countdown readings (> 5 seconds old)
        stale_threshold = observed_monotonic - 5.0
        self.table_countdown_readings = {
            table: reading
            for table, reading in self.table_countdown_readings.items()
            if reading[1] >= stale_threshold
        }
        current_eligible = {
            table_name
            for table_name, seconds in self.remaining_seconds().items()
            if seconds >= DAILY_CANDIDATE_MIN_REMAINING_SECONDS
        }
        self._auto_arm_daily_experiment()
        self._auto_arm_run_length_hourly()
        self._auto_arm_ensemble_majority_hourly()
        self._auto_arm_adaptive_regime_hourly()
        if previous_eligible != current_eligible:
            self.dispatch_broadcast({"type": "countdowns"})

    def remaining_seconds(self) -> dict[str, float]:
        return _estimated_remaining_seconds(
            self.table_countdown_readings,
            snapshots=getattr(self.engine, "snapshots", None),
        )

    def on_cdp_status(self, message: str) -> None:
        self.cdp_status_text = message
        self.cdp_connected = (
            "Attached" in message
            or "CDP connected" in message
            or "Listening" in message
            or "Live update" in message
        )
        self.log(f"[CDP] {message}", "info")
        self.dispatch_broadcast({"type": "cdp_status", "connected": self.cdp_connected, "message": message})

    def _settle_daily_experiment(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_daily_experiment_bets"):
                settled_ids = self.store.settle_stale_daily_experiment_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self.log(f"🎯 Đã tự động giải phóng {len(settled_ids)} lệnh pending quá hạn", "info")
            return
        row = self.store.pending_daily_experiment_row(table_name)
        if row is None:
            return
        result_event = self.store.next_round_after_fingerprint(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        )
        if result_event is not None:
            outcome = str(result_event["outcome"])
            result, pnl = _daily_experiment_result(
                str(row["side"]),
                outcome,
                float(row["stake"]),
                self.config.money.banker_commission,
            )
            self.store.settle_daily_experiment_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
            )
            result_label = "Thắng" if result == "W" else ("Hòa" if result == "=" else "Thua")
            level = "success" if result == "W" else ("info" if result == "=" else "error")
            self.log(
                f"🎯 Settle {table_name}: {result_label} ({outcome}) | P&L: {pnl:+.2f} điểm",
                level,
            )
        elif self.store.is_shoe_finished_after_signal(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        ):
            self.store.settle_daily_experiment_bet(
                bet_id=int(row["id"]),
                settled_at=utc_now_iso_ms(),
                outcome="VOID",
                result="T",
                pnl=0.0,
            )
            self.log(
                f"🎯 Settle {table_name}: Hủy/Hòa (Hết giày bài) | P&L: +0.00 điểm",
                "info",
            )

    def _auto_arm_daily_experiment(self) -> None:
        now = datetime.now().astimezone()
        active_window = _active_daily_experiment_window(now, self.selected_windows)
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_daily_experiment_bets"):
            self.store.settle_stale_daily_experiment_bets(
                banker_commission=self.config.money.banker_commission,
            )
        session_date = now.date().isoformat()
        today_rows = self.store.daily_experiment_rows(session_date)
        used_today = {str(row["table_name"]) for row in today_rows}
        window_rows = [row for row in today_rows if str(row["session_window"]) == active_window]
        recorded_in_window = len(window_rows)
        if recorded_in_window >= DAILY_EXPERIMENT_MAX_PER_WINDOW:
            return
        if self.daily_stop_win_enabled and any(str(r.get("result", "")) == "W" for r in window_rows):
            return

        with contextlib.suppress(Exception):
            from ..config import load_config
            fresh = load_config()
            if fresh.daily_stake > 0 and fresh.daily_stake != self.daily_stake:
                self.daily_stake = fresh.daily_stake
                self.config = fresh

        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        candidates = _rank_daily_candidates(
            scores,
            self.engine.snapshots,
            self.remaining_seconds(),
        )
        inserted = 0
        new_orders: list[BetOrder] = []

        for score in candidates:
            if (
                score.table_name in used_today
                or recorded_in_window + inserted >= DAILY_EXPERIMENT_MAX_PER_WINDOW
                or self.store.pending_daily_experiment_row(score.table_name) is not None
            ):
                continue
            signal = score.best_signal
            if signal is None or signal.side is None:
                continue
            if not _signal_created_in_active_window(signal, now, active_window):
                continue
            stake = float(self.daily_stake)
            if stake <= 0:
                return
            probability = float(signal.features.get("ml_probability_win", signal.confidence))
            saved = self.store.save_daily_experiment_bet(
                session_date=session_date,
                session_window=active_window,
                created_at=utc_now_iso_ms(),
                table_name=score.table_name,
                strategy_id=signal.strategy_id,
                side=signal.side.value,
                stake=stake,
                signal_fingerprint=signal.round_fingerprint,
                confidence=probability,
                stop_win_enabled=self.daily_stop_win_enabled,
            )
            if saved:
                inserted += 1
                used_today.add(score.table_name)
                order_key = f"{session_date}|{active_window}|{score.table_name}|{signal.round_fingerprint}"
                if order_key not in self.armed_orders_today:
                    self.armed_orders_today.add(order_key)
                    snap = self.engine.snapshots.get(score.table_name)
                    target_round, target_shoe = extract_target_round_and_shoe(
                        fingerprint=signal.round_fingerprint,
                        current_round_no=snap.current_round_no if snap else None,
                        current_shoe=snap.shoe if snap else None,
                    )
                    new_orders.append(
                        BetOrder(
                            table_name=score.table_name,
                            side=signal.side.value,
                            stake=stake,
                            session_window=active_window,
                            order_id=order_key,
                            target_round_no=target_round,
                            target_shoe=target_shoe,
                            signal_fingerprint=signal.round_fingerprint,
                            signal_created_at=signal.created_at,
                            countdown_at_signal=self.remaining_seconds().get(score.table_name),
                        )
                    )
                break

        if inserted:
            self.log(
                f"📝 Khung {active_window}: Tự động ghi nhận {inserted} lệnh Paper mới.",
                "info",
            )
            if new_orders and self.daily_autobet_enabled:
                self.dispatch_autobet(new_orders)

    def dispatch_autobet(self, orders: list[BetOrder], *, source: str = "daily") -> None:
        if not orders:
            return
        orders = prepare_bet_orders(orders, source=source)

        def persist_audit(event: dict[str, Any]) -> None:
            self.store.enqueue_autobet_audit(event)

        for order in orders:
            persist_audit(
                autobet_audit_event(
                    order,
                    stage="DISPATCH_REQUESTED",
                    status="requested",
                    reason_code="DISPATCH_REQUESTED",
                    message="Tín hiệu đủ điều kiện và yêu cầu Auto-Bet đã được tạo.",
                    countdown_seconds=order.countdown_at_signal,
                )
            )
        if self.auto_bettor.is_running:
            for order in orders:
                persist_audit(
                    autobet_audit_event(
                        order,
                        stage="DISPATCH_REJECTED",
                        status="skipped",
                        reason_code="EXECUTOR_BUSY",
                        message="Executor đang xử lý một yêu cầu khác; lệnh mới bị bỏ qua.",
                        countdown_seconds=order.countdown_at_signal,
                    )
                )
            self.log("⚠ [Auto-Bet] Đang có phiên cược khác chạy ngầm. Bỏ qua yêu cầu mới.", "warning")
            return

        order_desc = ", ".join(
            f"{o.table_name}: {'Con (Player)' if normalize_bet_side(o.side) == 'PLAYER' else 'Cái (Banker)'} {o.stake:g} điểm"
            for o in orders
        )
        self.log(f"🚀 [Auto-Bet] Bắt đầu tự động đánh {len(orders)} lệnh ({order_desc})...", "info")

        def on_status(msg: str) -> None:
            self.log(f"[Auto-Bet] {msg}", "info")

        def on_order_done(result: BetResult) -> None:
            level = "success" if result.success else "error"
            self.log(f"[Auto-Bet] {result.order.table_name}: {result.message}", level)

        def on_finished(results: list[BetResult]) -> None:
            confirmed_clicks = sum(1 for result in results if result.confirm_clicked_at)
            provider_accepted = sum(1 for result in results if result.reason_code == "PROVIDER_ACCEPTED")
            self.log(
                f"🏁 [Auto-Bet] Hoàn tất: đã click Xác nhận {confirmed_clicks}/{len(results)} bàn; "
                f"nhà cung cấp xác nhận {provider_accepted}.",
                "success",
            )

        self.auto_bettor.cdp_url = self.config.cdp_url
        self.auto_bettor.execute_orders_background(
            orders=orders,
            on_status=on_status,
            on_order_done=on_order_done,
            on_finished=on_finished,
            on_audit=persist_audit,
        )

    def _ensure_run_length_slot_cache(self, session_date: str) -> None:
        if self._run_length_slot_cache_date == session_date:
            return
        self._run_length_consumed_slots = self.store.run_length_hourly_slot_keys(session_date)
        self._run_length_slot_cache_date = session_date

    def _run_length_candidates(self) -> list[Any]:
        return _rank_run_length_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            self.remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
        )

    def _auto_arm_run_length_hourly(self) -> None:
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self.run_length_selected_windows,
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_run_length_hourly_bets"):
            settled = self.store.settle_stale_run_length_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._run_length_pending = self.store.pending_run_length_hourly_row()
        if self._run_length_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_run_length_slot_cache(session_date)
        except Exception as exc:
            self.log(f"Không kiểm tra được slot Run Length: {exc}", "warning")
            return
        slot_key = (session_date, active_window)
        if slot_key in self._run_length_consumed_slots:
            return

        candidates = [
            signal
            for signal in self._run_length_candidates()
            if _signal_created_in_active_window(signal, now, active_window)
        ]
        if not candidates:
            return
        signal = candidates[0]
        stake = float(self.run_length_stake)
        bet_id = self.store.save_run_length_hourly_bet(
            session_date=session_date,
            session_window=active_window,
            created_at=utc_now_iso_ms(),
            table_name=signal.table_name,
            side=signal.side.value if signal.side else "",
            stake=stake,
            signal_fingerprint=signal.round_fingerprint,
            confidence=float(signal.features.get("ml_probability_win", signal.confidence)),
        )
        if bet_id is None:
            if self.store.run_length_hourly_slot_used(session_date, active_window):
                self._run_length_consumed_slots.add(slot_key)
            self._run_length_pending = self.store.pending_run_length_hourly_row()
            return

        self._run_length_consumed_slots.add(slot_key)
        self._run_length_pending = {
            "id": bet_id,
            "session_date": session_date,
            "session_window": active_window,
            "table_name": signal.table_name,
            "side": signal.side.value if signal.side else "",
            "stake": stake,
            "signal_fingerprint": signal.round_fingerprint,
        }

        order_key = f"rl|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        if self.run_length_autobet_enabled and order_key not in self._run_length_autobet_armed_today:
            self._run_length_autobet_armed_today.add(order_key)
            snap = self.engine.snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=signal.side.value if signal.side else "",
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self.remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and self.run_length_autobet_enabled) else ""
        self.log(
            f"⚡ Run Length: Ghi nhận 1 lệnh paper cho {active_window} tại bàn {signal.table_name} (ML {float(signal.features.get('ml_probability_win', 0)):.1%}){autobet_msg}; chờ kết quả...",
            "info",
        )
        if new_orders and self.run_length_autobet_enabled:
            self.dispatch_autobet(new_orders, source="run_length")

    def _settle_run_length_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_run_length_hourly_bets"):
                settled_ids = self.store.settle_stale_run_length_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._run_length_pending = self.store.pending_run_length_hourly_row()
            return
        row = self._run_length_pending
        if row is None or str(row["table_name"]) != table_name:
            return
        result_event = self.store.exact_next_round_after_fingerprint(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        )
        settled = False
        if result_event is not None:
            outcome = str(result_event["outcome"])
            result, pnl = _daily_experiment_result(
                str(row["side"]),
                outcome,
                float(row["stake"]),
                self.config.money.banker_commission,
            )
            settled = self.store.settle_run_length_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
            )
            result_label = "Thắng" if result == "W" else ("Hòa" if result == "=" else "Thua")
            level = "success" if result == "W" else ("info" if result == "=" else "error")
            self.log(
                f"⚡ Settle Run Length {table_name}: {result_label} ({outcome}) | P&L: {pnl:+.2f} điểm",
                level,
            )
        elif self.store.is_shoe_finished_after_signal(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        ):
            settled = self.store.settle_run_length_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=utc_now_iso_ms(),
                outcome="VOID",
                result="T",
                pnl=0.0,
            )
            self.log(
                f"⚡ Settle Run Length {table_name}: Hủy/Hòa (Hết giày bài) | P&L: +0.00 điểm",
                "info",
            )
        if settled:
            self._run_length_pending = None

    def _ensure_ensemble_majority_slot_cache(self, session_date: str) -> None:
        if self._ensemble_majority_slot_cache_date == session_date:
            return
        self._ensemble_majority_consumed_slots = self.store.ensemble_majority_hourly_slot_keys(session_date)
        self._ensemble_majority_slot_cache_date = session_date

    def _ensemble_majority_candidates(self) -> list[Any]:
        return _rank_ensemble_majority_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            self.remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
            min_probability=self.ensemble_majority_ml_min_probability,
        )

    def _auto_arm_ensemble_majority_hourly(self) -> None:
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self.ensemble_majority_selected_windows,
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_ensemble_majority_hourly_bets"):
            settled = self.store.settle_stale_ensemble_majority_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
        if self._ensemble_majority_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_ensemble_majority_slot_cache(session_date)
        except Exception as exc:
            self.log(f"Không kiểm tra được slot Ensemble Majority: {exc}", "warning")
            return
        slot_key = (session_date, active_window)
        if slot_key in self._ensemble_majority_consumed_slots:
            return

        candidates = [
            signal
            for signal in self._ensemble_majority_candidates()
            if _signal_created_in_active_window(signal, now, active_window)
        ]
        if not candidates:
            return
        signal = candidates[0]
        stake = float(self.ensemble_majority_stake)
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
        bet_id = self.store.save_ensemble_majority_hourly_bet(
            session_date=session_date,
            session_window=active_window,
            created_at=utc_now_iso_ms(),
            table_name=signal.table_name,
            side=signal.side.value if signal.side else "",
            stake=stake,
            signal_fingerprint=signal.round_fingerprint,
            confidence=prob,
        )
        if bet_id is None:
            if self.store.ensemble_majority_hourly_slot_used(session_date, active_window):
                self._ensemble_majority_consumed_slots.add(slot_key)
            self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
            return

        self._ensemble_majority_consumed_slots.add(slot_key)
        self._ensemble_majority_pending = {
            "id": bet_id,
            "session_date": session_date,
            "session_window": active_window,
            "table_name": signal.table_name,
            "side": signal.side.value if signal.side else "",
            "stake": stake,
            "signal_fingerprint": signal.round_fingerprint,
        }

        order_key = f"em|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        if self.ensemble_majority_autobet_enabled and order_key not in self._ensemble_majority_autobet_armed_today:
            self._ensemble_majority_autobet_armed_today.add(order_key)
            snap = self.engine.snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=signal.side.value if signal.side else "",
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self.remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and self.ensemble_majority_autobet_enabled) else ""
        self.log(
            f"⚡ Ensemble Majority: Ghi nhận 1 lệnh paper cho {active_window} tại bàn {signal.table_name} (ML {prob:.1%}){autobet_msg}; chờ kết quả...",
            "info",
        )
        if new_orders and self.ensemble_majority_autobet_enabled:
            self.dispatch_autobet(new_orders, source="ensemble_majority")

    def _settle_ensemble_majority_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_ensemble_majority_hourly_bets"):
                settled_ids = self.store.settle_stale_ensemble_majority_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
            return
        row = self._ensemble_majority_pending
        if row is None or str(row["table_name"]) != table_name:
            return
        result_event = self.store.exact_next_round_after_fingerprint(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        )
        settled = False
        if result_event is not None:
            outcome = str(result_event["outcome"])
            result, pnl = _daily_experiment_result(
                str(row["side"]),
                outcome,
                float(row["stake"]),
                self.config.money.banker_commission,
            )
            settled = self.store.settle_ensemble_majority_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
            )
            result_label = "Thắng" if result == "W" else ("Hòa" if result == "=" else "Thua")
            level = "success" if result == "W" else ("info" if result == "=" else "error")
            self.log(
                f"⚡ Settle Ensemble Majority {table_name}: {result_label} ({outcome}) | P&L: {pnl:+.2f} điểm",
                level,
            )
        elif self.store.is_shoe_finished_after_signal(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        ):
            settled = self.store.settle_ensemble_majority_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=utc_now_iso_ms(),
                outcome="VOID",
                result="T",
                pnl=0.0,
            )
            self.log(
                f"⚡ Settle Ensemble Majority {table_name}: Hủy/Hòa (Hết giày bài) | P&L: +0.00 điểm",
                "info",
            )
        if settled:
            self._ensemble_majority_pending = None

    def _ensure_adaptive_regime_slot_cache(self, session_date: str) -> None:
        if self._adaptive_regime_slot_cache_date == session_date:
            return
        self._adaptive_regime_consumed_slots = self.store.adaptive_regime_hourly_slot_keys(session_date)
        self._adaptive_regime_slot_cache_date = session_date

    def _adaptive_regime_candidates(self) -> list[Any]:
        return _rank_adaptive_regime_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            self.remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
            banker_min_prob=ADAPTIVE_REGIME_BANKER_MIN_ML,
            player_min_prob=ADAPTIVE_REGIME_PLAYER_MIN_ML,
        )

    def _auto_arm_adaptive_regime_hourly(self) -> None:
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self.adaptive_regime_selected_windows,
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_adaptive_regime_hourly_bets"):
            settled = self.store.settle_stale_adaptive_regime_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
        if self._adaptive_regime_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_adaptive_regime_slot_cache(session_date)
        except Exception as exc:
            self.log(f"Không kiểm tra được slot Đa Cầu Thích Ứng: {exc}", "warning")
            return
        slot_key = (session_date, active_window)
        if slot_key in self._adaptive_regime_consumed_slots:
            return

        candidates = [
            signal
            for signal in self._adaptive_regime_candidates()
            if _signal_created_in_active_window(signal, now, active_window)
        ]
        if not candidates:
            return
        signal = candidates[0]
        stake = float(self.adaptive_regime_stake)
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
        pattern = signal.features.get("road_pattern", "")
        bet_id = self.store.save_adaptive_regime_hourly_bet(
            session_date=session_date,
            session_window=active_window,
            created_at=utc_now_iso_ms(),
            table_name=signal.table_name,
            side=signal.side.value if signal.side else "",
            stake=stake,
            signal_fingerprint=signal.round_fingerprint,
            confidence=prob,
        )
        if bet_id is None:
            if self.store.adaptive_regime_hourly_slot_used(session_date, active_window):
                self._adaptive_regime_consumed_slots.add(slot_key)
            self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
            return

        self._adaptive_regime_consumed_slots.add(slot_key)
        self._adaptive_regime_pending = {
            "id": bet_id,
            "session_date": session_date,
            "session_window": active_window,
            "table_name": signal.table_name,
            "side": signal.side.value if signal.side else "",
            "stake": stake,
            "signal_fingerprint": signal.round_fingerprint,
        }

        order_key = f"ar|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        if self.adaptive_regime_autobet_enabled and order_key not in self._adaptive_regime_autobet_armed_today:
            self._adaptive_regime_autobet_armed_today.add(order_key)
            snap = self.engine.snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=signal.side.value if signal.side else "",
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self.remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and self.adaptive_regime_autobet_enabled) else ""
        pattern_msg = f" [{pattern}]" if pattern else ""
        self.log(
            f"⚡ Đa Cầu: Ghi nhận 1 lệnh paper cho {active_window} tại bàn {signal.table_name}{pattern_msg} (ML {prob:.1%}){autobet_msg}; chờ kết quả...",
            "info",
        )
        if new_orders and self.adaptive_regime_autobet_enabled:
            self.dispatch_autobet(new_orders, source="adaptive_regime")

    def _settle_adaptive_regime_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_adaptive_regime_hourly_bets"):
                settled_ids = self.store.settle_stale_adaptive_regime_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
            return
        row = self._adaptive_regime_pending
        if row is None:
            row = self.store.pending_adaptive_regime_hourly_row(table_name)
            if row is not None:
                self._adaptive_regime_pending = row
        if row is None or str(row["table_name"]) != table_name:
            return
        result_event = self.store.exact_next_round_after_fingerprint(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        )
        if result_event is None:
            result_event = self.store.next_round_after_fingerprint(
                table_name=table_name,
                signal_fingerprint=str(row["signal_fingerprint"]),
            )
        settled = False
        if result_event is not None:
            outcome = str(result_event["outcome"])
            result, pnl = _daily_experiment_result(
                str(row["side"]),
                outcome,
                float(row["stake"]),
                self.config.money.banker_commission,
            )
            settled = self.store.settle_adaptive_regime_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
            )
            result_label = "Thắng" if result == "W" else ("Hòa" if result == "=" else "Thua")
            level = "success" if result == "W" else ("info" if result == "=" else "error")
            self.log(
                f"⚡ Settle Đa Cầu {table_name}: {result_label} ({outcome}) | P&L: {pnl:+.2f} điểm",
                level,
            )
        elif self.store.is_shoe_finished_after_signal(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        ):
            settled = self.store.settle_adaptive_regime_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=utc_now_iso_ms(),
                outcome="VOID",
                result="T",
                pnl=0.0,
            )
            self.log(
                f"⚡ Settle Đa Cầu {table_name}: Hủy/Hòa (Hết giày bài) | P&L: +0.00 điểm",
                "info",
            )
        if settled:
            self._adaptive_regime_pending = None

    def _on_monitor_snapshot(self, snapshot: TableSnapshot) -> None:
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self.on_snapshot, snapshot)
        else:
            self.on_snapshot(snapshot)

    def _on_monitor_status(self, message: str) -> None:
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self.on_cdp_status, message)
        else:
            self.on_cdp_status(message)

    def _on_monitor_countdowns(self, countdowns: dict[str, float]) -> None:
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self.on_countdowns, countdowns)
        else:
            self.on_countdowns(countdowns)

    def start_monitor(self) -> None:
        if os.environ.get("AE_TESTING"):
            return
        if self._monitor_running and self.monitor_thread and self.monitor_thread.is_alive():
            return

        self._monitor_running = True

        def _monitor_thread_runner():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            first_run = True
            try:
                while self._monitor_running:
                    try:
                        self.monitor = AeSexyCdpMonitor(
                            cdp_url=self.config.cdp_url,
                            on_snapshot=self._on_monitor_snapshot,
                            on_status=self._on_monitor_status,
                            on_countdowns=self._on_monitor_countdowns,
                            auto_refresh_seconds=self.config.auto_refresh_seconds if self.config.auto_refresh_enabled else None,
                        )
                        loop.run_until_complete(self.monitor.run())
                    except Exception as exc:
                        self.cdp_connected = False
                        self.cdp_status_text = f"Mất kết nối CDP: {exc}"
                        if first_run:
                            self.log("Chưa kết nối được Chrome CDP (cổng 9222). Hãy khởi động Chrome hoặc bấm 'Đăng nhập & Sảnh'.", "warning")
                            first_run = False
                        logger.debug("CDP monitor thread error: %s", exc)
                    finally:
                        if self.monitor:
                            self.monitor.stop()

                    if not self._monitor_running:
                        break
                    time.sleep(2.0)
            finally:
                with contextlib.suppress(Exception):
                    loop.close()

        self.monitor_thread = threading.Thread(target=_monitor_thread_runner, name="ae-web-cdp-monitor", daemon=True)
        self.monitor_thread.start()

    def stop_monitor(self) -> None:
        self._monitor_running = False
        if self.monitor:
            self.monitor.stop()
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread = None


state = WebState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state._loop = asyncio.get_running_loop()
    state.log("Khởi động máy chủ Web Baccarat Workbench Dashboard.", "info")
    import os
    if not os.environ.get("AE_TESTING"):
        state.start_monitor()

    async def heartbeat_loop():
        while True:
            try:
                await asyncio.sleep(1.0)
                state._auto_arm_daily_experiment()
                state._auto_arm_run_length_hourly()
                state._auto_arm_ensemble_majority_hourly()
                state._auto_arm_adaptive_regime_hourly()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Web server heartbeat error: %s", exc)

    heartbeat_task = asyncio.create_task(heartbeat_loop())
    try:
        yield
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        state.stop_monitor()
        state.store.close()
        state.log("Đã dừng máy chủ Web.", "info")


app = FastAPI(title="AE Baccarat Workbench Web Dashboard", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/api/status")
async def get_status():
    now = datetime.now().astimezone()
    active_win = _active_daily_experiment_window(now, state.selected_windows)
    today_rows = state.store.daily_experiment_rows(now.date().isoformat())
    settled_pnl = sum(float(r["pnl"] or 0) for r in today_rows if r["status"] == "settled")
    now_bangkok = datetime.now(BANGKOK_TIMEZONE)
    rl_active_win = _active_daily_experiment_window(now_bangkok, state.run_length_selected_windows)
    rl_rows = state.store.run_length_hourly_rows(now_bangkok.date().isoformat(), None)
    rl_summary = state.store.run_length_hourly_summary(now_bangkok.date().isoformat(), None)
    em_active_win = _active_daily_experiment_window(now_bangkok, state.ensemble_majority_selected_windows)
    em_rows = state.store.ensemble_majority_hourly_rows(now_bangkok.date().isoformat(), None)
    em_summary = state.store.ensemble_majority_hourly_summary(now_bangkok.date().isoformat(), None)
    return {
        "cdp_connected": state.cdp_connected,
        "cdp_status_text": state.cdp_status_text,
        "cdp_url": state.config.cdp_url,
        "active_window": active_win,
        "selected_windows": list(state.selected_windows),
        "daily_autobet_enabled": state.daily_autobet_enabled,
        "daily_stop_win_enabled": state.daily_stop_win_enabled,
        "daily_stake": state.daily_stake,
        "today_pnl": settled_pnl,
        "today_total_bets": len(today_rows),
        "run_length_stake": state.run_length_stake,
        "run_length_selected_windows": list(state.run_length_selected_windows),
        "run_length_active_window": rl_active_win,
        "run_length_autobet_enabled": state.run_length_autobet_enabled,
        "run_length_today_pnl": float(rl_summary.get("total_pnl", 0) or 0),
        "run_length_today_bets": len(rl_rows),
        "ensemble_majority_stake": state.ensemble_majority_stake,
        "ensemble_majority_selected_windows": list(state.ensemble_majority_selected_windows),
        "ensemble_majority_active_window": em_active_win,
        "ensemble_majority_autobet_enabled": state.ensemble_majority_autobet_enabled,
        "ensemble_majority_ml_min_probability": state.ensemble_majority_ml_min_probability,
        "ensemble_majority_today_pnl": float(em_summary.get("total_pnl", 0) or 0),
        "ensemble_majority_today_bets": len(em_rows),
        "is_betting": state.auto_bettor.is_running,
        "ml_ready": state.ml_filter.ready,
        "ml_message": state.ml_filter.status_message(),
        "target_url": state.config.target_url,
        "account_id": state.config.account_id,
        "account_password": state.config.account_password if state.config.remember_credentials else "",
        "ae_lobby_name": state.config.ae_lobby_name,
    }


@app.get("/api/tables")
async def get_tables():
    scores = _filter_live_scores(state.engine.table_scores(), state.config.live_table_stale_seconds)
    tables_data: list[dict[str, Any]] = []

    for score in sorted(scores, key=lambda s: s.table_name):
        snapshot = state.engine.snapshots.get(score.table_name)
        road = snapshot.current_shoe_road() if snapshot else ""
        best_signal = score.best_signal
        ml_prob = 0.0
        ml_side = ""
        ml_strategy = ""

        if best_signal and best_signal.side:
            ml_prob = float(best_signal.features.get("ml_probability_win", best_signal.confidence))
            ml_side = best_signal.side.vi_label
            ml_strategy = best_signal.strategy_id

        tables_data.append({
            "table_name": score.table_name,
            "round_no": score.current_round_no,
            "observed_rounds": score.observed_rounds,
            "road": list(road)[-60:] if road else [],
            "road_counts": {
                "B": road.count("B"),
                "P": road.count("P"),
                "T": road.count("T"),
            },
            "best_signal": {
                "side": ml_side,
                "probability": ml_prob,
                "strategy": ml_strategy,
                "is_pass": ml_prob >= state.config.ml_decision_threshold,
            } if ml_side else None,
            "last_seen": score.last_seen,
        })

    return {"tables": tables_data}


@app.get("/api/autobet/audit")
async def get_autobet_audit(limit: int = 250, attempt_id: str = ""):
    await asyncio.to_thread(state.store.flush_autobet_audit, 1.0)
    safe_limit = max(1, min(int(limit), 1000))
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    return {
        "summary_24h": state.store.autobet_audit_summary(since=since),
        "attempts": [dict(row) for row in state.store.autobet_attempt_rows(safe_limit)],
        "events": [dict(row) for row in state.store.autobet_event_rows(attempt_id)] if attempt_id else [],
    }

@app.get("/api/daily")
async def get_daily():
    now = datetime.now().astimezone()
    session_date = now.date().isoformat()
    active_win = _active_daily_experiment_window(now, state.selected_windows)
    today_rows = state.store.daily_experiment_rows(session_date)

    scores = _filter_live_scores(state.engine.table_scores(), state.config.live_table_stale_seconds)
    remaining_seconds = state.remaining_seconds()
    candidates_ranked = _rank_daily_candidates(
        scores,
        state.engine.snapshots,
        remaining_seconds,
    )
    if active_win:
        candidates_ranked = [
            score
            for score in candidates_ranked
            if _signal_created_in_active_window(score.best_signal, now, active_win)
        ]
    used_today = {str(row["table_name"]) for row in today_rows}

    window_has_won = False
    if active_win:
        window_has_won = any(
            str(r.get("result", "")) == "W"
            for r in today_rows
            if str(r.get("session_window", "")) == active_win
        )

    candidates_list: list[dict[str, Any]] = []
    if not (state.daily_stop_win_enabled and window_has_won):
        for score in candidates_ranked:
            if score.table_name in used_today:
                continue
            sig = score.best_signal
            if not sig or not sig.side:
                continue
            prob = float(sig.features.get("ml_probability_win", sig.confidence))
            candidates_list.append({
                "table_name": score.table_name,
                "strategy": sig.strategy_id,
                "side": sig.side.vi_label,
                "probability": prob,
                "round_no": score.current_round_no,
                "remaining_seconds": round(remaining_seconds.get(score.table_name, 0.0), 1),
                "stake": state.daily_stake,
            })

    return {
        "active_window": active_win,
        "selected_windows": list(state.selected_windows),
        "all_windows": list(DAILY_EXPERIMENT_WINDOW_LABELS),
        "stake": state.daily_stake,
        "autobet_enabled": state.daily_autobet_enabled,
        "stop_win_enabled": state.daily_stop_win_enabled,
        "window_has_won": window_has_won,
        "today_rows": [
            {
                "id": r["id"],
                "window": r["session_window"],
                "table_name": r["table_name"],
                "strategy": r["strategy_id"],
                "side": "Cái" if r["side"] in ("BANKER", "B") else ("Con" if r["side"] in ("PLAYER", "P") else r["side"]),
                "confidence": float(r["confidence"]),
                "stake": float(r["stake"]),
                "result": r["result"] or "-",
                "pnl": float(r["pnl"] or 0),
                "status": r["status"],
            }
            for r in today_rows
        ],
        "candidates": candidates_list[:DAILY_EXPERIMENT_MAX_PER_WINDOW],
    }


@app.post("/api/daily/config")
async def update_daily_config(payload: DailyConfigPayload):
    if payload.stake is not None and payload.stake > 0:
        state.daily_stake = payload.stake
    if payload.windows is not None:
        state.selected_windows = set(payload.windows)
    if payload.autobet is not None:
        state.daily_autobet_enabled = payload.autobet
        state.log(f"Chế độ Live Auto-Bet: {'ĐÃ BẬT' if payload.autobet else 'ĐÃ TẮT'}", "info")
    if payload.stop_win is not None:
        state.daily_stop_win_enabled = payload.stop_win
        state.log(f"Chế độ Stop Win (Khung giờ): {'ĐÃ BẬT' if payload.stop_win else 'ĐÃ TẮT'}", "info")

    from dataclasses import replace
    from ..config import save_config
    try:
        updated = replace(
            state.config,
            daily_selected_windows=tuple(state.selected_windows),
            daily_autobet_enabled=state.daily_autobet_enabled,
            daily_stake=state.daily_stake,
            daily_stop_win_enabled=state.daily_stop_win_enabled,
        )
        save_config(updated)
        state.config = updated
    except Exception as exc:
        state.log(f"Không thể lưu cấu hình ra file: {exc}", "warning")

    return {
        "success": True,
        "stake": state.daily_stake,
        "selected_windows": list(state.selected_windows),
        "autobet_enabled": state.daily_autobet_enabled,
        "stop_win_enabled": state.daily_stop_win_enabled,
    }


@app.post("/api/daily/trigger")
async def trigger_manual_autobet():
    now = datetime.now().astimezone()
    active_win = _active_daily_experiment_window(now, state.selected_windows) or "Thu-cong"
    session_date = now.date().isoformat()
    if active_win and active_win != "Thu-cong" and state.daily_stop_win_enabled:
        if state.store.daily_window_has_won(session_date, active_win):
            return {
                "success": False,
                "message": f"Khung giờ {active_win} đã có lệnh Thắng (Stop Win đang bật). Không vào thêm lệnh.",
            }

    scores = _filter_live_scores(state.engine.table_scores(), state.config.live_table_stale_seconds)
    remaining_seconds = state.remaining_seconds()
    candidates = _rank_daily_candidates(
        scores,
        state.engine.snapshots,
        remaining_seconds,
    )
    if not candidates:
        candidates = [
            s for s in scores
            if s.table_name not in EXCLUDED_TABLE_NAMES
            and s.best_signal
            and s.best_signal.side
            and remaining_seconds.get(s.table_name, -1.0)
            >= DAILY_CANDIDATE_MIN_REMAINING_SECONDS
        ]
        candidates.sort(
            key=lambda s: float(s.best_signal.features.get("ml_probability_win", s.best_signal.confidence)),
            reverse=True,
        )

    if not candidates:
        return {"success": False, "message": "Không có ứng viên ML Pass nào đạt chuẩn."}

    orders: list[BetOrder] = []
    for score in candidates[:DAILY_EXPERIMENT_MAX_PER_WINDOW]:
        sig = score.best_signal
        if sig and sig.side:
            snap = state.engine.snapshots.get(score.table_name) if state.engine else None
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=sig.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            orders.append(
                BetOrder(
                    table_name=score.table_name,
                    side=sig.side.value,
                    stake=state.daily_stake,
                    session_window=active_win,
                    order_id=f"web-manual-{score.table_name}-{utc_now_iso_ms()}",
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=sig.round_fingerprint,
                    signal_created_at=sig.created_at,
                    countdown_at_signal=remaining_seconds.get(score.table_name),
                )
            )

    if not orders:
        return {"success": False, "message": "Không tạo được lệnh cược hợp lệ."}

    state.dispatch_autobet(orders)
    return {
        "success": True,
        "message": f"Đã gửi {len(orders)} lệnh vào hàng đợi cược tự động.",
        "orders": [
            {
                "table": o.table_name,
                "side": o.side,
                "stake": o.stake,
                "target_round": o.target_round_no,
                "target_shoe": o.target_shoe,
            }
            for o in orders
        ],
    }


@app.get("/api/run_length")
async def get_run_length():
    now = datetime.now(BANGKOK_TIMEZONE)
    session_date = now.date().isoformat()
    active_win = _active_daily_experiment_window(now, state.run_length_selected_windows)
    today_rows = state.store.run_length_hourly_rows(session_date, None)
    summary = state.store.run_length_hourly_summary(session_date, None)
    candidates = state._run_length_candidates()
    if active_win:
        candidates = [
            signal
            for signal in candidates
            if _signal_created_in_active_window(signal, now, active_win)
        ]
    history_dates = state.store.run_length_hourly_dates()

    cand_info = None
    if candidates:
        c = candidates[0]
        prob = float(c.features.get("ml_probability_win", c.confidence))
        snap = state.engine.snapshots.get(c.table_name)
        road = snap.current_shoe_road() if snap else ""
        cand_info = {
            "table_name": c.table_name,
            "side": c.side.vi_label if c.side else "",
            "probability": prob,
            "strategy_id": c.strategy_id,
            "round_no": snap.current_round_no if snap else None,
            "shoe": snap.shoe if snap else None,
            "road_tail": list(road)[-20:] if road else [],
            "stake": state.run_length_stake,
        }

    rows_data = []
    for r in today_rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "active_window": active_win,
        "selected_windows": list(state.run_length_selected_windows),
        "all_windows": list(DAILY_EXPERIMENT_WINDOW_LABELS),
        "stake": state.run_length_stake,
        "autobet_enabled": state.run_length_autobet_enabled,
        "pending": state._run_length_pending is not None,
        "pending_bet": state._run_length_pending,
        "candidate": cand_info,
        "today_rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        },
        "dates": history_dates,
    }


@app.post("/api/run_length/config")
async def update_run_length_config(payload: RunLengthConfigPayload):
    if payload.stake is not None and payload.stake > 0:
        state.run_length_stake = payload.stake
    if payload.windows is not None:
        state.run_length_selected_windows = set(payload.windows)
    if payload.autobet is not None:
        state.run_length_autobet_enabled = payload.autobet
        state.log(f"Chế độ Live Auto-Bet Run Length >=58%: {'ĐÃ BẬT' if payload.autobet else 'ĐÃ TẮT'}", "info")

    from dataclasses import replace
    try:
        updated = replace(
            state.config,
            run_length_selected_windows=tuple(state.run_length_selected_windows),
            run_length_stake=state.run_length_stake,
            run_length_autobet_enabled=state.run_length_autobet_enabled,
        )
        save_config(updated)
        state.config = updated
        state.log(f"Đã lưu cấu hình Run Length >=58% (Stake: {state.run_length_stake:g} điểm, {len(state.run_length_selected_windows)} khung giờ, Auto-Bet: {'BẬT' if state.run_length_autobet_enabled else 'TẮT'}).", "info")
    except Exception as exc:
        state.log(f"Không thể lưu cấu hình Run Length: {exc}", "warning")

    return {
        "success": True,
        "stake": state.run_length_stake,
        "selected_windows": list(state.run_length_selected_windows),
        "autobet_enabled": state.run_length_autobet_enabled,
    }


@app.post("/api/run_length/trigger")
async def trigger_manual_run_length_autobet():
    now = datetime.now(BANGKOK_TIMEZONE)
    active_win = _active_daily_experiment_window(now, state.run_length_selected_windows) or "Thu-cong"

    candidates = state._run_length_candidates()
    if not candidates:
        return {
            "success": False,
            "message": "Hiện chưa có bàn nào có tín hiệu Run Length với ML >=58% và còn thời gian cược.",
        }

    signal = candidates[0]
    if not signal or not signal.side:
        return {"success": False, "message": "Không tìm thấy tín hiệu đặt cược Run Length hợp lệ."}

    snap = state.engine.snapshots.get(signal.table_name)
    target_round, target_shoe = extract_target_round_and_shoe(
        fingerprint=signal.round_fingerprint,
        current_round_no=snap.current_round_no if snap else None,
        current_shoe=snap.shoe if snap else None,
    )
    prob = float(signal.features.get("ml_probability_win", signal.confidence))
    order = BetOrder(
        table_name=signal.table_name,
        side=signal.side.value,
        stake=state.run_length_stake,
        session_window=active_win,
        order_id=f"web-manual-rl-{signal.table_name}-{utc_now_iso_ms()}",
        target_round_no=target_round,
        target_shoe=target_shoe,
        signal_fingerprint=signal.round_fingerprint,
        signal_created_at=signal.created_at,
        countdown_at_signal=state.remaining_seconds().get(signal.table_name),
    )

    state.dispatch_autobet([order], source="run_length")
    side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
    return {
        "success": True,
        "message": f"Đã gửi lệnh Run Length ({order.table_name}: {side_str} {order.stake:g} điểm, ML {prob*100:.1f}%) vào hàng đợi cược tự động.",
        "order": {
            "table": order.table_name,
            "side": order.side,
            "stake": order.stake,
            "probability": prob,
            "target_round": order.target_round_no,
            "target_shoe": order.target_shoe,
        },
    }


@app.get("/api/run_length/history")
async def get_run_length_history(date: str | None = None, window: str | None = None):
    session_date = None if not date or date == "Tất cả" else date
    session_window = None if not window or window == "Tất cả" else window
    rows = state.store.run_length_hourly_rows(session_date, session_window, limit=RUN_LENGTH_HISTORY_ROW_LIMIT)
    summary = state.store.run_length_hourly_summary(session_date, session_window)

    rows_data = []
    for r in rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        }
    }


@app.get("/api/ensemble_majority")
async def get_ensemble_majority():
    now = datetime.now(BANGKOK_TIMEZONE)
    session_date = now.date().isoformat()
    active_win = _active_daily_experiment_window(now, state.ensemble_majority_selected_windows)
    today_rows = state.store.ensemble_majority_hourly_rows(session_date, None)
    summary = state.store.ensemble_majority_hourly_summary(session_date, None)
    candidates = state._ensemble_majority_candidates()
    if active_win:
        candidates = [
            signal
            for signal in candidates
            if _signal_created_in_active_window(signal, now, active_win)
        ]
    history_dates = state.store.ensemble_majority_hourly_dates()

    cand_info = None
    if candidates:
        c = candidates[0]
        prob = float(c.features.get("ml_probability_win", c.confidence))
        snap = state.engine.snapshots.get(c.table_name)
        road = snap.current_shoe_road() if snap else ""
        cand_info = {
            "table_name": c.table_name,
            "side": c.side.vi_label if c.side else "",
            "probability": prob,
            "strategy_id": c.strategy_id,
            "round_no": snap.current_round_no if snap else None,
            "shoe": snap.shoe if snap else None,
            "road_tail": list(road)[-20:] if road else [],
            "stake": state.ensemble_majority_stake,
        }

    rows_data = []
    for r in today_rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "active_window": active_win,
        "selected_windows": list(state.ensemble_majority_selected_windows),
        "all_windows": list(DAILY_EXPERIMENT_WINDOW_LABELS),
        "stake": state.ensemble_majority_stake,
        "autobet_enabled": state.ensemble_majority_autobet_enabled,
        "min_probability": state.ensemble_majority_ml_min_probability,
        "pending": state._ensemble_majority_pending is not None,
        "pending_bet": state._ensemble_majority_pending,
        "candidate": cand_info,
        "today_rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        },
        "dates": history_dates,
    }


@app.post("/api/ensemble_majority/config")
async def update_ensemble_majority_config(payload: EnsembleMajorityConfigPayload):
    if payload.stake is not None and payload.stake > 0:
        state.ensemble_majority_stake = payload.stake
    if payload.windows is not None:
        state.ensemble_majority_selected_windows = set(payload.windows)
    if payload.autobet is not None:
        state.ensemble_majority_autobet_enabled = payload.autobet
        state.log(f"Chế độ Live Auto-Bet Ensemble Majority: {'ĐÃ BẬT' if payload.autobet else 'ĐÃ TẮT'}", "info")
    if payload.min_probability is not None and 0.50 <= payload.min_probability <= 1.0:
        state.ensemble_majority_ml_min_probability = payload.min_probability

    from dataclasses import replace
    try:
        updated = replace(
            state.config,
            ensemble_majority_selected_windows=tuple(state.ensemble_majority_selected_windows),
            ensemble_majority_stake=state.ensemble_majority_stake,
            ensemble_majority_autobet_enabled=state.ensemble_majority_autobet_enabled,
            ensemble_majority_ml_min_probability=state.ensemble_majority_ml_min_probability,
        )
        save_config(updated)
        state.config = updated
        state.log(f"Đã lưu cấu hình Ensemble Majority (Stake: {state.ensemble_majority_stake:g} điểm, ML >= {state.ensemble_majority_ml_min_probability:.0%}, {len(state.ensemble_majority_selected_windows)} khung giờ, Auto-Bet: {'BẬT' if state.ensemble_majority_autobet_enabled else 'TẮT'}).", "info")
    except Exception as exc:
        state.log(f"Không thể lưu cấu hình Ensemble Majority: {exc}", "warning")

    return {
        "success": True,
        "stake": state.ensemble_majority_stake,
        "selected_windows": list(state.ensemble_majority_selected_windows),
        "autobet_enabled": state.ensemble_majority_autobet_enabled,
        "min_probability": state.ensemble_majority_ml_min_probability,
    }


@app.post("/api/ensemble_majority/trigger")
async def trigger_manual_ensemble_majority_autobet():
    now = datetime.now(BANGKOK_TIMEZONE)
    active_win = _active_daily_experiment_window(now, state.ensemble_majority_selected_windows) or "Thu-cong"

    candidates = state._ensemble_majority_candidates()
    if not candidates:
        return {
            "success": False,
            "message": f"Hiện chưa có bàn nào có tín hiệu Ensemble Majority với ML >={state.ensemble_majority_ml_min_probability:.0%} và còn thời gian cược.",
        }

    signal = candidates[0]
    if not signal or not signal.side:
        return {"success": False, "message": "Không tìm thấy tín hiệu đặt cược Ensemble Majority hợp lệ."}

    snap = state.engine.snapshots.get(signal.table_name)
    target_round, target_shoe = extract_target_round_and_shoe(
        fingerprint=signal.round_fingerprint,
        current_round_no=snap.current_round_no if snap else None,
        current_shoe=snap.shoe if snap else None,
    )
    prob = float(signal.features.get("ml_probability_win", signal.confidence))
    order = BetOrder(
        table_name=signal.table_name,
        side=signal.side.value,
        stake=state.ensemble_majority_stake,
        session_window=active_win,
        order_id=f"web-manual-em-{signal.table_name}-{utc_now_iso_ms()}",
        target_round_no=target_round,
        target_shoe=target_shoe,
        signal_fingerprint=signal.round_fingerprint,
        signal_created_at=signal.created_at,
        countdown_at_signal=state.remaining_seconds().get(signal.table_name),
    )

    state.dispatch_autobet([order], source="ensemble_majority")
    side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
    return {
        "success": True,
        "message": f"Đã gửi lệnh Ensemble Majority ({order.table_name}: {side_str} {order.stake:g} điểm, ML {prob*100:.1f}%) vào hàng đợi cược tự động.",
        "order": {
            "table": order.table_name,
            "side": order.side,
            "stake": order.stake,
            "probability": prob,
            "target_round": order.target_round_no,
            "target_shoe": order.target_shoe,
        },
    }


@app.get("/api/ensemble_majority/history")
async def get_ensemble_majority_history(date: str | None = None, window: str | None = None):
    session_date = None if not date or date == "Tất cả" else date
    session_window = None if not window or window == "Tất cả" else window
    rows = state.store.ensemble_majority_hourly_rows(session_date, session_window, limit=ENSEMBLE_MAJORITY_HISTORY_ROW_LIMIT)
    summary = state.store.ensemble_majority_hourly_summary(session_date, session_window)

    rows_data = []
    for r in rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        }
    }


@app.get("/api/adaptive_regime")
async def get_adaptive_regime():
    now = datetime.now(BANGKOK_TIMEZONE)
    session_date = now.date().isoformat()
    active_win = _active_daily_experiment_window(now, state.adaptive_regime_selected_windows)
    today_rows = state.store.adaptive_regime_hourly_rows(session_date, None)
    summary = state.store.adaptive_regime_hourly_summary(session_date, None)
    candidates = state._adaptive_regime_candidates()
    if active_win:
        candidates = [
            signal
            for signal in candidates
            if _signal_created_in_active_window(signal, now, active_win)
        ]
    history_dates = state.store.adaptive_regime_hourly_dates()

    cand_info = None
    if candidates:
        c = candidates[0]
        prob = float(c.features.get("ml_probability_win", c.confidence))
        pattern = c.features.get("road_pattern", "")
        snap = state.engine.snapshots.get(c.table_name)
        road = snap.current_shoe_road() if snap else ""
        cand_info = {
            "table_name": c.table_name,
            "side": c.side.vi_label if c.side else "",
            "probability": prob,
            "pattern": pattern,
            "strategy_id": c.strategy_id,
            "round_no": snap.current_round_no if snap else None,
            "shoe": snap.shoe if snap else None,
            "road_tail": list(road)[-20:] if road else [],
            "stake": state.adaptive_regime_stake,
        }

    rows_data = []
    for r in today_rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "active_window": active_win,
        "selected_windows": list(state.adaptive_regime_selected_windows),
        "all_windows": list(DAILY_EXPERIMENT_WINDOW_LABELS),
        "stake": state.adaptive_regime_stake,
        "autobet_enabled": state.adaptive_regime_autobet_enabled,
        "banker_min_probability": ADAPTIVE_REGIME_BANKER_MIN_ML,
        "player_min_probability": ADAPTIVE_REGIME_PLAYER_MIN_ML,
        "pending": state._adaptive_regime_pending is not None,
        "pending_bet": state._adaptive_regime_pending,
        "candidate": cand_info,
        "today_rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        },
        "dates": history_dates,
    }


@app.post("/api/adaptive_regime/config")
async def update_adaptive_regime_config(payload: AdaptiveRegimeConfigPayload):
    if payload.stake is not None and payload.stake > 0:
        state.adaptive_regime_stake = payload.stake
    if payload.windows is not None:
        state.adaptive_regime_selected_windows = set(payload.windows)
    if payload.autobet is not None:
        state.adaptive_regime_autobet_enabled = payload.autobet
        state.log(f"Chế độ Live Auto-Bet Đa Cầu Thích Ứng: {'ĐÃ BẬT' if payload.autobet else 'ĐÃ TẮT'}", "info")

    from dataclasses import replace
    try:
        updated = replace(
            state.config,
            adaptive_regime_selected_windows=tuple(state.adaptive_regime_selected_windows),
            adaptive_regime_stake=state.adaptive_regime_stake,
            adaptive_regime_autobet_enabled=state.adaptive_regime_autobet_enabled,
        )
        save_config(updated)
        state.config = updated
        state.log(f"Đã lưu cấu hình Đa Cầu (Stake: {state.adaptive_regime_stake:g} điểm, {len(state.adaptive_regime_selected_windows)} khung giờ, Auto-Bet: {'BẬT' if state.adaptive_regime_autobet_enabled else 'TẮT'}).", "info")
    except Exception as exc:
        state.log(f"Không thể lưu cấu hình Đa Cầu: {exc}", "warning")

    return {
        "success": True,
        "stake": state.adaptive_regime_stake,
        "selected_windows": list(state.adaptive_regime_selected_windows),
        "autobet_enabled": state.adaptive_regime_autobet_enabled,
    }


@app.post("/api/adaptive_regime/trigger")
async def trigger_manual_adaptive_regime_autobet():
    now = datetime.now(BANGKOK_TIMEZONE)
    active_win = _active_daily_experiment_window(now, state.adaptive_regime_selected_windows) or "Thu-cong"

    candidates = state._adaptive_regime_candidates()
    if not candidates:
        return {
            "success": False,
            "message": "Hiện chưa có bàn nào có tín hiệu Đa Cầu Thích Ứng đạt chuẩn (Banker >=57%, Player >=52.5%) và còn thời gian cược.",
        }

    signal = candidates[0]
    if not signal or not signal.side:
        return {"success": False, "message": "Không tìm thấy tín hiệu đặt cược Đa Cầu hợp lệ."}

    snap = state.engine.snapshots.get(signal.table_name)
    target_round, target_shoe = extract_target_round_and_shoe(
        fingerprint=signal.round_fingerprint,
        current_round_no=snap.current_round_no if snap else None,
        current_shoe=snap.shoe if snap else None,
    )
    prob = float(signal.features.get("ml_probability_win", signal.confidence))
    pattern = signal.features.get("road_pattern", "")
    order = BetOrder(
        table_name=signal.table_name,
        side=signal.side.value,
        stake=state.adaptive_regime_stake,
        session_window=active_win,
        order_id=f"web-manual-ar-{signal.table_name}-{utc_now_iso_ms()}",
        target_round_no=target_round,
        target_shoe=target_shoe,
        signal_fingerprint=signal.round_fingerprint,
        signal_created_at=signal.created_at,
        countdown_at_signal=state.remaining_seconds().get(signal.table_name),
    )

    state.dispatch_autobet([order], source="adaptive_regime")
    side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
    pattern_str = f" [{pattern}]" if pattern else ""
    return {
        "success": True,
        "message": f"Đã gửi lệnh Đa Cầu ({order.table_name}: {side_str}{pattern_str} {order.stake:g} điểm, ML {prob*100:.1f}%) vào hàng đợi cược tự động.",
        "order": {
            "table": order.table_name,
            "side": order.side,
            "stake": order.stake,
            "probability": prob,
            "pattern": pattern,
            "target_round": order.target_round_no,
            "target_shoe": order.target_shoe,
        },
    }


@app.get("/api/adaptive_regime/history")
async def get_adaptive_regime_history(date: str | None = None, window: str | None = None):
    session_date = None if not date or date == "Tất cả" else date
    session_window = None if not window or window == "Tất cả" else window
    rows = state.store.adaptive_regime_hourly_rows(session_date, session_window, limit=ADAPTIVE_REGIME_HISTORY_ROW_LIMIT)
    summary = state.store.adaptive_regime_hourly_summary(session_date, session_window)

    rows_data = []
    for r in rows:
        shoe, round_no = _signal_shoe_round(str(r["signal_fingerprint"]))
        rows_data.append({
            "id": r["id"],
            "date": r["session_date"],
            "window": r["session_window"],
            "table_name": r["table_name"],
            "shoe": shoe,
            "round_no": round_no,
            "side": _daily_side_label(str(r["side"])),
            "confidence": float(r["confidence"]),
            "stake": float(r["stake"]),
            "result": r["result"] or "-",
            "pnl": float(r["pnl"] or 0),
            "status": r["status"],
        })

    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0

    return {
        "rows": rows_data,
        "summary": {
            "total_count": int(summary.get("total_count", 0) or 0),
            "win_count": wins,
            "loss_count": losses,
            "tie_count": int(summary.get("tie_count", 0) or 0),
            "pending_count": int(summary.get("pending_count", 0) or 0),
            "total_pnl": float(summary.get("total_pnl", 0) or 0),
            "win_rate": win_rate,
        }
    }


@app.post("/api/browser/login")
async def trigger_browser_login(payload: LoginPayload):
    url = (payload.url or state.config.target_url or "").strip()
    acc_id = (payload.account_id or state.config.account_id or "").strip()
    acc_pwd = (payload.account_password or state.config.account_password or "").strip()
    lobby = (payload.lobby_name or state.config.ae_lobby_name or "Sexy Casino, AE Sexy").strip()

    from dataclasses import replace
    try:
        updated = replace(
            state.config,
            target_url=url,
            account_id=acc_id,
            account_password=acc_pwd if state.config.remember_credentials else "",
            ae_lobby_name=lobby,
        )
        save_config(updated)
        state.config = updated
    except Exception as exc:
        logger.warning("Could not save config: %s", exc)

    def run_login_thread():
        try:
            port = _extract_port_from_cdp_url(state.config.cdp_url, default=9222)
            if not is_cdp_port_open(port):
                state.log(f"Cổng CDP {port} chưa mở, đang tự động khởi chạy Chrome...", "info")
                ok, launch_msg = launch_chrome_cdp(
                    port=port,
                    chrome_path=state.config.chrome_path or None,
                    target_url=url or None,
                )
                state.log(f"{'✅' if ok else '❌'} {launch_msg}", "info" if ok else "error")
                if not ok:
                    return
                time.sleep(2.0)
            else:
                state.log(f"Cổng CDP {port} đã mở và sẵn sàng.", "info")

            state.log("Đang kết nối Playwright và tự động đăng nhập vào sảnh AE Sexy...", "info")
            success, final_msg = asyncio.run(
                run_browser_automation(
                    cdp_url=state.config.cdp_url,
                    target_url=url,
                    username=acc_id,
                    password=acc_pwd,
                    lobby_name=lobby,
                    on_status=lambda m: state.log(f"[Auto-Login] {m}", "info"),
                    custom_casino_selector=state.config.custom_casino_selector or None,
                    custom_ae_selector=state.config.custom_ae_selector or None,
                )
            )
            level = "success" if success else "error"
            state.log(f"🏁 {final_msg}", level)
            if success:
                state.log("Đăng nhập thành công! Bắt đầu kích hoạt giám sát sảnh cược...", "success")
                if state._loop:
                    state._loop.call_soon_threadsafe(state.start_monitor)
        except Exception as exc:
            state.log(f"❌ Lỗi tự động hóa đăng nhập: {exc}", "error")
            logger.exception("Error in run_login_thread")

    import threading
    t = threading.Thread(target=run_login_thread, name="browser-login-worker", daemon=True)
    t.start()
    return {"success": True, "message": "Đang mở trình duyệt Chrome và tự động đăng nhập..."}


@app.post("/api/browser/launch")
async def trigger_browser_launch():
    port = _extract_port_from_cdp_url(state.config.cdp_url, default=9222)
    url = state.config.target_url or ""

    def run_launch():
        try:
            state.log(f"Đang khởi chạy Google Chrome tại cổng {port}...", "info")
            ok, msg = launch_chrome_cdp(
                port=port,
                chrome_path=state.config.chrome_path or None,
                target_url=url or None,
            )
            level = "success" if ok else "error"
            state.log(f"{'✅' if ok else '❌'} {msg}", level)
            if ok and state._loop:
                state._loop.call_soon_threadsafe(state.start_monitor)
        except Exception as exc:
            state.log(f"❌ Lỗi khởi chạy Chrome: {exc}", "error")

    import threading
    threading.Thread(target=run_launch, name="browser-launch-worker", daemon=True).start()
    return {"success": True, "message": f"Đang khởi chạy Chrome cổng {port}..."}


@app.post("/api/cdp/reconnect")
async def trigger_cdp_reconnect():
    state.stop_monitor()
    await asyncio.sleep(0.5)
    state.start_monitor()
    state.log("Đã yêu cầu kết nối lại Chrome CDP.", "info")
    return {"success": True, "message": "Đang kết nối lại CDP..."}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    state.ws_clients.add(websocket)
    try:
        # Send initial state
        await websocket.send_json({
            "type": "init",
            "logs": state.logs[-30:],
            "cdp_connected": state.cdp_connected,
            "autobet_enabled": state.daily_autobet_enabled,
        })
        while True:
            data = await websocket.receive_text()
            # Handle client messages if any
    except WebSocketDisconnect:
        state.ws_clients.discard(websocket)
    except Exception:
        state.ws_clients.discard(websocket)


def main():
    import uvicorn
    import webbrowser
    port = 8000
    host = "0.0.0.0"
    print(f"\n=======================================================", flush=True)
    print(f"  AE BACCARAT WORKBENCH - WEB DASHBOARD DANG KHOI CHAY  ", flush=True)
    print(f"  Truy cap tren may tinh:   http://localhost:{port}", flush=True)
    print(f"  Truy cap tren dien thoai: http://<IP_MAY_TINH>:{port}", flush=True)
    print(f"=======================================================\n", flush=True)
    
    # Auto open browser
    with contextlib.suppress(Exception):
        webbrowser.open(f"http://localhost:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
