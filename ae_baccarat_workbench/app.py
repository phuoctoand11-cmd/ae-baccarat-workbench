from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import queue
import threading
import time
import tkinter as tk
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from tkinter import messagebox, ttk
from typing import Any

from .ae_decode import parse_manual_sequence
from .config import AppConfig, URL_PRESETS, load_config, parse_stake_chain, save_config
from .engine import WorkbenchEngine, signal_side_label
from .ml_live import MlSignalFilter
from .models import (
    BetSide,
    LatencySample,
    MoneyConfig,
    StrategyAction,
    StrategySignal,
    TableSnapshot,
    utc_now_iso_ms,
)
from .monitor.auto_bettor import (
    BetOrder,
    BetResult,
    LiveAutoBettor,
    autobet_audit_event,
    extract_target_round_and_shoe,
    normalize_bet_side,
    prepare_bet_orders,
    resolve_bet_side,
)
from .monitor.browser_launcher import DEFAULT_CDP_PORT, is_cdp_port_open, launch_chrome_cdp
from .monitor.browser_navigator import run_browser_automation
from .monitor.cdp import AeSexyCdpMonitor
from .storage import WorkbenchStore

logger = logging.getLogger(__name__)

QUEUE_IDLE_REFRESH_MS = 300
QUEUE_BUSY_REFRESH_MS = 50
MAX_QUEUE_ITEMS_PER_TICK = 80
QUEUE_PROCESS_TIME_BUDGET_MS = 250
LIVE_VIEW_REFRESH_MIN_INTERVAL_MS = 400
HISTORY_VIEW_REFRESH_MIN_INTERVAL_MS = 5000
STALE_QUEUE_THRESHOLD_MS = 5000
EXCLUDED_TABLE_NAMES = {f"Table {index}" for index in range(1, 15)}
DAILY_EXPERIMENT_WINDOWS = tuple(
    (f"{hour:02d}:00-{hour + 1:02d}:00", hour * 60, (hour + 1) * 60)
    for hour in range(24)
)
DAILY_EXPERIMENT_WINDOW_LABELS = tuple(label for label, _start, _end in DAILY_EXPERIMENT_WINDOWS)
DAILY_HISTORY_WINDOW_LABELS = (*DAILY_EXPERIMENT_WINDOW_LABELS, "18:00-20:00")
DAILY_EXPERIMENT_MAX_PER_WINDOW = 2
DAILY_CANDIDATE_MIN_REMAINING_SECONDS = 10.0
COUNTDOWN_READING_MAX_AGE_SECONDS = 2.0
DAILY_HISTORY_ALL = "Tất cả"
DAILY_HISTORY_ROW_LIMIT = 250
RUN_LENGTH_STRATEGY_ID = "run_length"
RUN_LENGTH_ML_MIN_PROBABILITY = 0.58
RUN_LENGTH_HISTORY_ROW_LIMIT = 250
ENSEMBLE_MAJORITY_STRATEGY_ID = "ensemble_majority"
ENSEMBLE_MAJORITY_ML_MIN_PROBABILITY = 0.55
ENSEMBLE_MAJORITY_HISTORY_ROW_LIMIT = 250
ADAPTIVE_REGIME_STRATEGY_ID = "adaptive_regime"
ADAPTIVE_REGIME_BANKER_MIN_ML = 0.57
ADAPTIVE_REGIME_PLAYER_MIN_ML = 0.525
ADAPTIVE_REGIME_HISTORY_ROW_LIMIT = 250
BANGKOK_TIMEZONE = timezone(timedelta(hours=7))
_REAL_DATETIME = datetime
BET_MODE_FORWARD = "Đánh Thuận"
BET_MODE_INVERSE = "Đánh Ngược"
BET_MODE_OPTIONS = (BET_MODE_FORWARD, BET_MODE_INVERSE)


def _bet_mode_to_label(mode: str) -> str:
    return BET_MODE_INVERSE if str(mode or "").strip().lower() in ("inverse", "nguoc", "ngược", "reverse", "flip", "đánh ngược", "danh nguoc") else BET_MODE_FORWARD


def _label_to_bet_mode(label: str) -> str:
    return "inverse" if label == BET_MODE_INVERSE else "forward"


class BaccaratWorkbenchApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("AE Baccarat Workbench - Signal + Paper Trading")
        self.root.geometry("1180x760")
        self.root.minsize(980, 620)

        self.config = load_config()
        self.store = WorkbenchStore(
            self.config.sqlite_abs_path,
            self.config.duckdb_abs_path,
            enable_duckdb=self.config.enable_duckdb,
        )
        self.ml_filter = MlSignalFilter(
            self.config.ml_model_abs_path,
            threshold=self.config.ml_decision_threshold,
            enabled=self.config.ml_filter_enabled,
        )
        self.engine = WorkbenchEngine(
            self.store,
            money_config=self.config.money,
            min_confidence=self.config.min_confidence,
            paper_trading_enabled=self.config.paper_trading_enabled,
            expected_shoe_rounds=self.config.expected_shoe_rounds,
            stop_signals_after_round=self.config.stop_signals_after_round,
            ml_filter=self.ml_filter,
        )
        self._seed_snapshots_from_store()
        self.queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.monitor: AeSexyCdpMonitor | None = None
        self.monitor_thread: threading.Thread | None = None
        self.monitor_running = False
        self._pending_latency_samples: list[dict[str, Any]] = []
        self._queued_snapshot_payloads: dict[str, dict[str, Any]] = {}
        self._queued_snapshot_lock = threading.Lock()
        self._last_live_views_refresh_monotonic = 0.0
        self._last_history_views_refresh_monotonic = 0.0
        self._live_views_dirty = False
        self._history_views_dirty = False
        self._dashboard_wl_cache: dict[str, dict[str, str]] = {}
        self._dashboard_totals_cache: dict[str, float | int] = {
            "settled_count": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "pnl": 0.0,
        }
        self._dashboard_stats_pending_tables: set[str] = set()
        self._dashboard_stats_refresh_running = False
        self._dashboard_stats_retry_after_monotonic = 0.0
        self._dashboard_row_values: dict[str, tuple[Any, ...]] = {}
        self._autobet_audit_dirty = True
        self._autobet_audit_refresh_scheduled = False
        self._table_countdown_readings: dict[str, tuple[float, float]] = {}
        configured_daily_windows = set(self.config.daily_selected_windows)
        self.daily_window_vars = {
            label: tk.BooleanVar(value=label in configured_daily_windows)
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
        }
        self.daily_stop_win_var = tk.BooleanVar(
            value=bool(getattr(self.config, "daily_stop_win_enabled", True))
        )
        configured_run_length_windows = set(self.config.run_length_selected_windows)
        self.run_length_window_vars = {
            label: tk.BooleanVar(value=label in configured_run_length_windows)
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
        }
        self._run_length_pending = self.store.pending_run_length_hourly_row()
        self._run_length_slot_cache_date = ""
        self._run_length_consumed_slots: set[tuple[str, str]] = set()
        self._run_length_today_dirty = True
        self._run_length_history_dirty = True
        self._run_length_history_loaded_key: tuple[str | None, str | None] | None = None
        self.run_length_autobet_var = tk.BooleanVar(
            value=bool(getattr(self.config, "run_length_autobet_enabled", False))
        )
        self.run_length_bet_mode_var = tk.StringVar(
            value=_bet_mode_to_label(getattr(self.config, "run_length_bet_mode", "forward"))
        )
        self._run_length_autobet_armed_today: set[str] = set()

        configured_ensemble_majority_windows = set(getattr(self.config, "ensemble_majority_selected_windows", ()))
        self.ensemble_majority_window_vars = {
            label: tk.BooleanVar(value=label in configured_ensemble_majority_windows)
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
        }
        self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
        self._ensemble_majority_slot_cache_date = ""
        self._ensemble_majority_consumed_slots: set[tuple[str, str]] = set()
        self._ensemble_majority_today_dirty = True
        self._ensemble_majority_history_dirty = True
        self._ensemble_majority_history_loaded_key: tuple[str | None, str | None] | None = None
        self.ensemble_majority_autobet_var = tk.BooleanVar(
            value=bool(getattr(self.config, "ensemble_majority_autobet_enabled", False))
        )
        self.ensemble_majority_bet_mode_var = tk.StringVar(
            value=_bet_mode_to_label(getattr(self.config, "ensemble_majority_bet_mode", "forward"))
        )
        self._ensemble_majority_autobet_armed_today: set[str] = set()

        configured_adaptive_regime_windows = set(getattr(self.config, "adaptive_regime_selected_windows", ()))
        self.adaptive_regime_window_vars = {
            label: tk.BooleanVar(value=label in configured_adaptive_regime_windows)
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
        }
        self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
        self._adaptive_regime_slot_cache_date = ""
        self._adaptive_regime_consumed_slots: set[tuple[str, str]] = set()
        self._adaptive_regime_today_dirty = True
        self._adaptive_regime_history_dirty = True
        self._adaptive_regime_history_loaded_key: tuple[str | None, str | None] | None = None
        self.adaptive_regime_autobet_var = tk.BooleanVar(
            value=bool(getattr(self.config, "adaptive_regime_autobet_enabled", False))
        )
        self.adaptive_regime_bet_mode_var = tk.StringVar(
            value=_bet_mode_to_label(getattr(self.config, "adaptive_regime_bet_mode", "forward"))
        )
        self._adaptive_regime_autobet_armed_today: set[str] = set()

        self.cdp_var = tk.StringVar(value=self.config.cdp_url)
        self.auto_refresh_enabled_var = tk.BooleanVar(value=self.config.auto_refresh_enabled)
        self.auto_refresh_seconds_var = tk.StringVar(value=str(self.config.auto_refresh_seconds))
        self.manual_table_var = tk.StringVar(value=self.config.manual_table_name)
        self.target_url_var = tk.StringVar(value=self.config.target_url)
        self.preset_var = tk.StringVar()
        if "svft388" in self.config.target_url or "sv388" in self.config.target_url:
            self.preset_var.set("SV388 (svft388.com)")
        elif "8887799" in self.config.target_url or "bong88" in self.config.target_url:
            self.preset_var.set("Bong88 (8887799.net)")
        else:
            self.preset_var.set("Tùy chỉnh")
        self.account_id_var = tk.StringVar(value=self.config.account_id)
        self.account_password_var = tk.StringVar(value=self.config.account_password)
        self.remember_credentials_var = tk.BooleanVar(value=self.config.remember_credentials)
        self.show_password_var = tk.BooleanVar(value=False)
        self.browser_port_var = tk.StringVar(value=str(_parse_cdp_port(self.config.cdp_url)))
        self.ae_lobby_var = tk.StringVar(value=self.config.ae_lobby_name or "AE Sexy, Sexy Casino")
        self.automation_status_var = tk.StringVar(value="Sẵn sàng: Nhập URL, ID & Pass để tự động vào sảnh AE Sexy.")
        self.automation_running = False
        self.daily_autobet_var = tk.BooleanVar(value=bool(self.config.daily_autobet_enabled))
        self.daily_bet_mode_var = tk.StringVar(
            value=_bet_mode_to_label(getattr(self.config, "daily_bet_mode", "forward"))
        )
        self.auto_bettor = LiveAutoBettor(cdp_url=self.config.cdp_url)
        self._autobet_armed_today: set[str] = set()
        self._last_auto_arm_monotonic: float = 0.0
        self._monitor_user_requested_stop: bool = False

        self._build_ui()
        self._initial_view_population()
        analytics_status = self.store.analytics_status()
        self._append_live_log(analytics_status)
        quality_exclusion_count = len(self.store.data_quality_exclusion_rows())
        if quality_exclusion_count:
            self._append_live_log(
                f"Data quality: {quality_exclusion_count} khoảng nghi vấn đang bị loại khỏi ML/thống kê."
            )
        self._append_live_log(self.ml_filter.status_message())
        if "NOT enabled" in analytics_status:
            self._set_status(analytics_status)
        else:
            self._set_status("Sẵn sàng. Có thể nhập tay hoặc bắt đầu CDP monitor.")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(QUEUE_IDLE_REFRESH_MS, self._process_queue)
        self._start_background_watchdog()

    def _seed_snapshots_from_store(self) -> None:
        try:
            stored_snapshots = self.store.load_latest_snapshots()
            for snap in stored_snapshots:
                if snap.table_name.strip() in EXCLUDED_TABLE_NAMES:
                    continue
                self.engine.ingest(snap, generate_signals=False)
        except Exception as exc:
            logger.warning("Không thể khởi tạo snapshots từ SQLite: %s", exc)

    def _initial_view_population(self) -> None:
        try:
            self._refresh_latency_tree()
            self._refresh_autobet_audit_tree()
            self._refresh_dashboard_tree()
            self._refresh_signal_tree()
            self._refresh_paper_tree()
            self._refresh_daily_tab()
            self._refresh_run_length_tab()
            self._refresh_ensemble_majority_tab()
            self._refresh_adaptive_regime_tab()
        except Exception as exc:
            logger.warning("Lỗi khởi tạo dữ liệu views ban đầu: %s", exc)

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self.root)
        self.notebook = notebook
        notebook.grid(row=0, column=0, sticky="nsew")

        self.live_tab = ttk.Frame(notebook, padding=12)
        self.dashboard_tab = ttk.Frame(notebook, padding=12)
        self.signals_tab = ttk.Frame(notebook, padding=12)
        self.latency_tab = ttk.Frame(notebook, padding=12)
        self.autobet_audit_tab = ttk.Frame(notebook, padding=12)
        self.daily_tab = ttk.Frame(notebook, padding=12)
        self.run_length_tab = ttk.Frame(notebook, padding=12)
        self.ensemble_majority_tab = ttk.Frame(notebook, padding=12)
        self.adaptive_regime_tab = ttk.Frame(notebook, padding=12)
        self.config_tab = ttk.Frame(notebook, padding=12)

        notebook.add(self.live_tab, text="Live Monitor")
        notebook.add(self.dashboard_tab, text="Theo dõi nhiều bàn")
        notebook.add(self.signals_tab, text="Signal + Paper")
        notebook.add(self.latency_tab, text="Latency")
        notebook.add(self.autobet_audit_tab, text="Auto-Bet Audit")
        notebook.add(self.daily_tab, text="Paper theo khung giờ")
        notebook.add(self.run_length_tab, text="Run Length >=58%")
        notebook.add(self.ensemble_majority_tab, text="Ensemble Majority >=55%")
        notebook.add(self.adaptive_regime_tab, text="Đa Cầu Thích Ứng (1-1 & 2-2)")
        notebook.add(self.config_tab, text="Cấu hình")

        self.status_var = tk.StringVar(value="")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, anchor="w", padding=(8, 4))
        status_bar.grid(row=1, column=0, sticky="ew")

        self._build_live_tab()
        self._build_dashboard_tab()
        self._build_signals_tab()
        self._build_latency_tab()
        self._build_autobet_audit_tab()
        self._build_daily_tab()
        self._build_run_length_tab()
        self._build_ensemble_majority_tab()
        self._build_adaptive_regime_tab()
        self._build_config_tab()
        notebook.bind("<<NotebookTabChanged>>", self._on_main_notebook_changed)

    def _build_live_tab(self) -> None:
        self.live_tab.columnconfigure(0, weight=1)
        self.live_tab.rowconfigure(3, weight=1)

        # 1. Trình duyệt & Tự động Đăng nhập vào sảnh AE Sexy
        browser_card = ttk.LabelFrame(self.live_tab, text="Trình duyệt & Tự động Đăng nhập vào sảnh AE Sexy", padding=10)
        browser_card.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        browser_card.columnconfigure(1, weight=1)

        # Row 0: Link URL web / Preset + Cổng CDP + Mở Chrome CDP
        ttk.Label(browser_card, text="Trang web (Preset):").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        preset_frame = ttk.Frame(browser_card)
        preset_frame.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=4)
        preset_frame.columnconfigure(2, weight=1)

        self.preset_combo = ttk.Combobox(
            preset_frame,
            textvariable=self.preset_var,
            values=("Bong88 (8887799.net)", "SV388 (svft388.com)", "Tùy chỉnh"),
            state="readonly",
            width=20,
        )
        self.preset_combo.grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.preset_combo.bind("<<ComboboxSelected>>", self._on_preset_selected)

        ttk.Label(preset_frame, text="URL:").grid(row=0, column=1, sticky="w", padx=(0, 4))
        ttk.Entry(preset_frame, textvariable=self.target_url_var).grid(row=0, column=2, sticky="ew")

        port_frame = ttk.Frame(browser_card)
        port_frame.grid(row=0, column=2, sticky="e", pady=4)
        ttk.Label(port_frame, text="Cổng:").pack(side="left", padx=(0, 2))
        ttk.Entry(port_frame, textvariable=self.browser_port_var, width=6).pack(side="left", padx=(0, 6))
        self.launch_chrome_btn = ttk.Button(port_frame, text="Mở Chrome CDP", command=self._launch_chrome)
        self.launch_chrome_btn.pack(side="left")

        # Row 1: Tài khoản + Mật khẩu + Hiện pass + Nhớ
        ttk.Label(browser_card, text="Tài khoản (ID):").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(browser_card, textvariable=self.account_id_var).grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=4)

        pass_frame = ttk.Frame(browser_card)
        pass_frame.grid(row=1, column=2, sticky="e", pady=4)
        ttk.Label(pass_frame, text="Pass:").pack(side="left", padx=(0, 2))
        self.password_entry = ttk.Entry(pass_frame, textvariable=self.account_password_var, show="*", width=12)
        self.password_entry.pack(side="left", padx=(0, 4))
        ttk.Checkbutton(pass_frame, text="Hiện", variable=self.show_password_var, command=self._toggle_show_password).pack(side="left", padx=(0, 4))
        ttk.Checkbutton(pass_frame, text="Nhớ", variable=self.remember_credentials_var).pack(side="left", padx=(0, 2))

        # Row 2: Tên sảnh tìm kiếm + Nút Đăng nhập & Vào sảnh
        ttk.Label(browser_card, text="Tên sảnh tìm:").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=4)
        lobby_combo = ttk.Combobox(
            browser_card,
            textvariable=self.ae_lobby_var,
            values=(
                "AE Sexy, Sexy Casino",
                "SEXYBCRT, Sexy Casino, AE Sexy",
                "SEXYBCRT",
                "AE Sexy",
                "Sexy Casino",
                "Sexy Gaming",
                "AE Casino",
                "SEXY",
            ),
        )
        lobby_combo.grid(row=2, column=1, sticky="ew", padx=(0, 8), pady=4)

        btn_frame = ttk.Frame(browser_card)
        btn_frame.grid(row=2, column=2, sticky="e", pady=4)
        self.auto_login_btn = ttk.Button(
            btn_frame,
            text="Đăng nhập & Vào sảnh",
            command=self._start_auto_login_and_navigate,
        )
        self.auto_login_btn.pack(side="left")

        # Row 3: Status
        status_box = ttk.Frame(browser_card)
        status_box.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(4, 0))
        ttk.Label(status_box, text="Tiến trình:", font=("Segoe UI", 9, "bold")).pack(side="left", padx=(0, 4))
        self.automation_status_label = ttk.Label(
            status_box,
            textvariable=self.automation_status_var,
            foreground="#0055aa",
            wraplength=950,
        )
        self.automation_status_label.pack(side="left", fill="x", expand=True)

        # 2. Quét CDP Live Monitor
        monitor_card = ttk.LabelFrame(self.live_tab, text="Quét CDP Live Monitor", padding=8)
        monitor_card.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        monitor_card.columnconfigure(1, weight=1)

        ttk.Label(monitor_card, text="Chrome CDP URL:").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(monitor_card, textvariable=self.cdp_var).grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=4)

        controls = ttk.Frame(monitor_card)
        controls.grid(row=0, column=2, sticky="e", pady=4)
        self.start_button = ttk.Button(controls, text="Bắt đầu CDP", command=self._start_monitor)
        self.start_button.grid(row=0, column=0, padx=4)
        self.stop_button = ttk.Button(controls, text="Dừng", command=self._stop_monitor, state="disabled")
        self.stop_button.grid(row=0, column=1, padx=4)
        ttk.Checkbutton(
            controls,
            text="Watchdog mất live",
            variable=self.auto_refresh_enabled_var,
        ).grid(row=0, column=2, padx=(8, 4))
        ttk.Entry(controls, textvariable=self.auto_refresh_seconds_var, width=5).grid(row=0, column=3, padx=2)
        ttk.Label(controls, text="giây").grid(row=0, column=4, padx=(0, 4))

        # 3. Nhập tay B/P/T (Offline test)
        manual_card = ttk.LabelFrame(self.live_tab, text="Kiểm tra chiến lược bằng chuỗi B/P/T (Offline)", padding=8)
        manual_card.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        manual_card.columnconfigure(1, weight=1)

        ttk.Label(manual_card, text="Bàn nhập tay:").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(manual_card, textvariable=self.manual_table_var).grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=4)
        ttk.Button(manual_card, text="Nạp chuỗi B/P/T", command=self._ingest_manual).grid(
            row=0, column=2, sticky="e", pady=4
        )

        self.manual_text = tk.Text(manual_card, height=3, wrap="word")
        self.manual_text.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=(4, 0))
        self.manual_text.insert("1.0", "B P P P P B P B P B B B B P P B P")

        # 4. Live log
        log_card = ttk.LabelFrame(self.live_tab, text="Nhật ký hoạt động (Live Log)", padding=8)
        log_card.grid(row=3, column=0, sticky="nsew")
        log_card.columnconfigure(0, weight=1)
        log_card.rowconfigure(0, weight=1)

        self.live_log = tk.Text(log_card, height=6, wrap="word", state="disabled")
        self.live_log.grid(row=0, column=0, sticky="nsew")

    def _build_dashboard_tab(self) -> None:
        self.dashboard_tab.columnconfigure(0, weight=1)
        self.dashboard_tab.rowconfigure(1, weight=1)

        header = ttk.Frame(self.dashboard_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(header, text="Theo dõi tín hiệu, kết quả và W/L của nhiều bàn cùng lúc").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(header, text="Làm mới", command=self._refresh_views).grid(row=0, column=1, sticky="e", padx=(8, 0))
        header.columnconfigure(0, weight=1)

        columns = (
            "rank",
            "table",
            "updated",
            "rounds",
            "road",
            "signal",
            "confidence",
            "strategy",
            "pending",
            "ml_wl_history",
            "ml_wl_streak",
            "pnl",
        )
        self.dashboard_tree = ttk.Treeview(self.dashboard_tab, columns=columns, show="headings", height=22)
        headings = {
            "rank": "#",
            "table": "Bàn",
            "updated": "Cập nhật",
            "rounds": "Ván/Lưu",
            "road": "KQ bàn",
            "signal": "Tín hiệu tới",
            "confidence": "Tin cậy",
            "strategy": "Chiến lược",
            "pending": "Đang chờ",
            "ml_wl_history": "Dự đoán W/L ML",
            "ml_wl_streak": "Chuỗi W/L ML",
            "pnl": "Paper P&L",
        }
        widths = {
            "rank": 42,
            "table": 120,
            "updated": 160,
            "rounds": 70,
            "road": 360,
            "signal": 95,
            "confidence": 80,
            "strategy": 140,
            "pending": 150,
            "ml_wl_history": 170,
            "ml_wl_streak": 95,
            "pnl": 90,
        }
        for col in columns:
            self.dashboard_tree.heading(col, text=headings[col])
            self.dashboard_tree.column(
                col, width=widths[col], anchor="w", stretch=col in {"road", "pending", "ml_wl_history"}
            )
        self.dashboard_tree.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(self.dashboard_tab, orient="vertical", command=self.dashboard_tree.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.dashboard_tree.configure(yscrollcommand=scrollbar.set)
        self.dashboard_tree.tag_configure("bet", foreground="#006b2e")
        self.dashboard_tree.tag_configure("watch", foreground="#1f2937")
        self.dashboard_tree.tag_configure("skip", foreground="#1f2937")
        self.dashboard_tree.tag_configure("total", foreground="#111827", background="#eef2f7")

    def _build_x3_sim_tab(self) -> None:
        self.x3_sim_tab.columnconfigure(0, weight=1)
        self.x3_sim_tab.rowconfigure(4, weight=1)
        self.x3_sim_tab.rowconfigure(6, weight=1)

        header = ttk.Frame(self.x3_sim_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(header, text="Gia lap x3: W8+ dao ML, L6+ thuan ML, stake 10 x3").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Button(header, text="Lam moi", command=self._refresh_x3_sim_tree).grid(
            row=0, column=1, sticky="e", padx=(8, 0)
        )
        header.columnconfigure(0, weight=1)

        self.x3_summary_var = tk.StringVar(value="Chua co du lieu gia lap x3.")
        ttk.Label(self.x3_sim_tab, textvariable=self.x3_summary_var, anchor="w", wraplength=1100).grid(
            row=1, column=0, sticky="ew", pady=(0, 8)
        )

        summary_columns = (
            "scope",
            "cycles",
            "closed",
            "open",
            "pnl",
            "avg",
            "bets",
            "push",
            "miss",
            "max_stake",
            "capital",
            "drawdown",
        )
        self.x3_summary_tree = ttk.Treeview(
            self.x3_sim_tab, columns=summary_columns, show="headings", height=4
        )
        summary_headings = {
            "scope": "Nhom",
            "cycles": "Chu ky",
            "closed": "Da dong",
            "open": "Dang mo",
            "pnl": "P&L",
            "avg": "TB/chu ky",
            "bets": "Luot cuoc",
            "push": "Push",
            "miss": "Miss max",
            "max_stake": "Stake max",
            "capital": "Von chiu max",
            "drawdown": "DD chu ky",
        }
        summary_widths = {
            "scope": 160,
            "cycles": 70,
            "closed": 70,
            "open": 70,
            "pnl": 85,
            "avg": 85,
            "bets": 80,
            "push": 70,
            "miss": 80,
            "max_stake": 90,
            "capital": 105,
            "drawdown": 90,
        }
        for col in summary_columns:
            self.x3_summary_tree.heading(col, text=summary_headings[col])
            self.x3_summary_tree.column(col, width=summary_widths[col], anchor="w", stretch=col == "scope")
        self.x3_summary_tree.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        self.x3_summary_tree.tag_configure("total", foreground="#111827", background="#eef2f7")

        ttk.Label(self.x3_sim_tab, text="Chu ky rui ro cao nhat").grid(row=3, column=0, sticky="w")
        cycle_columns = (
            "table",
            "mode",
            "trigger",
            "start",
            "end",
            "bets",
            "push",
            "miss",
            "pnl",
            "max_stake",
            "capital",
            "drawdown",
        )
        self.x3_cycle_tree = ttk.Treeview(self.x3_sim_tab, columns=cycle_columns, show="headings", height=10)
        cycle_headings = {
            "table": "Ban",
            "mode": "Logic",
            "trigger": "Trigger",
            "start": "Bat dau",
            "end": "Ket thuc",
            "bets": "Cuoc",
            "push": "Push",
            "miss": "Miss",
            "pnl": "P&L",
            "max_stake": "Stake max",
            "capital": "Von chiu",
            "drawdown": "DD",
        }
        cycle_widths = {
            "table": 130,
            "mode": 110,
            "trigger": 70,
            "start": 160,
            "end": 160,
            "bets": 60,
            "push": 60,
            "miss": 60,
            "pnl": 80,
            "max_stake": 85,
            "capital": 85,
            "drawdown": 80,
        }
        for col in cycle_columns:
            self.x3_cycle_tree.heading(col, text=cycle_headings[col])
            self.x3_cycle_tree.column(col, width=cycle_widths[col], anchor="w", stretch=col in {"table", "mode"})
        self.x3_cycle_tree.grid(row=4, column=0, sticky="nsew", pady=(4, 10))
        cycle_scroll = ttk.Scrollbar(self.x3_sim_tab, orient="vertical", command=self.x3_cycle_tree.yview)
        cycle_scroll.grid(row=4, column=1, sticky="ns", pady=(4, 10))
        self.x3_cycle_tree.configure(yscrollcommand=cycle_scroll.set)

        ttk.Label(self.x3_sim_tab, text="Chu ky dang mo").grid(row=5, column=0, sticky="w")
        open_columns = (
            "table",
            "mode",
            "trigger",
            "start",
            "bets",
            "push",
            "miss",
            "pnl",
            "next_stake",
            "risk",
        )
        self.x3_open_tree = ttk.Treeview(self.x3_sim_tab, columns=open_columns, show="headings", height=5)
        open_headings = {
            "table": "Ban",
            "mode": "Logic",
            "trigger": "Trigger",
            "start": "Bat dau",
            "bets": "Cuoc",
            "push": "Push",
            "miss": "Miss",
            "pnl": "P&L tam",
            "next_stake": "Stake tiep",
            "risk": "Rui ro + stake",
        }
        open_widths = {
            "table": 130,
            "mode": 110,
            "trigger": 70,
            "start": 160,
            "bets": 60,
            "push": 60,
            "miss": 60,
            "pnl": 80,
            "next_stake": 90,
            "risk": 110,
        }
        for col in open_columns:
            self.x3_open_tree.heading(col, text=open_headings[col])
            self.x3_open_tree.column(col, width=open_widths[col], anchor="w", stretch=col in {"table", "mode"})
        self.x3_open_tree.grid(row=6, column=0, sticky="nsew", pady=(4, 0))
        open_scroll = ttk.Scrollbar(self.x3_sim_tab, orient="vertical", command=self.x3_open_tree.yview)
        open_scroll.grid(row=6, column=1, sticky="ns", pady=(4, 0))
        self.x3_open_tree.configure(yscrollcommand=open_scroll.set)

    def _build_signals_tab(self) -> None:
        self.signals_tab.columnconfigure(0, weight=1)
        self.signals_tab.rowconfigure(0, weight=1)

        paned = ttk.PanedWindow(self.signals_tab, orient="vertical")
        paned.grid(row=0, column=0, sticky="nsew")

        signal_frame = ttk.Frame(paned, padding=(0, 0, 0, 8))
        paper_frame = ttk.Frame(paned)
        paned.add(signal_frame, weight=2)
        paned.add(paper_frame, weight=1)

        signal_frame.columnconfigure(0, weight=1)
        signal_frame.rowconfigure(1, weight=1)
        ttk.Label(signal_frame, text="Tín hiệu hiện tại theo từng bàn/chiến lược").grid(row=0, column=0, sticky="w")
        signal_columns = ("time", "table", "strategy", "action", "side", "confidence", "reason")
        self.signal_tree = ttk.Treeview(signal_frame, columns=signal_columns, show="headings", height=12)
        for col, label, width in (
            ("time", "Thời gian", 150),
            ("table", "Bàn", 130),
            ("strategy", "Chiến lược", 140),
            ("action", "Lệnh", 70),
            ("side", "Cửa", 70),
            ("confidence", "Tin cậy", 80),
            ("reason", "Lý do", 500),
        ):
            self.signal_tree.heading(col, text=label)
            self.signal_tree.column(col, width=width, anchor="w")
        self.signal_tree.grid(row=1, column=0, sticky="nsew", pady=(6, 0))

        paper_frame.columnconfigure(0, weight=1)
        paper_frame.rowconfigure(1, weight=1)
        ttk.Label(paper_frame, text="Paper trading đã settle").grid(row=0, column=0, sticky="w")
        paper_columns = ("settled", "table", "strategy", "side", "stake", "outcome", "delta", "pnl")
        self.paper_tree = ttk.Treeview(paper_frame, columns=paper_columns, show="headings", height=8)
        for col, label, width in (
            ("settled", "Settle", 150),
            ("table", "Bàn", 130),
            ("strategy", "Chiến lược", 140),
            ("side", "Cửa", 70),
            ("stake", "Stake", 80),
            ("outcome", "KQ", 70),
            ("delta", "P&L ván", 90),
            ("pnl", "P&L lũy kế", 100),
        ):
            self.paper_tree.heading(col, text=label)
            self.paper_tree.column(col, width=width, anchor="w")
        self.paper_tree.grid(row=1, column=0, sticky="nsew", pady=(6, 0))

    def _build_latency_tab(self) -> None:
        self.latency_tab.columnconfigure(0, weight=1)
        self.latency_tab.rowconfigure(2, weight=1)

        header = ttk.Frame(self.latency_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(header, text="Passive CDP latency monitor").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="Lam moi", command=self._refresh_latency_tree).grid(row=0, column=1, sticky="e")
        header.columnconfigure(0, weight=1)

        self.latency_summary_var = tk.StringVar(
            value="Chua co mau latency. Bat CDP monitor de bat dau do queue / engine / UI refresh."
        )
        ttk.Label(
            self.latency_tab,
            textvariable=self.latency_summary_var,
            anchor="w",
            wraplength=1000,
        ).grid(row=1, column=0, sticky="ew", pady=(0, 8))

        columns = (
            "created",
            "table",
            "source",
            "round",
            "queue",
            "engine",
            "ui",
            "total",
            "signals",
            "actionable",
            "pending",
        )
        self.latency_tree = ttk.Treeview(self.latency_tab, columns=columns, show="headings", height=22)
        headings = {
            "created": "UI refresh",
            "table": "Ban",
            "source": "Nguon",
            "round": "Van/Luu",
            "queue": "Queue ms",
            "engine": "Engine ms",
            "ui": "UI ms",
            "total": "Total ms",
            "signals": "Signals",
            "actionable": "Actionable",
            "pending": "Pending",
        }
        widths = {
            "created": 170,
            "table": 140,
            "source": 90,
            "round": 75,
            "queue": 90,
            "engine": 90,
            "ui": 90,
            "total": 90,
            "signals": 70,
            "actionable": 80,
            "pending": 80,
        }
        for col in columns:
            self.latency_tree.heading(col, text=headings[col])
            self.latency_tree.column(col, width=widths[col], anchor="w", stretch=col == "table")
        self.latency_tree.grid(row=2, column=0, sticky="nsew")

    def _build_autobet_audit_tab(self) -> None:
        self.autobet_audit_tab.columnconfigure(0, weight=1)
        self.autobet_audit_tab.rowconfigure(2, weight=3)
        self.autobet_audit_tab.rowconfigure(4, weight=2)

        header = ttk.Frame(self.autobet_audit_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Đối soát từng giai đoạn từ tín hiệu đến click Xác nhận và phản hồi nhà cung cấp",
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            text="Làm mới",
            command=lambda: self._refresh_autobet_audit_tree(force=True),
        ).grid(row=0, column=1, sticky="e")
        header.columnconfigure(0, weight=1)

        self.autobet_audit_summary_var = tk.StringVar(value="Chưa có lần thực thi Auto-Bet được audit.")
        ttk.Label(
            self.autobet_audit_tab,
            textvariable=self.autobet_audit_summary_var,
            anchor="w",
            wraplength=1100,
        ).grid(row=1, column=0, sticky="ew", pady=(0, 8))

        attempt_columns = (
            "created",
            "source",
            "table",
            "target",
            "side",
            "stake",
            "countdown",
            "status",
            "reason",
            "provider_id",
            "provider_error",
            "detail",
        )
        self.autobet_attempt_tree = ttk.Treeview(
            self.autobet_audit_tab,
            columns=attempt_columns,
            show="headings",
            height=11,
        )
        attempt_headings = {
            "created": "Bắt đầu",
            "source": "Nguồn",
            "table": "Bàn",
            "target": "Shoe/Ván",
            "side": "Cửa",
            "stake": "Stake",
            "countdown": "Giây",
            "status": "Trạng thái",
            "reason": "Mã nguyên nhân",
            "provider_id": "Bet/Txn ID",
            "provider_error": "Lỗi NCC",
            "detail": "Chi tiết cuối",
        }
        attempt_widths = {
            "created": 170,
            "source": 90,
            "table": 120,
            "target": 95,
            "side": 70,
            "stake": 70,
            "countdown": 55,
            "status": 90,
            "reason": 190,
            "provider_id": 120,
            "provider_error": 120,
            "detail": 360,
        }
        for column in attempt_columns:
            self.autobet_attempt_tree.heading(column, text=attempt_headings[column])
            self.autobet_attempt_tree.column(
                column,
                width=attempt_widths[column],
                anchor="w",
                stretch=column == "detail",
            )
        self.autobet_attempt_tree.grid(row=2, column=0, sticky="nsew")
        self.autobet_attempt_tree.tag_configure("accepted", foreground="#006b2e")
        self.autobet_attempt_tree.tag_configure("unknown", foreground="#9a6700")
        self.autobet_attempt_tree.tag_configure("failed", foreground="#b42318")
        self.autobet_attempt_tree.bind("<<TreeviewSelect>>", self._on_autobet_attempt_selected)

        ttk.Label(self.autobet_audit_tab, text="Timeline của lần thực thi đang chọn").grid(
            row=3,
            column=0,
            sticky="w",
            pady=(10, 4),
        )
        event_columns = ("time", "stage", "status", "reason", "countdown", "table_state", "message")
        self.autobet_event_tree = ttk.Treeview(
            self.autobet_audit_tab,
            columns=event_columns,
            show="headings",
            height=8,
        )
        event_headings = {
            "time": "Thời điểm",
            "stage": "Giai đoạn",
            "status": "Trạng thái",
            "reason": "Mã nguyên nhân",
            "countdown": "Giây",
            "table_state": "Shoe/Ván thực tế",
            "message": "Chi tiết",
        }
        event_widths = {
            "time": 170,
            "stage": 160,
            "status": 90,
            "reason": 190,
            "countdown": 55,
            "table_state": 120,
            "message": 430,
        }
        for column in event_columns:
            self.autobet_event_tree.heading(column, text=event_headings[column])
            self.autobet_event_tree.column(
                column,
                width=event_widths[column],
                anchor="w",
                stretch=column == "message",
            )
        self.autobet_event_tree.grid(row=4, column=0, sticky="nsew")

    def _build_daily_tab(self) -> None:
        self.daily_tab.columnconfigure(0, weight=1)
        self.daily_tab.rowconfigure(3, weight=1)
        header = ttk.Frame(self.daily_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Paper theo khung giờ đã tick (tối đa 2 lệnh/khung, xử lý lần lượt)",
        ).pack(side="left")
        ttk.Label(header, text="Stake (điểm):").pack(side="left", padx=(16, 4))
        current_stake = getattr(self.config, "daily_stake", 10.0)
        stake_val = str(int(current_stake) if float(current_stake).is_integer() else current_stake)
        self.daily_stake_var = tk.StringVar(value=stake_val)
        stake_entry = ttk.Entry(header, textvariable=self.daily_stake_var, width=8)
        stake_entry.pack(side="left")

        ttk.Label(header, text="Chiều đánh:").pack(side="left", padx=(10, 4))
        daily_mode_combo = ttk.Combobox(
            header,
            textvariable=self.daily_bet_mode_var,
            values=BET_MODE_OPTIONS,
            state="readonly",
            width=11,
        )
        daily_mode_combo.pack(side="left", padx=(0, 6))
        daily_mode_combo.bind("<<ComboboxSelected>>", self._on_daily_bet_mode_changed)

        def _on_daily_stake_changed(*_args: Any) -> None:
            raw = self.daily_stake_var.get().strip()
            with contextlib.suppress(ValueError):
                val = float(raw)
                if val > 0 and val != self.config.daily_stake:
                    self.config = replace(self.config, daily_stake=val)
                    save_config(self.config)
        self.daily_stake_var.trace_add("write", _on_daily_stake_changed)
        ttk.Checkbutton(
            header,
            text="Bật tự động đánh theo khung giờ (Live Auto-Bet)",
            variable=self.daily_autobet_var,
            command=self._on_daily_autobet_changed,
        ).pack(side="left", padx=(14, 6))
        ttk.Checkbutton(
            header,
            text="Stop Win (Thắng dừng khung)",
            variable=self.daily_stop_win_var,
            command=self._on_daily_stop_win_changed,
        ).pack(side="left", padx=(6, 6))
        ttk.Button(header, text="Làm mới ứng viên", command=self._refresh_daily_tab).pack(side="left", padx=4)
        ttk.Button(
            header,
            text="▶ Đánh ngay ứng viên",
            command=self._manual_trigger_autobet,
        ).pack(side="left", padx=4)

        window_selector = ttk.LabelFrame(self.daily_tab, text="Chọn khung giờ tự ghi Paper", padding=8)
        window_selector.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for column in range(6):
            window_selector.columnconfigure(column, weight=1)
        for index, label in enumerate(DAILY_EXPERIMENT_WINDOW_LABELS):
            ttk.Checkbutton(
                window_selector,
                text=label,
                variable=self.daily_window_vars[label],
                command=self._on_daily_window_selection_changed,
            ).grid(
                row=index // 6,
                column=index % 6,
                sticky="w",
                padx=(0, 12),
                pady=2,
            )

        self.daily_status_var = tk.StringVar(value="Chỉ paper, không đặt cược thật.")
        ttk.Label(
            self.daily_tab,
            textvariable=self.daily_status_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=2, column=0, sticky="ew")

        self.daily_notebook = ttk.Notebook(self.daily_tab)
        self.daily_notebook.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        self.daily_today_frame = ttk.Frame(self.daily_notebook)
        self.daily_history_frame = ttk.Frame(self.daily_notebook)
        self.daily_notebook.add(self.daily_today_frame, text="Hôm nay & ứng viên")
        self.daily_notebook.add(self.daily_history_frame, text="Lịch sử")

        self.daily_today_frame.columnconfigure(0, weight=1)
        self.daily_today_frame.rowconfigure(0, weight=1)
        columns = (
            "rank",
            "window",
            "table",
            "strategy",
            "side",
            "confidence",
            "round",
            "stake",
            "result",
            "points",
            "status",
        )
        self.daily_tree = ttk.Treeview(self.daily_today_frame, columns=columns, show="headings")
        headings = {
            "rank": "#",
            "window": "Khung giờ",
            "table": "Bàn",
            "strategy": "Cầu",
            "side": "ML Pass",
            "confidence": "ML %",
            "round": "Round",
            "stake": "Stake",
            "result": "W/L/T",
            "points": "Điểm +/-",
            "status": "Trạng thái",
        }
        widths = {
            "rank": 40,
            "window": 105,
            "table": 130,
            "strategy": 125,
            "side": 70,
            "confidence": 70,
            "round": 60,
            "stake": 65,
            "result": 60,
            "points": 80,
            "status": 130,
        }
        for col in columns:
            self.daily_tree.heading(col, text=headings[col])
            self.daily_tree.column(col, width=widths[col], anchor="w")
        self.daily_tree.grid(row=0, column=0, sticky="nsew")
        self._daily_candidates: list[Any] = []
        scrollbar = ttk.Scrollbar(self.daily_today_frame, orient="vertical", command=self.daily_tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.daily_tree.configure(yscrollcommand=scrollbar.set)

        self.daily_history_frame.columnconfigure(0, weight=1)
        self.daily_history_frame.rowconfigure(2, weight=1)
        history_filters = ttk.Frame(self.daily_history_frame)
        history_filters.grid(row=0, column=0, sticky="ew", pady=(8, 6))
        ttk.Label(history_filters, text="Ngày").pack(side="left")
        self.daily_history_date_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.daily_history_date_combo = ttk.Combobox(
            history_filters,
            textvariable=self.daily_history_date_var,
            values=(DAILY_HISTORY_ALL,),
            state="readonly",
            width=14,
        )
        self.daily_history_date_combo.pack(side="left", padx=(4, 14))
        ttk.Label(history_filters, text="Khung giờ").pack(side="left")
        self.daily_history_window_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.daily_history_window_combo = ttk.Combobox(
            history_filters,
            textvariable=self.daily_history_window_var,
            values=(DAILY_HISTORY_ALL, *DAILY_HISTORY_WINDOW_LABELS),
            state="readonly",
            width=14,
        )
        self.daily_history_window_combo.pack(side="left", padx=(4, 14))
        ttk.Button(
            history_filters,
            text="Làm mới lịch sử",
            command=lambda: self._refresh_daily_history(force=True),
        ).pack(side="left")

        self.daily_history_summary_var = tk.StringVar(value="Mở trang Lịch sử để tải dữ liệu.")
        ttk.Label(
            self.daily_history_frame,
            textvariable=self.daily_history_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))

        history_columns = (
            "date",
            "window",
            "table",
            "strategy",
            "side",
            "confidence",
            "round",
            "stake",
            "result",
            "points",
            "status",
        )
        history_headings = {
            "date": "Ngày",
            "window": "Khung giờ",
            "table": "Bàn",
            "strategy": "Cầu",
            "side": "ML Pass",
            "confidence": "ML %",
            "round": "Round",
            "stake": "Stake",
            "result": "W/L/T",
            "points": "P&L",
            "status": "Trạng thái",
        }
        history_widths = {
            "date": 95,
            "window": 100,
            "table": 120,
            "strategy": 120,
            "side": 70,
            "confidence": 65,
            "round": 55,
            "stake": 65,
            "result": 55,
            "points": 75,
            "status": 100,
        }
        self.daily_history_tree = ttk.Treeview(
            self.daily_history_frame,
            columns=history_columns,
            show="headings",
        )
        for col in history_columns:
            self.daily_history_tree.heading(col, text=history_headings[col])
            self.daily_history_tree.column(col, width=history_widths[col], anchor="w")
        self.daily_history_tree.grid(row=2, column=0, sticky="nsew")
        history_scrollbar = ttk.Scrollbar(
            self.daily_history_frame,
            orient="vertical",
            command=self.daily_history_tree.yview,
        )
        history_scrollbar.grid(row=2, column=1, sticky="ns")
        self.daily_history_tree.configure(yscrollcommand=history_scrollbar.set)

        self._daily_history_dirty = True
        self._daily_history_loaded_key: tuple[str | None, str | None] | None = None
        self.daily_notebook.bind("<<NotebookTabChanged>>", self._on_daily_notebook_changed)
        self.daily_history_date_combo.bind(
            "<<ComboboxSelected>>", lambda _event: self._refresh_daily_history(force=True)
        )
        self.daily_history_window_combo.bind(
            "<<ComboboxSelected>>", lambda _event: self._refresh_daily_history(force=True)
        )

    def _build_run_length_tab(self) -> None:
        self.run_length_tab.columnconfigure(0, weight=1)
        self.run_length_tab.rowconfigure(3, weight=1)

        header = ttk.Frame(self.run_length_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Chiến thuật Run Length (ML >=58%, tối đa 1 lệnh/khung):",
        ).pack(side="left")
        ttk.Label(header, text="Stake (điểm):").pack(side="left", padx=(12, 4))
        current_stake = float(self.config.run_length_stake)
        stake_text = str(int(current_stake) if current_stake.is_integer() else current_stake)
        self.run_length_stake_var = tk.StringVar(value=stake_text)
        ttk.Entry(header, textvariable=self.run_length_stake_var, width=8).pack(side="left")
        ttk.Button(
            header,
            text="Lưu stake",
            command=self._save_run_length_stake,
        ).pack(side="left", padx=(6, 0))
        ttk.Label(header, text="Chiều đánh:").pack(side="left", padx=(10, 4))
        run_length_mode_combo = ttk.Combobox(
            header,
            textvariable=self.run_length_bet_mode_var,
            values=BET_MODE_OPTIONS,
            state="readonly",
            width=11,
        )
        run_length_mode_combo.pack(side="left", padx=(0, 6))
        run_length_mode_combo.bind("<<ComboboxSelected>>", self._on_run_length_bet_mode_changed)
        ttk.Checkbutton(
            header,
            text="Bật tự động đánh theo khung giờ (Live Auto-Bet)",
            variable=self.run_length_autobet_var,
            command=self._on_run_length_autobet_changed,
        ).pack(side="left", padx=(14, 6))
        ttk.Button(
            header,
            text="▶ Đánh ngay ứng viên",
            command=self._manual_trigger_run_length_autobet,
        ).pack(side="left", padx=4)
        ttk.Button(
            header,
            text="Làm mới hôm nay",
            command=lambda: self._refresh_run_length_tab(force_today=True),
        ).pack(side="right")


        selector = ttk.LabelFrame(
            self.run_length_tab,
            text="Khung giờ Bangkok được phép ghi dự đoán",
            padding=8,
        )
        selector.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for column in range(6):
            selector.columnconfigure(column, weight=1)
        for index, label in enumerate(DAILY_EXPERIMENT_WINDOW_LABELS):
            ttk.Checkbutton(
                selector,
                text=label,
                variable=self.run_length_window_vars[label],
                command=self._on_run_length_window_selection_changed,
            ).grid(
                row=index // 6,
                column=index % 6,
                sticky="w",
                padx=(0, 12),
                pady=2,
            )

        self.run_length_status_var = tk.StringVar(
            value="Chưa chọn khung giờ; tab này không đặt cược thật."
        )
        ttk.Label(
            self.run_length_tab,
            textvariable=self.run_length_status_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=2, column=0, sticky="ew")

        self.run_length_candidate_var = tk.StringVar(value="Ứng viên hiện tại: chưa có.")
        self.run_length_notebook = ttk.Notebook(self.run_length_tab)
        self.run_length_notebook.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        self.run_length_today_frame = ttk.Frame(self.run_length_notebook)
        self.run_length_history_frame = ttk.Frame(self.run_length_notebook)
        self.run_length_notebook.add(self.run_length_today_frame, text="Hôm nay & ứng viên")
        self.run_length_notebook.add(self.run_length_history_frame, text="Lịch sử")

        columns = (
            "date",
            "window",
            "table",
            "shoe",
            "round",
            "side",
            "confidence",
            "stake",
            "result",
            "pnl",
            "status",
        )
        headings = {
            "date": "Ngày",
            "window": "Khung giờ",
            "table": "Bàn",
            "shoe": "Shoe",
            "round": "Round",
            "side": "Dự đoán",
            "confidence": "ML %",
            "stake": "Stake",
            "result": "W/L/T",
            "pnl": "P&L",
            "status": "Trạng thái",
        }
        widths = {
            "date": 92,
            "window": 100,
            "table": 125,
            "shoe": 75,
            "round": 55,
            "side": 75,
            "confidence": 65,
            "stake": 65,
            "result": 55,
            "pnl": 70,
            "status": 110,
        }

        self.run_length_today_frame.columnconfigure(0, weight=1)
        self.run_length_today_frame.rowconfigure(2, weight=1)
        ttk.Label(
            self.run_length_today_frame,
            textvariable=self.run_length_candidate_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=0, column=0, sticky="ew", pady=(8, 4))
        self.run_length_summary_var = tk.StringVar(value="Mở tab để tải dữ liệu hôm nay.")
        ttk.Label(
            self.run_length_today_frame,
            textvariable=self.run_length_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.run_length_tree = ttk.Treeview(
            self.run_length_today_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.run_length_tree.heading(column, text=headings[column])
            self.run_length_tree.column(column, width=widths[column], anchor="w")
        self.run_length_tree.grid(row=2, column=0, sticky="nsew")
        today_scrollbar = ttk.Scrollbar(
            self.run_length_today_frame,
            orient="vertical",
            command=self.run_length_tree.yview,
        )
        today_scrollbar.grid(row=2, column=1, sticky="ns")
        self.run_length_tree.configure(yscrollcommand=today_scrollbar.set)

        self.run_length_history_frame.columnconfigure(0, weight=1)
        self.run_length_history_frame.rowconfigure(2, weight=1)
        history_filters = ttk.Frame(self.run_length_history_frame)
        history_filters.grid(row=0, column=0, sticky="ew", pady=(8, 6))
        ttk.Label(history_filters, text="Ngày").pack(side="left")
        self.run_length_history_date_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.run_length_history_date_combo = ttk.Combobox(
            history_filters,
            textvariable=self.run_length_history_date_var,
            values=(DAILY_HISTORY_ALL,),
            state="readonly",
            width=14,
        )
        self.run_length_history_date_combo.pack(side="left", padx=(4, 14))
        ttk.Label(history_filters, text="Khung giờ").pack(side="left")
        self.run_length_history_window_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.run_length_history_window_combo = ttk.Combobox(
            history_filters,
            textvariable=self.run_length_history_window_var,
            values=(DAILY_HISTORY_ALL, *DAILY_EXPERIMENT_WINDOW_LABELS),
            state="readonly",
            width=14,
        )
        self.run_length_history_window_combo.pack(side="left", padx=(4, 14))
        ttk.Button(
            history_filters,
            text="Làm mới lịch sử",
            command=lambda: self._refresh_run_length_history(force=True),
        ).pack(side="left")

        self.run_length_history_summary_var = tk.StringVar(
            value="Mở trang Lịch sử để tải dữ liệu."
        )
        ttk.Label(
            self.run_length_history_frame,
            textvariable=self.run_length_history_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))

        self.run_length_history_tree = ttk.Treeview(
            self.run_length_history_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.run_length_history_tree.heading(column, text=headings[column])
            self.run_length_history_tree.column(column, width=widths[column], anchor="w")
        self.run_length_history_tree.grid(row=2, column=0, sticky="nsew")
        history_scrollbar = ttk.Scrollbar(
            self.run_length_history_frame,
            orient="vertical",
            command=self.run_length_history_tree.yview,
        )
        history_scrollbar.grid(row=2, column=1, sticky="ns")
        self.run_length_history_tree.configure(yscrollcommand=history_scrollbar.set)

        self.run_length_notebook.bind(
            "<<NotebookTabChanged>>",
            self._on_run_length_notebook_changed,
        )
        self.run_length_history_date_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_run_length_history(force=True),
        )
        self.run_length_history_window_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_run_length_history(force=True),
        )

    def _build_ensemble_majority_tab(self) -> None:
        self.ensemble_majority_tab.columnconfigure(0, weight=1)
        self.ensemble_majority_tab.rowconfigure(3, weight=1)

        header = ttk.Frame(self.ensemble_majority_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Chiến thuật Ensemble Majority (ML >=55%, tối đa 1 lệnh/khung):",
        ).pack(side="left")
        ttk.Label(header, text="Stake (điểm):").pack(side="left", padx=(10, 4))
        current_stake = float(getattr(self.config, "ensemble_majority_stake", 10.0))
        stake_text = str(int(current_stake) if current_stake.is_integer() else current_stake)
        self.ensemble_majority_stake_var = tk.StringVar(value=stake_text)
        ttk.Entry(header, textvariable=self.ensemble_majority_stake_var, width=7).pack(side="left")
        ttk.Button(
            header,
            text="Lưu stake",
            command=self._save_ensemble_majority_stake,
        ).pack(side="left", padx=(4, 8))

        ttk.Label(header, text="Min ML %:").pack(side="left", padx=(4, 4))
        current_min_prob = float(getattr(self.config, "ensemble_majority_ml_min_probability", 0.55)) * 100
        min_prob_text = f"{current_min_prob:.1f}" if not current_min_prob.is_integer() else str(int(current_min_prob))
        self.ensemble_majority_min_prob_var = tk.StringVar(value=min_prob_text)
        ttk.Entry(header, textvariable=self.ensemble_majority_min_prob_var, width=5).pack(side="left")
        ttk.Button(
            header,
            text="Lưu min ML",
            command=self._save_ensemble_majority_min_prob,
        ).pack(side="left", padx=(4, 8))
        ttk.Label(header, text="Chiều đánh:").pack(side="left", padx=(6, 4))
        ensemble_mode_combo = ttk.Combobox(
            header,
            textvariable=self.ensemble_majority_bet_mode_var,
            values=BET_MODE_OPTIONS,
            state="readonly",
            width=11,
        )
        ensemble_mode_combo.pack(side="left", padx=(0, 6))
        ensemble_mode_combo.bind("<<ComboboxSelected>>", self._on_ensemble_majority_bet_mode_changed)

        ttk.Checkbutton(
            header,
            text="Bật tự động đánh (Live Auto-Bet)",
            variable=self.ensemble_majority_autobet_var,
            command=self._on_ensemble_majority_autobet_changed,
        ).pack(side="left", padx=(8, 6))
        ttk.Button(
            header,
            text="▶ Đánh ngay ứng viên",
            command=self._manual_trigger_ensemble_majority_autobet,
        ).pack(side="left", padx=4)
        ttk.Button(
            header,
            text="Làm mới hôm nay",
            command=lambda: self._refresh_ensemble_majority_tab(force_today=True),
        ).pack(side="right")

        selector = ttk.LabelFrame(
            self.ensemble_majority_tab,
            text="Khung giờ Bangkok được phép ghi dự đoán",
            padding=8,
        )
        selector.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for column in range(6):
            selector.columnconfigure(column, weight=1)
        for index, label in enumerate(DAILY_EXPERIMENT_WINDOW_LABELS):
            ttk.Checkbutton(
                selector,
                text=label,
                variable=self.ensemble_majority_window_vars[label],
                command=self._on_ensemble_majority_window_selection_changed,
            ).grid(
                row=index // 6,
                column=index % 6,
                sticky="w",
                padx=(0, 12),
                pady=2,
            )

        self.ensemble_majority_status_var = tk.StringVar(
            value="Chưa chọn khung giờ; tab này không đặt cược thật."
        )
        ttk.Label(
            self.ensemble_majority_tab,
            textvariable=self.ensemble_majority_status_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=2, column=0, sticky="ew")

        self.ensemble_majority_candidate_var = tk.StringVar(value="Ứng viên hiện tại: chưa có.")
        self.ensemble_majority_notebook = ttk.Notebook(self.ensemble_majority_tab)
        self.ensemble_majority_notebook.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        self.ensemble_majority_today_frame = ttk.Frame(self.ensemble_majority_notebook)
        self.ensemble_majority_history_frame = ttk.Frame(self.ensemble_majority_notebook)
        self.ensemble_majority_notebook.add(self.ensemble_majority_today_frame, text="Hôm nay & ứng viên")
        self.ensemble_majority_notebook.add(self.ensemble_majority_history_frame, text="Lịch sử")

        columns = (
            "date",
            "window",
            "table",
            "shoe",
            "round",
            "side",
            "confidence",
            "stake",
            "result",
            "pnl",
            "status",
        )
        headings = {
            "date": "Ngày",
            "window": "Khung giờ",
            "table": "Bàn",
            "shoe": "Shoe",
            "round": "Round",
            "side": "Dự đoán",
            "confidence": "ML %",
            "stake": "Stake",
            "result": "W/L/T",
            "pnl": "P&L",
            "status": "Trạng thái",
        }
        widths = {
            "date": 92,
            "window": 100,
            "table": 125,
            "shoe": 75,
            "round": 55,
            "side": 75,
            "confidence": 65,
            "stake": 65,
            "result": 55,
            "pnl": 70,
            "status": 110,
        }

        self.ensemble_majority_today_frame.columnconfigure(0, weight=1)
        self.ensemble_majority_today_frame.rowconfigure(2, weight=1)
        ttk.Label(
            self.ensemble_majority_today_frame,
            textvariable=self.ensemble_majority_candidate_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=0, column=0, sticky="ew", pady=(8, 4))
        self.ensemble_majority_summary_var = tk.StringVar(value="Mở tab để tải dữ liệu hôm nay.")
        ttk.Label(
            self.ensemble_majority_today_frame,
            textvariable=self.ensemble_majority_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.ensemble_majority_tree = ttk.Treeview(
            self.ensemble_majority_today_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.ensemble_majority_tree.heading(column, text=headings[column])
            self.ensemble_majority_tree.column(column, width=widths[column], anchor="w")
        self.ensemble_majority_tree.grid(row=2, column=0, sticky="nsew")
        today_scrollbar = ttk.Scrollbar(
            self.ensemble_majority_today_frame,
            orient="vertical",
            command=self.ensemble_majority_tree.yview,
        )
        today_scrollbar.grid(row=2, column=1, sticky="ns")
        self.ensemble_majority_tree.configure(yscrollcommand=today_scrollbar.set)

        self.ensemble_majority_history_frame.columnconfigure(0, weight=1)
        self.ensemble_majority_history_frame.rowconfigure(2, weight=1)
        history_filters = ttk.Frame(self.ensemble_majority_history_frame)
        history_filters.grid(row=0, column=0, sticky="ew", pady=(8, 6))
        ttk.Label(history_filters, text="Ngày").pack(side="left")
        self.ensemble_majority_history_date_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.ensemble_majority_history_date_combo = ttk.Combobox(
            history_filters,
            textvariable=self.ensemble_majority_history_date_var,
            values=(DAILY_HISTORY_ALL,),
            state="readonly",
            width=14,
        )
        self.ensemble_majority_history_date_combo.pack(side="left", padx=(4, 14))
        ttk.Label(history_filters, text="Khung giờ").pack(side="left")
        self.ensemble_majority_history_window_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.ensemble_majority_history_window_combo = ttk.Combobox(
            history_filters,
            textvariable=self.ensemble_majority_history_window_var,
            values=(DAILY_HISTORY_ALL, *DAILY_EXPERIMENT_WINDOW_LABELS),
            state="readonly",
            width=14,
        )
        self.ensemble_majority_history_window_combo.pack(side="left", padx=(4, 14))
        ttk.Button(
            history_filters,
            text="Làm mới lịch sử",
            command=lambda: self._refresh_ensemble_majority_history(force=True),
        ).pack(side="left")

        self.ensemble_majority_history_summary_var = tk.StringVar(
            value="Mở trang Lịch sử để tải dữ liệu."
        )
        ttk.Label(
            self.ensemble_majority_history_frame,
            textvariable=self.ensemble_majority_history_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))

        self.ensemble_majority_history_tree = ttk.Treeview(
            self.ensemble_majority_history_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.ensemble_majority_history_tree.heading(column, text=headings[column])
            self.ensemble_majority_history_tree.column(column, width=widths[column], anchor="w")
        self.ensemble_majority_history_tree.grid(row=2, column=0, sticky="nsew")
        history_scrollbar = ttk.Scrollbar(
            self.ensemble_majority_history_frame,
            orient="vertical",
            command=self.ensemble_majority_history_tree.yview,
        )
        history_scrollbar.grid(row=2, column=1, sticky="ns")
        self.ensemble_majority_history_tree.configure(yscrollcommand=history_scrollbar.set)

        self.ensemble_majority_notebook.bind(
            "<<NotebookTabChanged>>",
            self._on_ensemble_majority_notebook_changed,
        )
        self.ensemble_majority_history_date_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_ensemble_majority_history(force=True),
        )
        self.ensemble_majority_history_window_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_ensemble_majority_history(force=True),
        )

    def _build_adaptive_regime_tab(self) -> None:
        self.adaptive_regime_tab.columnconfigure(0, weight=1)
        self.adaptive_regime_tab.rowconfigure(3, weight=1)

        header = ttk.Frame(self.adaptive_regime_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Đa Cầu Thích Ứng (1-1, 2-2, Bẻ bệt C4, Bệt muộn | Banker >=57%, Player >=52.5%):",
        ).pack(side="left")
        ttk.Label(header, text="Stake (điểm):").pack(side="left", padx=(10, 4))
        current_stake = float(getattr(self.config, "adaptive_regime_stake", 10.0))
        stake_text = str(int(current_stake) if current_stake.is_integer() else current_stake)
        self.adaptive_regime_stake_var = tk.StringVar(value=stake_text)
        ttk.Entry(header, textvariable=self.adaptive_regime_stake_var, width=7).pack(side="left")
        ttk.Button(
            header,
            text="Lưu stake",
            command=self._save_adaptive_regime_stake,
        ).pack(side="left", padx=(4, 8))
        ttk.Label(header, text="Chiều đánh:").pack(side="left", padx=(6, 4))
        adaptive_mode_combo = ttk.Combobox(
            header,
            textvariable=self.adaptive_regime_bet_mode_var,
            values=BET_MODE_OPTIONS,
            state="readonly",
            width=11,
        )
        adaptive_mode_combo.pack(side="left", padx=(0, 6))
        adaptive_mode_combo.bind("<<ComboboxSelected>>", self._on_adaptive_regime_bet_mode_changed)

        ttk.Checkbutton(
            header,
            text="Bật tự động đánh theo khung giờ (Live Auto-Bet)",
            variable=self.adaptive_regime_autobet_var,
            command=self._on_adaptive_regime_autobet_changed,
        ).pack(side="left", padx=(8, 6))
        ttk.Button(
            header,
            text="▶ Đánh ngay ứng viên",
            command=self._manual_trigger_adaptive_regime_autobet,
        ).pack(side="left", padx=4)
        ttk.Button(
            header,
            text="Làm mới hôm nay",
            command=lambda: self._refresh_adaptive_regime_tab(force_today=True),
        ).pack(side="right")

        selector = ttk.LabelFrame(
            self.adaptive_regime_tab,
            text="Khung giờ Bangkok được phép ghi dự đoán",
            padding=8,
        )
        selector.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        for column in range(6):
            selector.columnconfigure(column, weight=1)
        for index, label in enumerate(DAILY_EXPERIMENT_WINDOW_LABELS):
            ttk.Checkbutton(
                selector,
                text=label,
                variable=self.adaptive_regime_window_vars[label],
                command=self._on_adaptive_regime_window_selection_changed,
            ).grid(
                row=index // 6,
                column=index % 6,
                sticky="w",
                padx=(0, 12),
                pady=2,
            )

        self.adaptive_regime_status_var = tk.StringVar(
            value="Chưa chọn khung giờ; tab này không đặt cược thật."
        )
        ttk.Label(
            self.adaptive_regime_tab,
            textvariable=self.adaptive_regime_status_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=2, column=0, sticky="ew")

        self.adaptive_regime_candidate_var = tk.StringVar(value="Ứng viên hiện tại: chưa có.")
        self.adaptive_regime_notebook = ttk.Notebook(self.adaptive_regime_tab)
        self.adaptive_regime_notebook.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        self.adaptive_regime_today_frame = ttk.Frame(self.adaptive_regime_notebook)
        self.adaptive_regime_history_frame = ttk.Frame(self.adaptive_regime_notebook)
        self.adaptive_regime_notebook.add(self.adaptive_regime_today_frame, text="Hôm nay & ứng viên")
        self.adaptive_regime_notebook.add(self.adaptive_regime_history_frame, text="Lịch sử")

        columns = (
            "date",
            "window",
            "table",
            "shoe",
            "round",
            "side",
            "confidence",
            "stake",
            "result",
            "pnl",
            "status",
        )
        headings = {
            "date": "Ngày",
            "window": "Khung giờ",
            "table": "Bàn",
            "shoe": "Shoe",
            "round": "Round",
            "side": "Dự đoán",
            "confidence": "ML %",
            "stake": "Stake",
            "result": "W/L/T",
            "pnl": "P&L",
            "status": "Trạng thái",
        }
        widths = {
            "date": 92,
            "window": 100,
            "table": 125,
            "shoe": 75,
            "round": 55,
            "side": 75,
            "confidence": 65,
            "stake": 65,
            "result": 55,
            "pnl": 70,
            "status": 110,
        }

        self.adaptive_regime_today_frame.columnconfigure(0, weight=1)
        self.adaptive_regime_today_frame.rowconfigure(2, weight=1)
        ttk.Label(
            self.adaptive_regime_today_frame,
            textvariable=self.adaptive_regime_candidate_var,
            anchor="w",
            wraplength=1120,
        ).grid(row=0, column=0, sticky="ew", pady=(8, 4))
        self.adaptive_regime_summary_var = tk.StringVar(value="Mở tab để tải dữ liệu hôm nay.")
        ttk.Label(
            self.adaptive_regime_today_frame,
            textvariable=self.adaptive_regime_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.adaptive_regime_tree = ttk.Treeview(
            self.adaptive_regime_today_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.adaptive_regime_tree.heading(column, text=headings[column])
            self.adaptive_regime_tree.column(column, width=widths[column], anchor="w")
        self.adaptive_regime_tree.grid(row=2, column=0, sticky="nsew")
        today_scrollbar = ttk.Scrollbar(
            self.adaptive_regime_today_frame,
            orient="vertical",
            command=self.adaptive_regime_tree.yview,
        )
        today_scrollbar.grid(row=2, column=1, sticky="ns")
        self.adaptive_regime_tree.configure(yscrollcommand=today_scrollbar.set)

        self.adaptive_regime_history_frame.columnconfigure(0, weight=1)
        self.adaptive_regime_history_frame.rowconfigure(2, weight=1)
        history_filters = ttk.Frame(self.adaptive_regime_history_frame)
        history_filters.grid(row=0, column=0, sticky="ew", pady=(8, 6))
        ttk.Label(history_filters, text="Ngày").pack(side="left")
        self.adaptive_regime_history_date_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.adaptive_regime_history_date_combo = ttk.Combobox(
            history_filters,
            textvariable=self.adaptive_regime_history_date_var,
            values=(DAILY_HISTORY_ALL,),
            state="readonly",
            width=14,
        )
        self.adaptive_regime_history_date_combo.pack(side="left", padx=(4, 14))
        ttk.Label(history_filters, text="Khung giờ").pack(side="left")
        self.adaptive_regime_history_window_var = tk.StringVar(value=DAILY_HISTORY_ALL)
        self.adaptive_regime_history_window_combo = ttk.Combobox(
            history_filters,
            textvariable=self.adaptive_regime_history_window_var,
            values=(DAILY_HISTORY_ALL, *DAILY_EXPERIMENT_WINDOW_LABELS),
            state="readonly",
            width=14,
        )
        self.adaptive_regime_history_window_combo.pack(side="left", padx=(4, 14))
        ttk.Button(
            history_filters,
            text="Làm mới lịch sử",
            command=lambda: self._refresh_adaptive_regime_history(force=True),
        ).pack(side="left")

        self.adaptive_regime_history_summary_var = tk.StringVar(
            value="Mở trang Lịch sử để tải dữ liệu."
        )
        ttk.Label(
            self.adaptive_regime_history_frame,
            textvariable=self.adaptive_regime_history_summary_var,
            anchor="w",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 6))

        self.adaptive_regime_history_tree = ttk.Treeview(
            self.adaptive_regime_history_frame,
            columns=columns,
            show="headings",
        )
        for column in columns:
            self.adaptive_regime_history_tree.heading(column, text=headings[column])
            self.adaptive_regime_history_tree.column(column, width=widths[column], anchor="w")
        self.adaptive_regime_history_tree.grid(row=2, column=0, sticky="nsew")
        history_scrollbar = ttk.Scrollbar(
            self.adaptive_regime_history_frame,
            orient="vertical",
            command=self.adaptive_regime_history_tree.yview,
        )
        history_scrollbar.grid(row=2, column=1, sticky="ns")
        self.adaptive_regime_history_tree.configure(yscrollcommand=history_scrollbar.set)

        self.adaptive_regime_notebook.bind(
            "<<NotebookTabChanged>>",
            self._on_adaptive_regime_notebook_changed,
        )
        self.adaptive_regime_history_date_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_adaptive_regime_history(force=True),
        )
        self.adaptive_regime_history_window_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: self._refresh_adaptive_regime_history(force=True),
        )

    def _build_config_tab(self) -> None:
        self.config_tab.columnconfigure(1, weight=1)

        self.stake_chain_var = tk.StringVar(value=", ".join(_format_number(v) for v in self.config.money.stake_chain))
        self.progression_var = tk.StringVar(value=self.config.money.progression_mode)
        self.stop_loss_var = tk.StringVar(value=str(self.config.money.stop_loss))
        self.take_profit_var = tk.StringVar(value=str(self.config.money.take_profit))
        self.group_tp_var = tk.StringVar(value=str(self.config.money.group_take_profit))
        self.group_sl_var = tk.StringVar(value=str(self.config.money.group_stop_loss))
        self.min_conf_var = tk.StringVar(value=str(self.config.min_confidence))
        self.expected_shoe_rounds_var = tk.StringVar(value=str(self.config.expected_shoe_rounds))
        self.stop_after_round_var = tk.StringVar(value=str(self.config.stop_signals_after_round))
        self.live_table_stale_seconds_var = tk.StringVar(value=str(self.config.live_table_stale_seconds))
        self.ml_filter_enabled_var = tk.BooleanVar(value=self.config.ml_filter_enabled)
        self.ml_model_path_var = tk.StringVar(value=self.config.ml_model_path)
        self.ml_threshold_var = tk.StringVar(value=str(self.config.ml_decision_threshold))
        self.duckdb_enabled_var = tk.BooleanVar(value=True)

        rows = [
            ("Chuỗi tiền", self.stake_chain_var),
            ("Progression mode", self.progression_var),
            ("Stop-loss ngày", self.stop_loss_var),
            ("Take-profit ngày", self.take_profit_var),
            ("Group take-profit", self.group_tp_var),
            ("Group stop-loss", self.group_sl_var),
            ("Min confidence", self.min_conf_var),
            ("Expected shoe rounds", self.expected_shoe_rounds_var),
            ("Stop signals after round", self.stop_after_round_var),
            ("Hide stale tables after seconds", self.live_table_stale_seconds_var),
            ("Watchdog: giây không có live", self.auto_refresh_seconds_var),
            ("ML model path", self.ml_model_path_var),
            ("ML threshold", self.ml_threshold_var),
        ]
        for row, (label, var) in enumerate(rows):
            ttk.Label(self.config_tab, text=label).grid(row=row, column=0, sticky="w", padx=(0, 8), pady=5)
            if label == "Progression mode":
                combo = ttk.Combobox(
                    self.config_tab,
                    textvariable=var,
                    values=(
                        "flat",
                        "loss_up_win_reset",
                        "win_up_loss_reset",
                        "both_up",
                        "win_up_loss_hold",
                        "profit_lock_loss_up",
                    ),
                    state="readonly",
                )
                combo.grid(row=row, column=1, sticky="ew", pady=5)
            else:
                ttk.Entry(self.config_tab, textvariable=var).grid(row=row, column=1, sticky="ew", pady=5)

        ttk.Checkbutton(
            self.config_tab,
            text="Bat ML live filter cho tin hieu paper",
            variable=self.ml_filter_enabled_var,
        ).grid(row=len(rows), column=1, sticky="w", pady=8)
        ttk.Checkbutton(
            self.config_tab,
            text="Bật watchdog: chỉ reload khi mất snapshot live",
            variable=self.auto_refresh_enabled_var,
        ).grid(row=len(rows) + 1, column=1, sticky="w", pady=8)
        ttk.Checkbutton(
            self.config_tab,
            text="Mirror DuckDB analytics luon bat neu co package duckdb",
            variable=self.duckdb_enabled_var,
            state="disabled",
        ).grid(row=len(rows) + 2, column=1, sticky="w", pady=8)
        ttk.Button(self.config_tab, text="Lưu cấu hình", command=self._save_config_from_ui).grid(
            row=len(rows) + 3, column=1, sticky="e", pady=12
        )

        note = (
            "MVP hiện chỉ signal + paper trading. Stake 0 vẫn được tính là quan sát ảo, "
            "không có bất kỳ executor đặt chip nào trong project này."
        )
        ttk.Label(self.config_tab, text=note, wraplength=760, foreground="#555").grid(
            row=len(rows) + 4, column=0, columnspan=2, sticky="w", pady=(16, 0)
        )

    def _on_preset_selected(self, _event=None) -> None:
        val = self.preset_var.get()
        if "Bong88" in val or "8887799" in val:
            self.target_url_var.set("https://www.8887799.net")
            self.ae_lobby_var.set("AE Sexy, Sexy Casino")
        elif "SV388" in val or "svft388" in val:
            self.target_url_var.set("https://svft388.com")
            self.ae_lobby_var.set("SEXYBCRT, Sexy Casino, AE Sexy")

    def _toggle_show_password(self) -> None:
        if self.show_password_var.get():
            self.password_entry.configure(show="")
        else:
            self.password_entry.configure(show="*")

    def _launch_chrome(self) -> None:
        try:
            port = int(self.browser_port_var.get().strip() or str(DEFAULT_CDP_PORT))
        except ValueError:
            messagebox.showerror("Cổng chưa hợp lệ", "Vui lòng nhập số cổng hợp lệ (ví dụ: 9222).")
            return

        target_url = self.target_url_var.get().strip()
        self.cdp_var.set(f"http://localhost:{port}")
        self.launch_chrome_btn.configure(state="disabled")
        self.automation_status_var.set(f"Đang khởi chạy Google Chrome trên cổng {port}...")

        def worker() -> None:
            try:
                success, msg = launch_chrome_cdp(
                    port=port,
                    chrome_path=self.config.chrome_path or None,
                    target_url=target_url or None,
                )
                self.queue.put(("automation_status", msg))
                self.queue.put(("status", msg))
            except Exception as exc:
                self.queue.put(("automation_status", f"Lỗi mở Chrome: {exc}"))
            finally:
                self.queue.put(("launch_chrome_finished", None))

        threading.Thread(target=worker, name="launch-chrome-worker", daemon=True).start()

    def _start_auto_login_and_navigate(self) -> None:
        if self.automation_running:
            return

        target_url = self.target_url_var.get().strip()
        account_id = self.account_id_var.get().strip()
        account_password = self.account_password_var.get().strip()
        ae_lobby_name = self.ae_lobby_var.get().strip() or "AE Sexy, Sexy Casino"

        if not target_url:
            messagebox.showwarning("Thiếu URL", "Vui lòng nhập Link URL trang web.")
            return

        try:
            port = int(self.browser_port_var.get().strip() or str(DEFAULT_CDP_PORT))
        except ValueError:
            messagebox.showerror("Cổng chưa hợp lệ", "Vui lòng nhập số cổng hợp lệ (ví dụ: 9222).")
            return

        cdp_url = f"http://localhost:{port}"
        self.cdp_var.set(cdp_url)

        # Update and save config
        self.config = replace(
            self.config,
            target_url=target_url,
            account_id=account_id,
            account_password=account_password if self.remember_credentials_var.get() else "",
            remember_credentials=bool(self.remember_credentials_var.get()),
            ae_lobby_name=ae_lobby_name,
            cdp_url=cdp_url,
        )
        save_config(self.config)

        self.automation_running = True
        self.auto_login_btn.configure(state="disabled")
        self.launch_chrome_btn.configure(state="disabled")
        self.automation_status_var.set("Đang khởi tạo quy trình tự động hóa...")

        def worker() -> None:
            try:
                # If CDP port is not open, launch Chrome first
                if not is_cdp_port_open(port):
                    self.queue.put(("automation_status", f"Cổng {port} chưa mở, đang tự động khởi chạy Chrome..."))
                    ok, launch_msg = launch_chrome_cdp(
                        port=port,
                        chrome_path=self.config.chrome_path or None,
                        target_url=target_url,
                    )
                    if not ok:
                        self.queue.put(("automation_status", launch_msg))
                        return
                    time.sleep(1.0)

                self.queue.put(("automation_status", "Đang kết nối CDP và thực hiện điều hướng..."))
                success, final_msg = asyncio.run(
                    run_browser_automation(
                        cdp_url=cdp_url,
                        target_url=target_url,
                        username=account_id,
                        password=account_password,
                        lobby_name=ae_lobby_name,
                        on_status=lambda m: self.queue.put(("automation_status", m)),
                        custom_casino_selector=self.config.custom_casino_selector or None,
                        custom_ae_selector=self.config.custom_ae_selector or None,
                    )
                )
                self.queue.put(("automation_status", final_msg))
                if success:
                    self.queue.put(("automation_success", None))
            except Exception as exc:
                self.queue.put(("automation_status", f"Lỗi tự động hóa: {exc}"))
            finally:
                self.queue.put(("automation_finished", None))

        threading.Thread(target=worker, name="auto-login-worker", daemon=True).start()

    def _start_monitor(self) -> None:
        if self.monitor_running:
            return
        cdp_url = self.cdp_var.get().strip() or self.config.cdp_url
        auto_refresh_enabled = bool(self.auto_refresh_enabled_var.get())
        try:
            parsed_auto_refresh_seconds = _parse_auto_refresh_seconds(self.auto_refresh_seconds_var.get())
        except ValueError as exc:
            messagebox.showerror("Auto refresh chưa hợp lệ", str(exc))
            return
        auto_refresh_seconds = parsed_auto_refresh_seconds if auto_refresh_enabled else None
        self.config = replace(
            self.config,
            cdp_url=cdp_url,
            auto_refresh_enabled=auto_refresh_enabled,
            auto_refresh_seconds=parsed_auto_refresh_seconds,
        )
        save_config(self.config)
        self.monitor = AeSexyCdpMonitor(
            cdp_url,
            on_snapshot=self._enqueue_snapshot,
            on_status=lambda message: self.queue.put(("status", message)),
            on_countdowns=self._enqueue_countdowns,
            auto_refresh_seconds=auto_refresh_seconds,
        )
        self._monitor_user_requested_stop = False
        self.monitor_running = True
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")

        def runner() -> None:
            try:
                asyncio.run(self.monitor.run() if self.monitor else _noop())
            except Exception as exc:
                self.queue.put(("status", f"Lỗi CDP monitor: {exc}"))
            finally:
                self.queue.put(("monitor_stopped", None))

        self.monitor_thread = threading.Thread(target=runner, name="ae-cdp-monitor", daemon=True)
        self.monitor_thread.start()

    def _stop_monitor(self) -> None:
        self._monitor_user_requested_stop = True
        if self.monitor:
            self.monitor.stop()
        self.monitor_running = False
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self._set_status("Đang dừng CDP monitor...")

    def _restart_monitor_if_needed(self) -> None:
        if getattr(self, "_monitor_user_requested_stop", False):
            return
        thread = getattr(self, "monitor_thread", None)
        if thread is not None and thread.is_alive():
            return
        self._append_live_log("🔄 [Watchdog] Tự động khởi chạy lại CDP monitor...")
        self._start_monitor()

    def _start_background_watchdog(self) -> None:
        def watchdog_worker() -> None:
            while True:
                time.sleep(3.0)
                try:
                    if hasattr(self, "store") and self.store:
                        daily_settled = self.store.settle_stale_daily_experiment_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if daily_settled:
                            self.queue.put(("daily_history_dirty", None))
                        rl_settled = self.store.settle_stale_run_length_hourly_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if rl_settled:
                            self.queue.put(("run_length_history_dirty", None))
                        em_settled = self.store.settle_stale_ensemble_majority_hourly_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if em_settled:
                            self.queue.put(("ensemble_majority_history_dirty", None))

                    if getattr(self, "monitor_running", False) and not getattr(self, "_monitor_user_requested_stop", False):
                        thread = getattr(self, "monitor_thread", None)
                        if thread is None or not thread.is_alive():
                            self.queue.put(("restart_monitor", None))
                except Exception as exc:
                    logger.debug("Background watchdog tick exception: %s", exc)

        t = threading.Thread(target=watchdog_worker, name="ae-app-watchdog", daemon=True)
        t.start()

    def _ingest_manual(self) -> None:
        text = self.manual_text.get("1.0", "end").strip()
        if not text:
            messagebox.showwarning("Thiếu dữ liệu", "Vui lòng nhập chuỗi B/P/T.")
            return
        try:
            snapshot = parse_manual_sequence(text, self.manual_table_var.get().strip() or self.config.manual_table_name)
        except Exception as exc:
            messagebox.showerror("Không đọc được chuỗi", str(exc))
            return
        self._enqueue_snapshot(snapshot)

    def _save_config_from_ui(self) -> None:
        try:
            money = MoneyConfig(
                stake_chain=parse_stake_chain(self.stake_chain_var.get()),
                progression_mode=self.progression_var.get(),
                stop_loss=float(self.stop_loss_var.get()),
                take_profit=float(self.take_profit_var.get()),
                group_take_profit=float(self.group_tp_var.get()),
                group_stop_loss=float(self.group_sl_var.get()),
            )
            ml_threshold = float(self.ml_threshold_var.get())
            if ml_threshold <= 0 or ml_threshold > 1:
                raise ValueError("ML threshold phai > 0 va <= 1.")
            auto_refresh_seconds = _parse_auto_refresh_seconds(self.auto_refresh_seconds_var.get())
            live_table_stale_seconds = _parse_live_table_stale_seconds(self.live_table_stale_seconds_var.get())
            self.config = replace(
                self.config,
                cdp_url=self.cdp_var.get().strip() or self.config.cdp_url,
                sqlite_path=self.config.sqlite_path,
                duckdb_path=self.config.duckdb_path,
                enable_duckdb=True,
                default_table_name=self.config.default_table_name,
                manual_table_name=self.manual_table_var.get().strip() or self.config.manual_table_name,
                paper_trading_enabled=True,
                auto_refresh_enabled=bool(self.auto_refresh_enabled_var.get()),
                auto_refresh_seconds=auto_refresh_seconds,
                live_table_stale_seconds=live_table_stale_seconds,
                daily_selected_windows=self._selected_daily_windows(),
                daily_autobet_enabled=bool(self.daily_autobet_var.get()),
                daily_stake=float(self.daily_stake_var.get().strip() or self.config.daily_stake),
                daily_bet_mode=_label_to_bet_mode(self.daily_bet_mode_var.get()),
                run_length_selected_windows=self._selected_run_length_windows(),
                run_length_stake=float(self.config.run_length_stake),
                run_length_autobet_enabled=bool(self.run_length_autobet_var.get()),
                run_length_bet_mode=_label_to_bet_mode(self.run_length_bet_mode_var.get()),
                ensemble_majority_bet_mode=_label_to_bet_mode(self.ensemble_majority_bet_mode_var.get()),
                adaptive_regime_bet_mode=_label_to_bet_mode(self.adaptive_regime_bet_mode_var.get()),
                min_confidence=float(self.min_conf_var.get()),
                expected_shoe_rounds=int(self.expected_shoe_rounds_var.get()),
                stop_signals_after_round=int(self.stop_after_round_var.get()),
                ml_filter_enabled=bool(self.ml_filter_enabled_var.get()),
                ml_model_path=self.ml_model_path_var.get().strip() or self.config.ml_model_path,
                ml_decision_threshold=ml_threshold,
                target_url=self.target_url_var.get().strip(),
                account_id=self.account_id_var.get().strip(),
                account_password=self.account_password_var.get().strip() if self.remember_credentials_var.get() else "",
                remember_credentials=bool(self.remember_credentials_var.get()),
                ae_lobby_name=self.ae_lobby_var.get().strip(),
                money=money,
            )
            save_config(self.config)
            self.engine.update_money_config(money)
            self.engine.min_confidence = self.config.min_confidence
            self.engine.expected_shoe_rounds = self.config.expected_shoe_rounds
            self.engine.stop_signals_after_round = self.config.stop_signals_after_round
            self.ml_filter = MlSignalFilter(
                self.config.ml_model_abs_path,
                threshold=self.config.ml_decision_threshold,
                enabled=self.config.ml_filter_enabled,
            )
            self.engine.ml_filter = self.ml_filter
            if self.monitor is not None:
                interval = _auto_refresh_interval(self.config.auto_refresh_enabled, str(self.config.auto_refresh_seconds))
                self.monitor.auto_refresh_seconds = float(interval or 0.0)
        except Exception as exc:
            messagebox.showerror("Cấu hình chưa hợp lệ", str(exc))
            return
        self._set_status("Đã lưu cấu hình.")
        self._append_live_log(self.ml_filter.status_message())

    def _process_queue(self) -> None:
        delay = QUEUE_IDLE_REFRESH_MS
        try:
            snapshot_processed = False
            tick_started = time.perf_counter()
            for _ in range(MAX_QUEUE_ITEMS_PER_TICK):
                try:
                    kind, payload = self.queue.get_nowait()
                except queue.Empty:
                    break
                if kind == "snapshot_latest":
                    table_name = str(payload)
                    with self._queued_snapshot_lock:
                        snapshot_payload = self._queued_snapshot_payloads.pop(table_name, None)
                    if snapshot_payload is None:
                        continue
                    try:
                        self._handle_snapshot(snapshot_payload)
                        snapshot_processed = True
                    finally:
                        pass
                elif kind == "countdowns":
                    countdown_payload = payload if isinstance(payload, dict) else {}
                    observed_monotonic = float(
                        countdown_payload.get("observed_monotonic", time.perf_counter())
                    )
                    values = countdown_payload.get("values")
                    self._table_countdown_readings = {
                        str(table_name): (float(seconds), observed_monotonic)
                        for table_name, seconds in (values.items() if isinstance(values, dict) else [])
                    }
                    self._live_views_dirty = True
                    if hasattr(self, "_auto_arm_daily_experiment"):
                        self._auto_arm_daily_experiment()
                    if hasattr(self, "_auto_arm_run_length_hourly"):
                        self._auto_arm_run_length_hourly()
                    if hasattr(self, "_auto_arm_ensemble_majority_hourly"):
                        self._auto_arm_ensemble_majority_hourly()
                    if hasattr(self, "_auto_arm_adaptive_regime_hourly"):
                        self._auto_arm_adaptive_regime_hourly()
                elif kind == "dashboard_stats":
                    stats_payload = payload if isinstance(payload, dict) else {}
                    summaries = stats_payload.get("summaries")
                    totals = stats_payload.get("totals")
                    if isinstance(summaries, dict):
                        self._dashboard_wl_cache.update(summaries)
                    if isinstance(totals, dict):
                        self._dashboard_totals_cache = totals
                    self._dashboard_stats_refresh_running = False
                    self._dashboard_stats_retry_after_monotonic = 0.0
                    self._live_views_dirty = True
                    if self._dashboard_stats_pending_tables and self._dashboard_tab_visible():
                        self._request_dashboard_stats_refresh()
                elif kind == "dashboard_stats_error":
                    self._dashboard_stats_refresh_running = False
                    error_payload = payload if isinstance(payload, dict) else {}
                    failed_tables = error_payload.get("tables")
                    if isinstance(failed_tables, (list, tuple, set)):
                        self._dashboard_stats_pending_tables.update(str(name) for name in failed_tables)
                    self._dashboard_stats_retry_after_monotonic = time.perf_counter() + 5.0
                    self._append_live_log(
                        f"Không tải được cache W/L dashboard: {error_payload.get('message', payload)}"
                    )
                elif kind == "status":
                    self._set_status(str(payload))
                    self._append_live_log(str(payload))
                elif kind == "automation_status":
                    self.automation_status_var.set(str(payload))
                    self._append_live_log(f"[Auto] {payload}")
                elif kind == "automation_finished":
                    self.automation_running = False
                    self.auto_login_btn.configure(state="normal")
                    self.launch_chrome_btn.configure(state="normal")
                elif kind == "launch_chrome_finished":
                    self.launch_chrome_btn.configure(state="normal")
                elif kind == "automation_success":
                    if not self.monitor_running:
                        self._append_live_log("[Auto] Sảnh đã sẵn sàng, tự động khởi chạy CDP monitor...")
                        self._start_monitor()
                elif kind == "monitor_stopped":
                    if not getattr(self, "_monitor_user_requested_stop", False):
                        self._append_live_log("⚠ [Watchdog] Mất kết nối CDP monitor. Đang tự động kết nối lại sau 2s...")
                        self.root.after(2000, self._restart_monitor_if_needed)
                    else:
                        self.monitor_running = False
                        self.start_button.configure(state="normal")
                        self.stop_button.configure(state="disabled")
                elif kind == "restart_monitor":
                    self._restart_monitor_if_needed()
                elif kind == "daily_history_dirty":
                    self._daily_history_dirty = True
                elif kind == "run_length_history_dirty":
                    self._run_length_pending = self.store.pending_run_length_hourly_row()
                    self._run_length_today_dirty = True
                    self._run_length_history_dirty = True
                elif kind == "ensemble_majority_history_dirty":
                    self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
                    self._ensemble_majority_today_dirty = True
                    self._ensemble_majority_history_dirty = True
                if _elapsed_ms(tick_started, time.perf_counter()) >= QUEUE_PROCESS_TIME_BUDGET_MS:
                    break
            if snapshot_processed:
                self._live_views_dirty = True
                self._history_views_dirty = True

            now = time.perf_counter()
            last_arm = getattr(self, "_last_auto_arm_monotonic", 0.0)
            if _elapsed_ms(last_arm, now) >= 1000.0:
                if hasattr(self, "store") and self.store:
                    if hasattr(self.store, "settle_stale_daily_experiment_bets"):
                        daily_settled = self.store.settle_stale_daily_experiment_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if daily_settled:
                            self._daily_history_dirty = True
                    if hasattr(self.store, "settle_stale_run_length_hourly_bets"):
                        rl_settled = self.store.settle_stale_run_length_hourly_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if rl_settled:
                            self._run_length_pending = self.store.pending_run_length_hourly_row()
                            self._run_length_today_dirty = True
                            self._run_length_history_dirty = True
                    if hasattr(self.store, "settle_stale_ensemble_majority_hourly_bets"):
                        em_settled = self.store.settle_stale_ensemble_majority_hourly_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if em_settled:
                            self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
                            self._ensemble_majority_today_dirty = True
                            self._ensemble_majority_history_dirty = True
                    if hasattr(self.store, "settle_stale_adaptive_regime_hourly_bets"):
                        ar_settled = self.store.settle_stale_adaptive_regime_hourly_bets(
                            banker_commission=self.config.money.banker_commission,
                        )
                        if ar_settled:
                            self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
                            self._adaptive_regime_today_dirty = True
                            self._adaptive_regime_history_dirty = True
                if hasattr(self, "_auto_arm_daily_experiment"):
                    self._auto_arm_daily_experiment()
                if hasattr(self, "_auto_arm_run_length_hourly"):
                    self._auto_arm_run_length_hourly()
                if hasattr(self, "_auto_arm_ensemble_majority_hourly"):
                    self._auto_arm_ensemble_majority_hourly()
                if hasattr(self, "_auto_arm_adaptive_regime_hourly"):
                    self._auto_arm_adaptive_regime_hourly()
                self._last_auto_arm_monotonic = now

            live_refresh_due = self._live_views_dirty and (
                _elapsed_ms(self._last_live_views_refresh_monotonic, now)
                >= LIVE_VIEW_REFRESH_MIN_INTERVAL_MS
                if self._last_live_views_refresh_monotonic
                else True
            )
            live_refreshed = False
            scores: list[Any] | None = None
            if live_refresh_due:
                scores = _filter_live_scores(
                    self.engine.table_scores(),
                    self.config.live_table_stale_seconds,
                )
                self._refresh_live_views(scores)
                ui_refresh_done = time.perf_counter()
                self._last_live_views_refresh_monotonic = ui_refresh_done
                self._live_views_dirty = False
                live_refreshed = True
                self._finalize_latency_samples(ui_refresh_done, utc_now_iso_ms())

            # The latency/history view refreshes periodically without starving live views.
            now = time.perf_counter()
            history_elapsed = (
                _elapsed_ms(self._last_history_views_refresh_monotonic, now)
                if self._last_history_views_refresh_monotonic
                else 999999.0
            )
            history_refresh_due = (
                self._history_views_dirty or history_elapsed >= HISTORY_VIEW_REFRESH_MIN_INTERVAL_MS
            ) and history_elapsed >= HISTORY_VIEW_REFRESH_MIN_INTERVAL_MS
            if history_refresh_due and not live_refreshed:
                if scores is None:
                    scores = _filter_live_scores(
                        self.engine.table_scores(),
                        self.config.live_table_stale_seconds,
                    )
                self._refresh_history_views(scores)
                self._last_history_views_refresh_monotonic = time.perf_counter()
                self._history_views_dirty = False
            delay = QUEUE_BUSY_REFRESH_MS if not self.queue.empty() else QUEUE_IDLE_REFRESH_MS
            if self._live_views_dirty and self._last_live_views_refresh_monotonic:
                elapsed_since_live_refresh = _elapsed_ms(
                    self._last_live_views_refresh_monotonic,
                    time.perf_counter(),
                )
                remaining_live_wait = LIVE_VIEW_REFRESH_MIN_INTERVAL_MS - elapsed_since_live_refresh
                if remaining_live_wait > 0:
                    delay = min(delay, max(1, int(remaining_live_wait) + 1))
        except Exception as exc:
            logger.exception("Lỗi trong _process_queue: %s", exc)
        finally:
            self.root.after(delay, self._process_queue)

    def _enqueue_snapshot(self, snapshot: TableSnapshot) -> None:
        table_name = snapshot.table_name
        if table_name.strip() in EXCLUDED_TABLE_NAMES:
            return
        payload = {
            "snapshot": snapshot,
            "queue_key": _snapshot_queue_key(snapshot),
            "monitor_seen_at": utc_now_iso_ms(),
            "monitor_seen_monotonic": time.perf_counter(),
        }
        with self._queued_snapshot_lock:
            already_queued = table_name in self._queued_snapshot_payloads
            # Replace an older unprocessed snapshot for this table. The next
            # engine pass must use the freshest state, not a stale backlog.
            self._queued_snapshot_payloads[table_name] = payload
        if not already_queued:
            self.queue.put(("snapshot_latest", table_name))

    def _enqueue_countdowns(self, countdowns: dict[str, float]) -> None:
        self.queue.put(
            (
                "countdowns",
                {
                    "values": dict(countdowns),
                    "observed_monotonic": time.perf_counter(),
                },
            )
        )

    def _release_snapshot_queue_key(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        table_name = payload.get("snapshot").table_name if isinstance(payload.get("snapshot"), TableSnapshot) else None
        if not isinstance(table_name, str):
            return
        with self._queued_snapshot_lock:
            self._queued_snapshot_payloads.pop(table_name, None)

    def _handle_snapshot(self, payload: Any) -> None:
        snapshot, monitor_seen_at, monitor_seen_monotonic = _unpack_snapshot_payload(payload)
        app_received_monotonic = time.perf_counter()
        app_received_at = utc_now_iso_ms()
        queue_delay_ms = _elapsed_ms(monitor_seen_monotonic, app_received_monotonic)
        stale_snapshot = queue_delay_ms > STALE_QUEUE_THRESHOLD_MS
        engine_started = time.perf_counter()
        paper_log_count_before = len(self.engine.paper_log)
        try:
            # Stale snapshots still update rounds/last_seen, but never create
            # a prediction from an obsolete state.
            signals = self.engine.ingest(snapshot, generate_signals=not stale_snapshot)
        except Exception as exc:
            self._set_status(f"Lỗi xử lý snapshot: {exc}")
            return
        self._settle_daily_experiment(snapshot.table_name)
        self._settle_run_length_hourly(snapshot.table_name)
        self._settle_ensemble_majority_hourly(snapshot.table_name)
        self._settle_adaptive_regime_hourly(snapshot.table_name)
        if len(self.engine.paper_log) > paper_log_count_before:
            self._dashboard_stats_pending_tables.add(snapshot.table_name)
        if not stale_snapshot:
            self._auto_arm_daily_experiment()
            self._auto_arm_run_length_hourly()
            self._auto_arm_ensemble_majority_hourly()
            self._auto_arm_adaptive_regime_hourly()
        engine_done_monotonic = time.perf_counter()
        engine_done_at = utc_now_iso_ms()
        actionable = [s for s in signals if s.is_actionable]
        engine_ms = _elapsed_ms(engine_started, engine_done_monotonic)
        pending_count = sum(1 for (table_name, _), _bet in self.engine.pending.items() if table_name == snapshot.table_name)
        self._pending_latency_samples.append(
            {
                "table_name": snapshot.table_name,
                "source": snapshot.source,
                "current_round_no": snapshot.current_round_no,
                "observed_rounds": snapshot.observed_rounds,
                "known_missing_rounds": snapshot.known_missing_rounds,
                "monitor_seen_at": monitor_seen_at,
                "monitor_seen_monotonic": monitor_seen_monotonic,
                "app_received_at": app_received_at,
                "engine_done_at": engine_done_at,
                "engine_done_monotonic": engine_done_monotonic,
                "queue_delay_ms": queue_delay_ms,
                "engine_ms": engine_ms,
                "signal_count": len(signals),
                "actionable_count": len(actionable),
                "pending_count": pending_count,
            }
        )
        message = (
            f"{snapshot.table_name}: van hien tai {snapshot.current_round_no}, "
            f"da luu {snapshot.observed_rounds}, thieu {snapshot.known_missing_rounds}, "
            f"{len(actionable)} tin hieu co the theo doi, "
            f"latency queue {queue_delay_ms:.0f}ms, engine {engine_ms:.0f}ms"
        )
        if stale_snapshot:
            message += f"; stale > {STALE_QUEUE_THRESHOLD_MS}ms, khong tao du doan"
        self._set_status(message)
        self._append_live_log(message)

    def _finalize_latency_samples(
        self,
        ui_refresh_done: float,
        ui_refresh_at: str,
    ) -> None:
        if not self._pending_latency_samples:
            return
        pending = self._pending_latency_samples
        self._pending_latency_samples = []
        for item in pending:
            sample = LatencySample(
                table_name=str(item["table_name"]),
                source=str(item["source"]),
                current_round_no=int(item["current_round_no"]),
                observed_rounds=int(item["observed_rounds"]),
                known_missing_rounds=int(item["known_missing_rounds"]),
                monitor_seen_at=str(item["monitor_seen_at"]),
                app_received_at=str(item["app_received_at"]),
                engine_done_at=str(item["engine_done_at"]),
                ui_refresh_at=ui_refresh_at,
                queue_delay_ms=round(float(item["queue_delay_ms"]), 3),
                engine_ms=round(float(item["engine_ms"]), 3),
                ui_delay_ms=round(_elapsed_ms(float(item["engine_done_monotonic"]), ui_refresh_done), 3),
                total_ms=round(_elapsed_ms(float(item["monitor_seen_monotonic"]), ui_refresh_done), 3),
                signal_count=int(item["signal_count"]),
                actionable_count=int(item["actionable_count"]),
                pending_count=int(item["pending_count"]),
                created_at=ui_refresh_at,
            )
            try:
                self.store.save_latency_sample(sample)
            except Exception as exc:
                self._set_status(f"Loi luu latency sample: {exc}")
                self._append_live_log(f"Loi luu latency sample: {exc}")
                break

    def _refresh_views(self) -> None:
        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        self._refresh_live_views(scores)
        self._refresh_history_views(scores)

    def _refresh_live_views(self, scores: list[Any]) -> None:
        """Refresh only the visible live tab without historical scans."""
        if not hasattr(self, "notebook"):
            self._refresh_dashboard_tree(scores)
            self._refresh_signal_tree()
            self._refresh_paper_tree()
            self._refresh_daily_tab(scores)
            self._refresh_run_length_tab()
            self._refresh_ensemble_majority_tab()
            self._refresh_adaptive_regime_tab()
            return

        selected = self.notebook.select()
        if hasattr(self, "dashboard_tab") and selected == str(self.dashboard_tab):
            self._refresh_dashboard_tree(scores)
        elif hasattr(self, "signals_tab") and selected == str(self.signals_tab):
            self._refresh_signal_tree()
            self._refresh_paper_tree()
        elif hasattr(self, "daily_tab") and selected == str(self.daily_tab):
            self._refresh_daily_tab(scores)
        elif hasattr(self, "run_length_tab") and selected == str(self.run_length_tab):
            self._refresh_run_length_tab()
        elif hasattr(self, "ensemble_majority_tab") and selected == str(self.ensemble_majority_tab):
            self._refresh_ensemble_majority_tab()
        elif hasattr(self, "adaptive_regime_tab") and selected == str(self.adaptive_regime_tab):
            self._refresh_adaptive_regime_tab()

    def _refresh_history_views(self, scores: list[Any] | None = None) -> None:
        """Refresh slower historical views only while they are visible."""
        if not hasattr(self, "notebook"):
            self._refresh_latency_tree()
            return
        selected = self.notebook.select()
        if hasattr(self, "latency_tab") and selected == str(self.latency_tab):
            self._refresh_latency_tree()

    def _refresh_dashboard_tree(self, scores: list[Any] | None = None) -> None:
        if not hasattr(self, "dashboard_tree"):
            return
        if scores is None or len(scores) == 0:
            scores = _filter_live_scores(
                self.engine.table_scores(),
                self.config.live_table_stale_seconds,
            )
            if not scores:
                scores = self.engine.table_scores()
        wl_cache = getattr(self, "_dashboard_wl_cache", {})
        row_values = getattr(self, "_dashboard_row_values", {})
        desired_iids: set[str] = set()
        missing_stats: set[str] = set()
        for index, score in enumerate(scores, start=1):
            best_signal = score.best_signal
            display_signal = score.display_signal or best_signal
            filtered_signal = self._filtered_signal_for(display_signal)
            signal_label = signal_side_label(display_signal.side) if display_signal and display_signal.is_actionable else "Không vào"
            confidence = _signal_confidence_label(display_signal, filtered_signal)
            strategy = display_signal.strategy_id if display_signal and display_signal.is_actionable else "-"
            pending = self._pending_label(score.table_name)
            ml_streak = wl_cache.get(score.table_name)
            if ml_streak is None:
                ml_streak = {"history": "Đang tải...", "current": "-"}
                missing_stats.add(score.table_name)
            tag = "bet" if best_signal and best_signal.is_actionable else "watch" if display_signal and display_signal.is_actionable else "skip"
            iid = f"table:{score.table_name}"
            desired_iids.add(iid)
            values = (
                index,
                score.table_name,
                score.last_seen,
                _rounds_label(score),
                score.road,
                signal_label,
                confidence,
                strategy,
                pending,
                ml_streak["history"],
                ml_streak["current"],
                f"{score.paper_pnl:.2f}",
            )
            if not self.dashboard_tree.exists(iid):
                self.dashboard_tree.insert("", "end", iid=iid, values=values, tags=(tag,))
            elif row_values.get(iid) != values:
                self.dashboard_tree.item(iid, values=values, tags=(tag,))
            self.dashboard_tree.move(iid, "", index - 1)
            row_values[iid] = values

        for iid in tuple(row_values):
            if iid.startswith("table:") and iid not in desired_iids:
                if self.dashboard_tree.exists(iid):
                    self.dashboard_tree.delete(iid)
                row_values.pop(iid, None)

        totals = getattr(
            self,
            "_dashboard_totals_cache",
            {"settled_count": 0, "wins": 0, "losses": 0, "pushes": 0, "pnl": 0.0},
        )
        win_loss_count = int(totals["wins"]) + int(totals["losses"])
        win_rate = (int(totals["wins"]) / win_loss_count) if win_loss_count else 0.0
        total_values = (
            "Σ",
            "Tổng ML Pass",
            "",
            f"{totals['settled_count']} lệnh",
            f"W {totals['wins']} / L {totals['losses']} / Push {totals['pushes']}",
            "Tổng",
            f"{win_rate:.1%}" if win_loss_count else "-",
            "ML pass >=55%",
            "-",
            "Tất cả bàn",
            "-",
            f"{float(totals['pnl']):.2f}",
        )
        total_iid = "dashboard:total"
        if not self.dashboard_tree.exists(total_iid):
            self.dashboard_tree.insert("", "end", iid=total_iid, values=total_values, tags=("total",))
        elif row_values.get(total_iid) != total_values:
            self.dashboard_tree.item(total_iid, values=total_values, tags=("total",))
        self.dashboard_tree.move(total_iid, "", len(scores))
        row_values[total_iid] = total_values
        self._dashboard_row_values = row_values

        pending_stats = set(getattr(self, "_dashboard_stats_pending_tables", set()))
        tables_to_refresh = missing_stats | pending_stats
        if tables_to_refresh:
            self._request_dashboard_stats_refresh(tables_to_refresh)

    def _pending_label(self, table_name: str) -> str:
        labels = []
        for (pending_table, strategy_id), bet in self.engine.pending.items():
            if pending_table == table_name:
                labels.append(f"{bet.side.vi_label} {bet.stake:.0f} {strategy_id}")
        return "; ".join(labels[:3]) if labels else "-"

    def _filtered_signal_for(self, signal: StrategySignal | None) -> StrategySignal | None:
        if signal is None:
            return None
        return self.engine.latest_signals.get((signal.table_name, signal.strategy_id))

    def _refresh_signal_tree(self) -> None:
        self.signal_tree.delete(*self.signal_tree.get_children())
        for signal in self.engine.signal_rows():
            self.signal_tree.insert(
                "",
                "end",
                values=(
                    signal.created_at,
                    signal.table_name,
                    signal.strategy_id,
                    signal.action.value,
                    signal_side_label(signal.side),
                    f"{signal.confidence:.1%}",
                    signal.reason,
                ),
            )

    def _refresh_paper_tree(self) -> None:
        self.paper_tree.delete(*self.paper_tree.get_children())
        for bet in self.engine.paper_log[:120]:
            self.paper_tree.insert(
                "",
                "end",
                values=(
                    bet.settled_at or "-",
                    bet.table_name,
                    bet.strategy_id,
                    bet.side.vi_label,
                    f"{bet.stake:.0f}",
                    bet.outcome.vi_label if bet.outcome else "-",
                    f"{bet.pnl_delta:.2f}",
                    f"{bet.pnl_after:.2f}",
                ),
            )

    def _refresh_latency_tree(self) -> None:
        if not hasattr(self, "latency_tree"):
            return
        rows = self.store.recent_latency_samples(120)
        self.latency_summary_var.set(_latency_summary_label(rows))
        self.latency_tree.delete(*self.latency_tree.get_children())
        for row in rows:
            current = int(row["current_round_no"] or 0)
            observed = int(row["observed_rounds"] or 0)
            round_label = str(current) if current == observed else f"{current}/{observed}"
            self.latency_tree.insert(
                "",
                "end",
                values=(
                    row["ui_refresh_at"],
                    row["table_name"],
                    row["source"],
                    round_label,
                    _format_ms(row["queue_delay_ms"]),
                    _format_ms(row["engine_ms"]),
                    _format_ms(row["ui_delay_ms"]),
                    _format_ms(row["total_ms"]),
                    row["signal_count"],
                    row["actionable_count"],
                    row["pending_count"],
                ),
            )

    def _autobet_audit_tab_visible(self) -> bool:
        return (
            hasattr(self, "notebook")
            and hasattr(self, "autobet_audit_tab")
            and self.notebook.select() == str(self.autobet_audit_tab)
        )

    def _mark_autobet_audit_dirty(self) -> None:
        self._autobet_audit_dirty = True
        if not self._autobet_audit_tab_visible() or self._autobet_audit_refresh_scheduled:
            return
        self._autobet_audit_refresh_scheduled = True
        self.root.after(300, self._refresh_scheduled_autobet_audit)

    def _refresh_scheduled_autobet_audit(self) -> None:
        self._autobet_audit_refresh_scheduled = False
        if self._autobet_audit_tab_visible():
            self._refresh_autobet_audit_tree()

    def _refresh_autobet_audit_tree(self, *, force: bool = False) -> None:
        if not hasattr(self, "autobet_attempt_tree"):
            return
        if not force and not getattr(self, "_autobet_audit_dirty", True):
            return
        selected = self.autobet_attempt_tree.selection()
        selected_attempt = selected[0] if selected else ""
        rows = self.store.autobet_attempt_rows(250)
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        summary = self.store.autobet_audit_summary(since=since)
        status_counts = summary["status_counts"]
        self.autobet_audit_summary_var.set(
            "Auto-Bet chỉ bật/tắt bằng thao tác tay | 24 giờ gần nhất: "
            f"{summary['total']} lần | "
            f"Accepted {status_counts.get('provider_accepted', 0)} | "
            f"Đang chờ/legacy {status_counts.get('confirm_clicked', 0) + status_counts.get('unknown', 0)} | "
            f"Rejected {status_counts.get('provider_rejected', 0)} | "
            f"Timeout {status_counts.get('ack_timeout', 0)} | "
            f"Bỏ qua {status_counts.get('skipped', 0)} | "
            f"Lỗi {status_counts.get('failed', 0)} | "
            "Chỉ PROVIDER_ACCEPTED mới được coi là nhà cung cấp đã tiếp nhận."
        )
        self.autobet_attempt_tree.delete(*self.autobet_attempt_tree.get_children())
        for row in rows:
            attempt_id = str(row["attempt_id"])
            shoe = str(row["target_shoe"] or "?")
            round_no = str(row["target_round_no"] or "?")
            countdown = row["countdown_seconds"]
            status = str(row["status"])
            if status == "provider_accepted":
                tag = "accepted"
            elif status in {"unknown", "confirm_clicked"}:
                tag = "unknown"
            elif status in {"failed", "skipped", "provider_rejected", "ack_timeout"}:
                tag = "failed"
            else:
                tag = ""
            self.autobet_attempt_tree.insert(
                "",
                "end",
                iid=attempt_id,
                values=(
                    row["created_at"],
                    row["source"],
                    row["table_name"],
                    f"{shoe}/{round_no}",
                    row["side"],
                    f"{float(row['stake']):g}",
                    f"{float(countdown):.1f}" if countdown is not None else "-",
                    status,
                    row["reason_code"],
                    row["provider_bet_id"] or "-",
                    row["provider_error_code"] or "-",
                    row["reason_detail"],
                ),
                tags=(tag,) if tag else (),
            )
        children = self.autobet_attempt_tree.get_children()
        if selected_attempt and selected_attempt in children:
            self.autobet_attempt_tree.selection_set(selected_attempt)
        elif children:
            self.autobet_attempt_tree.selection_set(children[0])
            selected_attempt = children[0]
        else:
            selected_attempt = ""
        self._populate_autobet_event_tree(selected_attempt)
        self._autobet_audit_dirty = False

    def _on_autobet_attempt_selected(self, _event: Any = None) -> None:
        selected = self.autobet_attempt_tree.selection()
        self._populate_autobet_event_tree(selected[0] if selected else "")

    def _populate_autobet_event_tree(self, attempt_id: str) -> None:
        if not hasattr(self, "autobet_event_tree"):
            return
        self.autobet_event_tree.delete(*self.autobet_event_tree.get_children())
        if not attempt_id:
            return
        for row in self.store.autobet_event_rows(attempt_id):
            countdown = row["countdown_seconds"]
            table_shoe = str(row["table_shoe"] or "?")
            table_round = str(row["table_round_no"] or "?")
            self.autobet_event_tree.insert(
                "",
                "end",
                values=(
                    row["occurred_at"],
                    row["stage"],
                    row["status"],
                    row["reason_code"],
                    f"{float(countdown):.1f}" if countdown is not None else "-",
                    f"{table_shoe}/{table_round}",
                    row["message"],
                ),
            )

    def _on_daily_notebook_changed(self, _event: Any = None) -> None:
        if self.daily_notebook.select() == str(self.daily_history_frame):
            self._refresh_daily_history()

    def _refresh_daily_history(self, *, force: bool = False) -> None:
        try:
            date_values = (DAILY_HISTORY_ALL, *self.store.daily_experiment_dates())
            self.daily_history_date_combo.configure(values=date_values)
            selected_date = self.daily_history_date_var.get().strip() or DAILY_HISTORY_ALL
            if selected_date not in date_values:
                selected_date = DAILY_HISTORY_ALL
                self.daily_history_date_var.set(selected_date)
            selected_window = self.daily_history_window_var.get().strip() or DAILY_HISTORY_ALL
            session_date = None if selected_date == DAILY_HISTORY_ALL else selected_date
            session_window = None if selected_window == DAILY_HISTORY_ALL else selected_window
            cache_key = (session_date, session_window)
            if not force and not self._daily_history_dirty and self._daily_history_loaded_key == cache_key:
                return

            rows = self.store.daily_experiment_rows(
                session_date,
                session_window,
                limit=DAILY_HISTORY_ROW_LIMIT,
            )
            summary = self.store.daily_experiment_summary(session_date, session_window)
        except Exception as exc:
            self.daily_history_summary_var.set(f"Không tải được lịch sử: {exc}")
            return

        self.daily_history_tree.delete(*self.daily_history_tree.get_children())
        for row in rows:
            settled = str(row["status"]) == "settled"
            self.daily_history_tree.insert(
                "",
                "end",
                values=(
                    row["session_date"],
                    row["session_window"],
                    row["table_name"],
                    row["strategy_id"],
                    _daily_side_label(str(row["side"])),
                    f"{float(row['confidence'] or 0):.1%}",
                    _daily_round_label(str(row["signal_fingerprint"])),
                    _format_number(float(row["stake"])),
                    row["result"] or "-",
                    f"{float(row['pnl'] or 0):+.2f}" if settled else "-",
                    "Đã settle" if settled else "Đang chờ",
                ),
            )

        self.daily_history_summary_var.set(_daily_history_summary_label(summary, len(rows)))
        self._daily_history_dirty = False
        self._daily_history_loaded_key = cache_key

    def _selected_daily_windows(self) -> tuple[str, ...]:
        if not hasattr(self, "daily_window_vars"):
            return getattr(getattr(self, "config", None), "daily_selected_windows", ())
        return tuple(
            label
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
            if label in self.daily_window_vars and self.daily_window_vars[label].get()
        )

    def _daily_remaining_seconds(self) -> dict[str, float]:
        return _estimated_remaining_seconds(
            getattr(self, "_table_countdown_readings", {}),
            snapshots=getattr(self.engine, "snapshots", None),
        )

    def _on_daily_window_selection_changed(self) -> None:
        selected_windows = self._selected_daily_windows()
        updated_config = replace(
            self.config,
            daily_selected_windows=selected_windows,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được khung giờ", str(exc))
            return
        self.config = updated_config
        self._refresh_daily_tab()

    def _on_daily_autobet_changed(self) -> None:
        enabled = bool(self.daily_autobet_var.get())
        updated_config = replace(
            self.config,
            daily_autobet_enabled=enabled,
        )
        try:
            save_config(updated_config)
            self.config = updated_config
            status_text = "ĐÃ BẬT" if enabled else "ĐÃ TẮT"
            self._append_live_log(f"⚙ [Auto-Bet] Chế độ đánh thật theo khung giờ: {status_text}")
        except Exception as exc:
            self._append_live_log(f"⚠ [Auto-Bet] Không lưu được cấu hình: {exc}")
        self._refresh_daily_tab()

    def _on_daily_bet_mode_changed(self, *_args: Any) -> None:
        label = self.daily_bet_mode_var.get()
        mode = _label_to_bet_mode(label)
        if mode != getattr(self.config, "daily_bet_mode", "forward"):
            updated_config = replace(self.config, daily_bet_mode=mode)
            try:
                save_config(updated_config)
                self.config = updated_config
                self._append_live_log(f"⚙ [Auto-Bet Daily] Chiều đánh thật: {label} ({mode})")
            except Exception as exc:
                self._append_live_log(f"⚠ [Auto-Bet Daily] Không lưu được cấu hình: {exc}")

    def _is_daily_stop_win_enabled(self) -> bool:
        if hasattr(self, "daily_stop_win_var"):
            return bool(self.daily_stop_win_var.get())
        return bool(getattr(getattr(self, "config", None), "daily_stop_win_enabled", False))

    def _on_daily_stop_win_changed(self) -> None:
        enabled = bool(self.daily_stop_win_var.get())
        updated_config = replace(
            self.config,
            daily_stop_win_enabled=enabled,
        )
        try:
            save_config(updated_config)
            self.config = updated_config
            status_text = "ĐÃ BẬT" if enabled else "ĐÃ TẮT"
            self._append_live_log(f"⚙ [Daily] Chế độ Stop Win trong khung giờ: {status_text}")
        except Exception as exc:
            self._append_live_log(f"⚠ [Daily] Không lưu được cấu hình Stop Win: {exc}")
        self._refresh_daily_tab()

    def _on_main_notebook_changed(self, _event: Any = None) -> None:
        if not hasattr(self, "notebook"):
            return
        selected = self.notebook.select()
        if hasattr(self, "dashboard_tab") and selected == str(self.dashboard_tab):
            self._refresh_dashboard_tree()
        elif hasattr(self, "latency_tab") and selected == str(self.latency_tab):
            self._refresh_latency_tree()
        elif hasattr(self, "autobet_audit_tab") and selected == str(self.autobet_audit_tab):
            self._refresh_autobet_audit_tree(force=True)
        elif hasattr(self, "signals_tab") and selected == str(self.signals_tab):
            self._refresh_signal_tree()
            self._refresh_paper_tree()
        elif hasattr(self, "daily_tab") and selected == str(self.daily_tab):
            self._refresh_daily_tab()
        elif hasattr(self, "run_length_tab") and selected == str(self.run_length_tab):
            self._refresh_run_length_tab()
        elif hasattr(self, "ensemble_majority_tab") and selected == str(self.ensemble_majority_tab):
            self._refresh_ensemble_majority_tab()
        elif hasattr(self, "adaptive_regime_tab") and selected == str(self.adaptive_regime_tab):
            self._refresh_adaptive_regime_tab(force_today=True)

    def _dashboard_tab_visible(self) -> bool:
        if not hasattr(self, "notebook") or not hasattr(self, "dashboard_tab"):
            return True
        return self.notebook.select() == str(self.dashboard_tab)

    def _request_dashboard_stats_refresh(self, table_names: Any = None) -> None:
        pending = getattr(self, "_dashboard_stats_pending_tables", None)
        if pending is None:
            pending = set()
            self._dashboard_stats_pending_tables = pending
        if table_names is not None:
            pending.update(str(name) for name in table_names if str(name))
        if getattr(self, "_dashboard_stats_refresh_running", False) or not pending:
            return
        if time.perf_counter() < getattr(self, "_dashboard_stats_retry_after_monotonic", 0.0):
            return

        requested_tables = tuple(sorted(pending))
        pending.difference_update(requested_tables)
        requested_shoes = {
            table_name: getattr(self.engine.snapshots.get(table_name), "shoe", None)
            for table_name in requested_tables
        }
        self._dashboard_stats_refresh_running = True

        def worker() -> None:
            try:
                stats = self.store.dashboard_ml_pass_snapshot(requested_shoes)
            except Exception as exc:
                self.queue.put(
                    (
                        "dashboard_stats_error",
                        {"message": str(exc), "tables": requested_tables},
                    )
                )
                return
            self.queue.put(("dashboard_stats", stats))

        threading.Thread(
            target=worker,
            daemon=True,
            name="dashboard-wl-cache",
        ).start()

    def _on_run_length_notebook_changed(self, _event: Any = None) -> None:
        if (
            self._run_length_tab_visible()
            and self.run_length_notebook.select() == str(self.run_length_history_frame)
        ):
            self._refresh_run_length_history()

    def _run_length_tab_visible(self) -> bool:
        return (
            hasattr(self, "notebook")
            and hasattr(self, "run_length_tab")
            and self.notebook.select() == str(self.run_length_tab)
        )

    def _selected_run_length_windows(self) -> tuple[str, ...]:
        if not hasattr(self, "run_length_window_vars"):
            return getattr(getattr(self, "config", None), "run_length_selected_windows", ())
        return tuple(
            label
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
            if label in self.run_length_window_vars and self.run_length_window_vars[label].get()
        )

    def _save_run_length_stake(self) -> None:
        raw = self.run_length_stake_var.get().strip()
        try:
            stake = float(raw)
        except ValueError:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số lớn hơn 0.")
            return
        if not math.isfinite(stake) or stake <= 0:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số hữu hạn lớn hơn 0.")
            return
        updated_config = replace(self.config, run_length_stake=stake)
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được stake", str(exc))
            return
        self.config = updated_config
        self.run_length_status_var.set(
            f"Đã lưu stake {_format_number(stake)} điểm; chỉ áp dụng cho lệnh mới."
        )

    def _on_run_length_window_selection_changed(self) -> None:
        selected_windows = self._selected_run_length_windows()
        updated_config = replace(
            self.config,
            run_length_selected_windows=selected_windows,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được khung giờ Run Length", str(exc))
            return
        self.config = updated_config
        self._refresh_run_length_tab()

    def _on_run_length_autobet_changed(self) -> None:
        enabled = bool(self.run_length_autobet_var.get())
        if enabled:
            confirm = messagebox.askyesno(
                "Xác nhận BẬT Auto-Bet Run Length",
                "⚠ CẢNH BÁO CƯỢC THẬT:\n\n"
                "Khi đến khung giờ đã chọn, nếu có bàn đạt chiến thuật Run Length (ML >=58%), "
                "tool sẽ tự động đặt cược bằng TIỀN THẬT trên sảnh AE Sexy.\n\n"
                "Bạn có chắc chắn muốn BẬT tính năng này?",
            )
            if not confirm:
                self.run_length_autobet_var.set(False)
                return
        updated_config = replace(
            self.config,
            run_length_autobet_enabled=enabled,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được cấu hình Auto-Bet Run Length", str(exc))
            return
        self.config = updated_config
        state_text = "ĐÃ BẬT" if enabled else "ĐÃ TẮT"
        msg = f"⚙ Chế độ Live Auto-Bet cho Run Length >=58%: {state_text}."
        self._append_live_log(msg)
        if hasattr(self, "run_length_status_var"):
            self.run_length_status_var.set(msg)

    def _on_run_length_bet_mode_changed(self, *_args: Any) -> None:
        label = self.run_length_bet_mode_var.get()
        mode = _label_to_bet_mode(label)
        if mode != getattr(self.config, "run_length_bet_mode", "forward"):
            updated_config = replace(self.config, run_length_bet_mode=mode)
            try:
                save_config(updated_config)
                self.config = updated_config
                self._append_live_log(f"⚙ [Auto-Bet Run Length] Chiều đánh thật: {label} ({mode})")
            except Exception as exc:
                self._append_live_log(f"⚠ [Auto-Bet Run Length] Không lưu được cấu hình: {exc}")

    def _ensure_run_length_slot_cache(self, session_date: str) -> None:
        if self._run_length_slot_cache_date == session_date:
            return
        self._run_length_consumed_slots = self.store.run_length_hourly_slot_keys(session_date)
        self._run_length_slot_cache_date = session_date

    def _run_length_candidates(
        self,
        remaining_seconds: dict[str, float] | None = None,
    ) -> list[StrategySignal]:
        return _rank_run_length_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            remaining_seconds if remaining_seconds is not None else self._daily_remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
        )

    def _refresh_run_length_tab(self, *, force_today: bool = False) -> None:
        if not hasattr(self, "run_length_tree"):
            return
        if not force_today and not self._run_length_tab_visible():
            return

        now = datetime.now(BANGKOK_TIMEZONE)
        session_date = now.date().isoformat()
        selected_windows = self._selected_run_length_windows()
        active_window = _active_daily_experiment_window(now, selected_windows)
        try:
            self._ensure_run_length_slot_cache(session_date)
        except Exception as exc:
            self.run_length_status_var.set(f"Không tải được trạng thái khung giờ: {exc}")
            return

        pending = self._run_length_pending
        slot_used = bool(
            active_window
            and (session_date, active_window) in self._run_length_consumed_slots
        )
        remaining_seconds = self._daily_remaining_seconds()
        candidates: list[StrategySignal] = []
        if active_window and pending is None and not slot_used:
            candidates = [
                signal
                for signal in self._run_length_candidates(remaining_seconds)
                if _signal_created_in_active_window(signal, now, active_window)
            ]

        if pending is not None:
            self.run_length_status_var.set(
                f"Đang chờ settle lệnh {pending['session_window']} tại {pending['table_name']}; "
                "W, L hoặc Tie đều kết thúc lượt của khung giờ đó."
            )
        elif not selected_windows:
            self.run_length_status_var.set(
                "Chưa tick khung giờ nào; không tạo dự đoán Run Length mới."
            )
        elif active_window is None:
            self.run_length_status_var.set(
                f"{_daily_window_selection_summary(selected_windows)}; hiện ngoài khung đã tick."
            )
        elif slot_used:
            self.run_length_status_var.set(
                f"Khung {active_window} đã dùng đủ 1 dự đoán; chờ khung được tick tiếp theo."
            )
        else:
            self.run_length_status_var.set(
                f"Khung {active_window} đang mở; chờ run_length có ML >=58% và còn ít nhất 10 giây."
            )

        if candidates:
            signal = candidates[0]
            shoe, round_no = _signal_shoe_round(signal.round_fingerprint)
            seconds = remaining_seconds.get(signal.table_name, 0.0)
            probability = float(signal.features["ml_probability_win"])
            self.run_length_candidate_var.set(
                f"Ứng viên hiện tại: {signal.table_name} | shoe {shoe} | round {round_no} | "
                f"{signal.side.vi_label if signal.side else '-'} | ML {probability:.1%} | còn {seconds:.1f}s."
            )
        else:
            self.run_length_candidate_var.set("Ứng viên hiện tại: chưa có dự đoán đủ điều kiện.")

        if force_today or self._run_length_today_dirty:
            try:
                rows = self.store.run_length_hourly_rows(
                    session_date,
                    limit=RUN_LENGTH_HISTORY_ROW_LIMIT,
                )
                summary = self.store.run_length_hourly_summary(session_date)
            except Exception as exc:
                self.run_length_summary_var.set(f"Không tải được dữ liệu hôm nay: {exc}")
                return
            self._populate_run_length_tree(self.run_length_tree, rows)
            self.run_length_summary_var.set(_run_length_summary_label(summary, len(rows)))
            self._run_length_today_dirty = False

        if (
            hasattr(self, "run_length_notebook")
            and self.run_length_notebook.select() == str(self.run_length_history_frame)
        ):
            self._refresh_run_length_history()

    def _refresh_run_length_history(self, *, force: bool = False) -> None:
        selected_date_value = (
            self.run_length_history_date_var.get().strip() or DAILY_HISTORY_ALL
        )
        selected_window_value = (
            self.run_length_history_window_var.get().strip() or DAILY_HISTORY_ALL
        )
        requested_key = (
            None if selected_date_value == DAILY_HISTORY_ALL else selected_date_value,
            None if selected_window_value == DAILY_HISTORY_ALL else selected_window_value,
        )
        if (
            not force
            and not self._run_length_history_dirty
            and self._run_length_history_loaded_key == requested_key
        ):
            return
        try:
            date_values = (DAILY_HISTORY_ALL, *self.store.run_length_hourly_dates())
            self.run_length_history_date_combo.configure(values=date_values)
            selected_date = selected_date_value
            if selected_date not in date_values:
                selected_date = DAILY_HISTORY_ALL
                self.run_length_history_date_var.set(selected_date)
            selected_window = selected_window_value
            session_date = None if selected_date == DAILY_HISTORY_ALL else selected_date
            session_window = None if selected_window == DAILY_HISTORY_ALL else selected_window
            cache_key = (session_date, session_window)
            rows = self.store.run_length_hourly_rows(
                session_date,
                session_window,
                limit=RUN_LENGTH_HISTORY_ROW_LIMIT,
            )
            summary = self.store.run_length_hourly_summary(session_date, session_window)
        except Exception as exc:
            self.run_length_history_summary_var.set(
                f"Không tải được lịch sử Run Length: {exc}"
            )
            return

        self._populate_run_length_tree(self.run_length_history_tree, rows)
        self.run_length_history_summary_var.set(
            _run_length_summary_label(summary, len(rows))
        )
        self._run_length_history_dirty = False
        self._run_length_history_loaded_key = cache_key

    def _populate_run_length_tree(self, tree: ttk.Treeview, rows: list[Any]) -> None:
        tree.delete(*tree.get_children())
        for row in rows:
            shoe, round_no = _signal_shoe_round(str(row["signal_fingerprint"]))
            settled = str(row["status"]) == "settled"
            tree.insert(
                "",
                "end",
                values=(
                    row["session_date"],
                    row["session_window"],
                    row["table_name"],
                    shoe,
                    round_no,
                    _daily_side_label(str(row["side"])),
                    f"{float(row['confidence']):.1%}",
                    _format_number(float(row["stake"])),
                    row["result"] or "-",
                    f"{float(row['pnl'] or 0):+.2f}" if settled else "-",
                    "Đã settle" if settled else "Đang chờ",
                ),
            )

    def _auto_arm_run_length_hourly(self) -> None:
        if not hasattr(self, "store"):
            return
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self._selected_run_length_windows(),
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_run_length_hourly_bets"):
            settled = self.store.settle_stale_run_length_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._run_length_today_dirty = True
                self._run_length_history_dirty = True
        self._run_length_pending = self.store.pending_run_length_hourly_row()
        if self._run_length_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_run_length_slot_cache(session_date)
        except Exception as exc:
            if hasattr(self, "run_length_status_var"):
                self.run_length_status_var.set(f"Không kiểm tra được khung giờ Run Length: {exc}")
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
        try:
            stake = float(self.run_length_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(self.config.run_length_stake)
        if stake <= 0:
            return
        bet_id = self.store.save_run_length_hourly_bet(
            session_date=session_date,
            session_window=active_window,
            created_at=utc_now_iso_ms(),
            table_name=signal.table_name,
            side=signal.side.value if signal.side else "",
            stake=stake,
            signal_fingerprint=signal.round_fingerprint,
            confidence=float(signal.features["ml_probability_win"]),
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
        self._run_length_today_dirty = True
        self._run_length_history_dirty = True

        order_key = f"rl|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        autobet_var = getattr(self, "run_length_autobet_var", None)
        autobet_enabled = bool(autobet_var.get()) if autobet_var else False
        armed_today = getattr(self, "_run_length_autobet_armed_today", None)
        if armed_today is None:
            armed_today = set()
            self._run_length_autobet_armed_today = armed_today

        if autobet_enabled and order_key not in armed_today:
            armed_today.add(order_key)
            snapshots = getattr(self.engine, "snapshots", {}) if hasattr(self, "engine") and self.engine else {}
            snap = snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=resolve_bet_side(
                        signal.side.value if signal.side else "",
                        getattr(self.config, "run_length_bet_mode", "forward"),
                    ),
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and autobet_enabled) else ""
        if hasattr(self, "run_length_status_var"):
            self.run_length_status_var.set(
                f"Đã ghi 1 lệnh paper Run Length cho {active_window} tại {signal.table_name}{autobet_msg}; chờ settle."
            )
        if new_orders and autobet_enabled:
            self._dispatch_live_autobet(new_orders, source="run_length")


    def _settle_run_length_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_run_length_hourly_bets"):
                settled_ids = self.store.settle_stale_run_length_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._run_length_pending = self.store.pending_run_length_hourly_row()
                    self._run_length_today_dirty = True
                    self._run_length_history_dirty = True
            return
        row = self._run_length_pending
        if row is None:
            row = self.store.pending_run_length_hourly_row(table_name)
            if row is not None:
                self._run_length_pending = row
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
            settled = self.store.settle_run_length_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
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
        if settled:
            self._run_length_pending = None
            self._run_length_today_dirty = True
            self._run_length_history_dirty = True
            if hasattr(self, "auto_bettor") and self.auto_bettor:
                self.auto_bettor.return_to_lobby_background()

    def _on_ensemble_majority_notebook_changed(self, _event: Any = None) -> None:
        if (
            self._ensemble_majority_tab_visible()
            and self.ensemble_majority_notebook.select() == str(self.ensemble_majority_history_frame)
        ):
            self._refresh_ensemble_majority_history()

    def _ensemble_majority_tab_visible(self) -> bool:
        return (
            hasattr(self, "notebook")
            and hasattr(self, "ensemble_majority_tab")
            and self.notebook.select() == str(self.ensemble_majority_tab)
        )

    def _selected_ensemble_majority_windows(self) -> tuple[str, ...]:
        if not hasattr(self, "ensemble_majority_window_vars"):
            return getattr(getattr(self, "config", None), "ensemble_majority_selected_windows", ())
        return tuple(
            label
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
            if label in self.ensemble_majority_window_vars and self.ensemble_majority_window_vars[label].get()
        )

    def _save_ensemble_majority_stake(self) -> None:
        raw = self.ensemble_majority_stake_var.get().strip()
        try:
            stake = float(raw)
        except ValueError:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số lớn hơn 0.")
            return
        if not math.isfinite(stake) or stake <= 0:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số hữu hạn lớn hơn 0.")
            return
        updated_config = replace(self.config, ensemble_majority_stake=stake)
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được stake", str(exc))
            return
        self.config = updated_config
        self.ensemble_majority_status_var.set(
            f"Đã lưu stake {_format_number(stake)} điểm; chỉ áp dụng cho lệnh mới."
        )

    def _save_ensemble_majority_min_prob(self) -> None:
        raw = self.ensemble_majority_min_prob_var.get().strip()
        try:
            val = float(raw)
            prob = val / 100.0 if val > 1.0 else val
        except ValueError:
            messagebox.showerror("Tỷ lệ ML chưa hợp lệ", "Tỷ lệ ML phải là số từ 50 đến 100 (%).")
            return
        if not (0.50 <= prob <= 1.0):
            messagebox.showerror("Tỷ lệ ML chưa hợp lệ", "Tỷ lệ ML tối thiểu phải từ 50% đến 100%.")
            return
        updated_config = replace(self.config, ensemble_majority_ml_min_probability=prob)
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được cấu hình", str(exc))
            return
        self.config = updated_config
        self.ensemble_majority_status_var.set(
            f"Đã lưu ngưỡng ML tối thiểu: {prob:.1%}; chỉ áp dụng cho lệnh mới."
        )
        self._refresh_ensemble_majority_tab(force_today=True)

    def _on_ensemble_majority_window_selection_changed(self) -> None:
        selected_windows = self._selected_ensemble_majority_windows()
        updated_config = replace(
            self.config,
            ensemble_majority_selected_windows=selected_windows,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được khung giờ Ensemble Majority", str(exc))
            return
        self.config = updated_config
        self._refresh_ensemble_majority_tab()

    def _on_ensemble_majority_autobet_changed(self) -> None:
        enabled = bool(self.ensemble_majority_autobet_var.get())
        if enabled:
            min_prob = float(getattr(self.config, "ensemble_majority_ml_min_probability", 0.55))
            confirm = messagebox.askyesno(
                "Xác nhận BẬT Auto-Bet Ensemble Majority",
                "⚠ CẢNH BÁO CƯỢC THẬT:\n\n"
                f"Khi đến khung giờ đã chọn, nếu có bàn đạt chiến thuật Ensemble Majority (ML >={min_prob:.0%}), "
                "tool sẽ tự động đặt cược bằng TIỀN THẬT trên sảnh AE Sexy.\n\n"
                "Bạn có chắc chắn muốn BẬT tính năng này?",
            )
            if not confirm:
                self.ensemble_majority_autobet_var.set(False)
                return
        updated_config = replace(
            self.config,
            ensemble_majority_autobet_enabled=enabled,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được cấu hình Auto-Bet Ensemble Majority", str(exc))
            return
        self.config = updated_config
        state_text = "ĐÃ BẬT" if enabled else "ĐÃ TẮT"
        msg = f"⚙ Chế độ Live Auto-Bet cho Ensemble Majority: {state_text}."
        self._append_live_log(msg)
        if hasattr(self, "ensemble_majority_status_var"):
            self.ensemble_majority_status_var.set(msg)

    def _on_ensemble_majority_bet_mode_changed(self, *_args: Any) -> None:
        label = self.ensemble_majority_bet_mode_var.get()
        mode = _label_to_bet_mode(label)
        if mode != getattr(self.config, "ensemble_majority_bet_mode", "forward"):
            updated_config = replace(self.config, ensemble_majority_bet_mode=mode)
            try:
                save_config(updated_config)
                self.config = updated_config
                self._append_live_log(f"⚙ [Auto-Bet Ensemble Majority] Chiều đánh thật: {label} ({mode})")
            except Exception as exc:
                self._append_live_log(f"⚠ [Auto-Bet Ensemble Majority] Không lưu được cấu hình: {exc}")

    def _ensure_ensemble_majority_slot_cache(self, session_date: str) -> None:
        if self._ensemble_majority_slot_cache_date == session_date:
            return
        self._ensemble_majority_consumed_slots = self.store.ensemble_majority_hourly_slot_keys(session_date)
        self._ensemble_majority_slot_cache_date = session_date

    def _ensemble_majority_candidates(
        self,
        remaining_seconds: dict[str, float] | None = None,
    ) -> list[StrategySignal]:
        min_prob = float(getattr(self.config, "ensemble_majority_ml_min_probability", ENSEMBLE_MAJORITY_ML_MIN_PROBABILITY))
        return _rank_ensemble_majority_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            remaining_seconds if remaining_seconds is not None else self._daily_remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
            min_probability=min_prob,
        )

    def _refresh_ensemble_majority_tab(self, *, force_today: bool = False) -> None:
        if not hasattr(self, "ensemble_majority_tree"):
            return
        if not force_today and not self._ensemble_majority_tab_visible():
            return

        now = datetime.now(BANGKOK_TIMEZONE)
        session_date = now.date().isoformat()
        selected_windows = self._selected_ensemble_majority_windows()
        active_window = _active_daily_experiment_window(now, selected_windows)
        try:
            self._ensure_ensemble_majority_slot_cache(session_date)
        except Exception as exc:
            self.ensemble_majority_status_var.set(f"Không tải được trạng thái khung giờ: {exc}")
            return

        pending = self._ensemble_majority_pending
        slot_used = bool(
            active_window
            and (session_date, active_window) in self._ensemble_majority_consumed_slots
        )
        remaining_seconds = self._daily_remaining_seconds()
        candidates: list[StrategySignal] = []
        if active_window and pending is None and not slot_used:
            candidates = [
                signal
                for signal in self._ensemble_majority_candidates(remaining_seconds)
                if _signal_created_in_active_window(signal, now, active_window)
            ]

        min_prob = float(getattr(self.config, "ensemble_majority_ml_min_probability", 0.55))
        if pending is not None:
            self.ensemble_majority_status_var.set(
                f"Đang chờ settle lệnh {pending['session_window']} tại {pending['table_name']}; "
                "W, L hoặc Tie đều kết thúc lượt của khung giờ đó."
            )
        elif not selected_windows:
            self.ensemble_majority_status_var.set(
                "Chưa tick khung giờ nào; không tạo dự đoán Ensemble Majority mới."
            )
        elif active_window is None:
            self.ensemble_majority_status_var.set(
                f"{_daily_window_selection_summary(selected_windows)}; hiện ngoài khung đã tick."
            )
        elif slot_used:
            self.ensemble_majority_status_var.set(
                f"Khung {active_window} đã dùng đủ 1 dự đoán; chờ khung được tick tiếp theo."
            )
        else:
            self.ensemble_majority_status_var.set(
                f"Khung {active_window} đang mở; chờ ensemble_majority có ML >={min_prob:.0%} và còn ít nhất 10 giây."
            )

        if candidates:
            signal = candidates[0]
            shoe, round_no = _signal_shoe_round(signal.round_fingerprint)
            seconds = remaining_seconds.get(signal.table_name, 0.0)
            probability = float(signal.features.get("ml_probability_win", signal.confidence))
            self.ensemble_majority_candidate_var.set(
                f"Ứng viên hiện tại: {signal.table_name} | shoe {shoe} | round {round_no} | "
                f"{signal.side.vi_label if signal.side else '-'} | ML {probability:.1%} | còn {seconds:.1f}s."
            )
        else:
            self.ensemble_majority_candidate_var.set("Ứng viên hiện tại: chưa có dự đoán đủ điều kiện.")

        if force_today or self._ensemble_majority_today_dirty:
            try:
                rows = self.store.ensemble_majority_hourly_rows(
                    session_date,
                    limit=ENSEMBLE_MAJORITY_HISTORY_ROW_LIMIT,
                )
                summary = self.store.ensemble_majority_hourly_summary(session_date)
            except Exception as exc:
                self.ensemble_majority_summary_var.set(f"Không tải được dữ liệu hôm nay: {exc}")
                return
            self._populate_ensemble_majority_tree(self.ensemble_majority_tree, rows)
            self.ensemble_majority_summary_var.set(_ensemble_majority_summary_label(summary, len(rows)))
            self._ensemble_majority_today_dirty = False

        if (
            hasattr(self, "ensemble_majority_notebook")
            and self.ensemble_majority_notebook.select() == str(self.ensemble_majority_history_frame)
        ):
            self._refresh_ensemble_majority_history()

    def _refresh_ensemble_majority_history(self, *, force: bool = False) -> None:
        selected_date_value = (
            self.ensemble_majority_history_date_var.get().strip() or DAILY_HISTORY_ALL
        )
        selected_window_value = (
            self.ensemble_majority_history_window_var.get().strip() or DAILY_HISTORY_ALL
        )
        requested_key = (
            None if selected_date_value == DAILY_HISTORY_ALL else selected_date_value,
            None if selected_window_value == DAILY_HISTORY_ALL else selected_window_value,
        )
        if (
            not force
            and not self._ensemble_majority_history_dirty
            and self._ensemble_majority_history_loaded_key == requested_key
        ):
            return
        try:
            date_values = (DAILY_HISTORY_ALL, *self.store.ensemble_majority_hourly_dates())
            self.ensemble_majority_history_date_combo.configure(values=date_values)
            selected_date = selected_date_value
            if selected_date not in date_values:
                selected_date = DAILY_HISTORY_ALL
                self.ensemble_majority_history_date_var.set(selected_date)
            selected_window = selected_window_value
            session_date = None if selected_date == DAILY_HISTORY_ALL else selected_date
            session_window = None if selected_window == DAILY_HISTORY_ALL else selected_window
            cache_key = (session_date, session_window)
            rows = self.store.ensemble_majority_hourly_rows(
                session_date,
                session_window,
                limit=ENSEMBLE_MAJORITY_HISTORY_ROW_LIMIT,
            )
            summary = self.store.ensemble_majority_hourly_summary(session_date, session_window)
        except Exception as exc:
            self.ensemble_majority_history_summary_var.set(
                f"Không tải được lịch sử Ensemble Majority: {exc}"
            )
            return

        self._populate_ensemble_majority_tree(self.ensemble_majority_history_tree, rows)
        self.ensemble_majority_history_summary_var.set(
            _ensemble_majority_summary_label(summary, len(rows))
        )
        self._ensemble_majority_history_dirty = False
        self._ensemble_majority_history_loaded_key = cache_key

    def _populate_ensemble_majority_tree(self, tree: ttk.Treeview, rows: list[Any]) -> None:
        tree.delete(*tree.get_children())
        for row in rows:
            shoe, round_no = _signal_shoe_round(str(row["signal_fingerprint"]))
            settled = str(row["status"]) == "settled"
            tree.insert(
                "",
                "end",
                values=(
                    row["session_date"],
                    row["session_window"],
                    row["table_name"],
                    shoe,
                    round_no,
                    _daily_side_label(str(row["side"])),
                    f"{float(row['confidence']):.1%}",
                    _format_number(float(row["stake"])),
                    row["result"] or "-",
                    f"{float(row['pnl'] or 0):+.2f}" if settled else "-",
                    "Đã settle" if settled else "Đang chờ",
                ),
            )

    def _auto_arm_ensemble_majority_hourly(self) -> None:
        if not hasattr(self, "store"):
            return
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self._selected_ensemble_majority_windows(),
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_ensemble_majority_hourly_bets"):
            settled = self.store.settle_stale_ensemble_majority_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._ensemble_majority_today_dirty = True
                self._ensemble_majority_history_dirty = True
        self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
        if self._ensemble_majority_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_ensemble_majority_slot_cache(session_date)
        except Exception as exc:
            if hasattr(self, "ensemble_majority_status_var"):
                self.ensemble_majority_status_var.set(f"Không kiểm tra được khung giờ Ensemble: {exc}")
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
        try:
            stake = float(self.ensemble_majority_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(getattr(self.config, "ensemble_majority_stake", 10.0))
        if stake <= 0:
            return
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
        self._ensemble_majority_today_dirty = True
        self._ensemble_majority_history_dirty = True

        order_key = f"em|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        autobet_var = getattr(self, "ensemble_majority_autobet_var", None)
        autobet_enabled = bool(autobet_var.get()) if autobet_var else False
        armed_today = getattr(self, "_ensemble_majority_autobet_armed_today", None)
        if armed_today is None:
            armed_today = set()
            self._ensemble_majority_autobet_armed_today = armed_today

        if autobet_enabled and order_key not in armed_today:
            armed_today.add(order_key)
            snapshots = getattr(self.engine, "snapshots", {}) if hasattr(self, "engine") and self.engine else {}
            snap = snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=resolve_bet_side(
                        signal.side.value if signal.side else "",
                        getattr(self.config, "ensemble_majority_bet_mode", "forward"),
                    ),
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and autobet_enabled) else ""
        if hasattr(self, "ensemble_majority_status_var"):
            self.ensemble_majority_status_var.set(
                f"Đã ghi 1 lệnh paper Ensemble Majority cho {active_window} tại {signal.table_name}{autobet_msg}; chờ settle."
            )
        if new_orders and autobet_enabled:
            self._dispatch_live_autobet(new_orders, source="ensemble_majority")

    def _settle_ensemble_majority_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_ensemble_majority_hourly_bets"):
                settled_ids = self.store.settle_stale_ensemble_majority_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._ensemble_majority_pending = self.store.pending_ensemble_majority_hourly_row()
                    self._ensemble_majority_today_dirty = True
                    self._ensemble_majority_history_dirty = True
            return
        row = self._ensemble_majority_pending
        if row is None:
            row = self.store.pending_ensemble_majority_hourly_row(table_name)
            if row is not None:
                self._ensemble_majority_pending = row
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
            settled = self.store.settle_ensemble_majority_hourly_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
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
        if settled:
            self._ensemble_majority_pending = None
            self._ensemble_majority_today_dirty = True
            self._ensemble_majority_history_dirty = True
            if hasattr(self, "auto_bettor") and self.auto_bettor:
                self.auto_bettor.return_to_lobby_background()

    def _on_adaptive_regime_notebook_changed(self, _event: Any = None) -> None:
        if (
            self._adaptive_regime_tab_visible()
            and self.adaptive_regime_notebook.select() == str(self.adaptive_regime_history_frame)
        ):
            self._refresh_adaptive_regime_history()

    def _adaptive_regime_tab_visible(self) -> bool:
        return (
            hasattr(self, "notebook")
            and hasattr(self, "adaptive_regime_tab")
            and self.notebook.select() == str(self.adaptive_regime_tab)
        )

    def _selected_adaptive_regime_windows(self) -> tuple[str, ...]:
        if not hasattr(self, "adaptive_regime_window_vars"):
            return getattr(getattr(self, "config", None), "adaptive_regime_selected_windows", ())
        return tuple(
            label
            for label in DAILY_EXPERIMENT_WINDOW_LABELS
            if label in self.adaptive_regime_window_vars and self.adaptive_regime_window_vars[label].get()
        )

    def _save_adaptive_regime_stake(self) -> None:
        raw = self.adaptive_regime_stake_var.get().strip()
        try:
            stake = float(raw)
        except ValueError:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số lớn hơn 0.")
            return
        if not math.isfinite(stake) or stake <= 0:
            messagebox.showerror("Stake chưa hợp lệ", "Stake phải là một số hữu hạn lớn hơn 0.")
            return
        updated_config = replace(self.config, adaptive_regime_stake=stake)
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được stake", str(exc))
            return
        self.config = updated_config
        self.adaptive_regime_status_var.set(
            f"Đã lưu stake {_format_number(stake)} điểm; chỉ áp dụng cho lệnh mới."
        )

    def _on_adaptive_regime_window_selection_changed(self) -> None:
        selected_windows = self._selected_adaptive_regime_windows()
        updated_config = replace(
            self.config,
            adaptive_regime_selected_windows=selected_windows,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được khung giờ Đa Cầu", str(exc))
            return
        self.config = updated_config
        self._refresh_adaptive_regime_tab()

    def _on_adaptive_regime_autobet_changed(self) -> None:
        enabled = bool(self.adaptive_regime_autobet_var.get())
        if enabled:
            confirm = messagebox.askyesno(
                "Xác nhận BẬT Auto-Bet Đa Cầu Thích Ứng",
                "⚠ CẢNH BÁO CƯỢC THẬT:\n\n"
                "Khi đến khung giờ đã chọn, nếu có bàn đạt chiến thuật Đa Cầu Thích Ứng (1-1, 2-2, Bẻ bệt C4, Bệt muộn), "
                "tool sẽ tự động đặt cược bằng TIỀN THẬT trên sảnh AE Sexy.\n\n"
                "Bạn có chắc chắn muốn BẬT tính năng này?",
            )
            if not confirm:
                self.adaptive_regime_autobet_var.set(False)
                return
        updated_config = replace(
            self.config,
            adaptive_regime_autobet_enabled=enabled,
        )
        try:
            save_config(updated_config)
        except Exception as exc:
            messagebox.showerror("Không lưu được cấu hình Auto-Bet Đa Cầu", str(exc))
            return
        self.config = updated_config
        state_text = "ĐÃ BẬT" if enabled else "ĐÃ TẮT"
        msg = f"⚙ Chế độ Live Auto-Bet cho Đa Cầu Thích Ứng: {state_text}."
        self._append_live_log(msg)
        if hasattr(self, "adaptive_regime_status_var"):
            self.adaptive_regime_status_var.set(msg)

    def _on_adaptive_regime_bet_mode_changed(self, *_args: Any) -> None:
        label = self.adaptive_regime_bet_mode_var.get()
        mode = _label_to_bet_mode(label)
        if mode != getattr(self.config, "adaptive_regime_bet_mode", "forward"):
            updated_config = replace(self.config, adaptive_regime_bet_mode=mode)
            try:
                save_config(updated_config)
                self.config = updated_config
                self._append_live_log(f"⚙ [Auto-Bet Adaptive Regime] Chiều đánh thật: {label} ({mode})")
            except Exception as exc:
                self._append_live_log(f"⚠ [Auto-Bet Adaptive Regime] Không lưu được cấu hình: {exc}")

    def _ensure_adaptive_regime_slot_cache(self, session_date: str) -> None:
        if self._adaptive_regime_slot_cache_date == session_date:
            return
        self._adaptive_regime_consumed_slots = self.store.adaptive_regime_hourly_slot_keys(session_date)
        self._adaptive_regime_slot_cache_date = session_date

    def _adaptive_regime_candidates(
        self,
        remaining_seconds: dict[str, float] | None = None,
    ) -> list[StrategySignal]:
        return _rank_adaptive_regime_candidates(
            self.engine.latest_signals,
            self.engine.snapshots,
            remaining_seconds if remaining_seconds is not None else self._daily_remaining_seconds(),
            stale_seconds=self.config.live_table_stale_seconds,
            banker_min_prob=ADAPTIVE_REGIME_BANKER_MIN_ML,
            player_min_prob=ADAPTIVE_REGIME_PLAYER_MIN_ML,
        )

    def _refresh_adaptive_regime_tab(self, *, force_today: bool = False) -> None:
        if not hasattr(self, "adaptive_regime_tree"):
            return
        if not force_today and not self._adaptive_regime_tab_visible():
            return

        now = datetime.now(BANGKOK_TIMEZONE)
        session_date = now.date().isoformat()
        selected_windows = self._selected_adaptive_regime_windows()
        active_window = _active_daily_experiment_window(now, selected_windows)
        try:
            self._ensure_adaptive_regime_slot_cache(session_date)
        except Exception as exc:
            self.adaptive_regime_status_var.set(f"Không tải được trạng thái khung giờ: {exc}")
            return

        pending = self._adaptive_regime_pending
        slot_used = bool(
            active_window
            and (session_date, active_window) in self._adaptive_regime_consumed_slots
        )
        remaining_seconds = self._daily_remaining_seconds()
        candidates: list[StrategySignal] = []
        if active_window and pending is None and not slot_used:
            candidates = [
                signal
                for signal in self._adaptive_regime_candidates(remaining_seconds)
                if _signal_created_in_active_window(signal, now, active_window)
            ]

        if pending is not None:
            self.adaptive_regime_status_var.set(
                f"Đang chờ settle lệnh {pending['session_window']} tại {pending['table_name']}; "
                "W, L hoặc Tie đều kết thúc lượt của khung giờ đó."
            )
        elif not selected_windows:
            self.adaptive_regime_status_var.set(
                "Chưa tick khung giờ nào; không tạo dự đoán Đa Cầu mới."
            )
        elif active_window is None:
            self.adaptive_regime_status_var.set(
                f"{_daily_window_selection_summary(selected_windows)}; hiện ngoài khung đã tick."
            )
        elif slot_used:
            self.adaptive_regime_status_var.set(
                f"Khung {active_window} đã dùng đủ 1 dự đoán; chờ khung được tick tiếp theo."
            )
        else:
            self.adaptive_regime_status_var.set(
                f"Khung {active_window} đang mở; chờ Đa Cầu (B >=57%, P >=52.5%) và còn ít nhất 10 giây."
            )

        if candidates:
            signal = candidates[0]
            shoe, round_no = _signal_shoe_round(signal.round_fingerprint)
            seconds = remaining_seconds.get(signal.table_name, 0.0)
            probability = float(signal.features.get("ml_probability_win", signal.confidence))
            pattern = signal.features.get("road_pattern", "-")
            self.adaptive_regime_candidate_var.set(
                f"Ứng viên hiện tại: {signal.table_name} | shoe {shoe} | round {round_no} | "
                f"{signal.side.vi_label if signal.side else '-'} [{pattern}] | ML {probability:.1%} | còn {seconds:.1f}s."
            )
        else:
            self.adaptive_regime_candidate_var.set("Ứng viên hiện tại: chưa có dự đoán đủ điều kiện.")

        if force_today or self._adaptive_regime_today_dirty:
            try:
                rows = self.store.adaptive_regime_hourly_rows(
                    session_date,
                    limit=ADAPTIVE_REGIME_HISTORY_ROW_LIMIT,
                )
                summary = self.store.adaptive_regime_hourly_summary(session_date)
            except Exception as exc:
                self.adaptive_regime_summary_var.set(f"Không tải được dữ liệu hôm nay: {exc}")
                return
            self._populate_adaptive_regime_tree(self.adaptive_regime_tree, rows)
            self.adaptive_regime_summary_var.set(_adaptive_regime_summary_label(summary, len(rows)))
            self._adaptive_regime_today_dirty = False

        if (
            hasattr(self, "adaptive_regime_notebook")
            and self.adaptive_regime_notebook.select() == str(self.adaptive_regime_history_frame)
        ):
            self._refresh_adaptive_regime_history()

    def _refresh_adaptive_regime_history(self, *, force: bool = False) -> None:
        selected_date_value = (
            self.adaptive_regime_history_date_var.get().strip() or DAILY_HISTORY_ALL
        )
        selected_window_value = (
            self.adaptive_regime_history_window_var.get().strip() or DAILY_HISTORY_ALL
        )
        requested_key = (
            None if selected_date_value == DAILY_HISTORY_ALL else selected_date_value,
            None if selected_window_value == DAILY_HISTORY_ALL else selected_window_value,
        )
        if (
            not force
            and not self._adaptive_regime_history_dirty
            and self._adaptive_regime_history_loaded_key == requested_key
        ):
            return
        try:
            date_values = (DAILY_HISTORY_ALL, *self.store.adaptive_regime_hourly_dates())
            self.adaptive_regime_history_date_combo.configure(values=date_values)
            selected_date = selected_date_value
            if selected_date not in date_values:
                selected_date = DAILY_HISTORY_ALL
                self.adaptive_regime_history_date_var.set(selected_date)
            selected_window = selected_window_value
            session_date = None if selected_date == DAILY_HISTORY_ALL else selected_date
            session_window = None if selected_window == DAILY_HISTORY_ALL else selected_window
            cache_key = (session_date, session_window)
            rows = self.store.adaptive_regime_hourly_rows(
                session_date,
                session_window,
                limit=ADAPTIVE_REGIME_HISTORY_ROW_LIMIT,
            )
            summary = self.store.adaptive_regime_hourly_summary(session_date, session_window)
        except Exception as exc:
            self.adaptive_regime_history_summary_var.set(
                f"Không tải được lịch sử Đa Cầu: {exc}"
            )
            return

        self._populate_adaptive_regime_tree(self.adaptive_regime_history_tree, rows)
        self.adaptive_regime_history_summary_var.set(
            _adaptive_regime_summary_label(summary, len(rows))
        )
        self._adaptive_regime_history_dirty = False
        self._adaptive_regime_history_loaded_key = cache_key

    def _populate_adaptive_regime_tree(self, tree: ttk.Treeview, rows: list[Any]) -> None:
        tree.delete(*tree.get_children())
        for row in rows:
            shoe, round_no = _signal_shoe_round(str(row["signal_fingerprint"]))
            settled = str(row["status"]) == "settled"
            tree.insert(
                "",
                "end",
                values=(
                    row["session_date"],
                    row["session_window"],
                    row["table_name"],
                    shoe,
                    round_no,
                    _daily_side_label(str(row["side"])),
                    f"{float(row['confidence']):.1%}",
                    _format_number(float(row["stake"])),
                    row["result"] or "-",
                    f"{float(row['pnl'] or 0):+.2f}" if settled else "-",
                    "Đã settle" if settled else "Đang chờ",
                ),
            )

    def _auto_arm_adaptive_regime_hourly(self) -> None:
        if not hasattr(self, "store"):
            return
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(
            now,
            self._selected_adaptive_regime_windows(),
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_adaptive_regime_hourly_bets"):
            settled = self.store.settle_stale_adaptive_regime_hourly_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._adaptive_regime_today_dirty = True
                self._adaptive_regime_history_dirty = True
        self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
        if self._adaptive_regime_pending is not None:
            return
        session_date = now.date().isoformat()
        try:
            self._ensure_adaptive_regime_slot_cache(session_date)
        except Exception as exc:
            if hasattr(self, "adaptive_regime_status_var"):
                self.adaptive_regime_status_var.set(f"Không kiểm tra được khung giờ Đa Cầu: {exc}")
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
        try:
            stake = float(self.adaptive_regime_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(getattr(self.config, "adaptive_regime_stake", 10.0))
        if stake <= 0:
            return
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
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
        self._adaptive_regime_today_dirty = True
        self._adaptive_regime_history_dirty = True

        order_key = f"ar|{session_date}|{active_window}|{signal.table_name}|{signal.round_fingerprint}"
        new_orders: list[BetOrder] = []
        autobet_var = getattr(self, "adaptive_regime_autobet_var", None)
        autobet_enabled = bool(autobet_var.get()) if autobet_var else False
        armed_today = getattr(self, "_adaptive_regime_autobet_armed_today", None)
        if armed_today is None:
            armed_today = set()
            self._adaptive_regime_autobet_armed_today = armed_today

        if autobet_enabled and order_key not in armed_today:
            armed_today.add(order_key)
            snapshots = getattr(self.engine, "snapshots", {}) if hasattr(self, "engine") and self.engine else {}
            snap = snapshots.get(signal.table_name)
            target_round, target_shoe = extract_target_round_and_shoe(
                fingerprint=signal.round_fingerprint,
                current_round_no=snap.current_round_no if snap else None,
                current_shoe=snap.shoe if snap else None,
            )
            new_orders.append(
                BetOrder(
                    table_name=signal.table_name,
                    side=resolve_bet_side(
                        signal.side.value if signal.side else "",
                        getattr(self.config, "adaptive_regime_bet_mode", "forward"),
                    ),
                    stake=stake,
                    session_window=active_window,
                    order_id=order_key,
                    target_round_no=target_round,
                    target_shoe=target_shoe,
                    signal_fingerprint=signal.round_fingerprint,
                    signal_created_at=signal.created_at,
                    countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
                )
            )

        autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and autobet_enabled) else ""
        if hasattr(self, "adaptive_regime_status_var"):
            self.adaptive_regime_status_var.set(
                f"Đã ghi 1 lệnh paper Đa Cầu cho {active_window} tại {signal.table_name}{autobet_msg}; chờ settle."
            )
        if new_orders and autobet_enabled:
            self._dispatch_live_autobet(new_orders, source="adaptive_regime")

    def _settle_adaptive_regime_hourly(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_adaptive_regime_hourly_bets"):
                settled_ids = self.store.settle_stale_adaptive_regime_hourly_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._adaptive_regime_pending = self.store.pending_adaptive_regime_hourly_row()
                    self._adaptive_regime_today_dirty = True
                    self._adaptive_regime_history_dirty = True
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
        if settled:
            self._adaptive_regime_pending = None
            self._adaptive_regime_today_dirty = True
            self._adaptive_regime_history_dirty = True
            if hasattr(self, "auto_bettor") and self.auto_bettor:
                self.auto_bettor.return_to_lobby_background()

    def _refresh_daily_tab(self, scores: list[Any] | None = None) -> None:
        self.daily_tree.delete(*self.daily_tree.get_children())
        now = datetime.now().astimezone()
        session_date = now.date().isoformat()
        selected_windows = self._selected_daily_windows()
        active_window = _active_daily_experiment_window(now, selected_windows)
        today_rows = self.store.daily_experiment_rows(session_date)
        pending_row = self.store.pending_daily_experiment_row()
        if scores is None:
            scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        self._daily_candidates = _rank_daily_candidates(
            scores,
            self.engine.snapshots,
            self._daily_remaining_seconds(),
        )
        if active_window:
            self._daily_candidates = [
                score
                for score in self._daily_candidates
                if _signal_created_in_active_window(score.best_signal, now, active_window)
            ]

        index = 1
        for row in today_rows:
            self.daily_tree.insert(
                "",
                "end",
                values=(
                    index,
                    row["session_window"],
                    row["table_name"],
                    row["strategy_id"],
                    _daily_side_label(str(row["side"])),
                    f"{float(row['confidence']):.1%}",
                    _daily_round_label(str(row["signal_fingerprint"])),
                    _format_number(float(row["stake"])),
                    row["result"] or "-",
                    f"{float(row['pnl']):+.2f}" if row["status"] == "settled" else "-",
                    "Đang chờ settle" if row["status"] == "pending" else "Đã settle",
                ),
            )
            index += 1

        preview_limit = 0 if pending_row is not None else 1
        window_has_won = False
        if active_window:
            window_rows = [row for row in today_rows if str(row["session_window"]) == active_window]
            recorded_in_window = len(window_rows)
            window_has_won = any(str(r["result"] or "") == "W" for r in window_rows)
            if recorded_in_window >= DAILY_EXPERIMENT_MAX_PER_WINDOW:
                preview_limit = 0
            elif self._is_daily_stop_win_enabled() and window_has_won:
                preview_limit = 0
        previewed = 0
        for score in self._daily_candidates:
            if previewed >= preview_limit:
                continue
            signal = score.best_signal
            if signal is None or signal.side is None:
                continue
            self.daily_tree.insert(
                "",
                "end",
                values=(
                    index,
                    active_window or "Xem trước",
                    score.table_name,
                    signal.strategy_id,
                    signal.side.vi_label,
                    f"{float(signal.features.get('ml_probability_win', 0)):.1%}",
                    score.current_round_no,
                    self.daily_stake_var.get().strip() or "-",
                    "-",
                    "-",
                    "Ứng viên" if active_window else "Ngoài khung đã chọn",
                ),
            )
            index += 1
            previewed += 1

        total_pnl = sum(float(row["pnl"] or 0) for row in today_rows if row["status"] == "settled")
        if active_window:
            recorded = sum(str(row["session_window"]) == active_window for row in today_rows)
            waiting = " | đang chờ 1 lệnh settle" if pending_row is not None else ""
            stop_win_note = ""
            if self._is_daily_stop_win_enabled() and window_has_won:
                stop_win_note = " | [Stop Win: Đã Thắng, dừng vào thêm lệnh]"
            self.daily_status_var.set(
                f"Phiên {active_window}: đã ghi {recorded}/{DAILY_EXPERIMENT_MAX_PER_WINDOW} lệnh{waiting}{stop_win_note} | "
                f"hôm nay {len(today_rows)} lệnh, P&L {total_pnl:+.2f}."
            )
        elif not selected_windows:
            self.daily_status_var.set(
                f"Chưa tick khung giờ nào: không tạo lệnh Paper mới | "
                f"hôm nay {len(today_rows)} lệnh, P&L {total_pnl:+.2f}."
            )
        else:
            self.daily_status_var.set(
                f"{_daily_window_selection_summary(selected_windows)}; hiện ngoài khung đã chọn | "
                f"hôm nay {len(today_rows)} lệnh, "
                f"P&L {total_pnl:+.2f}."
            )

    def _auto_arm_daily_experiment(self) -> None:
        if not hasattr(self, "store"):
            return
        now = datetime.now().astimezone()
        active_window = _active_daily_experiment_window(
            now,
            self._selected_daily_windows(),
        )
        if active_window is None:
            return
        if hasattr(self.store, "settle_stale_daily_experiment_bets"):
            settled = self.store.settle_stale_daily_experiment_bets(
                banker_commission=self.config.money.banker_commission,
            )
            if settled:
                self._daily_history_dirty = True
        if self.store.pending_daily_experiment_row() is not None:
            return
        session_date = now.date().isoformat()
        today_rows = self.store.daily_experiment_rows(session_date)
        window_rows = [row for row in today_rows if str(row["session_window"]) == active_window]
        recorded_in_window = len(window_rows)
        if recorded_in_window >= DAILY_EXPERIMENT_MAX_PER_WINDOW:
            return
        stop_win_active = self._is_daily_stop_win_enabled()
        if stop_win_active and any(str(r["result"] or "") == "W" for r in window_rows):
            self.daily_status_var.set(
                f"Phiên {active_window}: Đã Thắng lệnh trong khung giờ. Stop Win kích hoạt - dừng vào thêm lệnh."
            )
            return
        remaining_seconds = self._daily_remaining_seconds()
        candidates = _rank_daily_candidates(
            _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds),
            self.engine.snapshots,
            remaining_seconds,
        )
        inserted = 0
        new_orders: list[BetOrder] = []
        for score in candidates:
            if recorded_in_window + inserted >= DAILY_EXPERIMENT_MAX_PER_WINDOW:
                break
            signal = score.best_signal
            if signal is None or signal.side is None:
                continue
            if not _signal_created_in_active_window(signal, now, active_window):
                continue
            try:
                stake = float(self.daily_stake_var.get().strip())
            except ValueError:
                return
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
                stop_win_enabled=stop_win_active,
            )
            if saved:
                inserted += 1
                order_key = f"{session_date}|{active_window}|{score.table_name}|{signal.round_fingerprint}"
                if order_key not in self._autobet_armed_today:
                    self._autobet_armed_today.add(order_key)
                    snap = self.engine.snapshots.get(score.table_name)
                    target_round, target_shoe = extract_target_round_and_shoe(
                        fingerprint=signal.round_fingerprint,
                        current_round_no=snap.current_round_no if snap else None,
                        current_shoe=snap.shoe if snap else None,
                    )
                    actual_side = resolve_bet_side(
                        signal.side.value,
                        getattr(self.config, "daily_bet_mode", "forward"),
                    )
                    new_orders.append(
                        BetOrder(
                            table_name=score.table_name,
                            side=actual_side,
                            stake=stake,
                            session_window=active_window,
                            order_id=order_key,
                            target_round_no=target_round,
                            target_shoe=target_shoe,
                            signal_fingerprint=signal.round_fingerprint,
                            signal_created_at=signal.created_at,
                            countdown_at_signal=remaining_seconds.get(score.table_name),
                        )
                    )
                break
        if inserted:
            self._daily_history_dirty = True
            autobet_msg = " [Auto-Bet Live đang chạy]" if (new_orders and self.daily_autobet_var.get()) else ""
            self.daily_status_var.set(
                f"Phiên {active_window}: đã tự động ghi {inserted} lệnh paper{autobet_msg}; "
                "chờ round kế tiếp để settle."
            )
            if new_orders and self.daily_autobet_var.get():
                self._dispatch_live_autobet(new_orders)

    def _settle_daily_experiment(self, table_name: str | None = None) -> None:
        if table_name is None:
            if hasattr(self.store, "settle_stale_daily_experiment_bets"):
                settled_ids = self.store.settle_stale_daily_experiment_bets(
                    banker_commission=self.config.money.banker_commission,
                )
                if settled_ids:
                    self._daily_history_dirty = True
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
            settled = self.store.settle_daily_experiment_bet(
                bet_id=int(row["id"]),
                settled_at=str(result_event["observed_at"]),
                outcome=outcome,
                result=result,
                pnl=pnl,
            )
            if settled:
                self._daily_history_dirty = True
                if hasattr(self, "auto_bettor") and self.auto_bettor:
                    self.auto_bettor.return_to_lobby_background()
        elif self.store.is_shoe_finished_after_signal(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        ):
            settled = self.store.settle_daily_experiment_bet(
                bet_id=int(row["id"]),
                settled_at=utc_now_iso_ms(),
                outcome="VOID",
                result="T",
                pnl=0.0,
            )
            if settled:
                self._daily_history_dirty = True
                if hasattr(self, "auto_bettor") and self.auto_bettor:
                    self.auto_bettor.return_to_lobby_background()

    def _dispatch_live_autobet(self, orders: list[BetOrder], *, source: str = "daily") -> None:
        if not orders:
            return
        orders = prepare_bet_orders(orders, source=source)

        def persist_audit(event: dict[str, Any]) -> None:
            self.store.enqueue_autobet_audit(event)
            if hasattr(self, "root"):
                self.root.after(0, self._mark_autobet_audit_dirty)

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
            self._append_live_log("⚠ [Auto-Bet] Đang có phiên đặt cược chạy nền. Bỏ qua yêu cầu mới.")
            return

        order_desc = ", ".join(
            f"{o.table_name}: {'Con (Player)' if normalize_bet_side(o.side) == 'PLAYER' else 'Cái (Banker)'} {o.stake:g} điểm"
            for o in orders
        )
        if source == "run_length":
            tag = "[Auto-Bet Run Length]"
        elif source == "ensemble_majority":
            tag = "[Auto-Bet Ensemble Majority]"
        elif source == "adaptive_regime":
            tag = "[Auto-Bet Đa Cầu]"
        else:
            tag = "[Auto-Bet]"
        self._append_live_log(f"🚀 {tag} Bắt đầu tự động đánh {len(orders)} lệnh ({order_desc})...")
        if source == "run_length" and hasattr(self, "run_length_status_var"):
            self.run_length_status_var.set(f"Đang tự động đánh {len(orders)} bàn trên sảnh AE Sexy...")
        elif source == "ensemble_majority" and hasattr(self, "ensemble_majority_status_var"):
            self.ensemble_majority_status_var.set(f"Đang tự động đánh {len(orders)} bàn trên sảnh AE Sexy...")
        elif source == "adaptive_regime" and hasattr(self, "adaptive_regime_status_var"):
            self.adaptive_regime_status_var.set(f"Đang tự động đánh {len(orders)} bàn trên sảnh AE Sexy...")
        elif hasattr(self, "daily_status_var"):
            self.daily_status_var.set(f"Đang tự động đánh {len(orders)} bàn trên sảnh AE Sexy...")

        def on_status(msg: str) -> None:
            self.root.after(0, lambda: self._append_live_log(f"{tag} {msg}"))
            if source == "run_length" and hasattr(self, "run_length_status_var"):
                self.root.after(0, lambda: self.run_length_status_var.set(msg))
            elif source == "ensemble_majority" and hasattr(self, "ensemble_majority_status_var"):
                self.root.after(0, lambda: self.ensemble_majority_status_var.set(msg))
            elif source == "adaptive_regime" and hasattr(self, "adaptive_regime_status_var"):
                self.root.after(0, lambda: self.adaptive_regime_status_var.set(msg))
            elif hasattr(self, "daily_status_var"):
                self.root.after(0, lambda: self.daily_status_var.set(msg))

        def on_order_done(result: BetResult) -> None:
            status_sym = "✅" if result.reason_code == "PROVIDER_ACCEPTED" else "⚠" if result.reason_code == "ACK_TIMEOUT" else "❌"
            self.root.after(
                0,
                lambda: self._append_live_log(
                    f"{status_sym} {tag} Bàn {result.order.table_name}: {result.message}"
                ),
            )

        def on_finished(results: list[BetResult]) -> None:
            confirmed_clicks = sum(1 for result in results if result.confirm_clicked_at)
            provider_accepted = sum(1 for result in results if result.reason_code == "PROVIDER_ACCEPTED")
            summary = (
                f"Hoàn tất Auto-Bet: đã click Xác nhận {confirmed_clicks}/{len(results)} bàn; "
                f"nhà cung cấp xác nhận {provider_accepted}; xem tab Auto-Bet Audit để đối soát."
            )
            self.root.after(0, lambda: self._append_live_log(f"🏁 {tag} {summary}"))
            if source == "run_length":
                if hasattr(self, "run_length_status_var"):
                    self.root.after(0, lambda: self.run_length_status_var.set(summary))
                self.root.after(0, lambda: self._refresh_run_length_tab(force_today=True))
            elif source == "ensemble_majority":
                if hasattr(self, "ensemble_majority_status_var"):
                    self.root.after(0, lambda: self.ensemble_majority_status_var.set(summary))
                self.root.after(0, lambda: self._refresh_ensemble_majority_tab(force_today=True))
            elif source == "adaptive_regime":
                if hasattr(self, "adaptive_regime_status_var"):
                    self.root.after(0, lambda: self.adaptive_regime_status_var.set(summary))
                self.root.after(0, lambda: self._refresh_adaptive_regime_tab(force_today=True))
            else:
                if hasattr(self, "daily_status_var"):
                    self.root.after(0, lambda: self.daily_status_var.set(summary))
                self.root.after(0, lambda: self._refresh_daily_tab())

        self.auto_bettor.cdp_url = self.cdp_var.get().strip() or self.config.cdp_url
        self.auto_bettor.execute_orders_background(
            orders=orders,
            on_status=on_status,
            on_order_done=on_order_done,
            on_finished=on_finished,
            on_audit=persist_audit,
        )


    def _manual_trigger_autobet(self) -> None:
        """Manually trigger auto-bet on current top candidate tables."""
        now = datetime.now().astimezone()
        active_window = _active_daily_experiment_window(now, self._selected_daily_windows()) or "Thủ công"
        session_date = now.date().isoformat()
        if active_window != "Thủ công" and self._is_daily_stop_win_enabled():
            if self.store.daily_window_has_won(session_date, active_window):
                messagebox.showinfo(
                    "Stop Win",
                    f"Khung giờ {active_window} đã có lệnh Thắng (Stop Win đang bật).\n"
                    "Không vào thêm lệnh trong khung giờ này.",
                )
                return
        try:
            stake = float(self.daily_stake_var.get().strip())
        except ValueError:
            messagebox.showerror("Lỗi", "Số tiền stake không hợp lệ (phải là số, ví dụ: 10 = 10 điểm).")
            return
        if stake <= 0:
            messagebox.showerror("Lỗi", "Số tiền stake phải lớn hơn 0.")
            return

        # Refresh candidates
        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        remaining_seconds = self._daily_remaining_seconds()
        candidates = _rank_daily_candidates(
            scores,
            self.engine.snapshots,
            remaining_seconds,
        )
        if not candidates:
            # Fallback to any active tables with valid signals
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
            messagebox.showinfo(
                "Thông báo",
                "Hiện chưa có bàn nào có tín hiệu cược trên sảnh.\n"
                "Vui lòng đảm bảo trình duyệt đã mở sảnh AE Sexy và CDP đang đồng bộ."
            )
            return

        orders: list[BetOrder] = []
        for score in candidates[:DAILY_EXPERIMENT_MAX_PER_WINDOW]:
            signal = score.best_signal
            if signal and signal.side:
                snap = self.engine.snapshots.get(score.table_name)
                target_round, target_shoe = extract_target_round_and_shoe(
                    fingerprint=signal.round_fingerprint,
                    current_round_no=snap.current_round_no if snap else None,
                    current_shoe=snap.shoe if snap else None,
                )
                actual_side = resolve_bet_side(
                    signal.side.value,
                    getattr(self.config, "daily_bet_mode", "forward"),
                )
                orders.append(
                    BetOrder(
                        table_name=score.table_name,
                        side=actual_side,
                        stake=stake,
                        session_window=active_window,
                        order_id=f"manual-{score.table_name}-{utc_now_iso_ms()}",
                        target_round_no=target_round,
                        target_shoe=target_shoe,
                        signal_fingerprint=signal.round_fingerprint,
                        signal_created_at=signal.created_at,
                        countdown_at_signal=remaining_seconds.get(score.table_name),
                    )
                )

        if not orders:
            messagebox.showinfo("Thông báo", "Không tìm thấy tín hiệu đặt cược hợp lệ.")
            return

        mode_label = self.daily_bet_mode_var.get()
        confirm = messagebox.askyesno(
            f"Xác nhận Auto-Bet ({mode_label})",
            f"Bạn có chắc chắn muốn đặt cược thật ({mode_label}) {len(orders)} bàn:\n\n"
            + "\n".join(
                f"- {o.table_name}: {'Con (Player)' if normalize_bet_side(o.side) == 'PLAYER' else 'Cái (Banker)'} {o.stake:g} điểm (Ván {o.target_round_no if o.target_round_no else '?'})"
                for o in orders
            )
            + "\n\nTool sẽ tự động chọn bàn, kiểm tra đúng ván cược và xác nhận trên trình duyệt.",
        )
        if not confirm:
            return

        self._dispatch_live_autobet(orders)

    def _manual_trigger_run_length_autobet(self) -> None:
        """Manually trigger auto-bet for the top Run Length >=58% candidate."""
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(now, self._selected_run_length_windows()) or "Thủ công"
        try:
            stake = float(self.run_length_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(self.config.run_length_stake)
        if stake <= 0:
            messagebox.showerror("Lỗi", "Số tiền stake phải lớn hơn 0.")
            return

        candidates = self._run_length_candidates()
        if not candidates:
            messagebox.showinfo(
                "Thông báo",
                "Hiện chưa có bàn nào có tín hiệu Run Length với ML >=58% và còn thời gian cược.\n"
                "Vui lòng đảm bảo trình duyệt đã mở sảnh AE Sexy và CDP đang đồng bộ."
            )
            return

        signal = candidates[0]
        if not signal or not signal.side:
            messagebox.showinfo("Thông báo", "Không tìm thấy tín hiệu đặt cược hợp lệ.")
            return

        snap = self.engine.snapshots.get(signal.table_name)
        target_round, target_shoe = extract_target_round_and_shoe(
            fingerprint=signal.round_fingerprint,
            current_round_no=snap.current_round_no if snap else None,
            current_shoe=snap.shoe if snap else None,
        )
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
        actual_side = resolve_bet_side(
            signal.side.value,
            getattr(self.config, "run_length_bet_mode", "forward"),
        )
        order = BetOrder(
            table_name=signal.table_name,
            side=actual_side,
            stake=stake,
            session_window=active_window,
            order_id=f"manual-rl-{signal.table_name}-{utc_now_iso_ms()}",
            target_round_no=target_round,
            target_shoe=target_shoe,
            signal_fingerprint=signal.round_fingerprint,
            signal_created_at=signal.created_at,
            countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
        )

        side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
        mode_label = self.run_length_bet_mode_var.get()
        confirm = messagebox.askyesno(
            f"Xác nhận Auto-Bet Run Length ({mode_label})",
            f"Bạn có chắc chắn muốn đặt cược thật ({mode_label}) ứng viên Run Length:\n\n"
            f"- Bàn: {order.table_name}\n"
            f"- Cửa: {side_str}\n"
            f"- Tỷ lệ ML: {prob*100:.1f}%\n"
            f"- Stake: {order.stake:g} điểm (Ván {order.target_round_no if order.target_round_no else '?'})\n\n"
            "Tool sẽ tự động chọn bàn, kiểm tra đúng ván cược và xác nhận trên trình duyệt.",
        )
        if not confirm:
            return

        self._dispatch_live_autobet([order], source="run_length")

    def _manual_trigger_ensemble_majority_autobet(self) -> None:
        """Manually trigger auto-bet for the top Ensemble Majority candidate."""
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(now, self._selected_ensemble_majority_windows()) or "Thủ công"
        try:
            stake = float(self.ensemble_majority_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(getattr(self.config, "ensemble_majority_stake", 10.0))
        if stake <= 0:
            messagebox.showerror("Lỗi", "Số tiền stake phải lớn hơn 0.")
            return

        candidates = self._ensemble_majority_candidates()
        if not candidates:
            min_prob = float(getattr(self.config, "ensemble_majority_ml_min_probability", 0.55))
            messagebox.showinfo(
                "Thông báo",
                f"Hiện chưa có bàn nào có tín hiệu Ensemble Majority với ML >={min_prob:.0%} và còn thời gian cược.\n"
                "Vui lòng đảm bảo trình duyệt đã mở sảnh AE Sexy và CDP đang đồng bộ."
            )
            return

        signal = candidates[0]
        if not signal or not signal.side:
            messagebox.showinfo("Thông báo", "Không tìm thấy tín hiệu đặt cược hợp lệ.")
            return

        snap = self.engine.snapshots.get(signal.table_name)
        target_round, target_shoe = extract_target_round_and_shoe(
            fingerprint=signal.round_fingerprint,
            current_round_no=snap.current_round_no if snap else None,
            current_shoe=snap.shoe if snap else None,
        )
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
        actual_side = resolve_bet_side(
            signal.side.value,
            getattr(self.config, "ensemble_majority_bet_mode", "forward"),
        )
        order = BetOrder(
            table_name=signal.table_name,
            side=actual_side,
            stake=stake,
            session_window=active_window,
            order_id=f"manual-em-{signal.table_name}-{utc_now_iso_ms()}",
            target_round_no=target_round,
            target_shoe=target_shoe,
            signal_fingerprint=signal.round_fingerprint,
            signal_created_at=signal.created_at,
            countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
        )

        side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
        mode_label = self.ensemble_majority_bet_mode_var.get()
        confirm = messagebox.askyesno(
            f"Xác nhận Auto-Bet Ensemble Majority ({mode_label})",
            f"Bạn có chắc chắn muốn đặt cược thật ({mode_label}) ứng viên Ensemble Majority:\n\n"
            f"- Bàn: {order.table_name}\n"
            f"- Cửa: {side_str}\n"
            f"- Tỷ lệ ML: {prob*100:.1f}%\n"
            f"- Stake: {order.stake:g} điểm (Ván {order.target_round_no if order.target_round_no else '?'})\n\n"
            "Tool sẽ tự động chọn bàn, kiểm tra đúng ván cược và xác nhận trên trình duyệt.",
        )
        if not confirm:
            return

        self._dispatch_live_autobet([order], source="ensemble_majority")

    def _manual_trigger_adaptive_regime_autobet(self) -> None:
        """Manually trigger auto-bet for the top Adaptive Regime candidate."""
        now = datetime.now(BANGKOK_TIMEZONE)
        active_window = _active_daily_experiment_window(now, self._selected_adaptive_regime_windows()) or "Thủ công"
        try:
            stake = float(self.adaptive_regime_stake_var.get().strip())
        except (ValueError, AttributeError):
            stake = float(getattr(self.config, "adaptive_regime_stake", 10.0))
        if stake <= 0:
            messagebox.showerror("Lỗi", "Số tiền stake phải lớn hơn 0.")
            return

        candidates = self._adaptive_regime_candidates()
        if not candidates:
            messagebox.showinfo(
                "Thông báo",
                "Hiện chưa có bàn nào có tín hiệu Đa Cầu Thích Ứng đạt chuẩn (Banker >=57%, Player >=52.5%) và còn thời gian cược.\n"
                "Vui lòng đảm bảo trình duyệt đã mở sảnh AE Sexy và CDP đang đồng bộ."
            )
            return

        signal = candidates[0]
        if not signal or not signal.side:
            messagebox.showinfo("Thông báo", "Không tìm thấy tín hiệu đặt cược hợp lệ.")
            return

        snap = self.engine.snapshots.get(signal.table_name)
        target_round, target_shoe = extract_target_round_and_shoe(
            fingerprint=signal.round_fingerprint,
            current_round_no=snap.current_round_no if snap else None,
            current_shoe=snap.shoe if snap else None,
        )
        prob = float(signal.features.get("ml_probability_win", signal.confidence))
        pattern = signal.features.get("road_pattern", "")
        actual_side = resolve_bet_side(
            signal.side.value,
            getattr(self.config, "adaptive_regime_bet_mode", "forward"),
        )
        order = BetOrder(
            table_name=signal.table_name,
            side=actual_side,
            stake=stake,
            session_window=active_window,
            order_id=f"manual-ar-{signal.table_name}-{utc_now_iso_ms()}",
            target_round_no=target_round,
            target_shoe=target_shoe,
            signal_fingerprint=signal.round_fingerprint,
            signal_created_at=signal.created_at,
            countdown_at_signal=self._daily_remaining_seconds().get(signal.table_name),
        )

        side_str = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
        pattern_str = f" [{pattern}]" if pattern else ""
        mode_label = self.adaptive_regime_bet_mode_var.get()
        confirm = messagebox.askyesno(
            f"Xác nhận Auto-Bet Đa Cầu Thích Ứng ({mode_label})",
            f"Bạn có chắc chắn muốn đặt cược thật ({mode_label}) ứng viên Đa Cầu Thích Ứng:\n\n"
            f"- Bàn: {order.table_name}\n"
            f"- Cửa: {side_str}{pattern_str}\n"
            f"- Tỷ lệ ML: {prob*100:.1f}%\n"
            f"- Stake: {order.stake:g} điểm (Ván {order.target_round_no if order.target_round_no else '?'})\n\n"
            "Tool sẽ tự động chọn bàn, kiểm tra đúng ván cược và xác nhận trên trình duyệt.",
        )
        if not confirm:
            return

        self._dispatch_live_autobet([order], source="adaptive_regime")

    def _refresh_x3_sim_tree(self) -> None:
        if not hasattr(self, "x3_summary_tree"):
            return
        for tree in (self.x3_summary_tree, self.x3_cycle_tree, self.x3_open_tree):
            tree.delete(*tree.get_children())

        try:
            cutoff_info = self.store.ml_pass_duplicate_cutoff()
            cutoff_value = cutoff_info.get("cutoff")
            cutoff = str(cutoff_value) if cutoff_value else None
            duplicate_groups = int(cutoff_info.get("duplicate_groups") or 0)
            rows = self.store.selected_ml_pass_rows(since=cutoff)
            report = run_x3_simulation(
                rows,
                cutoff=cutoff,
                duplicate_groups_before_cutoff=duplicate_groups,
                base_stake=10.0,
                multiplier=3.0,
                w_trigger=8,
                l_trigger=6,
                banker_commission=self.config.money.banker_commission,
            )
        except Exception as exc:
            self.x3_summary_var.set(f"Loi nap gia lap x3: {exc}")
            return

        cutoff_label = cutoff or "toan bo du lieu"
        duplicate_label = f" | bo qua {duplicate_groups} nhom ML Pass trung truoc cutoff" if duplicate_groups else ""
        self.x3_summary_var.set(
            f"{report.rows_used} ML Pass sau cutoff {cutoff_label} | "
            f"{report.table_count} ban | {report.summary_all.cycles} chu ky | "
            f"P&L {report.summary_all.pnl:.2f} | "
            f"stake max {report.summary_all.max_single_stake:.2f} | "
            f"von chiu max {report.summary_all.max_capital_at_risk:.2f}"
            f"{duplicate_label}"
        )

        _insert_x3_summary(self.x3_summary_tree, "Tong", report.summary_all, tags=("total",))
        _insert_x3_summary(self.x3_summary_tree, "W8+ dao ML", report.summary_reverse)
        _insert_x3_summary(self.x3_summary_tree, "L6+ thuan ML", report.summary_follow)
        parallel = report.max_parallel_risk
        if parallel.active_cycles:
            self.x3_summary_tree.insert(
                "",
                "end",
                values=(
                    "Song song max",
                    parallel.active_cycles,
                    "-",
                    "-",
                    f"{parallel.unrealized_pnl:.2f}",
                    "-",
                    "-",
                    "-",
                    "-",
                    f"{parallel.next_stake_sum:.2f}",
                    f"{parallel.risk_plus_next_stake:.2f}",
                    f"{parallel.unrealized_pnl:.2f}",
                ),
            )

        worst_cycles = sorted(
            report.cycles,
            key=lambda cycle: (
                cycle.max_drawdown,
                -cycle.max_capital_at_risk,
                -cycle.max_stake,
                cycle.start_at,
                cycle.cycle_id,
            ),
        )
        for cycle in worst_cycles[:120]:
            self.x3_cycle_tree.insert("", "end", values=_x3_cycle_values(cycle))

        open_cycles = sorted(
            report.open_cycles,
            key=lambda cycle: (-(max(0.0, -cycle.pnl) + cycle.current_stake), cycle.start_at, cycle.cycle_id),
        )
        for cycle in open_cycles:
            self.x3_open_tree.insert("", "end", values=_x3_open_values(cycle))

    def _set_status(self, message: str) -> None:
        self.status_var.set(message)

    def _append_live_log(self, message: str) -> None:
        self.live_log.configure(state="normal")
        self.live_log.insert("end", message + "\n")
        self.live_log.see("end")
        self.live_log.configure(state="disabled")

    def _on_close(self) -> None:
        if self.monitor:
            self.monitor.stop()
        self.store.close()
        self.root.destroy()


def _insert_x3_summary(
    tree: ttk.Treeview,
    label: str,
    summary: X3Summary,
    tags: tuple[str, ...] = (),
) -> None:
    tree.insert("", "end", values=_x3_summary_values(label, summary), tags=tags)


def _x3_summary_values(label: str, summary: X3Summary) -> tuple[Any, ...]:
    return (
        label,
        summary.cycles,
        summary.closed,
        summary.open,
        f"{summary.pnl:.2f}",
        f"{summary.avg_pnl_closed:.2f}",
        summary.total_bets_closed,
        summary.pushes_closed,
        summary.max_misses_before_win,
        f"{summary.max_single_stake:.2f}",
        f"{summary.max_capital_at_risk:.2f}",
        f"{summary.max_cycle_drawdown:.2f}",
    )


def _x3_cycle_values(cycle: X3Cycle) -> tuple[Any, ...]:
    return (
        cycle.table_name,
        _x3_mode_label(cycle.mode),
        cycle.trigger,
        cycle.start_at,
        cycle.end_at or "-",
        cycle.bets,
        cycle.pushes,
        cycle.misses_before_win,
        f"{cycle.pnl:.2f}",
        f"{cycle.max_stake:.2f}",
        f"{cycle.max_capital_at_risk:.2f}",
        f"{cycle.max_drawdown:.2f}",
    )


def _x3_open_values(cycle: X3Cycle) -> tuple[Any, ...]:
    risk = max(0.0, -cycle.pnl) + cycle.current_stake
    return (
        cycle.table_name,
        _x3_mode_label(cycle.mode),
        cycle.trigger,
        cycle.start_at,
        cycle.bets,
        cycle.pushes,
        cycle.misses_before_win,
        f"{cycle.pnl:.2f}",
        f"{cycle.current_stake:.2f}",
        f"{risk:.2f}",
    )


def _x3_mode_label(mode: str) -> str:
    if mode == "reverse":
        return "Dao ML"
    if mode == "follow":
        return "Thuan ML"
    return mode


async def _noop() -> None:
    return None


def _unpack_snapshot_payload(payload: Any) -> tuple[TableSnapshot, str, float]:
    if isinstance(payload, TableSnapshot):
        return payload, payload.last_seen or utc_now_iso_ms(), time.perf_counter()
    if isinstance(payload, dict) and isinstance(payload.get("snapshot"), TableSnapshot):
        monitor_seen_at = str(payload.get("monitor_seen_at") or utc_now_iso_ms())
        monitor_seen_monotonic = payload.get("monitor_seen_monotonic")
        if not isinstance(monitor_seen_monotonic, (int, float)):
            monitor_seen_monotonic = time.perf_counter()
        return payload["snapshot"], monitor_seen_at, float(monitor_seen_monotonic)
    raise TypeError("Invalid snapshot payload.")


def _snapshot_queue_key(snapshot: TableSnapshot) -> str:
    latest = snapshot.latest_round
    table_key = str(snapshot.table_id or snapshot.table_name)
    if latest is None:
        return f"{table_key}|empty|{snapshot.last_seen}"
    shoe = latest.shoe if latest.shoe is not None else snapshot.shoe
    round_no = latest.round_no if latest.round_no is not None else snapshot.current_round_no
    road = snapshot.current_shoe_road()
    return (
        f"{table_key}|{shoe}|{round_no}|{latest.outcome.value}|"
        f"{snapshot.observed_rounds}|{snapshot.known_missing_rounds}|{road}"
    )


def _elapsed_ms(start: float, end: float) -> float:
    return max(0.0, (end - start) * 1000.0)


def _format_ms(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:.1f}"


def _latency_summary_label(rows: list[Any]) -> str:
    if not rows:
        return "Chua co mau latency. Bat CDP monitor de bat dau do queue / engine / UI refresh."
    total_values = [float(row["total_ms"] or 0) for row in rows]
    queue_values = [float(row["queue_delay_ms"] or 0) for row in rows]
    engine_values = [float(row["engine_ms"] or 0) for row in rows]
    ui_values = [float(row["ui_delay_ms"] or 0) for row in rows]
    return (
        f"{len(rows)} mau gan nhat | "
        f"total avg {_format_ms(_avg(total_values))}ms, "
        f"p50 {_format_ms(_percentile(total_values, 0.50))}ms, "
        f"p95 {_format_ms(_percentile(total_values, 0.95))}ms, "
        f"max {_format_ms(max(total_values))}ms | "
        f"queue avg {_format_ms(_avg(queue_values))}ms, "
        f"engine avg {_format_ms(_avg(engine_values))}ms, "
        f"ui avg {_format_ms(_avg(ui_values))}ms | "
        "Do tu CDP callback -> UI refresh, khong phai timestamp server casino."
    )


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    raw_rank = len(ordered) * max(0.0, min(1.0, percentile))
    rank = int(raw_rank)
    if raw_rank > rank:
        rank += 1
    index = rank - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def _format_number(value: float) -> str:
    number = float(value)
    return str(int(number)) if number.is_integer() else str(number)


def _wl_streak(values: list[str]) -> str:
    first = next((value for value in values if value in {"W", "L"}), "")
    if not first:
        return "-"
    count = 0
    for value in values:
        if value == "=":
            continue
        if value != first:
            break
        count += 1
    return f"{first}{count}"


def _format_display_signal(
    display_signal: StrategySignal | None,
    filtered_signal: StrategySignal | None = None,
) -> str:
    if display_signal is None or not display_signal.is_actionable or display_signal.side is None:
        return "Chưa có"
    return (
        f"{display_signal.side.vi_label} "
        f"{_signal_confidence_label(display_signal, filtered_signal)} - "
        f"{display_signal.strategy_id}"
    )


def _signal_confidence_label(
    display_signal: StrategySignal | None,
    filtered_signal: StrategySignal | None = None,
) -> str:
    if display_signal is None or not display_signal.is_actionable:
        return "-"
    probability = _ml_probability(filtered_signal)
    if probability is not None:
        return f"ML {probability:.1%}"
    return f"{display_signal.confidence:.1%}"


def _ml_probability(signal: StrategySignal | None) -> float | None:
    if signal is None:
        return None
    value = signal.features.get("ml_probability_win")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _active_daily_experiment_window(
    now: datetime | None = None,
    enabled_windows: tuple[str, ...] | list[str] | set[str] | None = None,
) -> str | None:
    current_time = now or datetime.now().astimezone()
    minute_of_day = current_time.hour * 60 + current_time.minute
    enabled = set(enabled_windows) if enabled_windows is not None else None
    for label, start_minute, end_minute in DAILY_EXPERIMENT_WINDOWS:
        if (
            start_minute <= minute_of_day < end_minute
            and (enabled is None or label in enabled)
        ):
            return label
    return None


def _daily_window_selection_summary(selected_windows: tuple[str, ...]) -> str:
    if not selected_windows:
        return "Chưa chọn khung giờ"
    if len(selected_windows) == len(DAILY_EXPERIMENT_WINDOW_LABELS):
        return "Đã chọn cả ngày (24 khung)"
    if len(selected_windows) <= 6:
        return f"Đã chọn {len(selected_windows)} khung: {', '.join(selected_windows)}"
    return f"Đã chọn {len(selected_windows)} khung giờ"


def _signal_created_in_active_window(
    signal: Any,
    now: datetime,
    session_window: str,
) -> bool:
    """Accept only signals created after the active hourly window opened."""
    created_at = str(getattr(signal, "created_at", "") or "")
    if not created_at:
        return False
    try:
        signal_time = _REAL_DATETIME.fromisoformat(created_at.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    if signal_time.tzinfo is None:
        signal_time = signal_time.replace(tzinfo=timezone.utc)

    window = next(
        (item for item in DAILY_EXPERIMENT_WINDOWS if item[0] == session_window),
        None,
    )
    if window is None:
        return False
    current_time = now if now.tzinfo is not None else now.replace(tzinfo=BANGKOK_TIMEZONE)
    window_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        minutes=window[1]
    )
    signal_utc = signal_time.astimezone(timezone.utc)
    return window_start.astimezone(timezone.utc) <= signal_utc <= current_time.astimezone(timezone.utc)


def _estimated_remaining_seconds(
    readings: dict[str, tuple[float, float]],
    now_monotonic: float | None = None,
    snapshots: dict[str, TableSnapshot] | None = None,
) -> dict[str, float]:
    now = time.perf_counter() if now_monotonic is None else now_monotonic
    remaining: dict[str, float] = {}
    for table_name, reading in readings.items():
        try:
            seconds, observed_monotonic = reading
            age = max(0.0, now - float(observed_monotonic))
            if age > COUNTDOWN_READING_MAX_AGE_SECONDS:
                continue
            remaining[str(table_name)] = max(0.0, float(seconds) - age)
        except (TypeError, ValueError):
            continue

    if snapshots:
        now_utc = datetime.now(timezone.utc)
        for table_name, snap in snapshots.items():
            t_name = str(table_name)
            if t_name not in remaining or remaining[t_name] <= 0.0:
                if snap and snap.last_seen and getattr(snap, "known_missing_rounds", 0) == 0:
                    last_seen_dt = _parse_iso_datetime(snap.last_seen)
                    if last_seen_dt is not None:
                        elapsed = (now_utc - last_seen_dt).total_seconds()
                        if 0.0 <= elapsed <= 17.0:
                            remaining[t_name] = max(0.0, 17.0 - elapsed)

    return remaining


def _rank_daily_candidates(
    scores: list[Any],
    snapshots: dict[str, TableSnapshot],
    remaining_seconds_by_table: dict[str, float],
) -> list[Any]:
    candidates = [
        score
        for score in scores
        if score.table_name not in EXCLUDED_TABLE_NAMES
        and score.best_signal is not None
        and score.best_signal.is_actionable
        and score.best_signal.features.get("ml_probability_win") is not None
        and score.table_name in snapshots
        and score.best_signal.round_fingerprint == snapshots[score.table_name].latest_fingerprint()
        and remaining_seconds_by_table.get(score.table_name, -1.0)
        >= DAILY_CANDIDATE_MIN_REMAINING_SECONDS
    ]
    candidates.sort(
        key=lambda score: (
            float(score.best_signal.features.get("ml_probability_win", 0)),
            score.best_signal.confidence,
            float(getattr(score, "score", 0)),
        ),
        reverse=True,
    )
    return candidates


def _rank_run_length_candidates(
    latest_signals: dict[tuple[str, str], StrategySignal],
    snapshots: dict[str, TableSnapshot],
    remaining_seconds_by_table: dict[str, float],
    *,
    stale_seconds: int,
    now: datetime | None = None,
) -> list[StrategySignal]:
    """Select run_length >=58% directly, independent of each table's best signal."""
    current_time = now or datetime.now(timezone.utc)
    candidates: list[StrategySignal] = []
    for signal in latest_signals.values():
        if signal.strategy_id != RUN_LENGTH_STRATEGY_ID:
            continue
        if signal.table_name.strip() in EXCLUDED_TABLE_NAMES:
            continue
        probability = _ml_probability(signal)
        if (
            probability is None
            or not math.isfinite(probability)
            or probability < RUN_LENGTH_ML_MIN_PROBABILITY
        ):
            continue
        snapshot = snapshots.get(signal.table_name)
        if snapshot is None or snapshot.known_missing_rounds > 0:
            continue
        if signal.round_fingerprint != snapshot.latest_fingerprint():
            continue
        if remaining_seconds_by_table.get(signal.table_name, -1.0) < DAILY_CANDIDATE_MIN_REMAINING_SECONDS:
            continue
        if stale_seconds > 0:
            last_seen = _parse_iso_datetime(snapshot.last_seen)
            if last_seen is None or (current_time - last_seen).total_seconds() > stale_seconds:
                continue

        side = signal.side
        if not signal.is_actionable:
            if not signal.reason.startswith("ML skip:"):
                continue
            try:
                side = BetSide(str(signal.features.get("strategy_side") or ""))
            except ValueError:
                continue
        if side is None:
            continue
        candidates.append(
            replace(
                signal,
                action=StrategyAction.BET,
                side=side,
                confidence=probability,
            )
        )

    candidates.sort(
        key=lambda signal: (
            -float(signal.features.get("ml_probability_win", 0)),
            -float(signal.features.get("strategy_confidence", signal.confidence)),
            signal.table_name,
        )
    )
    return candidates


def _rank_ensemble_majority_candidates(
    latest_signals: dict[tuple[str, str], StrategySignal],
    snapshots: dict[str, TableSnapshot],
    remaining_seconds_by_table: dict[str, float],
    *,
    stale_seconds: int,
    min_probability: float = ENSEMBLE_MAJORITY_ML_MIN_PROBABILITY,
    now: datetime | None = None,
) -> list[StrategySignal]:
    """Select ensemble_majority >=min_probability directly, independent of each table's best signal."""
    current_time = now or datetime.now(timezone.utc)
    candidates: list[StrategySignal] = []
    for signal in latest_signals.values():
        if signal.strategy_id != ENSEMBLE_MAJORITY_STRATEGY_ID:
            continue
        if signal.table_name.strip() in EXCLUDED_TABLE_NAMES:
            continue
        probability = _ml_probability(signal)
        if (
            probability is None
            or not math.isfinite(probability)
            or probability < min_probability
        ):
            continue
        snapshot = snapshots.get(signal.table_name)
        if snapshot is None or snapshot.known_missing_rounds > 0:
            continue
        if signal.round_fingerprint != snapshot.latest_fingerprint():
            continue
        if remaining_seconds_by_table.get(signal.table_name, -1.0) < DAILY_CANDIDATE_MIN_REMAINING_SECONDS:
            continue
        if stale_seconds > 0:
            last_seen = _parse_iso_datetime(snapshot.last_seen)
            if last_seen is None or (current_time - last_seen).total_seconds() > stale_seconds:
                continue

        side = signal.side
        if not signal.is_actionable:
            if not signal.reason.startswith("ML skip:"):
                continue
            try:
                side = BetSide(str(signal.features.get("strategy_side") or ""))
            except ValueError:
                continue
        if side is None:
            continue
        candidates.append(
            replace(
                signal,
                action=StrategyAction.BET,
                side=side,
                confidence=probability,
            )
        )

    candidates.sort(
        key=lambda signal: (
            -float(signal.features.get("ml_probability_win", 0)),
            -float(signal.features.get("strategy_confidence", signal.confidence)),
            signal.table_name,
        )
    )
    return candidates


def _ensemble_majority_summary_label(
    summary: dict[str, int | float],
    displayed_count: int,
) -> str:
    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0
    return (
        f"Ledger riêng Ensemble Majority: hiển thị {displayed_count}/{int(summary.get('total_count', 0) or 0)} lệnh | "
        f"W {wins} - L {losses} - T {int(summary.get('tie_count', 0) or 0)} | "
        f"Win rate W/L {win_rate:.2%} | "
        f"Đang chờ {int(summary.get('pending_count', 0) or 0)} | "
        f"P&L {float(summary.get('total_pnl', 0) or 0):+.2f}"
    )


def _rank_adaptive_regime_candidates(
    latest_signals: dict[tuple[str, str], StrategySignal],
    snapshots: dict[str, TableSnapshot],
    remaining_seconds_by_table: dict[str, float],
    *,
    stale_seconds: int,
    banker_min_prob: float = ADAPTIVE_REGIME_BANKER_MIN_ML,
    player_min_prob: float = ADAPTIVE_REGIME_PLAYER_MIN_ML,
    now: datetime | None = None,
) -> list[StrategySignal]:
    """Select adaptive_regime directly using dual-threshold ML filter (Banker >=57%, Player >=52.5%)."""
    current_time = now or datetime.now(timezone.utc)
    candidates: list[StrategySignal] = []
    for signal in latest_signals.values():
        if signal.strategy_id != ADAPTIVE_REGIME_STRATEGY_ID:
            continue
        if signal.table_name.strip() in EXCLUDED_TABLE_NAMES:
            continue

        side = signal.side
        if not signal.is_actionable:
            if not signal.reason.startswith("ML skip:"):
                continue
            try:
                side = BetSide(str(signal.features.get("strategy_side") or ""))
            except ValueError:
                continue
        if side is None:
            continue

        min_prob = banker_min_prob if side == BetSide.BANKER else player_min_prob
        probability = _ml_probability(signal)
        if (
            probability is None
            or not math.isfinite(probability)
            or probability < min_prob
        ):
            continue

        snapshot = snapshots.get(signal.table_name)
        if snapshot is None or snapshot.known_missing_rounds > 0:
            continue
        if signal.round_fingerprint != snapshot.latest_fingerprint():
            continue
        if remaining_seconds_by_table.get(signal.table_name, -1.0) < DAILY_CANDIDATE_MIN_REMAINING_SECONDS:
            continue
        if stale_seconds > 0:
            last_seen = _parse_iso_datetime(snapshot.last_seen)
            if last_seen is None or (current_time - last_seen).total_seconds() > stale_seconds:
                continue

        candidates.append(
            replace(
                signal,
                action=StrategyAction.BET,
                side=side,
                confidence=probability,
            )
        )

    # Ưu tiên ứng viên có độ vượt ngưỡng (margin) cao hơn, sau đó đến xác suất ML, rồi strategy confidence
    candidates.sort(
        key=lambda sig: (
            -(
                float(sig.features.get("ml_probability_win", 0))
                - (
                    banker_min_prob
                    if sig.side == BetSide.BANKER
                    else player_min_prob
                )
            ),
            -float(sig.features.get("ml_probability_win", 0)),
            -float(sig.features.get("strategy_confidence", sig.confidence)),
            sig.table_name,
        )
    )
    return candidates


def _adaptive_regime_summary_label(
    summary: dict[str, int | float],
    displayed_count: int,
) -> str:
    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0
    return (
        f"Ledger riêng Đa Cầu: hiển thị {displayed_count}/{int(summary.get('total_count', 0) or 0)} lệnh | "
        f"W {wins} - L {losses} - T {int(summary.get('tie_count', 0) or 0)} | "
        f"Win rate W/L {win_rate:.2%} | "
        f"Đang chờ {int(summary.get('pending_count', 0) or 0)} | "
        f"P&L {float(summary.get('total_pnl', 0) or 0):+.2f}"
    )


def _daily_experiment_result(
    side: str,
    outcome: str,
    stake: float,
    banker_commission: float,
) -> tuple[str, float]:
    if outcome == "T":
        return "T", 0.0
    if outcome != side:
        return "L", round(-stake, 2)
    multiplier = 1.0 - banker_commission if side == "B" else 1.0
    return "W", round(stake * multiplier, 2)


def _signal_shoe_round(signal_fingerprint: str) -> tuple[str, str]:
    parts = signal_fingerprint.split("|")
    if len(parts) < 4:
        return "-", "-"
    return parts[-3], parts[-2]


def _run_length_summary_label(
    summary: dict[str, int | float],
    displayed_count: int,
) -> str:
    wins = int(summary.get("win_count", 0) or 0)
    losses = int(summary.get("loss_count", 0) or 0)
    decisions = wins + losses
    win_rate = wins / decisions if decisions else 0.0
    return (
        f"Ledger riêng: hiển thị {displayed_count}/{int(summary.get('total_count', 0) or 0)} lệnh | "
        f"W {wins} - L {losses} - T {int(summary.get('tie_count', 0) or 0)} | "
        f"Win rate W/L {win_rate:.2%} | "
        f"Đang chờ {int(summary.get('pending_count', 0) or 0)} | "
        f"P&L {float(summary.get('total_pnl', 0) or 0):+.2f}"
    )


def _daily_history_summary_label(
    summary: dict[str, int | float],
    displayed_count: int,
) -> str:
    total_count = int(summary.get("total_count", 0) or 0)
    return (
        f"Hiển thị {displayed_count}/{total_count} lệnh | "
        f"W {int(summary.get('win_count', 0) or 0)} - "
        f"L {int(summary.get('loss_count', 0) or 0)} - "
        f"T {int(summary.get('tie_count', 0) or 0)} | "
        f"Đang chờ {int(summary.get('pending_count', 0) or 0)} | "
        f"P&L {float(summary.get('total_pnl', 0) or 0):+.2f}"
    )


def _daily_side_label(side: str) -> str:
    if side == "B":
        return "Cái"
    if side == "P":
        return "Con"
    return "-"


def _daily_round_label(signal_fingerprint: str) -> str:
    parts = signal_fingerprint.split("|")
    if len(parts) >= 3:
        return parts[-2]
    return "-"


def _filter_live_scores(scores: list[Any], stale_seconds: int, now: datetime | None = None) -> list[Any]:
    if stale_seconds <= 0:
        return list(scores)
    current_time = now or datetime.now(timezone.utc)
    return [score for score in scores if _is_live_score(score, stale_seconds, current_time)]


def _is_live_score(score: Any, stale_seconds: int, now: datetime | None = None) -> bool:
    if stale_seconds <= 0:
        return True
    last_seen = _parse_iso_datetime(str(getattr(score, "last_seen", "") or ""))
    if last_seen is None:
        return True
    current_time = now or datetime.now(timezone.utc)
    return (current_time - last_seen).total_seconds() <= stale_seconds


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _auto_refresh_interval(enabled: bool, text: str) -> int | None:
    if not enabled:
        return None
    return _parse_auto_refresh_seconds(text)


def _parse_auto_refresh_seconds(text: str) -> int:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Auto refresh seconds khong duoc rong.")
    try:
        value = float(cleaned)
    except ValueError as exc:
        raise ValueError("Auto refresh seconds phai la so.") from exc
    if value < 30:
        raise ValueError("Auto refresh seconds phai >= 30 de tranh reload qua day.")
    if value > 86400:
        raise ValueError("Auto refresh seconds phai <= 86400.")
    return int(value)


def _parse_live_table_stale_seconds(text: str) -> int:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Hide stale tables seconds khong duoc rong.")
    try:
        value = float(cleaned)
    except ValueError as exc:
        raise ValueError("Hide stale tables seconds phai la so.") from exc
    if value < 0:
        raise ValueError("Hide stale tables seconds phai >= 0.")
    if value > 86400:
        raise ValueError("Hide stale tables seconds phai <= 86400.")
    return int(value)


def _parse_cdp_port(cdp_url: str) -> int:
    with contextlib.suppress(Exception):
        import urllib.parse
        parsed = urllib.parse.urlparse(cdp_url)
        if parsed.port:
            return int(parsed.port)
    return DEFAULT_CDP_PORT


def _rounds_label(score: Any) -> str:
    current = getattr(score, "current_round_no", getattr(score, "total_rounds", 0))
    observed = getattr(score, "observed_rounds", current)
    if current == observed:
        return str(current)
    return f"{current}/{observed}"


def main() -> None:
    root = tk.Tk()
    app = BaccaratWorkbenchApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
