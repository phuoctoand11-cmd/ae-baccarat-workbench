from __future__ import annotations

import asyncio
import queue
import threading
import time
import tkinter as tk
from dataclasses import replace
from datetime import datetime, timezone
from tkinter import messagebox, ttk
from typing import Any

from .ae_decode import parse_manual_sequence
from .config import AppConfig, load_config, parse_stake_chain, save_config
from .engine import WorkbenchEngine, signal_side_label
from .ml_live import MlSignalFilter
from .models import LatencySample, MoneyConfig, StrategySignal, TableSnapshot, utc_now_iso_ms
from .monitor.cdp import AeSexyCdpMonitor
from .storage import WorkbenchStore


QUEUE_IDLE_REFRESH_MS = 300
QUEUE_BUSY_REFRESH_MS = 50
MAX_QUEUE_ITEMS_PER_TICK = 80
QUEUE_PROCESS_TIME_BUDGET_MS = 250
VIEW_REFRESH_MIN_INTERVAL_MS = 1000
STALE_QUEUE_THRESHOLD_MS = 5000
EXCLUDED_TABLE_NAMES = {f"Table {index}" for index in range(1, 15)}
DAILY_EXPERIMENT_WINDOWS = (
    ("12:00-13:00", 12 * 60, 13 * 60),
    ("14:00-15:00", 14 * 60, 15 * 60),
    ("18:00-20:00", 18 * 60, 20 * 60),
)
DAILY_EXPERIMENT_MAX_PER_WINDOW = 2
DAILY_HISTORY_ALL = "Tất cả"
DAILY_HISTORY_ROW_LIMIT = 250


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
        self.queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.monitor: AeSexyCdpMonitor | None = None
        self.monitor_thread: threading.Thread | None = None
        self.monitor_running = False
        self._pending_latency_samples: list[dict[str, Any]] = []
        self._queued_snapshot_payloads: dict[str, dict[str, Any]] = {}
        self._queued_snapshot_lock = threading.Lock()
        self._last_views_refresh_monotonic = 0.0

        self._build_ui()
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

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(self.root)
        notebook.grid(row=0, column=0, sticky="nsew")

        self.live_tab = ttk.Frame(notebook, padding=12)
        self.dashboard_tab = ttk.Frame(notebook, padding=12)
        self.tables_tab = ttk.Frame(notebook, padding=12)
        self.signals_tab = ttk.Frame(notebook, padding=12)
        self.latency_tab = ttk.Frame(notebook, padding=12)
        self.daily_tab = ttk.Frame(notebook, padding=12)
        self.config_tab = ttk.Frame(notebook, padding=12)

        notebook.add(self.live_tab, text="Live Monitor")
        notebook.add(self.dashboard_tab, text="Theo dõi nhiều bàn")
        notebook.add(self.tables_tab, text="Bàn & Cầu ruột")
        notebook.add(self.signals_tab, text="Signal + Paper")
        notebook.add(self.latency_tab, text="Latency")
        notebook.add(self.daily_tab, text="Paper theo khung giờ")
        notebook.add(self.config_tab, text="Cấu hình")

        self.status_var = tk.StringVar(value="")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, anchor="w", padding=(8, 4))
        status_bar.grid(row=1, column=0, sticky="ew")

        self._build_live_tab()
        self._build_dashboard_tab()
        self._build_tables_tab()
        self._build_signals_tab()
        self._build_latency_tab()
        self._build_daily_tab()
        self._build_config_tab()

    def _build_live_tab(self) -> None:
        self.live_tab.columnconfigure(1, weight=1)
        self.live_tab.rowconfigure(4, weight=1)

        ttk.Label(self.live_tab, text="Chrome CDP URL").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.cdp_var = tk.StringVar(value=self.config.cdp_url)
        ttk.Entry(self.live_tab, textvariable=self.cdp_var).grid(row=0, column=1, sticky="ew", pady=4)

        self.auto_refresh_enabled_var = tk.BooleanVar(value=self.config.auto_refresh_enabled)
        self.auto_refresh_seconds_var = tk.StringVar(value=str(self.config.auto_refresh_seconds))

        controls = ttk.Frame(self.live_tab)
        controls.grid(row=0, column=2, sticky="e", padx=(8, 0))
        self.start_button = ttk.Button(controls, text="Bắt đầu CDP", command=self._start_monitor)
        self.start_button.grid(row=0, column=0, padx=4)
        self.stop_button = ttk.Button(controls, text="Dừng", command=self._stop_monitor, state="disabled")
        self.stop_button.grid(row=0, column=1, padx=4)
        ttk.Checkbutton(
            controls,
            text="Watchdog mất live",
            variable=self.auto_refresh_enabled_var,
        ).grid(row=0, column=2, padx=(12, 4))
        ttk.Entry(controls, textvariable=self.auto_refresh_seconds_var, width=6).grid(row=0, column=3, padx=4)
        ttk.Label(controls, text="giay").grid(row=0, column=4, padx=(0, 4))

        ttk.Separator(self.live_tab).grid(row=1, column=0, columnspan=3, sticky="ew", pady=12)

        ttk.Label(self.live_tab, text="Bàn nhập tay").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.manual_table_var = tk.StringVar(value=self.config.manual_table_name)
        ttk.Entry(self.live_tab, textvariable=self.manual_table_var).grid(row=2, column=1, sticky="ew", pady=4)
        ttk.Button(self.live_tab, text="Nạp chuỗi B/P/T", command=self._ingest_manual).grid(
            row=2, column=2, sticky="e", padx=(8, 0), pady=4
        )

        ttk.Label(
            self.live_tab,
            text="Nhập chuỗi ví dụ: B P P P P hoặc Cai Con Hoa. Dùng để test chiến lược khi chưa nối live.",
        ).grid(row=3, column=0, columnspan=3, sticky="w", pady=(8, 4))

        self.manual_text = tk.Text(self.live_tab, height=10, wrap="word")
        self.manual_text.grid(row=4, column=0, columnspan=3, sticky="nsew", pady=4)
        self.manual_text.insert("1.0", "B P P P P B P B P B B B B P P B P")

        self.live_log = tk.Text(self.live_tab, height=8, wrap="word", state="disabled")
        self.live_log.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(12, 0))

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
            "ml_max_win",
            "ml_max_loss",
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
            "ml_max_win": "Chuỗi W ML",
            "ml_max_loss": "Chuỗi L ML",
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
            "ml_max_win": 90,
            "ml_max_loss": 90,
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

    def _build_tables_tab(self) -> None:
        self.tables_tab.columnconfigure(0, weight=1)
        self.tables_tab.rowconfigure(1, weight=1)
        header = ttk.Frame(self.tables_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(header, text="Bảng xếp hạng cầu ruột theo tín hiệu + độ dài dữ liệu").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="Làm mới", command=self._refresh_views).grid(row=0, column=1, sticky="e", padx=(8, 0))
        header.columnconfigure(0, weight=1)

        columns = ("rank", "table", "rounds", "road", "signal", "score", "pnl", "last_seen")
        self.table_tree = ttk.Treeview(self.tables_tab, columns=columns, show="headings", height=18)
        headings = {
            "rank": "#",
            "table": "Bàn",
            "rounds": "Ván/Lưu",
            "road": "KQ bàn",
            "signal": "Tín hiệu mạnh nhất",
            "score": "Điểm",
            "pnl": "Paper P&L",
            "last_seen": "Cập nhật",
        }
        widths = {
            "rank": 44,
            "table": 130,
            "rounds": 70,
            "road": 360,
            "signal": 250,
            "score": 80,
            "pnl": 90,
            "last_seen": 180,
        }
        for col in columns:
            self.table_tree.heading(col, text=headings[col])
            self.table_tree.column(col, width=widths[col], anchor="w")
        self.table_tree.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(self.tables_tab, orient="vertical", command=self.table_tree.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.table_tree.configure(yscrollcommand=scrollbar.set)

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

    def _build_daily_tab(self) -> None:
        self.daily_tab.columnconfigure(0, weight=1)
        self.daily_tab.rowconfigure(2, weight=1)
        header = ttk.Frame(self.daily_tab)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Label(
            header,
            text="Paper test: 12–13h, 14–15h, 18–20h (tối đa 2 bàn/phiên, 1 lệnh/bàn/ngày)",
        ).pack(side="left")
        ttk.Label(header, text="Stake").pack(side="left", padx=(20, 4))
        self.daily_stake_var = tk.StringVar(value="10")
        ttk.Entry(header, textvariable=self.daily_stake_var, width=10).pack(side="left")
        ttk.Button(header, text="Làm mới ứng viên", command=self._refresh_daily_tab).pack(side="left", padx=8)
        self.daily_status_var = tk.StringVar(value="Chỉ paper, không đặt cược thật.")
        ttk.Label(self.daily_tab, textvariable=self.daily_status_var, anchor="w").grid(row=1, column=0, sticky="ew")

        self.daily_notebook = ttk.Notebook(self.daily_tab)
        self.daily_notebook.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
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
            values=(DAILY_HISTORY_ALL, *(label for label, _start, _end in DAILY_EXPERIMENT_WINDOWS)),
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
            auto_refresh_seconds=auto_refresh_seconds,
        )
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
        if self.monitor:
            self.monitor.stop()
        self.monitor_running = False
        self.start_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self._set_status("Đang dừng CDP monitor...")

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
            self.config = AppConfig(
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
                min_confidence=float(self.min_conf_var.get()),
                expected_shoe_rounds=int(self.expected_shoe_rounds_var.get()),
                stop_signals_after_round=int(self.stop_after_round_var.get()),
                ml_filter_enabled=bool(self.ml_filter_enabled_var.get()),
                ml_model_path=self.ml_model_path_var.get().strip() or self.config.ml_model_path,
                ml_decision_threshold=ml_threshold,
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
        processed = False
        tick_started = time.perf_counter()
        for _ in range(MAX_QUEUE_ITEMS_PER_TICK):
            try:
                kind, payload = self.queue.get_nowait()
            except queue.Empty:
                break
            processed = True
            if kind == "snapshot_latest":
                table_name = str(payload)
                with self._queued_snapshot_lock:
                    snapshot_payload = self._queued_snapshot_payloads.pop(table_name, None)
                if snapshot_payload is None:
                    continue
                try:
                    self._handle_snapshot(snapshot_payload)
                finally:
                    pass
            elif kind == "status":
                self._set_status(str(payload))
                self._append_live_log(str(payload))
            elif kind == "monitor_stopped":
                self.monitor_running = False
                self.start_button.configure(state="normal")
                self.stop_button.configure(state="disabled")
            if _elapsed_ms(tick_started, time.perf_counter()) >= QUEUE_PROCESS_TIME_BUDGET_MS:
                break
        if processed:
            now = time.perf_counter()
            refresh_due = (
                _elapsed_ms(self._last_views_refresh_monotonic, now) >= VIEW_REFRESH_MIN_INTERVAL_MS
                if self._last_views_refresh_monotonic
                else True
            )
            if refresh_due:
                self._refresh_views()
                self._last_views_refresh_monotonic = now
            ui_refresh_done = time.perf_counter()
            self._finalize_latency_samples(ui_refresh_done, utc_now_iso_ms())
        delay = QUEUE_BUSY_REFRESH_MS if not self.queue.empty() else QUEUE_IDLE_REFRESH_MS
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
        try:
            # Stale snapshots still update rounds/last_seen, but never create
            # a prediction from an obsolete state.
            signals = self.engine.ingest(snapshot, generate_signals=not stale_snapshot)
        except Exception as exc:
            self._set_status(f"Lỗi xử lý snapshot: {exc}")
            return
        self._settle_daily_experiment(snapshot.table_name)
        if not stale_snapshot:
            self._auto_arm_daily_experiment()
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
        self._refresh_dashboard_tree()
        self._refresh_table_tree()
        self._refresh_signal_tree()
        self._refresh_paper_tree()
        self._refresh_latency_tree()
        self._refresh_daily_tab()

    def _refresh_dashboard_tree(self) -> None:
        self.dashboard_tree.delete(*self.dashboard_tree.get_children())
        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        for index, score in enumerate(scores, start=1):
            best_signal = score.best_signal
            display_signal = score.display_signal or best_signal
            filtered_signal = self._filtered_signal_for(display_signal)
            signal_label = signal_side_label(display_signal.side) if display_signal and display_signal.is_actionable else "Không vào"
            confidence = _signal_confidence_label(display_signal, filtered_signal)
            strategy = display_signal.strategy_id if display_signal and display_signal.is_actionable else "-"
            pending = self._pending_label(score.table_name)
            ml_streak = self.store.ml_pass_wl_summary(score.table_name)
            tag = "bet" if best_signal and best_signal.is_actionable else "watch" if display_signal and display_signal.is_actionable else "skip"
            self.dashboard_tree.insert(
                "",
                "end",
                values=(
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
                    ml_streak["max_win"],
                    ml_streak["max_loss"],
                    f"{score.paper_pnl:.2f}",
                ),
                tags=(tag,),
            )
        totals = self.store.ml_pass_totals()
        win_loss_count = int(totals["wins"]) + int(totals["losses"])
        win_rate = (int(totals["wins"]) / win_loss_count) if win_loss_count else 0.0
        self.dashboard_tree.insert(
            "",
            "end",
            values=(
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
                "-",
                "-",
                f"{float(totals['pnl']):.2f}",
            ),
            tags=("total",),
        )

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

    def _refresh_table_tree(self) -> None:
        self.table_tree.delete(*self.table_tree.get_children())
        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        for index, score in enumerate(scores, start=1):
            display_signal = score.display_signal or score.best_signal
            self.table_tree.insert(
                "",
                "end",
                values=(
                    index,
                    score.table_name,
                    _rounds_label(score),
                    score.road,
                    _format_display_signal(display_signal, self._filtered_signal_for(display_signal)),
                    f"{score.score:.3f}",
                    f"{score.paper_pnl:.2f}",
                    score.last_seen,
                ),
            )

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

    def _refresh_daily_tab(self) -> None:
        self.daily_tree.delete(*self.daily_tree.get_children())
        now = datetime.now().astimezone()
        session_date = now.date().isoformat()
        active_window = _active_daily_experiment_window(now)
        today_rows = self.store.daily_experiment_rows(session_date)
        used_today = {str(row["table_name"]) for row in today_rows}
        scores = _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds)
        self._daily_candidates = _rank_daily_candidates(scores, self.engine.snapshots)

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

        preview_limit = DAILY_EXPERIMENT_MAX_PER_WINDOW
        if active_window:
            recorded_in_window = sum(
                str(row["session_window"]) == active_window for row in today_rows
            )
            preview_limit = max(0, DAILY_EXPERIMENT_MAX_PER_WINDOW - recorded_in_window)
        previewed = 0
        for score in self._daily_candidates:
            if score.table_name in used_today or previewed >= preview_limit:
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
                    "Ứng viên" if active_window else "Ngoài giờ",
                ),
            )
            index += 1
            previewed += 1

        total_pnl = sum(float(row["pnl"] or 0) for row in today_rows if row["status"] == "settled")
        if active_window:
            recorded = sum(str(row["session_window"]) == active_window for row in today_rows)
            self.daily_status_var.set(
                f"Phiên {active_window}: đã ghi {recorded}/{DAILY_EXPERIMENT_MAX_PER_WINDOW} bàn | "
                f"hôm nay {len(today_rows)} lệnh, P&L {total_pnl:+.2f}."
            )
        else:
            windows = ", ".join(label for label, _start, _end in DAILY_EXPERIMENT_WINDOWS)
            self.daily_status_var.set(
                f"Ngoài giờ tự ghi ({windows}) | hôm nay {len(today_rows)} lệnh, "
                f"P&L {total_pnl:+.2f}."
            )

    def _auto_arm_daily_experiment(self) -> None:
        now = datetime.now().astimezone()
        active_window = _active_daily_experiment_window(now)
        if active_window is None:
            return
        session_date = now.date().isoformat()
        today_rows = self.store.daily_experiment_rows(session_date)
        used_today = {str(row["table_name"]) for row in today_rows}
        recorded_in_window = sum(
            str(row["session_window"]) == active_window for row in today_rows
        )
        if recorded_in_window >= DAILY_EXPERIMENT_MAX_PER_WINDOW:
            return
        candidates = _rank_daily_candidates(
            _filter_live_scores(self.engine.table_scores(), self.config.live_table_stale_seconds),
            self.engine.snapshots,
        )
        inserted = 0
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
            )
            if saved:
                inserted += 1
                used_today.add(score.table_name)
        if inserted:
            self._daily_history_dirty = True
            self.daily_status_var.set(
                f"Phiên {active_window}: đã tự động ghi {inserted} lệnh paper; "
                "chờ round kế tiếp để settle."
            )

    def _settle_daily_experiment(self, table_name: str) -> None:
        row = self.store.pending_daily_experiment_row(table_name)
        if row is None:
            return
        result_event = self.store.next_round_after_fingerprint(
            table_name=table_name,
            signal_fingerprint=str(row["signal_fingerprint"]),
        )
        if result_event is None:
            return
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


def _active_daily_experiment_window(now: datetime | None = None) -> str | None:
    current_time = now or datetime.now().astimezone()
    minute_of_day = current_time.hour * 60 + current_time.minute
    for label, start_minute, end_minute in DAILY_EXPERIMENT_WINDOWS:
        if start_minute <= minute_of_day < end_minute:
            return label
    return None


def _rank_daily_candidates(scores: list[Any], snapshots: dict[str, TableSnapshot]) -> list[Any]:
    candidates = [
        score
        for score in scores
        if score.table_name not in EXCLUDED_TABLE_NAMES
        and score.best_signal is not None
        and score.best_signal.is_actionable
        and score.best_signal.features.get("ml_probability_win") is not None
        and score.table_name in snapshots
        and score.best_signal.round_fingerprint == snapshots[score.table_name].latest_fingerprint()
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
