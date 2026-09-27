from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .models import MoneyConfig


import sys


def _detect_project_root() -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        # Candidate 1: Inside dist/<name> within project repository
        candidate_root = exe_dir.parent.parent
        is_repo_root = (candidate_root / "ae_baccarat_workbench").exists() or (candidate_root / "run_workbench.py").exists()
        if is_repo_root and (candidate_root / "data" / "workbench.sqlite").exists():
            return candidate_root
        # Candidate 2: Standalone bundle (e.g. on VPS) with data folder next to .exe
        if (exe_dir / "data" / "workbench.sqlite").exists():
            return exe_dir
        if is_repo_root:
            return candidate_root
        return exe_dir
    return Path(__file__).resolve().parents[1]


PROJECT_ROOT = _detect_project_root()
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.local.json"
DUCKDB_ALWAYS_ENABLED = True
DEFAULT_DAILY_SELECTED_WINDOWS = (
    "12:00-13:00",
    "14:00-15:00",
    "18:00-19:00",
    "19:00-20:00",
)
DEFAULT_RUN_LENGTH_SELECTED_WINDOWS: tuple[str, ...] = ()
DEFAULT_ENSEMBLE_MAJORITY_SELECTED_WINDOWS: tuple[str, ...] = ()
DEFAULT_ADAPTIVE_REGIME_SELECTED_WINDOWS: tuple[str, ...] = ()


@dataclass(frozen=True)
class AppConfig:
    cdp_url: str = "http://127.0.0.1:9222"
    sqlite_path: str = "data/workbench.sqlite"
    duckdb_path: str = "data/analytics.duckdb"
    enable_duckdb: bool = True
    default_table_name: str = "Baccarat C01"
    manual_table_name: str = "Manual Table"
    paper_trading_enabled: bool = True
    auto_refresh_enabled: bool = False
    auto_refresh_seconds: int = 300
    live_table_stale_seconds: int = 120
    daily_selected_windows: tuple[str, ...] = DEFAULT_DAILY_SELECTED_WINDOWS
    daily_autobet_enabled: bool = False
    daily_stake: float = 10.0
    daily_stop_win_enabled: bool = True
    run_length_selected_windows: tuple[str, ...] = DEFAULT_RUN_LENGTH_SELECTED_WINDOWS
    run_length_stake: float = 10.0
    run_length_autobet_enabled: bool = False
    ensemble_majority_selected_windows: tuple[str, ...] = DEFAULT_ENSEMBLE_MAJORITY_SELECTED_WINDOWS
    ensemble_majority_stake: float = 10.0
    ensemble_majority_autobet_enabled: bool = False
    ensemble_majority_ml_min_probability: float = 0.55
    adaptive_regime_selected_windows: tuple[str, ...] = DEFAULT_ADAPTIVE_REGIME_SELECTED_WINDOWS
    adaptive_regime_stake: float = 10.0
    adaptive_regime_autobet_enabled: bool = False
    min_confidence: float = 0.50
    expected_shoe_rounds: int = 72
    stop_signals_after_round: int = 65
    ml_filter_enabled: bool = True
    ml_model_path: str = "data/ml/xgboost.joblib"
    ml_decision_threshold: float = 0.55
    target_url: str = ""
    account_id: str = ""
    account_password: str = ""
    remember_credentials: bool = False
    chrome_path: str = ""
    ae_lobby_name: str = "AE Sexy, Sexy Casino"
    custom_casino_selector: str = ""
    custom_ae_selector: str = ""
    money: MoneyConfig = MoneyConfig()

    @property
    def sqlite_abs_path(self) -> Path:
        return _resolve_project_path(self.sqlite_path)

    @property
    def duckdb_abs_path(self) -> Path:
        return _resolve_project_path(self.duckdb_path)

    @property
    def ml_model_abs_path(self) -> Path:
        return _resolve_project_path(self.ml_model_path)


def _resolve_project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    resolved = PROJECT_ROOT / path
    if resolved.exists():
        return resolved
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if (exe_dir / path).exists():
            return exe_dir / path
        if (exe_dir / "_internal" / path).exists():
            return exe_dir / "_internal" / path
    return resolved


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    if not path.exists():
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            alt = exe_dir / "config.local.json"
            if alt.exists():
                path = alt
            else:
                return AppConfig()
        else:
            return AppConfig()
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    money_raw = raw.pop("money", {})
    raw["enable_duckdb"] = DUCKDB_ALWAYS_ENABLED
    # Filter known fields to avoid unexpected keyword errors
    valid_fields = set(AppConfig.__dataclass_fields__.keys())
    filtered = {k: v for k, v in raw.items() if k in valid_fields}
    configured_windows = filtered.get("daily_selected_windows")
    if isinstance(configured_windows, (list, tuple)):
        filtered["daily_selected_windows"] = tuple(str(value) for value in configured_windows)
    elif configured_windows is not None:
        filtered["daily_selected_windows"] = DEFAULT_DAILY_SELECTED_WINDOWS
    configured_run_length_windows = filtered.get("run_length_selected_windows")
    if isinstance(configured_run_length_windows, (list, tuple)):
        filtered["run_length_selected_windows"] = tuple(
            str(value) for value in configured_run_length_windows
        )
    elif configured_run_length_windows is not None:
        filtered["run_length_selected_windows"] = DEFAULT_RUN_LENGTH_SELECTED_WINDOWS
    configured_ensemble_windows = filtered.get("ensemble_majority_selected_windows")
    if isinstance(configured_ensemble_windows, (list, tuple)):
        filtered["ensemble_majority_selected_windows"] = tuple(
            str(value) for value in configured_ensemble_windows
        )
    elif configured_ensemble_windows is not None:
        filtered["ensemble_majority_selected_windows"] = DEFAULT_ENSEMBLE_MAJORITY_SELECTED_WINDOWS
    configured_adaptive_windows = filtered.get("adaptive_regime_selected_windows")
    if isinstance(configured_adaptive_windows, (list, tuple)):
        filtered["adaptive_regime_selected_windows"] = tuple(
            str(value) for value in configured_adaptive_windows
        )
    elif configured_adaptive_windows is not None:
        filtered["adaptive_regime_selected_windows"] = DEFAULT_ADAPTIVE_REGIME_SELECTED_WINDOWS
    return AppConfig(money=MoneyConfig(**money_raw), **filtered)


def save_config(config: AppConfig, path: Path = DEFAULT_CONFIG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = asdict(config)
    payload["enable_duckdb"] = DUCKDB_ALWAYS_ENABLED
    if not config.remember_credentials:
        payload["account_password"] = ""
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_stake_chain(text: str) -> tuple[float, ...]:
    cleaned = text.replace(";", ",").replace("|", ",")
    values: list[float] = []
    for part in cleaned.split(","):
        item = part.strip()
        if not item:
            continue
        values.append(float(item))
    if not values:
        raise ValueError("Chuỗi tiền không được rỗng")
    if any(v < 0 for v in values):
        raise ValueError("Chuỗi tiền không được có số âm")
    return tuple(values)
