from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .models import MoneyConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.local.json"
DUCKDB_ALWAYS_ENABLED = True


@dataclass(frozen=True)
class AppConfig:
    cdp_url: str = "http://localhost:9222"
    sqlite_path: str = "data/workbench.sqlite"
    duckdb_path: str = "data/analytics.duckdb"
    enable_duckdb: bool = True
    default_table_name: str = "Baccarat C01"
    manual_table_name: str = "Manual Table"
    paper_trading_enabled: bool = True
    auto_refresh_enabled: bool = False
    auto_refresh_seconds: int = 300
    live_table_stale_seconds: int = 120
    min_confidence: float = 0.50
    expected_shoe_rounds: int = 72
    stop_signals_after_round: int = 65
    ml_filter_enabled: bool = True
    ml_model_path: str = "data/ml/xgboost.joblib"
    ml_decision_threshold: float = 0.55
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
    return PROJECT_ROOT / path


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    if not path.exists():
        return AppConfig()
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    money_raw = raw.pop("money", {})
    raw["enable_duckdb"] = DUCKDB_ALWAYS_ENABLED
    return AppConfig(money=MoneyConfig(**money_raw), **raw)


def save_config(config: AppConfig, path: Path = DEFAULT_CONFIG_PATH) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = asdict(config)
    payload["enable_duckdb"] = DUCKDB_ALWAYS_ENABLED
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
