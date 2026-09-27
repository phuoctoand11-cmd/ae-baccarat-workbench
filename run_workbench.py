from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is in sys.path
root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))


def _headless_self_test() -> int:
    import json

    import duckdb  # noqa: F401
    import fastapi  # noqa: F401
    import joblib
    import playwright  # noqa: F401
    import sklearn  # noqa: F401
    import uvicorn  # noqa: F401
    import websockets  # noqa: F401
    import xgboost  # noqa: F401

    from ae_baccarat_workbench.app import BaccaratWorkbenchApp
    from ae_baccarat_workbench.config import AppConfig
    from ae_baccarat_workbench.ml import CATEGORICAL_FEATURES, NUMERIC_FEATURES
    from ae_baccarat_workbench.ml_live import MlSignalFilter
    from ae_baccarat_workbench.monitor.auto_bettor import LiveAutoBettor
    from ae_baccarat_workbench.monitor.provider_ack import ProviderAckMonitor  # noqa: F401

    config = AppConfig()
    model_path = config.ml_model_abs_path
    model_loads = False
    model_predicts = False
    if model_path.is_file():
        try:
            joblib.load(model_path)
            model_loads = True
            scorer = MlSignalFilter(model_path)
            feature_row = {column: 0.0 for column in NUMERIC_FEATURES}
            feature_row.update({column: "unknown" for column in CATEGORICAL_FEATURES})
            probability = scorer._predict_probability(feature_row)
            model_predicts = scorer.ready and 0.0 <= probability <= 1.0
        except Exception:
            model_loads = False
            model_predicts = False
    bundle_root = Path(getattr(sys, "_MEIPASS", root_dir))
    template_path = bundle_root / "ae_baccarat_workbench" / "web" / "templates" / "index.html"
    checks = {
        "model_exists": model_path.is_file(),
        "model_loads": model_loads,
        "model_predicts": model_predicts,
        "template_exists": template_path.is_file(),
        "circuit_breaker_removed": (
            not hasattr(LiveAutoBettor(), "circuit_state")
            and not hasattr(BaccaratWorkbenchApp, "_reset_autobet_circuit")
        ),
    }
    exit_code = 0 if all(checks.values()) else 1
    output = "\n".join(
        [
            json.dumps(checks, sort_keys=True),
            "HEADLESS_OK" if exit_code == 0 else "HEADLESS_FAILED",
        ]
    )
    if "--self-test-log" in sys.argv:
        log_index = sys.argv.index("--self-test-log") + 1
        if log_index >= len(sys.argv):
            return 2
        Path(sys.argv[log_index]).write_text(output + "\n", encoding="utf-8")
    if sys.stdout is not None:
        print(output, flush=True)
    return exit_code


if __name__ == "__main__":
    if "--headless-self-test" in sys.argv:
        raise SystemExit(_headless_self_test())
    if "--web" in sys.argv or "-w" in sys.argv:
        try:
            import ctypes
            ctypes.windll.kernel32.AllocConsole()
            sys.stdout = open("CONOUT$", "w")
            sys.stderr = open("CONOUT$", "w")
        except Exception:
            pass
        from ae_baccarat_workbench.web.server import main as web_main
        web_main()
    else:
        from ae_baccarat_workbench.app import main
        main()
