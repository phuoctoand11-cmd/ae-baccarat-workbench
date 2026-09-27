from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_CDP_PORT = 9222
DEFAULT_PROFILE_DIR_NAME = "chrome-cdp-profile"


def find_chrome_executable(custom_path: str | Path | None = None) -> Path | None:
    """Find the Chrome executable on Windows, checking custom path, PATH, and standard directories."""
    if custom_path:
        p = Path(custom_path).resolve()
        if p.is_file() and os.access(p, os.X_OK):
            return p

    which_chrome = shutil.which("chrome") or shutil.which("google-chrome")
    if which_chrome:
        return Path(which_chrome).resolve()

    candidates: list[Path] = []
    program_files = os.environ.get("PROGRAMFILES")
    if program_files:
        candidates.append(Path(program_files) / "Google" / "Chrome" / "Application" / "chrome.exe")
    program_files_x86 = os.environ.get("PROGRAMFILES(X86)")
    if program_files_x86:
        candidates.append(Path(program_files_x86) / "Google" / "Chrome" / "Application" / "chrome.exe")
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        candidates.append(Path(local_app_data) / "Google" / "Chrome" / "Application" / "chrome.exe")

    # Hardcoded fallbacks for standard Windows drives
    candidates.extend([
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ])

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    return None


def is_cdp_port_open(port: int = DEFAULT_CDP_PORT, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    """Check if Chrome CDP endpoint is responding on the given port."""
    url = f"http://{host}:{port}/json/version"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AE-Baccarat-Workbench"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                return bool(data.get("Browser") or data.get("webSocketDebuggerUrl"))
    except Exception:
        return False
    return False


def get_default_user_data_dir() -> Path:
    """Return a dedicated user data directory for Chrome CDP profile."""
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        base = Path(local_app_data) / "AEBaccaratWorkbench" / "chrome-profile"
    else:
        base = Path.home() / ".ae_baccarat_workbench" / "chrome-profile"
    base.mkdir(parents=True, exist_ok=True)
    return base


def launch_chrome_cdp(
    port: int = DEFAULT_CDP_PORT,
    user_data_dir: str | Path | None = None,
    chrome_path: str | Path | None = None,
    target_url: str | None = None,
    wait_timeout: float = 12.0,
) -> tuple[bool, str]:
    """Launch Google Chrome with CDP remote debugging port enabled.

    Returns:
        (success, message)
    """
    if is_cdp_port_open(port):
        logger.info("Chrome CDP is already open and responding on port %d", port)
        return True, f"Cổng CDP {port} đã mở và sẵn sàng."

    exe_path = find_chrome_executable(chrome_path)
    if not exe_path:
        return (
            False,
            "Không tìm thấy Google Chrome trên máy tính. Vui lòng cài đặt Google Chrome hoặc cấu hình đường dẫn chrome.exe.",
        )

    profile_dir = Path(user_data_dir) if user_data_dir else get_default_user_data_dir()
    profile_dir.mkdir(parents=True, exist_ok=True)

    args = [
        str(exe_path),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--start-maximized",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=CalculateNativeWinOcclusion,IntensiveWakeUpThrottling",
        "--disable-hang-monitor",
        "--disable-background-networking=false",
    ]
    if target_url and target_url.strip():
        url = target_url.strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            url = f"https://{url}"
        args.append(url)

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP

    logger.info("Launching Chrome CDP: %s", " ".join(args))
    try:
        subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=(os.name != "nt"),
            creationflags=creationflags,
        )
    except Exception as exc:
        return False, f"Lỗi khi khởi chạy Chrome: {exc}"

    # Wait for CDP endpoint to respond
    start_time = time.monotonic()
    while time.monotonic() - start_time < wait_timeout:
        if is_cdp_port_open(port):
            return True, f"Khởi động Chrome thành công trên cổng {port}."
        time.sleep(0.5)

    return False, f"Chrome đã khởi chạy nhưng cổng {port} chưa phản hồi sau {wait_timeout:.0f} giây."
