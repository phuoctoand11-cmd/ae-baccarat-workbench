from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .provider_ack import PROVIDER_ACK_TIMEOUT_SECONDS, ProviderAck, ProviderAckMonitor

logger = logging.getLogger(__name__)
LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS = 4
POST_CONFIRM_LOBBY_WAIT_SECONDS = 5.0

SUPPORTED_CHIPS = (
    "5", "10", "20", "30", "50", "100", "200", "500",
    "1000", "2000", "5000", "10000", "20000",
    "5k", "10k", "20k", "30k", "50k", "100k", "200k", "500k",
    "1m", "2m", "5m", "10m", "20m",
)


@dataclass(frozen=True)
class BetOrder:
    table_name: str
    side: str  # "PLAYER" / "Con" or "BANKER" / "Cái"
    stake: float
    session_window: str = ""
    order_id: str = ""
    target_round_no: int | None = None
    target_shoe: str = ""
    signal_fingerprint: str = ""
    signal_created_at: str = ""
    countdown_at_signal: float | None = None
    source: str = ""
    attempt_id: str = ""
    attempt_created_at: str = ""


@dataclass(frozen=True)
class BetResult:
    order: BetOrder
    success: bool
    message: str
    placed_at: str = ""
    started_at: str = ""
    confirm_clicked_at: str = ""
    reason_code: str = ""


AutoBetAuditCallback = Callable[[dict[str, Any]], None]
ProviderAckWaiter = Callable[[BetOrder, float, float], Awaitable[ProviderAck]]


def prepare_bet_orders(orders: list[BetOrder], *, source: str = "daily") -> list[BetOrder]:
    """Attach immutable audit identities without changing any betting fields."""
    prepared: list[BetOrder] = []
    for order in orders:
        created_at = order.attempt_created_at or datetime.now(timezone.utc).isoformat()
        prepared.append(
            replace(
                order,
                attempt_id=order.attempt_id or uuid4().hex,
                attempt_created_at=created_at,
                source=order.source or source,
            )
        )
    return prepared


def autobet_audit_event(
    order: BetOrder,
    *,
    stage: str,
    status: str,
    reason_code: str,
    message: str = "",
    countdown_seconds: float | int | None = None,
    table_shoe: str | int | None = None,
    table_round_no: int | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "attempt_id": order.attempt_id,
        "attempt_created_at": order.attempt_created_at,
        "order_id": order.order_id,
        "source": order.source or "unknown",
        "session_window": order.session_window,
        "signal_fingerprint": order.signal_fingerprint,
        "signal_created_at": order.signal_created_at,
        "table_name": order.table_name,
        "target_shoe": order.target_shoe,
        "target_round_no": order.target_round_no,
        "side": normalize_bet_side(order.side),
        "stake": order.stake,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "status": status,
        "reason_code": reason_code,
        "message": message,
        "countdown_seconds": countdown_seconds,
        "table_shoe": table_shoe,
        "table_round_no": table_round_no,
        "payload": payload or {},
    }


def emit_autobet_audit(
    callback: AutoBetAuditCallback | None,
    order: BetOrder,
    **event_fields: Any,
) -> None:
    if callback is None:
        return
    try:
        callback(autobet_audit_event(order, **event_fields))
    except Exception:
        logger.exception("Auto-Bet audit callback failed for %s", order.attempt_id)


def normalize_table_name(name: str) -> str:
    """Normalize table names for fuzzy matching."""
    if not name:
        return ""
    clean = re.sub(r"[^a-zA-Z0-9]", "", name).lower()
    clean = clean.replace("baccarat", "")
    return re.sub(r"c0*(\d+)", r"c\1", clean)


def normalize_bet_side(side: Any) -> str:
    """Normalize any side representation to 'PLAYER' or 'BANKER'.

    Accepts:
        - BetSide.PLAYER / BetSide.BANKER
        - 'P', 'p', 'PLAYER', 'Player', 'con', 'Con', 'tay con' -> 'PLAYER'
        - 'B', 'b', 'BANKER', 'Banker', 'cai', 'Cai', 'cái', 'Cái', 'nha cai', 'Nhà cái' -> 'BANKER'
    """
    if not side:
        return "BANKER"
    val = side.value if hasattr(side, "value") else str(side)
    clean = val.strip().upper()
    if clean in ("P", "PLAYER", "CON", "TAY CON", "TAYCON"):
        return "PLAYER"
    if clean in ("B", "BANKER", "CAI", "CÁI", "NHA CAI", "NHÀ CÁI", "NHACAI"):
        return "BANKER"
    if clean.startswith("P") or clean.startswith("CON"):
        return "PLAYER"
    return "BANKER"


def invert_bet_side(side: Any) -> str:
    """Invert a bet side: PLAYER/'P' -> 'B', BANKER/'B' -> 'P'."""
    norm = normalize_bet_side(side)
    return "B" if norm == "PLAYER" else "P"


def resolve_bet_side(side: Any, bet_mode: str = "forward") -> str:
    """Resolve bet side according to bet_mode ('forward' or 'inverse').

    If bet_mode is 'inverse' (Đánh Ngược):
        PLAYER -> 'B' (BANKER)
        BANKER -> 'P' (PLAYER)
    Otherwise returns 'P' or 'B'.
    """
    norm = normalize_bet_side(side)
    mode = str(bet_mode or "").strip().lower()
    if mode in ("inverse", "nguoc", "ngược", "reverse", "flip", "đánh ngược", "danh nguoc"):
        return "B" if norm == "PLAYER" else "P"
    return "P" if norm == "PLAYER" else "B"



def map_stake_to_chips(stake: float) -> list[str]:
    """Map numeric stake amount (points) directly to chip denomination keys:

    Chip denomination mapping (points):
        - 5     -> 5
        - 10    -> 10
        - 20    -> 20
        - 30    -> 30
        - 50    -> 50
        - 100   -> 100
        - 200   -> 200 (phỉnh 200)
        - 500   -> 500 (phỉnh 500)
        - 1000  -> 1000 (phỉnh 1000)
        - 2000  -> 2000
        - 5000  -> 5000
        - 10000 -> 10000
        - 20000 -> 20000
    """
    target = int(round(float(stake)))
    if target <= 0:
        return ["10"]

    chip_values = [
        (20000, "20000"),
        (10000, "10000"),
        (5000, "5000"),
        (2000, "2000"),
        (1000, "1000"),
        (500, "500"),
        (200, "200"),
        (100, "100"),
        (50, "50"),
        (30, "30"),
        (20, "20"),
        (10, "10"),
        (5, "5"),
    ]

    # Exact single chip match
    for val, key in chip_values:
        if target == val:
            return [key]

    # Greedy combination for split stakes
    remaining = target
    result: list[str] = []
    for val, key in chip_values:
        while remaining >= val and len(result) < 20:
            result.append(key)
            remaining -= val
        if remaining <= 0:
            break

    return result or ["10"]


def extract_target_round_and_shoe(
    fingerprint: str = "",
    current_round_no: int | None = None,
    current_shoe: str | int | None = None,
) -> tuple[int | None, str]:
    """Extract target betting round (signal round + 1) and shoe string.

    Fingerprint format: 'Baccarat C11|22914|19|B' -> target_round = 20, shoe = '22914'
    """
    target_round: int | None = None
    target_shoe: str = ""

    if fingerprint:
        parts = fingerprint.split("|")
        if len(parts) >= 3:
            if parts[1] and parts[1] != "shoe?":
                target_shoe = str(parts[1]).strip()
            if parts[2].isdigit():
                target_round = int(parts[2]) + 1

    if target_round is None and current_round_no is not None:
        target_round = current_round_no + 1

    if not target_shoe and current_shoe is not None and str(current_shoe) != "shoe?":
        target_shoe = str(current_shoe).strip()

    return target_round, target_shoe


def find_game_websocket_url(cdp_url: str) -> str | None:
    """Find the AE Sexy game console target WebSocket URL from CDP."""
    normalized_url = cdp_url.rstrip("/").replace("localhost", "127.0.0.1")
    endpoints = [f"{normalized_url}/json", f"{normalized_url}/json/list"]
    
    targets = None
    for ep in endpoints:
        try:
            req = urllib.request.Request(ep, headers={"User-Agent": "ae-workbench"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                targets = json.loads(resp.read().decode("utf-8"))
                break
        except Exception:
            continue

    if not targets or not isinstance(targets, list):
        return None

    # Priority 1: Direct target containing player/webMain or tgmeq.com
    for t in targets:
        url = str(t.get("url") or "")
        ws = t.get("webSocketDebuggerUrl")
        if ws and ("tgmeq.com" in url or "player/webMain" in url or "usplaynet.com" in url or "arrpar.com" in url):
            return str(ws)

    # Priority 2: Target containing vbgames88 console
    for t in targets:
        url = str(t.get("url") or "")
        ws = t.get("webSocketDebuggerUrl")
        if ws and "vbgames88" in url and "console" in url:
            return str(ws)

    # Priority 3: Popup launch ae-live page
    for t in targets:
        url = str(t.get("url") or "")
        ws = t.get("webSocketDebuggerUrl")
        if ws and ("ae-live" in url or "popup-launch" in url):
            return str(ws)

    return None


class LiveAutoBettor:
    """Automated live betting executor for AE Sexy Baccarat via Chrome CDP."""

    def __init__(self, cdp_url: str = "http://127.0.0.1:9222") -> None:
        self.cdp_url = cdp_url
        self._lock = threading.Lock()
        self._is_running = False

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def execute_orders_background(
        self,
        orders: list[BetOrder],
        on_status: Callable[[str], None] | None = None,
        on_order_done: Callable[[BetResult], None] | None = None,
        on_finished: Callable[[list[BetResult]], None] | None = None,
        on_audit: AutoBetAuditCallback | None = None,
    ) -> threading.Thread:
        """Run betting orders sequentially in a background worker thread."""
        prepared_orders = prepare_bet_orders(orders)
        for order in prepared_orders:
            emit_autobet_audit(
                on_audit,
                order,
                stage="EXECUTOR_QUEUED",
                status="requested",
                reason_code="EXECUTOR_QUEUED",
                message="Lệnh đã được đưa vào worker Auto-Bet.",
                countdown_seconds=order.countdown_at_signal,
            )
        thread = threading.Thread(
            target=self._worker_run,
            args=(prepared_orders, on_status, on_order_done, on_finished, on_audit),
            daemon=True,
            name="AE-LiveAutoBettor",
        )
        thread.start()
        return thread

    def return_to_lobby_background(self) -> None:
        """Asynchronously return to lobby if currently showing a table."""
        if self._is_running:
            return
        thread = threading.Thread(
            target=self._worker_return_to_lobby,
            daemon=True,
            name="AE-ReturnToLobby",
        )
        thread.start()

    def _worker_return_to_lobby(self) -> None:
        if self._is_running:
            return
        try:
            import websockets
        except ImportError:
            return
        ws_url = find_game_websocket_url(self.cdp_url)
        if not ws_url:
            return

        async def run() -> None:
            try:
                async with websockets.connect(ws_url, max_size=12_000_000) as ws:
                    req = {
                        "id": 9999,
                        "method": "Runtime.evaluate",
                        "params": {
                            "expression": r"""(() => {
                                const g = document.getElementById('iframeGame');
                                if (g && (g.offsetWidth > 0 || g.offsetHeight > 0)) {
                                    if (window.WebMainService && typeof window.WebMainService.backToGameHall === 'function') {
                                        window.WebMainService.backToGameHall();
                                        return 'returned_via_service';
                                    }
                                    if (g.contentDocument) {
                                        const btn = g.contentDocument.getElementById('goHome2') || g.contentDocument.getElementById('goHome');
                                        if (btn) {
                                            btn.click();
                                            return 'returned_via_btn';
                                        }
                                    }
                                }
                                return 'already_in_lobby';
                            })()""",
                            "returnByValue": True,
                        },
                    }
                    await ws.send(json.dumps(req))
                    await asyncio.wait_for(ws.recv(), timeout=3.0)
            except Exception as exc:
                logger.debug("return_to_lobby error: %s", exc)

        with contextlib.suppress(Exception):
            asyncio.run(run())

    def _worker_run(
        self,
        orders: list[BetOrder],
        on_status: Callable[[str], None] | None,
        on_order_done: Callable[[BetResult], None] | None,
        on_finished: Callable[[list[BetResult]], None] | None,
        on_audit: AutoBetAuditCallback | None,
    ) -> None:
        with self._lock:
            if self._is_running:
                if on_status:
                    on_status("Đang có một tiến trình đặt cược chạy nền khác.")
                results = []
                for order in orders:
                    message = "Executor đang xử lý một lệnh khác; yêu cầu này không được thực thi."
                    emit_autobet_audit(
                        on_audit,
                        order,
                        stage="EXECUTOR_REJECTED",
                        status="skipped",
                        reason_code="EXECUTOR_BUSY",
                        message=message,
                        countdown_seconds=order.countdown_at_signal,
                    )
                    result = BetResult(
                        order=order,
                        success=False,
                        message=message,
                        reason_code="EXECUTOR_BUSY",
                    )
                    results.append(result)
                    if on_order_done:
                        on_order_done(result)
                if on_finished:
                    on_finished(results)
                return
            self._is_running = True

        results: list[BetResult] = []
        for order in orders:
            emit_autobet_audit(
                on_audit,
                order,
                stage="EXECUTOR_STARTED",
                status="running",
                reason_code="EXECUTOR_STARTED",
                message="Worker Auto-Bet bắt đầu xử lý lệnh.",
                countdown_seconds=order.countdown_at_signal,
            )
        try:
            if on_audit is None:
                results = asyncio.run(self._async_execute_orders(orders, on_status, on_order_done))
            else:
                results = asyncio.run(self._async_execute_orders(orders, on_status, on_order_done, on_audit))
        except Exception as exc:
            logger.exception("Lỗi khi thực thi auto-bet: %s", exc)
            if on_status:
                on_status(f"Lỗi đặt cược tự động: {exc}")
            completed_ids = {result.order.attempt_id for result in results}
            for order in orders:
                if order.attempt_id in completed_ids:
                    continue
                message = f"Executor phát sinh lỗi: {exc}"
                emit_autobet_audit(
                    on_audit,
                    order,
                    stage="EXECUTOR_FAILED",
                    status="failed",
                    reason_code="EXECUTOR_EXCEPTION",
                    message=message,
                )
                result = BetResult(
                    order=order,
                    success=False,
                    message=message,
                    reason_code="EXECUTOR_EXCEPTION",
                )
                results.append(result)
                if on_order_done:
                    on_order_done(result)
        finally:
            with self._lock:
                self._is_running = False
            if on_finished:
                on_finished(results)

    async def _async_execute_orders(
        self,
        orders: list[BetOrder],
        on_status: Callable[[str], None] | None,
        on_order_done: Callable[[BetResult], None] | None,
        on_audit: AutoBetAuditCallback | None = None,
    ) -> list[BetResult]:
        def failed_results(reason_code: str, message: str) -> list[BetResult]:
            failures: list[BetResult] = []
            for order in orders:
                emit_autobet_audit(
                    on_audit,
                    order,
                    stage="EXECUTOR_FAILED",
                    status="failed",
                    reason_code=reason_code,
                    message=message,
                )
                result = BetResult(
                    order=order,
                    success=False,
                    message=message,
                    reason_code=reason_code,
                )
                failures.append(result)
                if on_order_done:
                    on_order_done(result)
            return failures

        try:
            import websockets
        except ImportError:
            msg = "websockets chưa được cài đặt. Chạy: pip install websockets"
            if on_status:
                on_status(msg)
            return failed_results("DEPENDENCY_MISSING", msg)

        ws_url = find_game_websocket_url(self.cdp_url)
        if not ws_url:
            msg = "Không tìm thấy phiên game AE Sexy trên Chrome CDP. Vui lòng đảm bảo sảnh đã mở."
            if on_status:
                on_status(msg)
            return failed_results("CDP_TARGET_NOT_FOUND", msg)

        if on_status:
            on_status(f"Đã kết nối CDP sảnh game. Bắt đầu xử lý {len(orders)} lệnh cược...")
        for order in orders:
            emit_autobet_audit(
                on_audit,
                order,
                stage="CDP_CONNECTED",
                status="running",
                reason_code="CDP_CONNECTED",
                message="Đã kết nối CDP tới trang game.",
            )

        results: list[BetResult] = []
        ack_monitor = ProviderAckMonitor(ws_url)
        if not await ack_monitor.start():
            reason = "ACK_MONITOR_UNAVAILABLE"
            message = (
                "Không khởi tạo được bộ thu phản hồi nhà cung cấp; "
                "đã ghi nhận nguyên nhân và không thực thi yêu cầu này."
            )
            if on_status:
                on_status(message)
            return failed_results(reason, message)
        async with websockets.connect(ws_url, max_size=12_000_000) as ws:
            msg_id = 0

            async def evaluate_js(expression: str) -> Any:
                nonlocal msg_id
                msg_id += 1
                call_id = msg_id
                req = {
                    "id": call_id,
                    "method": "Runtime.evaluate",
                    "params": {
                        "expression": expression,
                        "returnByValue": True,
                        "awaitPromise": True,
                    },
                }
                await ws.send(json.dumps(req))
                while True:
                    resp_raw = await ws.recv()
                    resp = json.loads(resp_raw)
                    if resp.get("id") == call_id:
                        result_obj = resp.get("result", {})
                        if "exceptionDetails" in result_obj:
                            desc = result_obj["exceptionDetails"].get("text", "JS Exception")
                            logger.debug("CDP JS Error: %s", desc)
                            return None
                        return result_obj.get("result", {}).get("value")

            async def dispatch_native_click(x: float, y: float) -> None:
                nonlocal msg_id
                for event_type in ("mousePressed", "mouseReleased"):
                    msg_id += 1
                    call_id = msg_id
                    req = {
                        "id": call_id,
                        "method": "Input.dispatchMouseEvent",
                        "params": {
                            "type": event_type,
                            "x": float(x),
                            "y": float(y),
                            "button": "left",
                            "clickCount": 1,
                        },
                    }
                    await ws.send(json.dumps(req))
                    while True:
                        resp_raw = await ws.recv()
                        resp = json.loads(resp_raw)
                        if resp.get("id") == call_id:
                            break
                    if event_type == "mousePressed":
                        await asyncio.sleep(0.08)

            # First ensure we are at the lobby (iframeGameHall visible)
            await self._ensure_lobby(evaluate_js, on_status)

            for idx, order in enumerate(orders, start=1):
                side_label = "Con (Player)" if normalize_bet_side(order.side) == "PLAYER" else "Cái (Banker)"
                round_str = f" [Ván {order.target_round_no}]" if order.target_round_no else ""
                if on_status:
                    on_status(
                        f"[{idx}/{len(orders)}] Bắt đầu cược bàn {order.table_name}: "
                        f"{side_label}{round_str} stake {order.stake:g} điểm"
                    )
                result = await self._execute_single_order(
                    order,
                    evaluate_js,
                    on_status,
                    on_audit,
                    ack_waiter=ack_monitor.wait_for_ack,
                    ack_marker=ack_monitor.mark,
                    dispatch_click=dispatch_native_click,
                )
                results.append(result)
                if on_order_done:
                    on_order_done(result)
                # Small pause between tables
                await asyncio.sleep(1.5)

        await ack_monitor.close()
        return results

    async def _ensure_lobby(
        self,
        eval_js: Callable[[str], Any],
        on_status: Callable[[str], None] | None,
    ) -> None:
        """Ensure the browser is currently showing the lobby (iframeGameHall)."""
        js = r"""(() => {
            const g = document.getElementById('iframeGame');
            if (g && (g.offsetWidth > 0 || g.offsetHeight > 0)) {
                if (window.WebMainService && typeof window.WebMainService.backToGameHall === 'function') {
                    window.WebMainService.backToGameHall();
                    return 'clicked_home';
                }
                if (g.contentDocument) {
                    const homeBtn = g.contentDocument.getElementById('goHome2') || g.contentDocument.getElementById('goHome');
                    if (homeBtn) {
                        homeBtn.click();
                        return 'clicked_home';
                    }
                }
            }
            return 'already_in_lobby';
        })()"""
        status = await eval_js(js)
        if status == "clicked_home":
            if on_status:
                on_status("Đang đóng bàn hiện tại để quay về sảnh danh sách bàn...")
            wait_start = time.monotonic()
            while time.monotonic() - wait_start < 5.0:
                is_lobby = await eval_js(r"""(() => {
                    const g = document.getElementById('iframeGame');
                    const gh = document.getElementById('iframeGameHall');
                    return (!g || g.offsetWidth === 0) && !!(gh && gh.contentDocument && gh.contentDocument.body);
                })()""")
                if is_lobby:
                    break
                await asyncio.sleep(0.2)
            await asyncio.sleep(0.5)

    async def _execute_single_order(
        self,
        order: BetOrder,
        eval_js: Callable[[str], Any],
        on_status: Callable[[str], None] | None,
        on_audit: AutoBetAuditCallback | None = None,
        *,
        ack_waiter: ProviderAckWaiter | None = None,
        ack_marker: Callable[[], float] | None = None,
        dispatch_click: Callable[[float, float], Any] | None = None,
    ) -> BetResult:
        started_at = datetime.now(timezone.utc).isoformat()
        norm_target = normalize_table_name(order.table_name)
        emit_autobet_audit(
            on_audit,
            order,
            stage="ORDER_STARTED",
            status="running",
            reason_code="ORDER_STARTED",
            message="Bắt đầu xử lý lệnh tại bàn mục tiêu.",
            countdown_seconds=order.countdown_at_signal,
        )

        # Step 1: Find and click table card in iframeGameHall (supports virtual scroller)
        enter_js = f"""(async () => {{
            const gh = document.getElementById('iframeGameHall');
            if (!gh || !gh.contentDocument) return {{ error: 'Khong tim thay sảnh iframeGameHall' }};
            const doc = gh.contentDocument;
            const scroller = doc.querySelector('.vue-recycle-scroller') || doc.documentElement;

            const targetNorm = '{norm_target}';

            function triggerClick(el) {{
                if (!el) return;
                try {{ el.focus(); }} catch (e) {{}}
                const win = el.ownerDocument.defaultView || window;
                const rect = el.getBoundingClientRect();
                const cx = Math.round(rect.left + rect.width / 2);
                const cy = Math.round(rect.top + rect.height / 2);
                const opts = {{
                    bubbles: true,
                    cancelable: true,
                    view: win,
                    clientX: cx,
                    clientY: cy,
                    screenX: cx,
                    screenY: cy,
                    buttons: 1
                }};
                el.dispatchEvent(new PointerEvent('pointerdown', opts));
                el.dispatchEvent(new MouseEvent('mousedown', opts));
                el.dispatchEvent(new PointerEvent('pointerup', opts));
                el.dispatchEvent(new MouseEvent('mouseup', opts));
                el.click();
            }}

            function findCard() {{
                // 1. Search cursor-pointer card containers first
                const cards = Array.from(doc.querySelectorAll('.cursor-pointer, [class*="card"]'));
                for (const card of cards) {{
                    const lines = (card.innerText || '').split('\\n').map(s => s.trim()).filter(Boolean);
                    for (const line of lines) {{
                        const clean = line.replace(/[^a-zA-Z0-9]/g, '').toLowerCase().replace('baccarat', '');
                        const norm = clean.replace(/c0*(\\d+)/, 'c$1');
                        if (norm === targetNorm || line.toLowerCase() === '{order.table_name.lower()}') {{
                            return card;
                        }}
                    }}
                }}
                // 2. Search leaf text elements
                const leafs = Array.from(doc.querySelectorAll('span, div, p')).filter(el => el.children.length === 0);
                for (const el of leafs) {{
                    const txt = (el.innerText || '').trim();
                    if (!txt) continue;
                    const clean = txt.replace(/[^a-zA-Z0-9]/g, '').toLowerCase().replace('baccarat', '');
                    const norm = clean.replace(/c0*(\\d+)/, 'c$1');
                    if (norm === targetNorm || txt.toLowerCase() === '{order.table_name.lower()}') {{
                        const card = el.closest('.cursor-pointer') || el.closest('[class*="card"]') || el;
                        return card;
                    }}
                }}
                return null;
            }}

            // Check immediately first without scrolling
            let found = findCard();
            if (!found && scroller) {{
                scroller.scrollTop = 0;
                await new Promise(r => setTimeout(r, 100));
                found = findCard();
                if (!found) {{
                    const maxScroll = scroller.scrollHeight;
                    const step = scroller.clientHeight * 0.7;
                    while (scroller.scrollTop + scroller.clientHeight < maxScroll + 50) {{
                        scroller.scrollTop += step;
                        await new Promise(r => setTimeout(r, 150));
                        found = findCard();
                        if (found) break;
                    }}
                }}
            }}

            if (!found) return {{ error: 'Khong tim thay the ban: ' + targetNorm }};
            found.scrollIntoView({{ block: 'center' }});
            triggerClick(found);
            return {{ success: true }};
        }})()"""

        click_res = await eval_js(enter_js)
        if not click_res or click_res.get("error"):
            err = click_res.get("error") if click_res else "Lỗi tìm thẻ bàn"
            if on_status:
                on_status(f"❌ {order.table_name}: {err}")
            emit_autobet_audit(
                on_audit,
                order,
                stage="TABLE_NAVIGATION_FAILED",
                status="failed",
                reason_code="TABLE_NOT_FOUND",
                message=err,
            )
            return BetResult(
                order=order,
                success=False,
                message=err,
                started_at=started_at,
                reason_code="TABLE_NOT_FOUND",
            )

        emit_autobet_audit(
            on_audit,
            order,
            stage="TABLE_SELECTED",
            status="running",
            reason_code="TABLE_SELECTED",
            message="Đã click thẻ bàn mục tiêu.",
        )

        if on_status:
            on_status(f"Đã chọn bàn {order.table_name}. Đang tải giao diện bàn cược...")

        # Fast poll until iframeGame is visible AND loaded with the target table (max 8.0s)
        load_start = time.monotonic()
        while time.monotonic() - load_start < 8.0:
            ready = await eval_js(f"""(() => {{
                const g = document.getElementById('iframeGame');
                if (!g || !g.contentDocument) return false;
                if (g.offsetWidth !== undefined && g.offsetWidth <= 0 && g.offsetHeight <= 0) return false;
                const doc = g.contentDocument;

                const tableEl = doc.getElementById('currentGameTable');
                if (tableEl) {{
                    const curTxt = (tableEl.innerText || '').trim();
                    const clean = curTxt.replace(/[^a-zA-Z0-9]/g, '').toLowerCase().replace('baccarat', '').replace(/c0*(\\d+)/, 'c$1');
                    if (clean && clean !== '{norm_target}') return false;
                }}

                return !!(doc.getElementById('betBoxPlayer') || doc.getElementById('betBoxBanker') || doc.getElementById('countdownTime'));
            }})()""")
            if ready:
                break
            await asyncio.sleep(0.08)

        # Step 2: Wait for singleBacTable and betting open countdown
        wait_start = time.monotonic()
        is_open = False
        last_countdown = -1
        mismatched_round = False
        mismatch_reason = ""
        mismatch_code = "ROUND_PASSED"
        too_late = False
        too_late_reason = ""
        table_state_logged = False
        last_table_shoe: str | int | None = None
        last_table_round: int | None = None

        while time.monotonic() - wait_start < 35.0:
            check_state_js = f"""(() => {{
                const g = document.getElementById('iframeGame');
                if (!g || !g.contentDocument) return {{ error: 'Loading table' }};
                if (g.offsetWidth !== undefined && g.offsetWidth <= 0 && g.offsetHeight <= 0) return {{ error: 'Table hidden' }};
                const doc = g.contentDocument;

                const tableEl = doc.getElementById('currentGameTable');
                if (tableEl) {{
                    const curTxt = (tableEl.innerText || '').trim();
                    const clean = curTxt.replace(/[^a-zA-Z0-9]/g, '').toLowerCase().replace('baccarat', '').replace(/c0*(\\d+)/, 'c$1');
                    if (clean && clean !== '{norm_target}') return {{ error: 'Waiting for target table transition' }};
                }}

                const playerBox = doc.getElementById('betBoxPlayer');
                const bankerBox = doc.getElementById('betBoxBanker');
                if (!playerBox || !bankerBox) return {{ error: 'Loading elements' }};

                const cdTime = doc.getElementById('countdownTime');
                const cd = doc.getElementById('countdown');
                const rawTxt = cdTime ? (cdTime.innerText || '').trim() : '';
                const m = rawTxt.match(/^(\\d+)$/);
                const countSec = m ? parseInt(m[1], 10) : -1;

                const cdClass = cd ? (cd.className || '') : '';
                const bodyText = (doc.body ? doc.body.innerText : '');
                const isDealing = cdClass.includes('progress_result') || rawTxt.includes('Mở bài') || bodyText.includes('Đang mở bài');
                const isShuffling = rawTxt.includes('xào bài') || bodyText.includes('xào bài') || bodyText.includes('Xào bài');

                // Read table shoe and round from #currentShoeRound (e.g. 'Trò chơi 22914 / 21')
                const shoeRoundEl = doc.getElementById('currentShoeRound') || doc.querySelector('[id*="ShoeRound"]');
                const shoeRoundTxt = shoeRoundEl ? (shoeRoundEl.innerText || '').trim() : '';
                let tableShoe = null;
                let tableRound = null;
                if (shoeRoundTxt) {{
                    const sm = shoeRoundTxt.match(/(\\d+)\\s*[/／]\\s*(\\d+)/);
                    if (sm) {{
                        tableShoe = sm[1];
                        tableRound = parseInt(sm[2], 10);
                    }}
                }}
                if (tableRound === null) {{
                    const mRound = bodyText.match(/(?:Trò chơi|Shoe|Game)\\s*(\\d+)\\s*[/／]\\s*(\\d+)/i);
                    if (mRound) {{
                        tableShoe = mRound[1];
                        tableRound = parseInt(mRound[2], 10);
                    }}
                }}

                return {{
                    loaded: true,
                    countdown: countSec,
                    rawText: rawTxt,
                    isDealing: isDealing,
                    isShuffling: isShuffling,
                    tableShoe: tableShoe,
                    tableRound: tableRound
                }};
            }})()"""

            state = await eval_js(check_state_js)
            if isinstance(state, dict) and state.get("loaded"):
                sec = state.get("countdown", -1)
                is_dealing = state.get("isDealing", False)
                is_shuffling = state.get("isShuffling", False)
                table_shoe = state.get("tableShoe")
                table_round = state.get("tableRound")
                last_countdown = sec
                last_table_shoe = table_shoe
                last_table_round = table_round

                if not table_state_logged:
                    emit_autobet_audit(
                        on_audit,
                        order,
                        stage="TABLE_OPENED",
                        status="running",
                        reason_code="TABLE_OPENED",
                        message="Đã đọc được trạng thái bàn mục tiêu.",
                        countdown_seconds=sec if sec >= 0 else None,
                        table_shoe=table_shoe,
                        table_round_no=table_round,
                    )
                    table_state_logged = True

                if is_shuffling:
                    if on_status:
                        on_status(f"⚠ {order.table_name}: Bàn đang xào bài.")
                    await self._exit_table_to_lobby(eval_js)
                    message = "Bàn đang xào bài, bỏ qua lượt này"
                    emit_autobet_audit(
                        on_audit,
                        order,
                        stage="ORDER_SKIPPED",
                        status="skipped",
                        reason_code="TABLE_SHUFFLING",
                        message=message,
                        countdown_seconds=sec if sec >= 0 else None,
                        table_shoe=table_shoe,
                        table_round_no=table_round,
                    )
                    return BetResult(
                        order=order,
                        success=False,
                        message=message,
                        started_at=started_at,
                        reason_code="TABLE_SHUFFLING",
                    )

                # Round Safety Check 1: Shoe changed
                if order.target_shoe and table_shoe and str(table_shoe) != str(order.target_shoe):
                    mismatch_code = "SHOE_MISMATCH"
                    mismatch_reason = (
                        f"Giày bài đã thay đổi (bàn: {table_shoe}, mục tiêu: {order.target_shoe}). "
                        "Thoát bàn an toàn, không đặt cược."
                    )
                    mismatched_round = True
                    break

                # Round Safety Check 2: Table round has already passed target round
                if order.target_round_no is not None and table_round is not None:
                    if table_round > order.target_round_no:
                        mismatch_reason = (
                            f"Ván bàn ({table_round}) đã vượt quá ván mục tiêu ({order.target_round_no}). "
                            "Thoát bàn an toàn, không đặt cược."
                        )
                        mismatched_round = True
                        break

                if not is_dealing:
                    # Round Safety Check 3: When open for betting, round must match target
                    if order.target_round_no is not None:
                        if table_round is None:
                            # Round number element not yet rendered/parsed; wait for next tick
                            await asyncio.sleep(0.1)
                            continue
                        if table_round < order.target_round_no:
                            # Table still transitioning or countdown just started; wait for target round
                            await asyncio.sleep(0.1)
                            continue
                        if table_round > order.target_round_no:
                            mismatch_reason = (
                                f"Ván mở cược ({table_round}) đã vượt quá ván mục tiêu ({order.target_round_no}). "
                                "Thoát bàn an toàn, không đặt cược."
                            )
                            mismatched_round = True
                            break
                    if sec >= LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS:
                        is_open = True
                        last_countdown = sec
                        break
                    if 0 <= sec < LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS:
                        too_late_reason = (
                            f"Countdown chỉ còn {sec}s, dưới ngưỡng an toàn "
                            f"{LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS}s. Không đặt cược."
                        )
                        too_late = True
                        break

            await asyncio.sleep(0.15)

        if mismatched_round:
            if on_status:
                on_status(f"⚠ {order.table_name}: {mismatch_reason}")
            await self._exit_table_to_lobby(eval_js)
            emit_autobet_audit(
                on_audit,
                order,
                stage="ORDER_SKIPPED",
                status="skipped",
                reason_code=mismatch_code,
                message=mismatch_reason,
                countdown_seconds=last_countdown if last_countdown >= 0 else None,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
            )
            return BetResult(
                order=order,
                success=False,
                message=mismatch_reason,
                started_at=started_at,
                reason_code=mismatch_code,
            )

        if too_late:
            if on_status:
                on_status(f"⚠ {order.table_name}: {too_late_reason}")
            await self._exit_table_to_lobby(eval_js)
            emit_autobet_audit(
                on_audit,
                order,
                stage="ORDER_SKIPPED",
                status="skipped",
                reason_code="COUNTDOWN_BELOW_4_AT_TABLE",
                message=too_late_reason,
                countdown_seconds=last_countdown if last_countdown >= 0 else None,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
            )
            return BetResult(
                order=order,
                success=False,
                message=too_late_reason,
                started_at=started_at,
                reason_code="COUNTDOWN_BELOW_4_AT_TABLE",
            )

        if not is_open:
            if on_status:
                on_status(
                    f"⚠ {order.table_name}: Hết thời gian chờ mở cược "
                    f"(countdown < {LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS}s hoặc bàn chưa mở cược)."
                )
            await self._exit_table_to_lobby(eval_js)
            message = "Hết thời gian chờ mở cược (bàn đang chia bài hoặc hết thời gian)"
            emit_autobet_audit(
                on_audit,
                order,
                stage="ORDER_FAILED",
                status="failed",
                reason_code="BETTING_WINDOW_TIMEOUT",
                message=message,
                countdown_seconds=last_countdown if last_countdown >= 0 else None,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
            )
            return BetResult(
                order=order,
                success=False,
                message=message,
                started_at=started_at,
                reason_code="BETTING_WINDOW_TIMEOUT",
            )

        if on_status:
            on_status(f"Bàn {order.table_name}: Thời gian cược còn {last_countdown}s. Đang đặt cược & xác nhận...")
        emit_autobet_audit(
            on_audit,
            order,
            stage="PLACEMENT_STARTED",
            status="running",
            reason_code="PLACEMENT_STARTED",
            message="Bắt đầu chọn chip và xác nhận cược.",
            countdown_seconds=last_countdown,
            table_shoe=last_table_shoe,
            table_round_no=last_table_round,
        )

        # Step 3: Select Chip, Place Bet, and Click Confirm (Atomic single transaction in browser)
        target_stake_num = int(round(float(order.stake)))
        chips = map_stake_to_chips(order.stake)
        norm_side = normalize_bet_side(order.side)

        chips_json = json.dumps(chips)
        place_bet_template = r"""(async () => {
            const g = document.getElementById('iframeGame');
            if (!g || !g.contentDocument) return { error: 'Mat ket noi ban (khong thay iframeGame)' };
            const doc = g.contentDocument;
            const win = g.contentWindow || doc.defaultView || window;

            const chipsToClick = __CHIPS_TO_CLICK__;
            const targetStake = __TARGET_STAKE__;
            const targetSide = '__TARGET_SIDE__';
            const betBoxId = targetSide === 'PLAYER' ? 'betBoxPlayer' : 'betBoxBanker';
            const betBox = doc.getElementById(betBoxId);

            if (!betBox) return { error: 'Khong tim thay o cuoc ' + betBoxId };

            function sleep(ms) {
                return new Promise(r => setTimeout(r, ms));
            }

            function triggerClick(el) {
                if (!el) return;
                try { el.focus(); } catch (e) {}
                const elWin = el.ownerDocument.defaultView || win;
                const rect = el.getBoundingClientRect();
                const cx = Math.round(rect.left + rect.width / 2);
                const cy = Math.round(rect.top + rect.height / 2);
                const opts = {
                    bubbles: true,
                    cancelable: true,
                    view: elWin,
                    clientX: cx,
                    clientY: cy,
                    screenX: cx,
                    screenY: cy,
                    buttons: 1
                };
                el.dispatchEvent(new PointerEvent('pointerdown', opts));
                el.dispatchEvent(new MouseEvent('mousedown', opts));
                el.dispatchEvent(new PointerEvent('pointerup', opts));
                el.dispatchEvent(new MouseEvent('mouseup', opts));
                el.click();
            }

            // 1. Inspect table tray (#chips) for active chip denominations
            let trayEls = Array.from(doc.querySelectorAll('#chips .chips3d'));
            if (trayEls.length === 0) {
                trayEls = Array.from(doc.querySelectorAll('#chips > div, #chips > li, #chips [class*="chip"]'));
                trayEls = trayEls.filter(el => !trayEls.some(p => p !== el && p.contains(el)));
            }
            const availableChips = [];
            const seenEls = new Set();

            for (const el of trayEls) {
                if (seenEls.has(el)) continue;
                let key = '';
                let val = 0;

                const bgEl = el.querySelector('.chips3d_bg') || el.querySelector('[class*="bg"]');
                const rawTxt = ((bgEl ? bgEl.innerText : null) || el.innerText || el.textContent || '').trim().replace(/,/g, '').toLowerCase();
                const tm = rawTxt.match(/^([0-9.]+)\s*([km])?$/i);
                if (tm) {
                    const num = parseFloat(tm[1]);
                    const unit = (tm[2] || '').toLowerCase();
                    if (unit === 'm') val = num * 1000000;
                    else if (unit === 'k') val = num * 1000;
                    else val = num;
                    key = rawTxt;
                }

                if (!val || isNaN(val)) {
                    const cls = el.className || '';
                    const m = cls.match(/(?:chips3d|chips|chip)[_-]([0-9]+)([km])?/i);
                    if (m) {
                        let num = parseFloat(m[1]);
                        let u = (m[2] || '').toLowerCase();
                        if (u === 'm') val = num * 1000000;
                        else if (u === 'k') val = num * 1000;
                        else val = num;
                        key = m[1] + (u || '');
                    }
                }

                if (!val || isNaN(val)) {
                    const elId = el.id || '';
                    const idMatch = elId.match(/(?:chips3d|chips|chip)[_-]([0-9]+)([km])?/i);
                    if (idMatch) {
                        let num = parseFloat(idMatch[1]);
                        let u = (idMatch[2] || '').toLowerCase();
                        if (u === 'm') val = num * 1000000;
                        else if (u === 'k') val = num * 1000;
                        else val = num;
                        key = idMatch[1] + (u || '');
                    }
                }

                if (!val || isNaN(val)) {
                    const dVal = el.getAttribute('data-value') || el.getAttribute('data-chip') || el.getAttribute('data-amount');
                    if (dVal) {
                        const num = parseFloat(dVal);
                        if (!isNaN(num) && num > 0) {
                            val = (num >= 1000 && num % 1000 === 0 && num > 20000) ? num / 1000 : num;
                            key = String(num);
                        }
                    }
                }

                if (val > 0 && !isNaN(val)) {
                    seenEls.add(el);
                    availableChips.push({ key, val, el });
                }
            }
            availableChips.sort((a, b) => b.val - a.val);

            // 2. Build multi-click bet plan based on available tray chips
            let plan = [];
            let minChipWarning = null;
            if (availableChips.length > 0 && targetStake > 0) {
                let remaining = targetStake;
                for (const chip of availableChips) {
                    if (remaining >= chip.val) {
                        const count = Math.floor(remaining / chip.val);
                        plan.push({ chip, count });
                        remaining -= count * chip.val;
                    }
                }
                if (plan.length === 0 && availableChips.length > 0) {
                    const minChip = availableChips[availableChips.length - 1];
                    plan.push({ chip: minChip, count: 1 });
                    if (minChip.val > targetStake) {
                        minChipWarning = 'Phỉnh nhỏ nhất hiện có trên bàn là ' + minChip.val + ' điểm (lớn hơn stake ' + targetStake + ' điểm)';
                    }
                }
            }

            // 3. Execute clicks
            if (plan.length > 0) {
                for (const step of plan) {
                    triggerClick(step.chip.el);
                    const bg = step.chip.el.querySelector('.chips3d_bg') || step.chip.el.querySelector('[class*="bg"]');
                    if (bg) triggerClick(bg);
                    await sleep(100);
                    for (let i = 0; i < step.count; i++) {
                        triggerClick(betBox);
                        if (step.count > 1) await sleep(80);
                    }
                }
            } else {
                function findFallbackChip(chipKey) {
                    const cleanKey = String(chipKey).replace(/[km]/gi, '');
                    return doc.querySelector('.chips3d-' + chipKey)
                        || doc.querySelector('.chips3d-' + cleanKey)
                        || doc.querySelector('.chips3d-' + cleanKey + 'k')
                        || doc.querySelector('#Chips_' + chipKey)
                        || doc.querySelector('#Chips_' + cleanKey)
                        || doc.querySelector('#Chips_' + cleanKey + 'k')
                        || doc.querySelector('[class*="chips3d-' + chipKey + '"]')
                        || doc.querySelector('[class*="chips3d-' + cleanKey + '"]')
                        || Array.from(doc.querySelectorAll('#chips [class*="chip"], #chips .chips3d, #chips li')).find(e => {
                            const t = (e.innerText || '').trim().toLowerCase();
                            return t === chipKey || t === cleanKey || t === (cleanKey + 'k');
                        });
                }

                for (const chipKey of chipsToClick) {
                    const chipEl = findFallbackChip(chipKey);
                    if (chipEl) {
                        triggerClick(chipEl);
                        const bg = chipEl.querySelector('.chips3d_bg') || chipEl.querySelector('[class*="bg"]');
                        if (bg) triggerClick(bg);
                        await sleep(100);
                    }
                    triggerClick(betBox);
                    await sleep(80);
                }
            }

            // 4. Fast-poll until the unconfirmed bet is visible and #confirm is enabled.
            // The actual confirm click happens in a second, immediately revalidated CDP call.
            let cf = doc.getElementById('confirm');
            let confirmReady = false;
            for (let i = 0; i < 30; i++) {
                if (cf && !cf.className.includes('disabled')) {
                    confirmReady = true;
                    break;
                }
                await sleep(50);
                cf = doc.getElementById('confirm');
            }

            const chipBoxId = targetSide === 'PLAYER' ? 'chipBoxPlayer' : 'chipBoxBanker';
            const chipBox = doc.getElementById(chipBoxId);
            const amtEl = chipBox ? chipBox.querySelector('.chips2d_amount') : null;
            const placedAmt = amtEl ? amtEl.innerText.trim() : '';

            if (!confirmReady) {
                return {
                    error: 'Nut Xac nhan bi vo hieu hoa (disabled) - chua nhan phinh vao o cuoc',
                    errorCode: 'PRECONFIRM_CONFIRM_DISABLED',
                    placedAmount: placedAmt,
                    minChipWarning: minChipWarning
                };
            }

            return {
                success: true,
                placedAmount: placedAmt,
                minChipWarning: minChipWarning,
                readyToConfirm: true
            };
        })()"""

        place_bet_js = (
            place_bet_template
            .replace("__CHIPS_TO_CLICK__", chips_json)
            .replace("__TARGET_STAKE__", str(target_stake_num))
            .replace("__TARGET_SIDE__", norm_side)
        )

        place_res = await eval_js(place_bet_js)
        if not place_res or not place_res.get("success"):
            err = place_res.get("error") if place_res else "Lỗi đặt phỉnh vào ô cược"
            err_lower = str(err).lower()
            if "disabled" in err_lower or "xác nhận" in err_lower or "xac nhan" in err_lower:
                reason_code = "CONFIRM_DISABLED"
            elif "iframegame" in err_lower or "mất kết nối" in err_lower or "mat ket noi" in err_lower:
                reason_code = "TABLE_CONNECTION_LOST"
            elif "ô cược" in err_lower or "o cuoc" in err_lower or "betbox" in err_lower:
                reason_code = "BET_BOX_NOT_FOUND"
            else:
                reason_code = "PLACEMENT_FAILED"
            if on_status:
                on_status(f"❌ {order.table_name}: {err}")
            await self._exit_table_to_lobby(eval_js)
            emit_autobet_audit(
                on_audit,
                order,
                stage="PLACEMENT_FAILED",
                status="failed",
                reason_code=reason_code,
                message=err,
                countdown_seconds=last_countdown,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
                payload={"browser_result": place_res or {}},
            )
            return BetResult(
                order=order,
                success=False,
                message=err,
                started_at=started_at,
                reason_code=reason_code,
            )

        placed_amt = place_res.get("placedAmount", "")
        min_warning = place_res.get("minChipWarning")
        if min_warning and on_status:
            on_status(f"⚠ Bàn {order.table_name}: {min_warning}")
        side_vi = "Con (Player)" if norm_side == "PLAYER" else "Cái (Banker)"
        if placed_amt and on_status:
            on_status(
                f"Bàn {order.table_name}: Đã đặt phỉnh {placed_amt} vào ô {side_vi}; "
                "đang kiểm tra lại ngay trước khi click Xác nhận."
            )

        target_round_js = "null" if order.target_round_no is None else str(int(order.target_round_no))
        preconfirm_template = r"""(async () => {
            const preConfirmCheck = true;
            const g = document.getElementById('iframeGame');
            if (!g || !g.contentDocument) {
                return {error: 'Mat ket noi ban truoc khi xac nhan', errorCode: 'PRECONFIRM_TABLE_CONNECTION_LOST'};
            }
            const doc = g.contentDocument;
            const targetTable = __TARGET_TABLE__;
            const targetShoe = __TARGET_SHOE__;
            const targetRound = __TARGET_ROUND__;
            const targetStake = __TARGET_STAKE__;
            const targetSide = __TARGET_SIDE__;

            const tableEl = doc.getElementById('currentGameTable');
            if (!tableEl) {
                return {error: 'Khong doc duoc ban hien tai truoc khi xac nhan', errorCode: 'PRECONFIRM_TABLE_UNREADABLE'};
            }
            const currentTable = (tableEl.innerText || '').replace(/[^a-zA-Z0-9]/g, '').toLowerCase()
                .replace('baccarat', '').replace(/c0*(\d+)/, 'c$1');
            if (!currentTable) {
                return {error: 'Khong doc duoc ban hien tai truoc khi xac nhan', errorCode: 'PRECONFIRM_TABLE_UNREADABLE'};
            }
            if (currentTable !== targetTable) {
                return {error: 'Ban hien tai khong khop ban muc tieu', errorCode: 'PRECONFIRM_TABLE_MISMATCH'};
            }

            const cdTime = doc.getElementById('countdownTime');
            const countdownEl = doc.getElementById('countdown');
            const rawCountdown = cdTime ? (cdTime.innerText || '').trim() : '';
            const countdownMatch = rawCountdown.match(/^(\d+)$/);
            const countdown = countdownMatch ? parseInt(countdownMatch[1], 10) : -1;
            const countdownClass = countdownEl ? (countdownEl.className || '') : '';
            if (countdownClass.includes('progress_result') || countdown < 0) {
                return {
                    error: 'Cua cuoc da dong truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_BETTING_CLOSED',
                    countdown: countdown
                };
            }
            if (countdown < __MIN_COUNTDOWN__) {
                return {
                    error: 'Countdown duoi nguong an toan ngay truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_COUNTDOWN_BELOW_4',
                    countdown: countdown
                };
            }

            const shoeRoundEl = doc.getElementById('currentShoeRound') || doc.querySelector('[id*="ShoeRound"]');
            const shoeRoundText = shoeRoundEl ? (shoeRoundEl.innerText || '').trim() : '';
            const shoeRoundMatch = shoeRoundText.match(/(\d+)\s*[/／]\s*(\d+)/);
            const tableShoe = shoeRoundMatch ? shoeRoundMatch[1] : null;
            const tableRound = shoeRoundMatch ? parseInt(shoeRoundMatch[2], 10) : null;
            if (targetShoe && !tableShoe) {
                return {
                    error: 'Khong doc duoc shoe ngay truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_SHOE_UNREADABLE',
                    countdown: countdown
                };
            }
            if (targetShoe && String(tableShoe) !== String(targetShoe)) {
                return {
                    error: 'Shoe da thay doi truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_SHOE_MISMATCH',
                    countdown: countdown,
                    tableShoe: tableShoe,
                    tableRound: tableRound
                };
            }
            if (targetRound !== null && tableRound === null) {
                return {
                    error: 'Khong doc duoc round ngay truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_ROUND_UNREADABLE',
                    countdown: countdown,
                    tableShoe: tableShoe
                };
            }
            if (targetRound !== null && tableRound !== targetRound) {
                return {
                    error: 'Round khong khop ngay truoc khi xac nhan',
                    errorCode: 'PRECONFIRM_ROUND_MISMATCH',
                    countdown: countdown,
                    tableShoe: tableShoe,
                    tableRound: tableRound
                };
            }

            const chipBoxId = targetSide === 'PLAYER' ? 'chipBoxPlayer' : 'chipBoxBanker';
            const chipBox = doc.getElementById(chipBoxId);
            const amountEl = chipBox ? chipBox.querySelector('.chips2d_amount') : null;
            const placedAmountText = amountEl ? (amountEl.innerText || '').trim() : '';
            function parseAmount(text) {
                const normalized = String(text || '').trim().toLowerCase().replace(/,/g, '');
                const match = normalized.match(/\d+(?:\.\d+)?/);
                if (!match) return NaN;
                let value = parseFloat(match[0]);
                if (normalized.endsWith('k')) value *= 1000;
                if (normalized.endsWith('m')) value *= 1000000;
                return value;
            }
            const placedAmount = parseAmount(placedAmountText);
            if (!Number.isFinite(placedAmount) || Math.abs(placedAmount - targetStake) > 0.001) {
                return {
                    error: 'So tien tren o cuoc khong khop stake muc tieu',
                    errorCode: 'PRECONFIRM_AMOUNT_MISMATCH',
                    countdown: countdown,
                    tableShoe: tableShoe,
                    tableRound: tableRound,
                    placedAmount: placedAmountText
                };
            }

            const confirmButton = doc.getElementById('confirm');
            const confirmDisabled = !confirmButton
                || Boolean(confirmButton.disabled)
                || (confirmButton.className || '').includes('disabled')
                || confirmButton.getAttribute('aria-disabled') === 'true';
            if (confirmDisabled) {
                return {
                    error: 'Nut Xac nhan bi vo hieu hoa ngay truoc khi click',
                    errorCode: 'PRECONFIRM_CONFIRM_DISABLED',
                    countdown: countdown,
                    tableShoe: tableShoe,
                    tableRound: tableRound,
                    placedAmount: placedAmountText
                };
            }

            function triggerClick(el) {
                try { el.focus(); } catch (e) {}
                const elWin = el.ownerDocument.defaultView || window;
                const rect = el.getBoundingClientRect();
                const cx = Math.round(rect.left + rect.width / 2);
                const cy = Math.round(rect.top + rect.height / 2);
                const opts = {
                    bubbles: true,
                    cancelable: true,
                    view: elWin,
                    clientX: cx,
                    clientY: cy,
                    screenX: cx,
                    screenY: cy,
                    buttons: 1
                };
                el.dispatchEvent(new PointerEvent('pointerdown', opts));
                if (typeof TouchEvent !== 'undefined' && typeof Touch !== 'undefined') {
                    try {
                        const touch = new Touch({
                            identifier: Date.now(),
                            target: el,
                            clientX: cx,
                            clientY: cy,
                            screenX: cx,
                            screenY: cy,
                            pageX: cx,
                            pageY: cy
                        });
                        el.dispatchEvent(new TouchEvent('touchstart', {
                            bubbles: true,
                            cancelable: true,
                            view: elWin,
                            touches: [touch],
                            targetTouches: [touch],
                            changedTouches: [touch]
                        }));
                    } catch (e) {}
                }
                el.dispatchEvent(new MouseEvent('mousedown', opts));
                el.dispatchEvent(new PointerEvent('pointerup', {...opts, buttons: 0}));
                if (typeof TouchEvent !== 'undefined' && typeof Touch !== 'undefined') {
                    try {
                        const touch = new Touch({
                            identifier: Date.now(),
                            target: el,
                            clientX: cx,
                            clientY: cy,
                            screenX: cx,
                            screenY: cy,
                            pageX: cx,
                            pageY: cy
                        });
                        el.dispatchEvent(new TouchEvent('touchend', {
                            bubbles: true,
                            cancelable: true,
                            view: elWin,
                            touches: [],
                            targetTouches: [],
                            changedTouches: [touch]
                        }));
                    } catch (e) {}
                }
                el.dispatchEvent(new MouseEvent('mouseup', {...opts, buttons: 0}));
                if (typeof el.onclick === 'function') {
                    try { el.onclick(); } catch (e) {}
                }
                el.click();
            }

            // Trigger jQuery click if present on iframe window
            try {
                const win = g.contentWindow || doc.defaultView || window;
                if (win && win.$ && typeof win.$(confirmButton).trigger === 'function') {
                    win.$(confirmButton).trigger('click');
                }
            } catch (e) {}
            triggerClick(confirmButton);

            // Wait a brief moment to allow UI to react and disable the button
            await new Promise(r => setTimeout(r, 120));

            // If confirm button is still enabled after click, retry click once
            const postClickDisabled = !confirmButton
                || Boolean(confirmButton.disabled)
                || (confirmButton.className || '').includes('disabled')
                || confirmButton.getAttribute('aria-disabled') === 'true';
            if (!postClickDisabled) {
                try {
                    const win = g.contentWindow || doc.defaultView || window;
                    if (win && win.$ && typeof win.$(confirmButton).trigger === 'function') {
                        win.$(confirmButton).trigger('click');
                    }
                } catch (e) {}
                triggerClick(confirmButton);
                await new Promise(r => setTimeout(r, 80));
            }

            let clickX = null;
            let clickY = null;
            try {
                const gRect = g.getBoundingClientRect();
                const btnRect = confirmButton.getBoundingClientRect();
                clickX = Math.round(gRect.left + btnRect.left + btnRect.width / 2);
                clickY = Math.round(gRect.top + btnRect.top + btnRect.height / 2);
            } catch (e) {}

            return {
                success: true,
                confirmed: true,
                countdown: countdown,
                tableShoe: tableShoe,
                tableRound: tableRound,
                placedAmount: placedAmountText,
                clickX: clickX,
                clickY: clickY
            };
        })()"""
        preconfirm_js = (
            preconfirm_template
            .replace("__TARGET_TABLE__", json.dumps(norm_target))
            .replace("__TARGET_SHOE__", json.dumps(str(order.target_shoe or "")))
            .replace("__TARGET_ROUND__", target_round_js)
            .replace("__TARGET_STAKE__", str(target_stake_num))
            .replace("__TARGET_SIDE__", json.dumps(norm_side))
            .replace("__MIN_COUNTDOWN__", str(LIVE_AUTOBET_MIN_COUNTDOWN_SECONDS))
        )
        ack_since = ack_marker() if ack_marker is not None else time.monotonic()
        confirm_res = await eval_js(preconfirm_js)
        if not isinstance(confirm_res, dict) or not confirm_res.get("success"):
            browser_result = confirm_res if isinstance(confirm_res, dict) else {}
            reason_code = str(browser_result.get("errorCode") or "PRECONFIRM_FAILED")
            message = str(browser_result.get("error") or "Kiểm tra cuối trước khi xác nhận không thành công.")
            actual_countdown = browser_result.get("countdown", last_countdown)
            actual_shoe = browser_result.get("tableShoe", last_table_shoe)
            actual_round = browser_result.get("tableRound", last_table_round)
            emit_autobet_audit(
                on_audit,
                order,
                stage="PRECONFIRM_FAILED",
                status="skipped",
                reason_code=reason_code,
                message=message,
                countdown_seconds=actual_countdown,
                table_shoe=actual_shoe,
                table_round_no=actual_round,
                payload={
                    "placed_amount": browser_result.get("placedAmount", placed_amt),
                    "expected_amount": target_stake_num,
                },
            )
            if on_status:
                on_status(f"❌ {order.table_name}: {message}")
            await self._exit_table_to_lobby(eval_js)
            return BetResult(
                order=order,
                success=False,
                message=message,
                started_at=started_at,
                reason_code=reason_code,
            )

        click_x = confirm_res.get("clickX") if isinstance(confirm_res, dict) else None
        click_y = confirm_res.get("clickY") if isinstance(confirm_res, dict) else None
        if dispatch_click is not None and click_x is not None and click_y is not None:
            try:
                await dispatch_click(float(click_x), float(click_y))
            except Exception as exc:
                logger.debug("Native CDP click failed: %s", exc)

        last_countdown = int(confirm_res.get("countdown", last_countdown))
        last_table_shoe = confirm_res.get("tableShoe", last_table_shoe)
        last_table_round = confirm_res.get("tableRound", last_table_round)
        placed_amt = confirm_res.get("placedAmount", placed_amt)
        emit_autobet_audit(
            on_audit,
            order,
            stage="PRECONFIRM_VALIDATED",
            status="running",
            reason_code="PRECONFIRM_VALIDATED",
            message="Đã xác minh lại đúng bàn, shoe, round, countdown, cửa cược và stake ngay trước click.",
            countdown_seconds=last_countdown,
            table_shoe=last_table_shoe,
            table_round_no=last_table_round,
            payload={"placed_amount": placed_amt, "expected_amount": target_stake_num},
        )

        confirm_clicked_at = datetime.now(timezone.utc).isoformat()
        click_message = (
            f"Đã click Xác nhận ở phía trình duyệt {order.table_name}: "
            f"{side_vi} - {order.stake:g} điểm; đang chờ phản hồi nhà cung cấp."
        )
        emit_autobet_audit(
            on_audit,
            order,
            stage="CONFIRM_CLICKED",
            status="confirm_clicked",
            reason_code="CONFIRM_CLICKED",
            message=click_message,
            countdown_seconds=last_countdown,
            table_shoe=last_table_shoe,
            table_round_no=last_table_round,
            payload={
                "placed_amount": placed_amt,
                "confirmed_dom_click": bool(confirm_res.get("confirmed")),
                "min_chip_warning": min_warning,
            },
        )
        if on_status:
            on_status(f"Bàn {order.table_name}: Đã click Xác nhận; đang chờ Provider ACK...")

        if ack_waiter is None:
            ack = ProviderAck(
                outcome="timeout",
                provider_status="ack_timeout",
                error_code="ACK_MONITOR_UNAVAILABLE",
                message="Không có bộ thu Provider ACK cho lần thực thi này.",
                source="none",
            )
        else:
            try:
                ack = await ack_waiter(order, ack_since, PROVIDER_ACK_TIMEOUT_SECONDS)
            except Exception as exc:
                logger.exception("Provider ACK waiter failed")
                ack = ProviderAck(
                    outcome="timeout",
                    provider_status="ack_timeout",
                    error_code="ACK_MONITOR_ERROR",
                    message=f"Bộ thu Provider ACK phát sinh lỗi: {exc}",
                    source="cdp_network",
                )

        # DOM Verification Fallback if network ACK did not return accepted
        if ack.outcome != "accepted":
            dom_check_template = r"""(() => {
                const g = document.getElementById('iframeGame');
                if (!g || !g.contentDocument) return null;
                const doc = g.contentDocument;
                
                // 1. Success message/toast
                const successEl = doc.querySelector('.message_success, .msg_success');
                const successText = successEl ? (successEl.innerText || '').trim() : '';
                
                // 2. Chip placed and confirmed on target side
                const targetSide = __TARGET_SIDE__;
                const chipBoxId = targetSide === 'PLAYER' ? 'chipBoxPlayer' : 'chipBoxBanker';
                const chipBox = doc.getElementById(chipBoxId);
                const amountEl = chipBox ? chipBox.querySelector('.chips2d_amount') : null;
                const placedAmountText = amountEl ? (amountEl.innerText || '').trim() : '';
                const parseAmount = (text) => {
                    const clean = String(text || '').toLowerCase().replace(/,/g, '');
                    const m = clean.match(/\d+(?:\.\d+)?/);
                    if (!m) return 0;
                    let v = parseFloat(m[0]);
                    if (clean.endsWith('k')) v *= 1000;
                    if (clean.endsWith('m')) v *= 1000000;
                    return v;
                };
                const placedAmt = parseAmount(placedAmountText);
                
                // 3. Confirm button state (disabled after confirm)
                const confirmBtn = doc.getElementById('confirm');
                const confirmDisabled = !confirmBtn || confirmBtn.disabled || (confirmBtn.className || '').includes('disabled');
                
                return {
                    hasSuccessToast: Boolean(successText),
                    successText: successText,
                    placedAmount: placedAmt,
                    confirmDisabled: confirmDisabled
                };
            })()"""
            dom_check_js = dom_check_template.replace("__TARGET_SIDE__", json.dumps(norm_side))
            try:
                dom_status = await eval_js(dom_check_js)
                if isinstance(dom_status, dict):
                    has_toast = bool(dom_status.get("hasSuccessToast"))
                    dom_amt = float(dom_status.get("placedAmount") or 0.0)
                    confirm_dis = bool(dom_status.get("confirmDisabled"))
                    target_stake = float(order.stake)
                    if has_toast or (confirm_dis and abs(dom_amt - target_stake) < 0.01):
                        ack = ProviderAck(
                            outcome="accepted",
                            provider_bet_id=ack.provider_bet_id or f"DOM-{norm_target}-{int(time.time())}",
                            provider_status="accepted",
                            source="cdp_dom",
                            table=order.table_name,
                            shoe=str(order.target_shoe or last_table_shoe),
                            round_no=str(order.target_round_no or last_table_round),
                            side=order.side,
                            amount=str(order.stake),
                            request_seen=True,
                            message=dom_status.get("successText") or "Đã xác thực cược thành công qua giao diện bàn chơi (DOM).",
                        )
            except Exception as exc:
                logger.debug("DOM ACK fallback error: %s", exc)

        ack_payload = ack.audit_payload()
        ack_payload.update(
            {
                "placed_amount": placed_amt,
                "confirmed_dom_click": True,
                "confirm_clicked_at": confirm_clicked_at,
            }
        )
        if ack.outcome == "accepted":
            provider_id = ack.provider_bet_id or "-"
            message = (
                f"Nhà cung cấp đã tiếp nhận cược {order.table_name}: {side_vi} - "
                f"{order.stake:g} điểm; Bet/Txn ID {provider_id}."
            )
            emit_autobet_audit(
                on_audit,
                order,
                stage="PROVIDER_ACCEPTED",
                status="provider_accepted",
                reason_code="PROVIDER_ACCEPTED",
                message=message,
                countdown_seconds=last_countdown,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
                payload=ack_payload,
            )
            if on_status:
                on_status(f"✅ {message}")
            if POST_CONFIRM_LOBBY_WAIT_SECONDS > 0:
                await asyncio.sleep(POST_CONFIRM_LOBBY_WAIT_SECONDS)
            await self._exit_table_to_lobby(eval_js)
            return BetResult(
                order=order,
                success=True,
                message=message,
                placed_at=confirm_clicked_at,
                started_at=started_at,
                confirm_clicked_at=confirm_clicked_at,
                reason_code="PROVIDER_ACCEPTED",
            )

        if ack.outcome == "rejected":
            error_detail = ack.error_code or ack.message or "PROVIDER_REJECTED"
            message = f"Nhà cung cấp từ chối cược {order.table_name}: {error_detail}."
            emit_autobet_audit(
                on_audit,
                order,
                stage="PROVIDER_REJECTED",
                status="provider_rejected",
                reason_code="PROVIDER_REJECTED",
                message=message,
                countdown_seconds=last_countdown,
                table_shoe=last_table_shoe,
                table_round_no=last_table_round,
                payload=ack_payload,
            )
            if on_status:
                on_status(f"❌ {message} Đã ghi nhận nguyên nhân; trạng thái Auto-Bet không thay đổi.")
            if POST_CONFIRM_LOBBY_WAIT_SECONDS > 0:
                await asyncio.sleep(POST_CONFIRM_LOBBY_WAIT_SECONDS)
            await self._exit_table_to_lobby(eval_js)
            return BetResult(
                order=order,
                success=False,
                message=message,
                started_at=started_at,
                confirm_clicked_at=confirm_clicked_at,
                reason_code="PROVIDER_REJECTED",
            )

        timeout_detail = (
            "đã thấy request cược nhưng không có ACK hợp lệ"
            if ack.request_seen
            else "không quan sát thấy request cược phát ra sau click"
        )
        message = (
            f"Hết thời gian chờ phản hồi nhà cung cấp tại {order.table_name}: {timeout_detail}."
        )
        emit_autobet_audit(
            on_audit,
            order,
            stage="ACK_TIMEOUT",
            status="ack_timeout",
            reason_code="ACK_TIMEOUT",
            message=message,
            countdown_seconds=last_countdown,
            table_shoe=last_table_shoe,
            table_round_no=last_table_round,
            payload=ack_payload,
        )
        if on_status:
            on_status(f"⚠ {message} Đã ghi nhận nguyên nhân; trạng thái Auto-Bet không thay đổi.")
        await self._exit_table_to_lobby(eval_js)
        return BetResult(
            order=order,
            success=False,
            message=message,
            started_at=started_at,
            confirm_clicked_at=confirm_clicked_at,
            reason_code="ACK_TIMEOUT",
        )

    async def _exit_table_to_lobby(self, eval_js: Callable[[str], Any]) -> None:
        """Exit table and return back to iframeGameHall."""
        js = r"""(() => {
            if (window.WebMainService && typeof window.WebMainService.backToGameHall === 'function') {
                window.WebMainService.backToGameHall();
                return true;
            }
            const g = document.getElementById('iframeGame');
            if (g) {
                if (g.contentWindow && g.contentWindow.parent && g.contentWindow.parent.WebMainService) {
                    try {
                        g.contentWindow.parent.WebMainService.backToGameHall();
                        return true;
                    } catch (e) {}
                }
                if (g.contentDocument) {
                    const btn = g.contentDocument.getElementById('goHome2') || g.contentDocument.getElementById('goHome');
                    if (btn) {
                        btn.click();
                        return true;
                    }
                }
            }
            return false;
        })()"""
        await eval_js(js)
        wait_start = time.monotonic()
        while time.monotonic() - wait_start < 4.0:
            lobby_ready = await eval_js(r"""(() => {
                const g = document.getElementById('iframeGame');
                const gh = document.getElementById('iframeGameHall');
                return (!g || g.offsetWidth === 0) && !!(gh && gh.contentDocument && gh.contentDocument.body);
            })()""")
            if lobby_ready:
                break
            await asyncio.sleep(0.1)
