from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from ..ae_decode import iter_payload_snapshots, looks_like_ae_payload
from ..models import TableSnapshot

logger = logging.getLogger(__name__)

SnapshotCallback = Callable[[TableSnapshot], None]
StatusCallback = Callable[[str], None]

TEXT_MIME_HINTS = (
    "application/json",
    "text/json",
    "text/plain",
    "javascript",
    "application/octet-stream",
)

RAW_TARGET_DOMAINS = (
    "mex777.com",
    "mhuxu.com",
    "vbgames88.com",
)
RAW_TARGET_EXCLUDED_DOMAINS = (
    "adform.net",
    "google.com",
    "stripe.com",
    "stripe.network",
    "supabase.com",
    "uuidksinc.net",
)

DOM_DATA_SCRIPT = r"""
() => {
  const needles = [
    "roadInfo",
    "markerRoads",
    "bigRoads",
    "beadRoads",
    "gameRound",
    "roadList",
    "roadMap",
    "history",
    "results",
    "gameResults",
    "rounds",
    "tableID",
    "tableId",
    "tableName"
  ];
  const maxChars = 250000;
  const out = [];
  const looksUseful = (value) => {
    if (!value) return false;
    const lower = String(value).toLowerCase();
    return needles.some((needle) => lower.includes(needle.toLowerCase()));
  };
  const push = (label, value) => {
    if (out.length >= 30 || value == null) return;
    let text = "";
    try {
      text = typeof value === "string" ? value : JSON.stringify(value);
    } catch {
      return;
    }
    if (looksUseful(text)) out.push(`${label}\n${text.slice(0, maxChars)}`);
  };

  for (const storeName of ["localStorage", "sessionStorage"]) {
    try {
      const store = window[storeName];
      for (let i = 0; i < store.length; i += 1) {
        const key = store.key(i);
        push(`${storeName}:${key}`, store.getItem(key));
      }
    } catch {}
  }

  try {
    for (const script of document.querySelectorAll("script[type='application/json'], script:not([src])")) {
      push("script", script.textContent || "");
      if (out.length >= 30) break;
    }
  } catch {}

  return out;
}
"""

RESOURCE_HINT_SCRIPT = r"""
() => performance.getEntriesByType("resource").map((entry) => entry.name).slice(-120)
"""


class AeSexyCdpMonitor:
    """Read AE SEXY table road data from Chrome CDP.

    This adapter is intentionally read-only. It listens to network traffic and
    reads page storage/script state, but never clicks, fills, or places bets.
    """

    def __init__(
        self,
        cdp_url: str,
        on_snapshot: SnapshotCallback,
        on_status: StatusCallback | None = None,
        *,
        poll_seconds: float = 2.0,
        dom_poll_seconds: float = 5.0,
        auto_refresh_seconds: float | None = None,
        max_payload_chars: int = 1_000_000,
    ) -> None:
        self.cdp_url = cdp_url
        self.on_snapshot = on_snapshot
        self.on_status = on_status or (lambda message: None)
        self.poll_seconds = poll_seconds
        self.dom_poll_seconds = dom_poll_seconds
        self.auto_refresh_seconds = max(0.0, float(auto_refresh_seconds or 0.0))
        self.max_payload_chars = max_payload_chars
        self._running = False
        self._seen_contexts: set[int] = set()
        self._seen_pages: set[int] = set()
        self._seen_raw_targets: set[str] = set()
        self._raw_tasks: set[asyncio.Task[None]] = set()
        self._seen_payload_hashes: set[str] = set()
        self._last_dom_poll: dict[int, float] = {}
        self._last_live_snapshot_at = 0.0
        self._last_watchdog_refresh_at = 0.0
        self._latest_snapshot_signatures: dict[str, str] = {}
        self._snapshot_count = 0
        self._started_at = 0.0
        self._last_no_snapshot_hint_at = 0.0

    async def run(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            self.on_status('Playwright is missing. Run: pip install -e ".[live]"')
            raise RuntimeError("Playwright is not installed") from exc

        self._running = True
        self._started_at = time.monotonic()
        self._last_live_snapshot_at = self._started_at
        self._last_watchdog_refresh_at = 0.0
        self._last_no_snapshot_hint_at = 0.0
        self.on_status(f"Connecting to Chrome CDP: {self.cdp_url}")
        async with async_playwright() as pw:
            try:
                browser = await pw.chromium.connect_over_cdp(self.cdp_url)
            except Exception as exc:
                self.on_status(
                    "Cannot connect Chrome CDP. Start Chrome with --remote-debugging-port=9222, "
                    "then open the live casino tab."
                )
                raise RuntimeError(f"Chrome CDP connection failed: {exc}") from exc

            self.on_status("CDP connected. Listening to WebSocket, XHR/Fetch, and page storage.")
            if self.auto_refresh_seconds > 0:
                self.on_status(
                    f"Live watchdog enabled: refresh only after "
                    f"{self.auto_refresh_seconds:.0f}s without a decoded snapshot."
                )
            try:
                while self._running:
                    await self._attach_raw_targets()
                    for context in browser.contexts:
                        await self._attach_context(context)
                        for page in context.pages:
                            await self._attach_page(page)
                            await self._poll_page_dom_if_due(page)
                            await self._refresh_page_if_due(page)
                    self._emit_no_snapshot_hint_if_due()
                    await asyncio.sleep(self.poll_seconds)
            finally:
                self._running = False
                for task in list(self._raw_tasks):
                    task.cancel()
                if self._raw_tasks:
                    await asyncio.gather(*self._raw_tasks, return_exceptions=True)
                with contextlib.suppress(Exception):
                    await browser.close()
                self.on_status("CDP monitor stopped")

    def stop(self) -> None:
        self._running = False

    async def _attach_context(self, context: Any) -> None:
        context_key = id(context)
        if context_key in self._seen_contexts:
            return
        self._seen_contexts.add(context_key)
        with contextlib.suppress(Exception):
            context.on("page", lambda page: self._schedule(self._attach_page(page)))

    async def _attach_page(self, page: Any) -> None:
        page_key = id(page)
        if page_key in self._seen_pages:
            return
        self._seen_pages.add(page_key)
        with contextlib.suppress(Exception):
            page.on("websocket", self._handle_websocket)
        with contextlib.suppress(Exception):
            session = await page.context.new_cdp_session(page)
            await session.send("Network.enable")
            session.on("Network.webSocketFrameReceived", self._handle_cdp_ws_frame)
            session.on(
                "Network.responseReceived",
                lambda params, session=session: self._schedule(self._handle_response_body(session, params)),
            )
        self.on_status(f"Attached live listeners to tab: {getattr(page, 'url', '')}")

    async def _attach_raw_targets(self) -> None:
        try:
            targets = await asyncio.to_thread(_fetch_cdp_targets, self.cdp_url)
        except Exception as exc:
            logger.debug("Cannot read CDP target list: %s", exc)
            return
        for target in targets:
            target_key = str(target.get("id") or target.get("webSocketDebuggerUrl") or "")
            websocket_url = str(target.get("webSocketDebuggerUrl") or "")
            if not target_key or not websocket_url:
                continue
            if target_key in self._seen_raw_targets or not _is_relevant_raw_target(target):
                continue
            self._seen_raw_targets.add(target_key)
            task = asyncio.create_task(self._run_raw_target(target_key, websocket_url, target))
            self._raw_tasks.add(task)
            task.add_done_callback(
                lambda finished, key=target_key: self._raw_target_finished(key, finished)
            )
            self.on_status(f"Attached raw CDP target: {_target_label(target)}")

    def _raw_target_finished(self, target_key: str, task: asyncio.Task[None]) -> None:
        self._raw_tasks.discard(task)
        # A provider iframe can navigate or silently close its debugger
        # connection while Chrome keeps the same target id. Forgetting the id
        # lets the two-second target poll reconnect instead of leaving a
        # permanent ingestion hole until a different target is created.
        self._seen_raw_targets.discard(target_key)

    async def _run_raw_target(self, target_key: str, websocket_url: str, target: dict[str, Any]) -> None:
        try:
            import websockets
        except Exception as exc:
            self.on_status('websockets is missing. Run: pip install -e ".[live]"')
            logger.debug("websockets is not installed: %s", exc)
            return

        message_id = 0
        pending: dict[int, str] = {}
        response_requests: dict[str, None] = {}

        async def send(websocket: Any, method: str, params: dict[str, Any] | None = None, tag: str | None = None) -> None:
            nonlocal message_id
            message_id += 1
            pending[message_id] = tag or method
            await websocket.send(json.dumps({"id": message_id, "method": method, "params": params or {}}))

        try:
            async with websockets.connect(websocket_url, max_size=12_000_000) as websocket:
                await send(websocket, "Network.enable")
                with contextlib.suppress(Exception):
                    await send(websocket, "Runtime.enable")
                    await send(
                        websocket,
                        "Runtime.evaluate",
                        {"expression": RESOURCE_HINT_SCRIPT, "returnByValue": True},
                        tag="resource-hints",
                    )
                while self._running:
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=self.poll_seconds)
                    except asyncio.TimeoutError:
                        continue
                    message = json.loads(raw)
                    message_tag = pending.pop(int(message.get("id", 0)), "")
                    if message_tag.startswith("body:"):
                        result = message.get("result") or {}
                        self._handle_payload(_cdp_body_text(result), source="xhr")
                        continue
                    if message_tag == "resource-hints":
                        self._handle_resource_hints(message.get("result"), target)
                        continue

                    method = message.get("method")
                    params = message.get("params") or {}
                    if method == "Network.webSocketFrameReceived":
                        payload = (params.get("response") or {}).get("payloadData")
                        self._handle_payload(payload, source="ws")
                    elif method == "Network.responseReceived":
                        response = params.get("response") or {}
                        mime_type = str(response.get("mimeType") or "").lower()
                        url = str(response.get("url") or "")
                        request_id = str(params.get("requestId") or "")
                        if request_id and _may_contain_text_payload(url, mime_type):
                            response_requests[request_id] = None
                    elif method == "Network.loadingFinished":
                        request_id = str(params.get("requestId") or "")
                        if request_id in response_requests:
                            response_requests.pop(request_id, None)
                            await send(
                                websocket,
                                "Network.getResponseBody",
                                {"requestId": request_id},
                                tag=f"body:{request_id}",
                            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._running:
                logger.debug("Raw CDP target stopped for %s: %s", target_key, exc)
                self.on_status(f"Raw CDP target stopped: {_target_label(target)}")

    async def _poll_page_dom_if_due(self, page: Any) -> None:
        page_key = id(page)
        now = time.monotonic()
        if now - self._last_dom_poll.get(page_key, 0.0) < self.dom_poll_seconds:
            return
        self._last_dom_poll[page_key] = now
        frames = list(getattr(page, "frames", []) or [])
        if not frames:
            frames = [page]
        for frame in frames:
            payloads: Any = []
            with contextlib.suppress(Exception):
                payloads = await frame.evaluate(DOM_DATA_SCRIPT)
            if not isinstance(payloads, list):
                continue
            for payload in payloads:
                if isinstance(payload, str):
                    self._handle_payload(payload, source="dom")

    async def _refresh_page_if_due(self, page: Any) -> None:
        if self.auto_refresh_seconds <= 0:
            return
        now = time.monotonic()
        last_live = self._last_live_snapshot_at or self._started_at or now
        if now - last_live < self.auto_refresh_seconds:
            return
        if now - self._last_watchdog_refresh_at < self.auto_refresh_seconds:
            return
        if not await _is_refresh_target_page(page):
            return
        url = str(getattr(page, "url", "") or "")
        self._last_watchdog_refresh_at = now
        # Give the reloaded page a full watchdog interval to reconnect. This
        # also prevents multiple relevant pages from being refreshed in the
        # same polling pass.
        self._last_live_snapshot_at = now
        try:
            await page.reload(wait_until="domcontentloaded", timeout=20_000)
        except Exception as exc:
            logger.debug("Watchdog refresh failed for %s: %s", url, exc)
            self.on_status(f"Watchdog refresh failed: {_short_label(url)}")
            return
        self._last_dom_poll.pop(id(page), None)
        self.on_status(f"Watchdog refreshed silent live tab: {_short_label(url)}")

    def _handle_websocket(self, websocket: Any) -> None:
        with contextlib.suppress(Exception):
            websocket.on("framereceived", lambda payload: self._handle_payload(payload, source="ws"))

    def _handle_cdp_ws_frame(self, params: dict[str, Any]) -> None:
        response = params.get("response") or {}
        payload = response.get("payloadData")
        self._handle_payload(payload, source="ws")

    async def _handle_response_body(self, session: Any, params: dict[str, Any]) -> None:
        if not self._running:
            return
        resource_type = str(params.get("type") or "").lower()
        if resource_type and resource_type not in {"fetch", "xhr", "document", "script"}:
            return
        response = params.get("response") or {}
        mime_type = str(response.get("mimeType") or "").lower()
        url = str(response.get("url") or "")
        if not _may_contain_text_payload(url, mime_type):
            return
        request_id = params.get("requestId")
        if not request_id:
            return
        try:
            body = await session.send("Network.getResponseBody", {"requestId": request_id})
        except Exception as exc:
            logger.debug("Cannot read CDP response body: %s", exc)
            return
        self._handle_payload(_cdp_body_text(body), source="xhr")

    def _handle_payload(self, payload: Any, *, source: str) -> None:
        text = _coerce_payload_text(payload)
        if not text or not _looks_interesting(text):
            return
        text = text[: self.max_payload_chars]
        payload_hash = hashlib.sha256(f"{source}:{text}".encode("utf-8", "ignore")).hexdigest()
        if payload_hash in self._seen_payload_hashes:
            return
        self._seen_payload_hashes.add(payload_hash)
        if len(self._seen_payload_hashes) > 5000:
            self._seen_payload_hashes = set(list(self._seen_payload_hashes)[-1000:])
        try:
            snapshots = iter_payload_snapshots(text, source=f"cdp-{source}")
        except Exception as exc:
            logger.debug("Cannot decode AE payload: %s", exc)
            return
        for snapshot in snapshots:
            self._emit_snapshot(snapshot)

    def _emit_snapshot(self, snapshot: TableSnapshot) -> None:
        # A duplicate decoded snapshot still proves that the provider stream
        # is alive, so it must suppress the silence watchdog as well.
        self._last_live_snapshot_at = time.monotonic()
        signature = _snapshot_signature(snapshot)
        table_key = str(snapshot.table_id or snapshot.table_name)
        if self._latest_snapshot_signatures.get(table_key) == signature:
            return
        self._latest_snapshot_signatures[table_key] = signature
        self._snapshot_count += 1
        latest = snapshot.latest_round.outcome.value if snapshot.latest_round else "-"
        self.on_status(
            f"Live update #{self._snapshot_count}: {snapshot.table_name}, "
            f"current_round={snapshot.current_round_no}, observed={snapshot.observed_rounds}, "
            f"missing={snapshot.known_missing_rounds}, latest={latest}, source={snapshot.source}"
        )
        self.on_snapshot(snapshot)

    def _emit_no_snapshot_hint_if_due(self) -> None:
        if self._snapshot_count > 0 or self._started_at <= 0:
            return
        now = time.monotonic()
        if now - self._started_at < 20:
            return
        if now - self._last_no_snapshot_hint_at < 30:
            return
        self._last_no_snapshot_hint_at = now
        self.on_status(
            "No Live update yet. If the AE tab was already loaded before CDP started, "
            "reload the AE live tab or enter a baccarat table after CDP is running."
        )

    def _handle_resource_hints(self, result: Any, target: dict[str, Any]) -> None:
        value = ((result or {}).get("result") or {}).get("value")
        if not isinstance(value, list):
            return
        if any(_looks_like_bootstrap_resource(str(item)) for item in value):
            self.on_status(
                f"AE bootstrap resources were already loaded in {_target_label(target)}. "
                "If no Live update appears, reload that AE tab after CDP is running."
            )

    def _schedule(self, task: Any) -> None:
        try:
            asyncio.create_task(task)
        except RuntimeError:
            close = getattr(task, "close", None)
            if callable(close):
                close()


def _coerce_payload_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="ignore")
    return str(payload)


def _looks_interesting(text: str) -> bool:
    sample = text[:250_000]
    if looks_like_ae_payload(sample):
        return True
    with contextlib.suppress(Exception):
        return bool(iter_payload_snapshots(sample, source="probe"))
    return False


def _may_contain_text_payload(url: str, mime_type: str) -> bool:
    lowered_url = url.lower()
    if any(token in mime_type for token in TEXT_MIME_HINTS):
        return True
    return any(
        token in lowered_url
        for token in (
            "road",
            "table",
            "baccarat",
            "game",
            "shoe",
            "history",
            "result",
            "winner",
            "gateway",
            "gateways",
            "session",
            "sessions",
            "enter",
            "init",
        )
    )


def _looks_like_bootstrap_resource(url: str) -> bool:
    lowered = url.lower()
    if "lobby~baccarat" not in lowered and "baccarat" not in lowered and "ae-live" not in lowered:
        return False
    return any(token in lowered for token in ("/init", "/enter", "/gateway", "/gateways", "/session", "/sessions"))


def _cdp_body_text(body: dict[str, Any]) -> str:
    value = body.get("body")
    if not value:
        return ""
    if not body.get("base64Encoded"):
        return _coerce_payload_text(value)
    try:
        raw = base64.b64decode(str(value), validate=False)
    except Exception:
        return ""
    return raw.decode("utf-8", errors="ignore")


def _fetch_cdp_targets(cdp_url: str) -> list[dict[str, Any]]:
    url = _target_list_url(cdp_url)
    try:
        with urllib.request.urlopen(url, timeout=2.5) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def _target_list_url(cdp_url: str) -> str:
    parsed = urllib.parse.urlsplit(cdp_url)
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc or parsed.path
    return urllib.parse.urlunsplit((scheme, netloc, "/json/list", "", ""))


def _is_relevant_raw_target(target: dict[str, Any]) -> bool:
    target_type = str(target.get("type") or "").lower()
    if target_type not in {"page", "iframe", "worker"}:
        return False
    title = str(target.get("title") or "")
    url = str(target.get("url") or "")
    text = f"{title} {url}".lower()
    if url.lower().startswith("blob:https://www.dafabet.com/"):
        return True
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower()
    if any(_host_matches(host, domain) for domain in RAW_TARGET_EXCLUDED_DOMAINS):
        return False
    if _is_live_provider_host(host):
        return True
    if _host_matches(host, "dafabet.com"):
        return "live-dealer" in parsed.path.lower() or "ae-live" in text or "sexy casino" in text
    return "sfcdf." in text or "sexy casino" in text or "sx~~lobby~baccarat" in text


async def _is_refresh_target_page(page: Any) -> bool:
    urls = [str(getattr(page, "url", "") or "")]
    for frame in list(getattr(page, "frames", []) or []):
        urls.append(str(getattr(frame, "url", "") or ""))
    if any(_is_relevant_refresh_url(url) for url in urls):
        return True
    with contextlib.suppress(Exception):
        title = str(await page.title())
        return _is_relevant_refresh_text(title)
    return False


def _is_relevant_refresh_url(url: str) -> bool:
    if not url:
        return False
    return _is_relevant_raw_target({"type": "page", "title": url, "url": url})


def _is_relevant_refresh_text(text: str) -> bool:
    lowered = text.lower()
    return "sfcdf." in lowered or "sexy casino" in lowered or "ae-live" in lowered or "sx~~lobby~baccarat" in lowered


def _is_live_provider_host(host: str) -> bool:
    return host.startswith("sfcdf.") or any(_host_matches(host, domain) for domain in RAW_TARGET_DOMAINS)


def _host_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith(f".{domain}")


def _target_label(target: dict[str, Any]) -> str:
    label = str(target.get("title") or target.get("url") or target.get("id") or "")
    label = label.replace("\r", " ").replace("\n", " ").strip()
    return label[:120] or "unknown"


def _short_label(value: str) -> str:
    label = value.replace("\r", " ").replace("\n", " ").strip()
    return label[:120] or "unknown"


def _snapshot_signature(snapshot: TableSnapshot) -> str:
    parts: list[str] = []
    for index, event in enumerate(snapshot.rounds[-120:], start=1):
        round_key = event.round_no if event.round_no is not None else index
        parts.append(f"{event.shoe}|{round_key}|{event.outcome.value}")
    return f"{snapshot.shoe}|{snapshot.total_rounds}|" + ",".join(parts)
