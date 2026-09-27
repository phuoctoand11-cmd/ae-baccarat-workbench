from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from ..ae_decode import parse_ae_ws_payload


logger = logging.getLogger(__name__)

PROVIDER_ACK_TIMEOUT_SECONDS = 5.0
_MAX_OBSERVATIONS = 250
_MAX_TEXT_LENGTH = 180

_SENSITIVE_KEY_PARTS = (
    "account",
    "authorization",
    "cookie",
    "credential",
    "csrf",
    "jwt",
    "member",
    "password",
    "secret",
    "session",
    "signature",
    "token",
    "username",
)

_ALIASES: dict[str, set[str]] = {
    "provider_bet_id": {
        "betid",
        "betno",
        "billno",
        "orderid",
        "receiptid",
        "receiptnumber",
        "serialno",
        "ticketid",
        "ticketno",
        "transactionid",
        "txid",
        "txn",
        "txnid",
        "wagerid",
        "wagerno",
    },
    "provider_status": {
        "200",
        "accepted",
        "betstatus",
        "isaccepted",
        "resultstatus",
        "state",
        "status",
        "success",
    },
    "error_code": {
        "code",
        "errorcode",
        "errcode",
        "rejectcode",
        "responsecode",
        "retcode",
    },
    "message": {
        "description",
        "errordescription",
        "errormessage",
        "message",
        "msg",
        "reason",
        "rejectreason",
    },
    "table": {
        "gametable",
        "table",
        "tablecode",
        "tableid",
        "tablename",
        "tableno",
    },
    "shoe": {
        "currentgameshoe",
        "gameshoe",
        "shoe",
        "shoeid",
        "shoeno",
    },
    "round": {
        "currentgameround",
        "gameround",
        "round",
        "roundid",
        "roundno",
    },
    "side": {
        "betside",
        "bettype",
        "categoryname",
        "playtype",
        "selection",
        "side",
    },
    "amount": {
        "amount",
        "betamount",
        "betamt",
        "credit",
        "stake",
        "wageramount",
        "wageramt",
    },
}

_POSITIVE_STATUS = {
    "200",
    "accept",
    "accepted",
    "confirmed",
    "ok",
    "placed",
    "success",
    "successful",
    "true",
}
_NEGATIVE_STATUS = {
    "declined",
    "denied",
    "error",
    "fail",
    "failed",
    "false",
    "reject",
    "rejected",
}
_BET_CONTEXT_PARTS = (
    "addmytransaction",
    "bet",
    "mytransaction",
    "order",
    "receipt",
    "ticket",
    "transaction",
    "txn",
    "wager",
)
_EXCLUDED_ENDPOINT_PARTS = (
    "querybetlimit",
    "betlimit",
    "getbetlimit",
    "querytable",
    "heartbeat",
    "ping",
)
_NON_BET_KEY_PARTS = (
    "limit",
    "maxbet",
    "minbet",
    "betcount",
    "currentbet",
    "totalcurrentbet",
    "maxbetround",
    "betinfo",
    "totalbet",
)


@dataclass(frozen=True)
class ProviderAck:
    outcome: str
    provider_bet_id: str = ""
    provider_status: str = ""
    error_code: str = ""
    message: str = ""
    table: str = ""
    shoe: str = ""
    round_no: str = ""
    side: str = ""
    amount: str = ""
    source: str = ""
    endpoint: str = ""
    request_seen: bool = False
    candidate_count: int = 0
    observed_fields: tuple[str, ...] = ()

    def audit_payload(self) -> dict[str, Any]:
        return {
            "provider_bet_id": self.provider_bet_id,
            "provider_status": self.provider_status,
            "provider_error_code": self.error_code,
            "provider_table": self.table,
            "provider_shoe": self.shoe,
            "provider_round": self.round_no,
            "provider_side": self.side,
            "provider_amount": self.amount,
            "provider_source": self.source,
            "provider_endpoint": self.endpoint,
            "request_seen": self.request_seen,
            "candidate_count": self.candidate_count,
            "observed_fields": list(self.observed_fields),
        }


@dataclass(frozen=True)
class ProviderObservation:
    observed_monotonic: float
    source: str
    direction: str
    endpoint: str
    request_id: str
    payload: dict[str, Any]


def _normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _is_sensitive_key(key: str) -> bool:
    normalized = _normalize_key(key)
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _scrub_text(value: Any, *, allow_identifier: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"(?i)(bearer|token|session|cookie|password)\s*[:=]\s*\S+", r"\1=[redacted]", text)
    if not allow_identifier:
        text = re.sub(r"\b[A-Za-z0-9_\-=/+.]{40,}\b", "[redacted]", text)
    return text[:_MAX_TEXT_LENGTH]


def _canonical_key(key: Any) -> str | None:
    normalized = _normalize_key(key)
    for canonical, aliases in _ALIASES.items():
        if normalized in aliases:
            return canonical
    return None


def sanitize_provider_payload(value: Any) -> dict[str, Any]:
    """Keep only non-sensitive fields needed to classify a provider acknowledgement."""
    result: dict[str, Any] = {}
    observed_keys: set[str] = set()
    has_bet_context = False

    def visit(item: Any) -> None:
        nonlocal has_bet_context
        if isinstance(item, dict):
            for raw_key, raw_value in item.items():
                key = str(raw_key)
                normalized = _normalize_key(key)
                if (
                    any(part in normalized for part in _BET_CONTEXT_PARTS)
                    and not any(non_bet in normalized for non_bet in _NON_BET_KEY_PARTS)
                ):
                    has_bet_context = True
                if (
                    normalized in {"action", "command", "event", "eventname", "method", "op", "operation", "type"}
                    and not isinstance(raw_value, (dict, list, tuple))
                    and any(part in _normalize_key(raw_value) for part in _BET_CONTEXT_PARTS)
                    and not any(non_bet in _normalize_key(raw_value) for non_bet in _NON_BET_KEY_PARTS)
                ):
                    has_bet_context = True
                if _is_sensitive_key(key):
                    continue
                observed_keys.add(key[:48])
                canonical = _canonical_key(key)
                if canonical and canonical not in result and not isinstance(raw_value, (dict, list, tuple)):
                    result[canonical] = _scrub_text(
                        raw_value,
                        allow_identifier=canonical == "provider_bet_id",
                    )
                if isinstance(raw_value, str):
                    trimmed = raw_value.strip()
                    if (trimmed.startswith("{") and trimmed.endswith("}")) or (
                        trimmed.startswith("[") and trimmed.endswith("]")
                    ):
                        with contextlib.suppress(Exception):
                            nested = json.loads(trimmed)
                            visit(nested)
                visit(raw_value)
        elif isinstance(item, (list, tuple)):
            for child in item:
                visit(child)

    visit(value)
    if result.get("provider_bet_id"):
        has_bet_context = True
    result["bet_context"] = has_bet_context
    result["observed_keys"] = sorted(observed_keys)[:40]
    return result


def _decoded_values(text: str) -> list[Any]:
    raw = str(text or "").strip()
    if not raw:
        return []
    values: list[Any] = []
    with contextlib.suppress(Exception):
        values.append(json.loads(raw))
    with contextlib.suppress(Exception):
        values.extend(parse_ae_ws_payload(raw))
    if "=" in raw and len(raw) <= 32_000:
        with contextlib.suppress(Exception):
            pairs = parse_qsl(raw, keep_blank_values=True, max_num_fields=100)
            if pairs:
                values.append(dict(pairs))
    unique: list[Any] = []
    seen: set[str] = set()
    for value in values:
        try:
            signature = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)
        except Exception:
            signature = repr(value)
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(value)
    return unique


def _status_flags(payload: dict[str, Any]) -> tuple[bool, bool]:
    raw_status = str(payload.get("provider_status") or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9]", "", raw_status)
    positive = normalized in _POSITIVE_STATUS
    negative = normalized in _NEGATIVE_STATUS
    message = str(payload.get("message") or "").lower()
    has_positive_text = bool(
        re.search(
            r"\b(betsplacedsuccessfully|placed\s*successfully|cược\s*thành\s*công|success|ok)\b",
            message,
        )
    )
    if has_positive_text and not negative:
        positive = True
    negative_message = bool(
        re.search(r"\b(rejected|declined|denied|failed|failure|closed)\b", message)
        or (
            re.search(r"\berror\b", message)
            and not re.search(r"msg\.error\.betTxns\.betsPlacedSuccessfully", message, re.I)
            and not has_positive_text
        )
    )
    explicitly_no_error = bool(
        re.search(r"\b(no|without)\s+(error|failure)\b", message) or has_positive_text
    )
    if negative_message and not explicitly_no_error:
        negative = True
    return positive, negative


def _safe_int(value: Any) -> int | None:
    match = re.search(r"-?\d+", str(value or ""))
    if not match:
        return None
    with contextlib.suppress(ValueError):
        return int(match.group())
    return None


def _safe_amount(value: Any) -> float | None:
    text = str(value or "").strip().lower().replace(",", "")
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    amount = float(match.group())
    if text.endswith("k"):
        amount *= 1000
    elif text.endswith("m"):
        amount *= 1_000_000
    return amount


def _normalize_table(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.isdigit():
        number = int(text)
        if number >= 1001:
            return f"c{number - 1000}"
        return f"c{number}"
    clean = re.sub(r"[^a-z0-9]", "", text).replace("baccarat", "")
    return re.sub(r"c0*(\d+)", r"c\1", clean)


def _normalize_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"P", "PLAYER", "CON", "1"} or text.startswith("PLAYER"):
        return "PLAYER"
    if text in {"B", "BANKER", "CAI", "0"} or text.startswith("BANKER"):
        return "BANKER"
    return ""


def _matches_order(payload: dict[str, Any], order: Any) -> bool:
    table = str(payload.get("table") or "")
    if table and _normalize_table(table) != _normalize_table(getattr(order, "table_name", "")):
        return False
    shoe = str(payload.get("shoe") or "").strip()
    target_shoe = str(getattr(order, "target_shoe", "") or "").strip()
    if shoe and target_shoe and shoe != target_shoe:
        return False
    round_no = _safe_int(payload.get("round"))
    target_round = getattr(order, "target_round_no", None)
    if round_no is not None and target_round is not None and round_no != int(target_round):
        return False
    side = _normalize_side(payload.get("side"))
    target_side = _normalize_side(getattr(order, "side", ""))
    if side and target_side and side != target_side:
        return False
    amount = _safe_amount(payload.get("amount"))
    target_amount = float(getattr(order, "stake", 0.0) or 0.0)
    if amount is not None and target_amount > 0 and abs(amount - target_amount) > 0.01:
        return False
    return True


def classify_provider_payload(
    payload: dict[str, Any],
    order: Any,
    *,
    source: str = "",
    endpoint: str = "",
) -> ProviderAck | None:
    if endpoint:
        endpoint_clean = re.sub(r"[^a-z0-9]", "", endpoint.lower())
        if any(excluded in endpoint_clean for excluded in _EXCLUDED_ENDPOINT_PARTS):
            return None
    if not payload.get("bet_context") or not _matches_order(payload, order):
        return None
    positive, negative = _status_flags(payload)
    provider_bet_id = str(payload.get("provider_bet_id") or "")
    provider_status = str(payload.get("provider_status") or "")
    error_code = str(payload.get("error_code") or "")
    message = str(payload.get("message") or "")
    table = str(payload.get("table") or "")
    shoe = str(payload.get("shoe") or "")
    round_no = str(payload.get("round") or "")
    side = str(payload.get("side") or "")
    amount = str(payload.get("amount") or "")
    observed_fields = tuple(str(key) for key in payload.get("observed_keys") or ())
    if negative:
        return ProviderAck(
            outcome="rejected",
            provider_bet_id=provider_bet_id,
            provider_status=provider_status or "rejected",
            error_code=error_code,
            table=table,
            shoe=shoe,
            round_no=round_no,
            side=side,
            amount=amount,
            message=message or "Nhà cung cấp từ chối giao dịch.",
            source=source,
            endpoint=endpoint,
            observed_fields=observed_fields,
        )
    if positive:
        # Require either:
        # 1. An explicit provider transaction/bet ID from the provider (e.g. txnID, txId, ticketId, betId)
        # 2. Or explicit matching side AND matching stake amount
        has_real_bet_id = bool(provider_bet_id)
        has_matching_side_and_amount = False
        if side and amount:
            norm_p_side = _normalize_side(side)
            norm_o_side = _normalize_side(getattr(order, "side", ""))
            p_amount = _safe_amount(amount)
            o_amount = float(getattr(order, "stake", 0.0) or 0.0)
            if norm_p_side and norm_p_side == norm_o_side and p_amount is not None and o_amount > 0 and abs(p_amount - o_amount) < 0.01:
                has_matching_side_and_amount = True

        if not has_real_bet_id and not has_matching_side_and_amount:
            # Broadcast or status message without a transaction ID or matching bet details cannot be accepted as a bet execution
            return None

        effective_bet_id = provider_bet_id or f"TXN-{table or getattr(order, 'table_name', 'T')}-{int(time.time())}"
        return ProviderAck(
            outcome="accepted",
            provider_bet_id=effective_bet_id,
            provider_status=provider_status or "accepted",
            error_code=error_code,
            table=table,
            shoe=shoe,
            round_no=round_no,
            side=side,
            amount=amount,
            message=message or "Nhà cung cấp đã tiếp nhận giao dịch.",
            source=source,
            endpoint=endpoint,
            observed_fields=observed_fields,
        )
    return None


def _safe_endpoint(url: str) -> str:
    with contextlib.suppress(Exception):
        parsed = urlsplit(str(url or ""))
        segments = []
        for segment in parsed.path.split("/"):
            clean = segment.split(";", 1)[0]
            if not clean:
                continue
            normalized = _normalize_key(clean)
            looks_like_secret = (
                len(clean) > 32
                or bool(re.fullmatch(r"[0-9a-fA-F]{24,}", clean))
                or bool(re.fullmatch(r"[A-Za-z0-9_\-=+.]{24,}", clean))
                or any(part in normalized for part in _SENSITIVE_KEY_PARTS)
            )
            segments.append("[redacted]" if looks_like_secret else clean)
        tail = "/".join(segments[-2:])
        return f"{parsed.hostname or ''}/{tail}".rstrip("/")[:160]
    return ""


def _endpoint_has_bet_context(endpoint: str) -> bool:
    lowered = endpoint.lower()
    clean = re.sub(r"[^a-z0-9]", "", lowered)
    if any(excluded in clean for excluded in _EXCLUDED_ENDPOINT_PARTS):
        return False
    return any(part in lowered for part in _BET_CONTEXT_PARTS)


class ProviderAckMonitor:
    """Read-only CDP listener for sanitized provider request/response evidence."""

    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self._websocket: Any = None
        self._reader_task: asyncio.Task[None] | None = None
        self._fetch_tasks: set[asyncio.Task[Any]] = set()
        self._send_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_id = 0
        self._requests: dict[str, dict[str, Any]] = {}
        self._observations: deque[ProviderObservation] = deque(maxlen=_MAX_OBSERVATIONS)
        self._changed = asyncio.Event()
        self.error = ""

    async def start(self) -> bool:
        try:
            import websockets

            self._websocket = await websockets.connect(self.websocket_url, max_size=12_000_000)
            self._reader_task = asyncio.create_task(self._reader(), name="provider-ack-reader")
            await asyncio.wait_for(self._command("Network.enable"), timeout=3.0)
            return True
        except Exception as exc:
            self.error = _scrub_text(exc)
            logger.warning("Provider ACK monitor unavailable: %s", self.error)
            await self.close()
            return False

    def mark(self) -> float:
        return time.monotonic()

    async def close(self) -> None:
        for task in list(self._fetch_tasks):
            task.cancel()
        if self._fetch_tasks:
            await asyncio.gather(*self._fetch_tasks, return_exceptions=True)
        self._fetch_tasks.clear()
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
            self._reader_task = None
        if self._websocket is not None:
            with contextlib.suppress(Exception):
                await self._websocket.close()
            self._websocket = None
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()

    async def _command(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._websocket is None:
            raise RuntimeError("Provider ACK monitor is not connected")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        async with self._send_lock:
            self._next_id += 1
            message_id = self._next_id
            self._pending[message_id] = future
            await self._websocket.send(
                json.dumps({"id": message_id, "method": method, "params": params or {}})
            )
        try:
            return await future
        finally:
            self._pending.pop(message_id, None)

    async def _reader(self) -> None:
        assert self._websocket is not None
        try:
            async for raw_message in self._websocket:
                message = json.loads(raw_message)
                message_id = message.get("id")
                if message_id is not None:
                    future = self._pending.get(int(message_id))
                    if future is not None and not future.done():
                        future.set_result(message)
                    continue
                method = str(message.get("method") or "")
                params = message.get("params") or {}
                if method == "Network.requestWillBeSent":
                    self._handle_request(params)
                elif method == "Network.responseReceived":
                    self._handle_response(params)
                elif method == "Network.loadingFinished":
                    self._schedule_body_fetch(str(params.get("requestId") or ""))
                elif method == "Network.loadingFailed":
                    self._handle_loading_failed(params)
                elif method == "Network.webSocketCreated":
                    request_id = str(params.get("requestId") or "")
                    self._requests[request_id] = {
                        "endpoint": _safe_endpoint(str(params.get("url") or "")),
                        "resource_type": "websocket",
                    }
                elif method in {"Network.webSocketFrameSent", "Network.webSocketFrameReceived"}:
                    response = params.get("response") or {}
                    direction = "sent" if method.endswith("Sent") else "received"
                    request_id = str(params.get("requestId") or "")
                    endpoint = str((self._requests.get(request_id) or {}).get("endpoint") or "")
                    self._record_text(
                        str(response.get("payloadData") or ""),
                        source="websocket",
                        direction=direction,
                        endpoint=endpoint,
                        request_id=request_id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = _scrub_text(exc)
            logger.debug("Provider ACK reader stopped: %s", self.error)
            self._changed.set()

    def _handle_request(self, params: dict[str, Any]) -> None:
        request_id = str(params.get("requestId") or "")
        request = params.get("request") or {}
        endpoint = _safe_endpoint(str(request.get("url") or ""))
        self._requests[request_id] = {
            "endpoint": endpoint,
            "resource_type": str(params.get("type") or ""),
        }
        post_data = request.get("postData")
        if post_data:
            self._record_text(
                str(post_data),
                source="http_request",
                direction="request",
                endpoint=endpoint,
                request_id=request_id,
            )
        elif _endpoint_has_bet_context(endpoint):
            self._record_payload(
                {"bet_context": True, "observed_keys": ["endpoint"]},
                source="http_request",
                direction="request",
                endpoint=endpoint,
                request_id=request_id,
            )

    def _handle_response(self, params: dict[str, Any]) -> None:
        request_id = str(params.get("requestId") or "")
        response = params.get("response") or {}
        meta = self._requests.setdefault(request_id, {})
        meta["endpoint"] = _safe_endpoint(str(response.get("url") or "")) or meta.get("endpoint", "")
        meta["mime_type"] = str(response.get("mimeType") or "").lower()
        meta["http_status"] = int(response.get("status") or 0)
        meta["resource_type"] = str(params.get("type") or meta.get("resource_type") or "")
        if meta["http_status"] >= 400 and _endpoint_has_bet_context(str(meta.get("endpoint") or "")):
            self._record_payload(
                {
                    "bet_context": True,
                    "provider_status": "rejected",
                    "error_code": f"HTTP_{meta['http_status']}",
                    "message": "Provider HTTP request was rejected.",
                    "observed_keys": ["http_status"],
                },
                source="http_response",
                direction="received",
                endpoint=str(meta.get("endpoint") or ""),
                request_id=request_id,
            )

    def _handle_loading_failed(self, params: dict[str, Any]) -> None:
        request_id = str(params.get("requestId") or "")
        meta = self._requests.get(request_id) or {}
        endpoint = str(meta.get("endpoint") or "")
        if not _endpoint_has_bet_context(endpoint):
            return
        self._record_payload(
            {
                "bet_context": True,
                "provider_status": "network_failed",
                "error_code": "NETWORK_LOADING_FAILED",
                "message": _scrub_text(params.get("errorText") or "Network request failed"),
                "observed_keys": ["errorText"],
            },
            source="http_response",
            direction="received",
            endpoint=endpoint,
            request_id=request_id,
        )

    def _schedule_body_fetch(self, request_id: str) -> None:
        meta = self._requests.get(request_id) or {}
        resource_type = str(meta.get("resource_type") or "").lower()
        mime_type = str(meta.get("mime_type") or "").lower()
        endpoint = str(meta.get("endpoint") or "")
        text_like = resource_type in {"fetch", "xhr"} or "json" in mime_type or "text" in mime_type
        if not request_id or not text_like:
            return
        task = asyncio.create_task(self._fetch_body(request_id, endpoint))
        self._fetch_tasks.add(task)
        task.add_done_callback(self._fetch_tasks.discard)

    async def _fetch_body(self, request_id: str, endpoint: str) -> None:
        try:
            response = await asyncio.wait_for(
                self._command("Network.getResponseBody", {"requestId": request_id}),
                timeout=2.0,
            )
            body = (response.get("result") or {}).get("body")
            if body:
                self._record_text(
                    str(body),
                    source="http_response",
                    direction="received",
                    endpoint=endpoint,
                    request_id=request_id,
                )
        except Exception:
            return

    def _record_text(
        self,
        text: str,
        *,
        source: str,
        direction: str,
        endpoint: str = "",
        request_id: str = "",
    ) -> None:
        for value in _decoded_values(text):
            payload = sanitize_provider_payload(value)
            if _endpoint_has_bet_context(endpoint):
                payload["bet_context"] = True
            if payload.get("bet_context") or payload.get("provider_status") or payload.get("error_code"):
                self._record_payload(
                    payload,
                    source=source,
                    direction=direction,
                    endpoint=endpoint,
                    request_id=request_id,
                )

    def _record_payload(
        self,
        payload: dict[str, Any],
        *,
        source: str,
        direction: str,
        endpoint: str = "",
        request_id: str = "",
    ) -> None:
        self._observations.append(
            ProviderObservation(
                observed_monotonic=time.monotonic(),
                source=source,
                direction=direction,
                endpoint=endpoint,
                request_id=request_id,
                payload=dict(payload),
            )
        )
        self._changed.set()

    async def wait_for_ack(
        self,
        order: Any,
        since_monotonic: float,
        timeout: float = PROVIDER_ACK_TIMEOUT_SECONDS,
    ) -> ProviderAck:
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            self._changed.clear()
            observations = [
                observation
                for observation in self._observations
                if observation.observed_monotonic >= since_monotonic
            ]
            request_seen = any(
                observation.direction in {"request", "sent"}
                and bool(observation.payload.get("bet_context"))
                for observation in observations
            )
            for observation in observations:
                if observation.direction not in {"received"}:
                    continue
                merged_payload = dict(observation.payload)
                related_request = next(
                    (
                        candidate
                        for candidate in reversed(observations)
                        if candidate.observed_monotonic <= observation.observed_monotonic
                        and candidate.direction in {"request", "sent"}
                        and candidate.request_id == observation.request_id
                    ),
                    None,
                )
                if related_request is not None:
                    for key in (
                        "provider_bet_id",
                        "table",
                        "shoe",
                        "round",
                        "side",
                        "amount",
                    ):
                        if not merged_payload.get(key) and related_request.payload.get(key):
                            merged_payload[key] = related_request.payload[key]
                    merged_payload["bet_context"] = bool(
                        merged_payload.get("bet_context")
                        or related_request.payload.get("bet_context")
                    )
                    merged_payload["observed_keys"] = sorted(
                        {
                            *merged_payload.get("observed_keys", []),
                            *related_request.payload.get("observed_keys", []),
                        }
                    )[:40]
                ack = classify_provider_payload(
                    merged_payload,
                    order,
                    source=observation.source,
                    endpoint=observation.endpoint,
                )
                if ack is not None:
                    return replace(
                        ack,
                        request_seen=request_seen,
                        candidate_count=len(observations),
                    )
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.error:
                fields = sorted(
                    {
                        str(field)
                        for observation in observations
                        for field in observation.payload.get("observed_keys") or ()
                    }
                )
                endpoints = [observation.endpoint for observation in observations if observation.endpoint]
                return ProviderAck(
                    outcome="timeout",
                    provider_status="ack_timeout",
                    error_code="ACK_MONITOR_ERROR" if self.error else "ACK_TIMEOUT",
                    message=self.error or "Không nhận được phản hồi xác nhận từ nhà cung cấp trong thời gian giới hạn.",
                    source="cdp_network",
                    endpoint=endpoints[-1] if endpoints else "",
                    request_seen=request_seen,
                    candidate_count=len(observations),
                    observed_fields=tuple(fields[:40]),
                )
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                continue
