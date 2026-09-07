from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import re
import time
import urllib.parse
import urllib.request
import zlib
from typing import Any

import websockets

from ae_baccarat_workbench.ae_decode import iter_payload_snapshots
from ae_baccarat_workbench.monitor.cdp import _is_relevant_raw_target, _looks_interesting


KEYWORDS = (
    "road",
    "game",
    "table",
    "shoe",
    "round",
    "winner",
    "result",
    "baccarat",
    "history",
    "dealer",
    "banker",
    "player",
)


def redact(value: Any, limit: int = 180) -> str:
    text = "".join(ch if 32 <= ord(ch) < 127 else "." for ch in str(value)[:limit])
    text = re.sub(r"(sid=)[^&/]+", r"\1<sid>", text)
    text = re.sub(r"(_sig=)[^&]+", r"\1<sig>", text)
    text = re.sub(r"(ticket%3D)[^%&]+", r"\1<ticket>", text)
    text = re.sub(r"[A-Za-z0-9_./:=?&%-]{70,}", "<LONG>", text)
    return text


def decoded_texts(payload: str) -> list[str]:
    texts = [payload]
    value = payload.strip()
    for offset in range(min(12, len(value))):
        candidate = value[offset:].strip()
        if len(candidate) < 12 or not re.fullmatch(r"[A-Za-z0-9+/_=-]+", candidate):
            continue
        try:
            raw = base64.b64decode(candidate + "=" * ((4 - len(candidate) % 4) % 4), altchars=b"-_", validate=False)
        except Exception:
            continue
        for decoder in (lambda b: b, gzip.decompress, zlib.decompress):
            try:
                decoded = decoder(raw).decode("utf-8", "ignore")
            except Exception:
                continue
            if decoded and decoded not in texts:
                texts.append(decoded)
    return texts


def payload_summary(payload: str) -> list[tuple[list[str], str]]:
    out: list[tuple[list[str], str]] = []
    for text in decoded_texts(payload):
        lower = text.lower()
        found = [keyword for keyword in KEYWORDS if keyword in lower]
        if found:
            out.append((found, redact(text)))
    return out[:2]


def cdp_targets(cdp_url: str) -> list[dict[str, Any]]:
    parsed = urllib.parse.urlsplit(cdp_url)
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc or parsed.path
    url = urllib.parse.urlunsplit((scheme, netloc, "/json/list", "", ""))
    with urllib.request.urlopen(url, timeout=3) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    return payload if isinstance(payload, list) else []


async def eval_resource_hints(target: dict[str, Any]) -> list[str]:
    expression = "(() => performance.getEntriesByType('resource').map(e => e.name).slice(-120))()"
    try:
        async with websockets.connect(str(target["webSocketDebuggerUrl"]), max_size=12_000_000) as websocket:
            await websocket.send(json.dumps({"id": 1, "method": "Runtime.enable", "params": {}}))
            await websocket.send(
                json.dumps(
                    {
                        "id": 2,
                        "method": "Runtime.evaluate",
                        "params": {"expression": expression, "returnByValue": True},
                    }
                )
            )
            end = time.monotonic() + 3
            while time.monotonic() < end:
                message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=1))
                if message.get("id") != 2:
                    continue
                result = ((message.get("result") or {}).get("result") or {}).get("value") or []
                hints: list[str] = []
                for item in result:
                    parsed = urllib.parse.urlsplit(str(item))
                    host_path = f"{parsed.netloc}{parsed.path}"
                    if any(keyword in host_path.lower() for keyword in KEYWORDS):
                        hints.append(redact(host_path, limit=140))
                return sorted(set(hints))[:20]
    except Exception as exc:
        return [f"ERR:{type(exc).__name__}"]
    return []


async def sniff_target(target: dict[str, Any], seconds: int) -> tuple[dict[str, int], list[tuple[str, int, list[tuple[list[str], str]]]]]:
    counts = {"recv": 0, "sent": 0, "responses": 0, "interesting": 0, "snapshots": 0}
    samples: list[tuple[str, int, list[tuple[list[str], str]]]] = []
    seen: set[tuple[str, str]] = set()
    try:
        async with websockets.connect(str(target["webSocketDebuggerUrl"]), max_size=12_000_000) as websocket:
            await websocket.send(json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                try:
                    message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=1))
                except asyncio.TimeoutError:
                    continue
                method = message.get("method")
                params = message.get("params") or {}
                if method == "Network.responseReceived":
                    counts["responses"] += 1
                    continue
                if method not in {"Network.webSocketFrameReceived", "Network.webSocketFrameSent"}:
                    continue
                direction = "recv" if str(method).endswith("Received") else "sent"
                counts[direction] += 1
                payload = str((params.get("response") or {}).get("payloadData") or "")
                if _looks_interesting(payload):
                    counts["interesting"] += 1
                    counts["snapshots"] += len(iter_payload_snapshots(payload, source="probe"))
                marker = (direction, payload[:100])
                if marker in seen:
                    continue
                seen.add(marker)
                summary = payload_summary(payload)
                if summary and len(samples) < 8:
                    samples.append((direction, len(payload), summary))
    except Exception as exc:
        samples.append(("ERR", 0, [([type(exc).__name__], redact(exc))]))
    return counts, samples


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    parser.add_argument("--seconds", type=int, default=25)
    args = parser.parse_args()

    targets = cdp_targets(args.cdp_url)
    relevant = [target for target in targets if _is_relevant_raw_target(target)]
    print(f"targets={len(targets)} relevant={len(relevant)}")
    for target in relevant:
        url = str(target.get("url") or "")
        host = urllib.parse.urlsplit(url).netloc or "blob"
        label = redact(target.get("title") or target.get("url") or target.get("id"))
        print(f"TARGET type={target.get('type')} host={host} label={label}")
        print("RESOURCES", await eval_resource_hints(target))
        counts, samples = await sniff_target(target, args.seconds)
        print("COUNTS", counts)
        for direction, length, summary in samples:
            print(f"SAMPLE direction={direction} len={length} summary={summary}")


if __name__ == "__main__":
    asyncio.run(main())
