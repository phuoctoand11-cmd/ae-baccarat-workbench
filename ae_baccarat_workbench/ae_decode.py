from __future__ import annotations

import base64
import json
import re
import unicodedata
from collections.abc import Iterable
from typing import Any

from .models import Outcome, RoundEvent, TableSnapshot


ROAD_TO_OUTCOME: dict[int, Outcome] = {
    0: Outcome.BANKER,
    1: Outcome.PLAYER,
    2: Outcome.TIE,
    8: Outcome.PLAYER,
    9: Outcome.BANKER,
    10: Outcome.PLAYER,
    12: Outcome.BANKER,
}

MARKER_ROAD_TO_OUTCOME: dict[int, Outcome] = {
    0: Outcome.BANKER,
    1: Outcome.PLAYER,
    2: Outcome.TIE,
    3: Outcome.BANKER,
    4: Outcome.BANKER,
    5: Outcome.BANKER,
    6: Outcome.PLAYER,
    7: Outcome.PLAYER,
    8: Outcome.PLAYER,
    9: Outcome.TIE,
    10: Outcome.TIE,
    11: Outcome.TIE,
    12: Outcome.BANKER,
}

ROAD_LIST_KEYS = (
    "markerRoads",
    "beadRoads",
    "roadList",
    "history",
    "results",
    "gameResults",
    "rounds",
    "roads",
    "roadMap",
    "bigRoads",
)
RESULT_FIELD_KEYS = (
    "result",
    "winner",
    "win",
    "winPlay",
    "side",
    "outcome",
    "gameResult",
    "winnerType",
    "value",
)
TABLE_ID_KEYS = ("tableID", "tableId", "table_id", "tableNo", "table_no", "tableCode")
TABLE_NAME_KEYS = ("tableName", "table_name", "name", "table", "tableTitle")
PRIMARY_PAYLOAD_HINTS = ("roadinfo", "markerroads", "bigroads", "beadroads", "gameround")
SECONDARY_ROAD_HINTS = ("roadlist", "roadmap", "history", "results", "gameresults", "rounds")
TABLE_HINTS = ("tableid", "table_id", "tableno", "table_no", "tablecode", "tablename", "table_name")


def table_name_to_ids(table_name: str) -> list[int]:
    match = re.search(r"C(\d+)", table_name, re.I)
    if not match:
        return []
    return [1000 + int(match.group(1))]


def table_id_to_name(table_id: int | str | None) -> str:
    if table_id is None:
        return ""
    try:
        value = int(table_id)
    except (TypeError, ValueError):
        return ""
    number = value - 1000
    if number < 1:
        return ""
    return f"Baccarat C{number:02d}"


def normalize_table_name(table_name: str) -> str:
    ids = table_name_to_ids(table_name)
    if ids:
        return table_id_to_name(ids[0])
    return table_name.strip() or "Unknown Table"


def decode_road(road: int | str | None) -> Outcome | None:
    if road is None:
        return None
    try:
        value = int(road)
    except (TypeError, ValueError):
        return _outcome_from_text(str(road))
    if value in ROAD_TO_OUTCOME:
        return ROAD_TO_OUTCOME[value]
    base = value & 0x0F
    if base in ROAD_TO_OUTCOME:
        return ROAD_TO_OUTCOME[base]
    return _outcome_from_int(value)


def decode_marker_road(road: int | str | None) -> Outcome | None:
    if road is None:
        return None
    try:
        value = int(road)
    except (TypeError, ValueError):
        return _outcome_from_text(str(road))
    if value in MARKER_ROAD_TO_OUTCOME:
        return MARKER_ROAD_TO_OUTCOME[value]
    low = value & 0x03
    if low == 0:
        return Outcome.BANKER
    if low == 1:
        return Outcome.PLAYER
    if low in (2, 3):
        return Outcome.TIE
    return decode_road(value)


def decode_marker_item(item: dict[str, Any]) -> Outcome | None:
    road = item.get("road")
    return decode_marker_road(road) or decode_road(road)


def decode_big_road_item(item: dict[str, Any]) -> Outcome | None:
    road = item.get("road")
    try:
        road_i = int(road)
    except (TypeError, ValueError):
        return decode_road(road)

    # AE's Baccarat ``bigRoads`` value is a bit field, not the ordinal
    # 0=Banker / 1=Player / 2=Tie used by winner fields.  The low two bits
    # carry pair markers, bit 2 marks a tie update, and bit 3 carries the
    # Player side.  Treating 1 and 9 as outcomes therefore flips perfectly
    # valid Banker/Player results whenever a pair marker is present.
    road_bits = road_i & 0x0F
    if road_bits & 0x04:
        return Outcome.TIE
    if road_bits & 0x08:
        return Outcome.PLAYER
    return Outcome.BANKER


def parse_ae_ws_payload(text: str) -> list[Any]:
    """Extract JSON objects/arrays from live frames.

    AE providers commonly wrap JSON in Socket.IO frames such as
    ``42["event", {...}]``, prefix base64 JSON frames, or place road data
    inside nested strings.
    """

    values: list[Any] = []
    seen: set[str] = set()
    for value in _iter_json_values(text):
        _append_payload_value(value, values, seen)
    for value in _iter_encoded_json_values(text):
        _append_payload_value(value, values, seen)
    return values


def iter_payload_snapshots(text: str, *, source: str = "ws") -> list[TableSnapshot]:
    snapshots: list[TableSnapshot] = []
    seen: set[str] = set()
    for payload in parse_ae_ws_payload(text):
        for road_info in find_road_infos(payload):
            snapshot = road_info_to_snapshot(road_info, source=source)
            if snapshot and snapshot.rounds:
                signature = _snapshot_decode_signature(snapshot)
                if signature in seen:
                    continue
                seen.add(signature)
                snapshots.append(snapshot)
    return snapshots


def looks_like_ae_payload(value: str) -> bool:
    lowered = value.lower()
    if any(token in lowered for token in PRIMARY_PAYLOAD_HINTS):
        return True
    return any(token in lowered for token in SECONDARY_ROAD_HINTS) and any(
        token in lowered for token in TABLE_HINTS
    )


def find_road_infos(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        has_identity = any(key in obj for key in (*TABLE_ID_KEYS, *TABLE_NAME_KEYS))
        has_roads = any(key in obj for key in ROAD_LIST_KEYS)
        has_latest_result = any(key in obj for key in RESULT_FIELD_KEYS)
        if has_identity and (has_roads or has_latest_result):
            yield obj
        road_info = _first_present(obj, "roadInfo", "road_info", "roadinfo")
        if isinstance(road_info, dict):
            yield road_info
        for value in obj.values():
            if value is road_info:
                continue
            yield from find_road_infos(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from find_road_infos(item)
    elif isinstance(obj, str) and _looks_like_payload(obj):
        for payload in parse_ae_ws_payload(obj):
            yield from find_road_infos(payload)


def road_info_to_snapshot(road: dict[str, Any], *, source: str = "ws") -> TableSnapshot | None:
    table_id = _first_present(road, *TABLE_ID_KEYS)
    table_name = _first_present(road, *TABLE_NAME_KEYS) or table_id_to_name(table_id)
    if not table_name and table_id is not None:
        table_name = f"Table {table_id}"
    if not table_name:
        return None

    shoe = _first_present(road, "gameShoe", "currentGameShoe", "shoe", "shoeNo", "shoe_no")
    game_round = _safe_int(_first_present(road, "gameRound", "currentGameRound", "round", "roundNo"))
    road_items = _road_items(road)
    events: list[RoundEvent] = []
    for index, item in enumerate(road_items, start=1):
        outcome = _decode_item(item)
        if outcome is None:
            continue
        round_no = _safe_int(_first_present(item, "round", "roundNo", "gameRound", "currentGameRound"))
        if round_no is None:
            round_no = game_round if game_round is not None and len(road_items) == 1 else index
        events.append(
            RoundEvent(
                table_name=normalize_table_name(str(table_name)),
                table_id=_safe_int(table_id),
                shoe=shoe,
                round_no=round_no,
                outcome=outcome,
                source=source,
            )
        )

    if not events:
        latest = _decode_winner(road)
        if latest is not None:
            events.append(
                RoundEvent(
                    table_name=normalize_table_name(str(table_name)),
                    table_id=_safe_int(table_id),
                    shoe=shoe,
                    round_no=game_round,
                    outcome=latest,
                    source=source,
                )
            )

    return TableSnapshot(
        table_name=normalize_table_name(str(table_name)),
        table_id=_safe_int(table_id),
        shoe=shoe,
        rounds=tuple(events),
        source=source,
    )


def parse_manual_sequence(text: str, table_name: str = "Manual Table") -> TableSnapshot:
    normalized_text = _fold_text(text)
    tokens = re.findall(r"BANKER|PLAYER|TIE|CAI|CON|HOA|[BPT]", normalized_text, flags=re.I)
    events: list[RoundEvent] = []
    normalized_table = normalize_table_name(table_name)
    table_ids = table_name_to_ids(normalized_table)
    table_id = table_ids[0] if table_ids else None
    for index, token in enumerate(tokens, start=1):
        outcome = _outcome_from_text(token)
        if outcome is None:
            continue
        events.append(
            RoundEvent(
                table_name=normalized_table,
                table_id=table_id,
                shoe="manual",
                round_no=index,
                outcome=outcome,
                source="manual",
            )
        )
    return TableSnapshot(
        table_name=normalized_table,
        table_id=table_id,
        rounds=tuple(events),
        source="manual",
        shoe="manual",
    )


def _road_items(road: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ROAD_LIST_KEYS:
        items = road.get(key)
        if isinstance(items, list):
            normalized_items = _flatten_road_items(items)
            if normalized_items:
                for item in normalized_items:
                    item["_roadKey"] = key
                return sorted(normalized_items, key=_item_sort_key)
    return []


def _decode_item(item: dict[str, Any]) -> Outcome | None:
    for key in RESULT_FIELD_KEYS:
        if key in item:
            value = item[key]
            decoded = _decode_winner(value) if isinstance(value, dict) else _outcome_from_text(str(value))
            if decoded:
                return decoded
    if "road" in item:
        road_key = str(item.get("_roadKey") or "")
        if road_key == "bigRoads":
            return decode_big_road_item(item)
        if road_key == "markerRoads":
            return decode_marker_item(item) or decode_big_road_item(item)
        return decode_marker_item(item) or decode_big_road_item(item)
    return None


def _decode_winner(data: dict[str, Any]) -> Outcome | None:
    for key in (*RESULT_FIELD_KEYS, "road", "w", "r"):
        if key not in data:
            continue
        value = data[key]
        if isinstance(value, int):
            return _outcome_from_int(value)
        decoded = _outcome_from_text(str(value))
        if decoded:
            return decoded
        if str(value).isdigit():
            return _outcome_from_int(int(value))
    return None


def _outcome_from_int(value: int) -> Outcome | None:
    if value == 0:
        return Outcome.TIE
    if value == 1:
        return Outcome.BANKER
    if value == 2:
        return Outcome.PLAYER
    return None


def _outcome_from_text(value: str) -> Outcome | None:
    v = _fold_text(value).replace(" ", "").replace("_", "").replace("-", "")
    if v in ("b", "banker", "bankerwin", "cai", "bank"):
        return Outcome.BANKER
    if v in ("p", "player", "playerwin", "con", "play"):
        return Outcome.PLAYER
    if v in ("t", "tie", "tiewin", "hoa", "draw"):
        return Outcome.TIE
    if v == "1":
        return Outcome.BANKER
    if v == "2":
        return Outcome.PLAYER
    if v == "0":
        return Outcome.TIE
    return None


def _iter_json_values(text: str) -> Iterable[Any]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"[\{\[]", text):
        try:
            value, _ = decoder.raw_decode(text[match.start() :])
        except json.JSONDecodeError:
            continue
        yield value


def _iter_encoded_json_values(text: str) -> Iterable[Any]:
    decoder = json.JSONDecoder()
    for decoded in _iter_base64_texts(text):
        for match in re.finditer(r"[\{\[]", decoded):
            try:
                value, _ = decoder.raw_decode(decoded[match.start() :])
            except json.JSONDecodeError:
                continue
            yield value


def _iter_base64_texts(text: str) -> Iterable[str]:
    value = text.strip()
    if len(value) < 12:
        return
    for offset in range(min(12, len(value))):
        candidate = value[offset:].strip()
        if len(candidate) < 12 or not re.fullmatch(r"[A-Za-z0-9+/_=-]+", candidate):
            continue
        padded = candidate + "=" * ((4 - len(candidate) % 4) % 4)
        try:
            raw = base64.b64decode(padded, altchars=b"-_", validate=False)
            decoded = raw.decode("utf-8")
        except Exception:
            continue
        if decoded.lstrip().startswith(("{", "[")) or looks_like_ae_payload(decoded):
            yield decoded


def _append_payload_value(value: Any, values: list[Any], seen: set[str]) -> None:
    if isinstance(value, (dict, list)):
        marker = _stable_json(value)
        if marker in seen:
            return
        seen.add(marker)
        values.append(value)
    elif isinstance(value, str) and (_looks_like_payload(value) or _looks_like_encoded_payload(value)):
        for nested in _iter_json_values(value):
            _append_payload_value(nested, values, seen)
        for nested in _iter_encoded_json_values(value):
            _append_payload_value(nested, values, seen)


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return repr(value)


def _looks_like_payload(value: str) -> bool:
    return looks_like_ae_payload(value)


def _looks_like_encoded_payload(value: str) -> bool:
    return any(True for _ in _iter_base64_texts(value))


def _flatten_road_items(items: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            item = dict(value)
            item.setdefault("_sourceIndex", len(out) + 1)
            out.append(item)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, (int, str)) and not isinstance(value, bool):
            out.append({"road": value, "round": len(out) + 1, "_sourceIndex": len(out) + 1})

    walk(items)
    return out


def _fold_text(value: str) -> str:
    lowered = value.strip().lower()
    normalized = unicodedata.normalize("NFD", lowered)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _snapshot_decode_signature(snapshot: TableSnapshot) -> str:
    round_parts = [
        f"{event.shoe}|{event.round_no if event.round_no is not None else index}|{event.outcome.value}"
        for index, event in enumerate(snapshot.rounds, start=1)
    ]
    return f"{snapshot.table_id}|{snapshot.table_name}|{snapshot.shoe}|" + ",".join(round_parts)


def _first_present(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _item_sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    stamp = _safe_int(item.get("stampTime")) or 0
    round_no = _safe_int(_first_present(item, "round", "roundNo", "gameRound")) or 0
    source_index = _safe_int(item.get("_sourceIndex")) or 0
    return (stamp, round_no, source_index)
