from __future__ import annotations

import asyncio
import json
import time

from ae_baccarat_workbench.monitor.auto_bettor import BetOrder
from ae_baccarat_workbench.monitor.provider_ack import (
    ProviderAckMonitor,
    _endpoint_has_bet_context,
    _safe_endpoint,
    classify_provider_payload,
    sanitize_provider_payload,
)


def _order() -> BetOrder:
    return BetOrder(
        table_name="Baccarat C03",
        side="BANKER",
        stake=500.0,
        target_shoe="26445",
        target_round_no=11,
    )


def test_sanitize_provider_payload_keeps_audit_fields_and_drops_secrets() -> None:
    payload = {
        "authorization": "Bearer top-secret",
        "cookie": "session=top-secret",
        "accountId": "member-123",
        "data": {
            "transactionId": "TXN-9001",
            "status": "accepted",
            "tableName": "Baccarat C03",
            "gameShoe": "26445",
            "gameRound": 11,
            "betSide": "BANKER",
            "betAmount": 500,
        },
    }

    sanitized = sanitize_provider_payload(payload)
    serialized = json.dumps(sanitized, ensure_ascii=False).lower()

    assert sanitized["provider_bet_id"] == "TXN-9001"
    assert sanitized["provider_status"] == "accepted"
    assert sanitized["table"] == "Baccarat C03"
    assert sanitized["shoe"] == "26445"
    assert sanitized["round"] == "11"
    assert sanitized["side"] == "BANKER"
    assert sanitized["amount"] == "500"
    assert "top-secret" not in serialized
    assert "authorization" not in serialized
    assert "cookie" not in serialized
    assert "accountid" not in serialized


def test_classify_provider_payload_requires_positive_status_and_bet_id() -> None:
    accepted = sanitize_provider_payload(
        {
            "betId": "BET-123",
            "status": "accepted",
            "table": "C03",
            "shoe": "26445",
            "round": 11,
            "side": "B",
            "amount": 500,
        }
    )
    ack = classify_provider_payload(accepted, _order(), source="websocket")
    assert ack is not None
    assert ack.outcome == "accepted"
    assert ack.provider_bet_id == "BET-123"
    assert ack.table == "C03"
    assert ack.shoe == "26445"
    assert ack.round_no == "11"
    assert ack.side == "B"
    assert ack.amount == "500"

    without_receipt = sanitize_provider_payload(
        {"status": "accepted", "table": "C03", "round": 11, "amount": 500}
    )
    assert classify_provider_payload(without_receipt, _order()) is None


def test_classify_provider_payload_rejects_mismatched_transaction_identity() -> None:
    mismatched = sanitize_provider_payload(
        {
            "transactionId": "TXN-OTHER",
            "status": "accepted",
            "table": "C03",
            "shoe": "26445",
            "round": 12,
            "side": "BANKER",
            "amount": 500,
        }
    )
    assert classify_provider_payload(mismatched, _order()) is None


def test_classify_provider_rejection_keeps_error_code() -> None:
    rejected = sanitize_provider_payload(
        {
            "transactionId": "TXN-REJECTED",
            "status": "rejected",
            "errorCode": "BETTING_CLOSED",
            "message": "Betting closed",
            "table": "C03",
            "shoe": "26445",
            "round": 11,
            "side": "BANKER",
            "amount": 500,
        }
    )
    ack = classify_provider_payload(rejected, _order(), source="http_response")
    assert ack is not None
    assert ack.outcome == "rejected"
    assert ack.error_code == "BETTING_CLOSED"


def test_no_error_message_does_not_create_false_rejection() -> None:
    payload = sanitize_provider_payload(
        {
            "transactionId": "TXN-OK",
            "status": "accepted",
            "message": "No error",
            "table": "C03",
            "round": 11,
            "amount": 500,
        }
    )
    ack = classify_provider_payload(payload, _order())
    assert ack is not None
    assert ack.outcome == "accepted"


def test_monitor_timeout_reports_whether_bet_request_was_seen() -> None:
    async def run() -> None:
        monitor = ProviderAckMonitor("ws://unused")
        since = time.monotonic()
        monitor._record_payload(
            {"bet_context": True, "observed_keys": ["endpoint"]},
            source="http_request",
            direction="request",
            endpoint="provider.example/bet/place",
        )
        ack = await monitor.wait_for_ack(_order(), since, timeout=0)
        assert ack.outcome == "timeout"
        assert ack.request_seen is True
        assert ack.candidate_count == 1

    asyncio.run(run())


def test_monitor_correlates_request_identity_with_provider_response() -> None:
    async def run() -> None:
        monitor = ProviderAckMonitor("ws://unused")
        since = time.monotonic()
        monitor._record_payload(
            sanitize_provider_payload(
                {
                    "event": "placeBet",
                    "table": "C03",
                    "shoe": "26445",
                    "round": 11,
                    "side": "BANKER",
                    "amount": 500,
                }
            ),
            source="websocket",
            direction="sent",
            endpoint="provider.example/socket",
            request_id="ws-1",
        )
        monitor._record_payload(
            sanitize_provider_payload({"transactionId": "TXN-9002", "status": "accepted"}),
            source="websocket",
            direction="received",
            endpoint="provider.example/socket",
            request_id="ws-1",
        )

        ack = await monitor.wait_for_ack(_order(), since, timeout=0)
        assert ack.outcome == "accepted"
        assert ack.provider_bet_id == "TXN-9002"
        assert ack.request_seen is True
        assert ack.table == "C03"
        assert ack.shoe == "26445"
        assert ack.round_no == "11"
        assert ack.side == "BANKER"
        assert ack.amount == "500"

    asyncio.run(run())


def test_endpoint_drops_query_and_redacts_credential_like_path_segments() -> None:
    endpoint = _safe_endpoint(
        "https://provider.example/api/"
        "eyJhbGciOiJIUzI1NiJ9.secret-token-value/place?token=do-not-store"
    )
    assert endpoint == "provider.example/[redacted]/place"
    assert "do-not-store" not in endpoint
    assert "secret-token-value" not in endpoint


def test_classify_ae_sexy_add_my_transaction_response() -> None:
    order = BetOrder(
        table_name="Baccarat C07",
        side="BANKER",
        stake=500.0,
        target_shoe="26445",
        target_round_no=11,
    )
    request_data = {
        "tableID": "1007",
        "gameShoe": "26445",
        "gameRound": "11",
        "data": json.dumps([{"categoryIdx": 0, "categoryName": "Banker", "stake": 500}]),
    }
    response_data = {
        "status": "200",
        "message": json.dumps({
            "balance": 15420.5,
            "txns": {
                "0": {
                    "success": True,
                    "stake": 500,
                    "categoryName": "Banker",
                    "txId": "TXN-1007-8892",
                }
            }
        })
    }
    sanitized_req = sanitize_provider_payload(request_data)
    sanitized_resp = sanitize_provider_payload(response_data)
    
    assert sanitized_req["table"] == "1007"
    assert sanitized_resp["provider_bet_id"] == "TXN-1007-8892"
    assert sanitized_resp["amount"] == "500"
    assert sanitized_resp["side"] == "Banker"

    # In monitor, request and response are merged
    merged = {**sanitized_req, **sanitized_resp}
    ack = classify_provider_payload(
        merged,
        order,
        source="http_response",
        endpoint="bpweb.arrpar.com/player/update/addMyTransaction",
    )
    assert ack is not None
    assert ack.outcome == "accepted"
    assert ack.provider_bet_id == "TXN-1007-8892"
    assert ack.amount == "500"


def test_query_bet_limit_excluded_from_provider_ack() -> None:
    # 1. Endpoint exclusion
    assert not _endpoint_has_bet_context("bpweb.arrpar.com/query/queryBetLimit")
    assert not _endpoint_has_bet_context("provider.com/api/getBetLimit")
    assert not _endpoint_has_bet_context("provider.com/heartbeat")

    # 2. classify_provider_payload rejects excluded endpoint even if payload looks positive
    limit_payload = {
        "status": "200",
        "provider_status": "200",
        "bet_context": True,
    }
    ack = classify_provider_payload(
        limit_payload,
        _order(),
        source="http_response",
        endpoint="bpweb.arrpar.com/query/queryBetLimit",
    )
    assert ack is None

    # 3. Limit fields do not activate bet_context
    limit_data = {
        "betLimit": 50000,
        "bankerBonusMaxBet": 10000,
        "playerBonusMinBet": 200,
        "status": "200",
    }
    sanitized = sanitize_provider_payload(limit_data)
    assert sanitized["bet_context"] is False


def test_table_broadcast_websocket_packet_does_not_trigger_false_accepted() -> None:
    order = _order()
    # Simulates the exact AE Sexy broadcast websocket packet observed during live bets
    broadcast_data = {
        "betCount": 29,
        "betInfo": [],
        "currentBet": 125000,
        "gameRound": 11,
        "gameShoe": "26445",
        "handler": "roomHandler",
        "maxBetRound": 75,
        "message": "success",
        "messageType": "tableState",
        "status": "200",
        "tableID": "1003",
        "tableName": "Baccarat C03",
        "timestamp": 1789826636,
        "totalCurrentBet": 350000,
        "typeCode": "baccarat",
    }
    sanitized = sanitize_provider_payload(broadcast_data)
    # Broadcast fields (betCount, currentBet, totalCurrentBet, maxBetRound, betInfo) must not activate bet_context
    assert sanitized["bet_context"] is False

    # Even if bet_context were forced True, without real bet_id or matching side/amount, it must return None
    sanitized["bet_context"] = True
    ack = classify_provider_payload(sanitized, order, source="websocket")
    assert ack is None


