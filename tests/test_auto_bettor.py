from __future__ import annotations

import asyncio
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from ae_baccarat_workbench.models import BetSide
from ae_baccarat_workbench.monitor.auto_bettor import (
    BetOrder,
    BetResult,
    LiveAutoBettor,
    extract_target_round_and_shoe,
    find_game_websocket_url,
    invert_bet_side,
    map_stake_to_chips,
    normalize_bet_side,
    normalize_table_name,
    prepare_bet_orders,
    resolve_bet_side,
)
from ae_baccarat_workbench.monitor.provider_ack import ProviderAck


async def accepted_provider_ack(*_args: Any, **_kwargs: Any) -> ProviderAck:
    return ProviderAck(
        outcome="accepted",
        provider_bet_id="bet-test-001",
        provider_status="accepted",
        source="test",
        request_seen=True,
    )


def test_normalize_table_name() -> None:
    assert normalize_table_name("Baccarat C03") == "c3"
    assert normalize_table_name("Baccarat C07") == "c7"
    assert normalize_table_name("Baccarat C14") == "c14"
    assert normalize_table_name("C01") == "c1"
    assert normalize_table_name("baccarat c22") == "c22"


def test_map_stake_to_chips() -> None:
    # 1:1 point mapping to chip denominations:
    # 5 -> 5, 10 -> 10, 20 -> 20, 30 -> 30, 50 -> 50, 100 -> 100, 200 -> 200, 500 -> 500
    assert map_stake_to_chips(5) == ["5"]
    assert map_stake_to_chips(10) == ["10"]
    assert map_stake_to_chips(20) == ["20"]
    assert map_stake_to_chips(30) == ["30"]
    assert map_stake_to_chips(50) == ["50"]
    assert map_stake_to_chips(100) == ["100"]
    assert map_stake_to_chips(200) == ["200"]
    assert map_stake_to_chips(500) == ["500"]

    # Higher point denominations (1000, 2000, 5000, 10000, 20000)
    assert map_stake_to_chips(1000) == ["1000"]
    assert map_stake_to_chips(2000) == ["2000"]
    assert map_stake_to_chips(5000) == ["5000"]
    assert map_stake_to_chips(10000) == ["10000"]
    assert map_stake_to_chips(20000) == ["20000"]

    # Split / combined stakes
    assert map_stake_to_chips(15) == ["10", "5"]
    assert map_stake_to_chips(35) == ["30", "5"]
    assert map_stake_to_chips(70) == ["50", "20"]
    assert map_stake_to_chips(80) == ["50", "30"]
    assert map_stake_to_chips(250) == ["200", "50"]
    assert map_stake_to_chips(300) == ["200", "100"]
    assert map_stake_to_chips(1500) == ["1000", "500"]
    assert map_stake_to_chips(12500) == ["10000", "2000", "500"]

    # Edge cases
    assert map_stake_to_chips(0) == ["10"]
    assert map_stake_to_chips(-5) == ["10"]


def test_find_game_websocket_url() -> None:
    mock_targets = [
        {"url": "https://www.vietdfvn.com/vn/live-dealer/#", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/p1"},
        {
            "url": "https://sfcdf.tgmeq.com/player/webMain.jsp;jsessionid=XYZ",
            "webSocketDebuggerUrl": "ws://127.0.0.1:9222/game_ws",
        },
    ]

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_targets).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        ws_url = find_game_websocket_url("http://127.0.0.1:9222")
        assert ws_url == "ws://127.0.0.1:9222/game_ws"


def test_live_auto_bettor_thread_lifecycle() -> None:
    bettor = LiveAutoBettor()
    assert not bettor.is_running

    order = BetOrder(table_name="Baccarat C03", side="PLAYER", stake=10.0)
    done_orders: list[BetResult] = []
    finished_results: list[BetResult] = []
    statuses: list[str] = []

    # Mock _async_execute_orders
    async def mock_execute(orders, on_status, on_order_done):
        if on_status:
            on_status("Mock status running")
        res = BetResult(order=orders[0], success=True, message="Mock success", placed_at="2026-09-09T00:00:00Z")
        if on_order_done:
            on_order_done(res)
        return [res]

    with patch.object(bettor, "_async_execute_orders", side_effect=mock_execute):
        thread = bettor.execute_orders_background(
            orders=[order],
            on_status=lambda s: statuses.append(s),
            on_order_done=lambda r: done_orders.append(r),
            on_finished=lambda rs: finished_results.extend(rs),
        )
        thread.join(timeout=3.0)

    assert not bettor.is_running
    assert len(done_orders) == 1
    assert done_orders[0].success
    assert done_orders[0].order.table_name == "Baccarat C03"
    assert len(finished_results) == 1
    assert "Mock status running" in statuses


def test_extract_target_round_and_shoe() -> None:
    # Standard format: 'Table|Shoe|Round|Side'
    target_round, target_shoe = extract_target_round_and_shoe("Baccarat C11|22914|19|B")
    assert target_round == 20
    assert target_shoe == "22914"

    # Fallback to current_round_no and shoe
    target_round, target_shoe = extract_target_round_and_shoe("", current_round_no=15, current_shoe="1002")
    assert target_round == 16
    assert target_shoe == "1002"

    # Non-digit round fallback
    target_round, target_shoe = extract_target_round_and_shoe("Baccarat C01|shoe?|ts123|P", current_round_no=8)
    assert target_round == 9
    assert target_shoe == ""


def test_execute_single_order_round_mismatch_exits() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C11",
            side="BANKER",
            stake=10.0,
            target_round_no=20,
            target_shoe="22914",
        )

        js_calls = []

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "currentShoeRound" in expression:
                # Table is already at round 21, but our target was round 20
                return {
                    "loaded": True,
                    "countdown": 15,
                    "rawText": "15",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "22914",
                    "tableRound": 21,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "goHome2" in expression:
                return "clicked_home"
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(order, mock_eval_js, None)

        assert result.success is False
        assert "đã vượt quá ván mục tiêu" in result.message or "không khớp ván mục tiêu" in result.message
        assert any("goHome2" in call for call in js_calls)
        assert not any("chipsToClick" in call for call in js_calls)

    asyncio.run(_run())


def test_execute_single_order_shoe_mismatch_exits() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C11",
            side="BANKER",
            stake=10.0,
            target_round_no=20,
            target_shoe="22914",
        )

        js_calls = []

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "currentShoeRound" in expression:
                # Shoe changed to 22915
                return {
                    "loaded": True,
                    "countdown": 15,
                    "rawText": "15",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "22915",
                    "tableRound": 20,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "goHome2" in expression:
                return "clicked_home"
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(order, mock_eval_js, None)

        assert result.success is False
        assert "Giày bài đã thay đổi" in result.message
        assert any("goHome2" in call for call in js_calls)
        assert not any("chipsToClick" in call for call in js_calls)

    asyncio.run(_run())


def test_execute_single_order_round_match_places_bet() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C11",
            side="BANKER",
            stake=10.0,
            target_round_no=20,
            target_shoe="22914",
        )

        js_calls = []

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 4,
                    "tableShoe": "22914",
                    "tableRound": 20,
                    "placedAmount": "10",
                }
            if "currentShoeRound" in expression:
                # Matches target round 20 and shoe 22914
                return {
                    "loaded": True,
                    "countdown": 4,
                    "rawText": "4",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "22914",
                    "tableRound": 20,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {
                    "success": True,
                    "clickedSide": "BANKER",
                    "clickedChips": ["10k"],
                    "confirmed": True,
                }
            if "confirmBtn" in expression:
                return {"success": True}
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                ack_waiter=accepted_provider_ack,
            )

        assert result.success is True
        assert result.reason_code == "PROVIDER_ACCEPTED"
        assert any("chipsToClick" in call for call in js_calls)

    asyncio.run(_run())


def test_execute_single_order_rejects_countdown_below_four_seconds() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = prepare_bet_orders(
            [
                BetOrder(
                    table_name="Baccarat C11",
                    side="BANKER",
                    stake=10.0,
                    target_round_no=20,
                    target_shoe="22914",
                    signal_fingerprint="Baccarat C11|22914|19|B",
                )
            ]
        )[0]

        js_calls: list[str] = []
        audit_events: list[dict[str, Any]] = []

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 15,
                    "tableShoe": "25229",
                    "tableRound": 23,
                    "placedAmount": "20",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 3,
                    "rawText": "3",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "22914",
                    "tableRound": 20,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "goHome2" in expression:
                return "clicked_home"
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(order, mock_eval_js, None, audit_events.append)

        assert result.success is False
        assert "dưới ngưỡng an toàn 4s" in result.message
        assert result.reason_code == "COUNTDOWN_BELOW_4_AT_TABLE"
        assert audit_events[-1]["status"] == "skipped"
        assert audit_events[-1]["reason_code"] == "COUNTDOWN_BELOW_4_AT_TABLE"
        assert audit_events[-1]["countdown_seconds"] == 3
        assert not any("chipsToClick" in call for call in js_calls)

    asyncio.run(_run())


def test_normalize_bet_side() -> None:
    # Player variants
    assert normalize_bet_side("P") == "PLAYER"
    assert normalize_bet_side("p") == "PLAYER"
    assert normalize_bet_side("PLAYER") == "PLAYER"
    assert normalize_bet_side("Player") == "PLAYER"
    assert normalize_bet_side("con") == "PLAYER"
    assert normalize_bet_side("Con") == "PLAYER"
    assert normalize_bet_side("tay con") == "PLAYER"
    assert normalize_bet_side(BetSide.PLAYER) == "PLAYER"

    # Banker variants
    assert normalize_bet_side("B") == "BANKER"
    assert normalize_bet_side("b") == "BANKER"
    assert normalize_bet_side("BANKER") == "BANKER"
    assert normalize_bet_side("Banker") == "BANKER"
    assert normalize_bet_side("cai") == "BANKER"
    assert normalize_bet_side("Cái") == "BANKER"
    assert normalize_bet_side("Nhà cái") == "BANKER"
    assert normalize_bet_side(BetSide.BANKER) == "BANKER"


def test_execute_single_order_places_player_bet_when_side_is_P() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        # Side is 'P' as produced by signal.side.value
        order = BetOrder(
            table_name="Baccarat C09",
            side="P",
            stake=20.0,
            target_round_no=23,
            target_shoe="25229",
        )

        js_calls = []

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 15,
                    "tableShoe": "25229",
                    "tableRound": 23,
                    "placedAmount": "20",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 15,
                    "rawText": "15",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "25229",
                    "tableRound": 23,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {
                    "success": True,
                    "clickedSide": "PLAYER",
                    "clickedChips": ["20k"],
                    "confirmed": True,
                }
            if "confirmBtn" in expression:
                return {"success": True}
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                ack_waiter=accepted_provider_ack,
            )

        assert result.success is True
        assert "Con (Player)" in result.message
        # Verify that the generated JS targeted PLAYER and betBoxPlayer, NOT Banker!
        place_call = next(c for c in js_calls if "chipsToClick" in c)
        assert "targetSide = 'PLAYER'" in place_call
        assert "betBoxPlayer" in place_call
        assert "betBoxBanker" in place_call  # exists in ternary, but targetSide is PLAYER

    asyncio.run(_run())


def test_execute_single_order_waits_if_table_round_not_yet_target_round() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C01",
            side="B",
            stake=50.0,
            target_round_no=15,
            target_shoe="100",
        )

        attempts = 0

        async def mock_eval_js(expression: str) -> Any:
            nonlocal attempts
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 14,
                    "tableShoe": "100",
                    "tableRound": 15,
                    "placedAmount": "50",
                }
            if "currentShoeRound" in expression:
                attempts += 1
                # First 2 checks: table still shows previous round 14
                if attempts < 3:
                    return {
                        "loaded": True,
                        "countdown": 16,
                        "rawText": "16",
                        "isDealing": False,
                        "isShuffling": False,
                        "tableShoe": "100",
                        "tableRound": 14,
                    }
                # 3rd check: table DOM updates to target round 15
                return {
                    "loaded": True,
                    "countdown": 14,
                    "rawText": "14",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "100",
                    "tableRound": 15,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {
                    "success": True,
                    "placedAmount": "50",
                    "confirmed": True,
                }
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                ack_waiter=accepted_provider_ack,
            )

        assert result.success is True
        assert "Cái (Banker)" in result.message
        assert attempts >= 3

    asyncio.run(_run())


def test_waits_for_provider_ack_instead_of_fixed_two_second_dwell() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C07",
            side="P",
            stake=500.0,
            target_round_no=22,
            target_shoe="24981",
        )

        sleep_calls: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_calls.append(duration)

        async def mock_eval_js(expression: str) -> Any:
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 15,
                    "tableShoe": "24981",
                    "tableRound": 22,
                    "placedAmount": "500",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 15,
                    "rawText": "15",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "24981",
                    "tableRound": 22,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {
                    "success": True,
                    "placedAmount": "500",
                    "confirmed": True,
                }
            if "iframeGameHall" in expression:
                return True
            return {}

        with patch("asyncio.sleep", side_effect=fake_sleep):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                ack_waiter=accepted_provider_ack,
            )

        assert result.success is True
        assert result.reason_code == "PROVIDER_ACCEPTED"
        assert 2.0 not in sleep_calls

    asyncio.run(_run())


def test_ack_timeout_records_diagnostic_and_exits_table_to_lobby() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C03",
            side="B",
            stake=500.0,
            target_round_no=12,
            target_shoe="26445",
        )
        js_calls: list[str] = []
        ack_calls = 0

        async def timeout_ack(*_args: Any, **_kwargs: Any) -> ProviderAck:
            nonlocal ack_calls
            ack_calls += 1
            return ProviderAck(
                outcome="timeout",
                provider_status="ack_timeout",
                error_code="ACK_TIMEOUT",
                source="test",
                request_seen=True,
            )

        async def mock_eval_js(expression: str) -> Any:
            js_calls.append(expression)
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 4,
                    "tableShoe": "26445",
                    "tableRound": 12,
                    "placedAmount": "500",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 5,
                    "rawText": "5",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "26445",
                    "tableRound": 12,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {"success": True, "placedAmount": "500", "confirmed": True}
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                ack_waiter=timeout_ack,
            )

        assert result.success is False
        assert result.reason_code == "ACK_TIMEOUT"
        assert result.confirm_clicked_at
        assert ack_calls == 1
        assert any("backToGameHall" in call or "goHome2" in call for call in js_calls)

    asyncio.run(_run())


def test_provider_rejection_records_terminal_status_without_changing_autobet_mode() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C18",
            side="P",
            stake=500.0,
            target_round_no=46,
            target_shoe="5041",
        )
        audit_events: list[dict[str, Any]] = []

        async def rejected_ack(*_args: Any, **_kwargs: Any) -> ProviderAck:
            return ProviderAck(
                outcome="rejected",
                provider_status="rejected",
                error_code="BETTING_CLOSED",
                source="websocket",
                request_seen=True,
            )

        async def mock_eval_js(expression: str) -> Any:
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "success": True,
                    "confirmed": True,
                    "countdown": 6,
                    "tableShoe": "5041",
                    "tableRound": 46,
                    "placedAmount": "500",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 7,
                    "rawText": "7",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "5041",
                    "tableRound": 46,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {"success": True, "placedAmount": "500", "confirmed": True}
            if "goHome2" in expression:
                return "clicked_home"
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                audit_events.append,
                ack_waiter=rejected_ack,
            )

        assert result.success is False
        assert result.reason_code == "PROVIDER_REJECTED"
        assert audit_events[-1]["stage"] == "PROVIDER_REJECTED"
        assert audit_events[-1]["status"] == "provider_rejected"
        assert audit_events[-1]["payload"]["provider_error_code"] == "BETTING_CLOSED"

    asyncio.run(_run())


def test_preconfirm_failure_never_calls_ack_waiter_or_confirm_stage() -> None:
    async def _run() -> None:
        bettor = LiveAutoBettor()
        order = BetOrder(
            table_name="Baccarat C03",
            side="B",
            stake=500.0,
            target_round_no=12,
            target_shoe="26445",
        )
        audit_events: list[dict[str, Any]] = []
        ack_called = False

        async def should_not_run(*_args: Any, **_kwargs: Any) -> ProviderAck:
            nonlocal ack_called
            ack_called = True
            return await accepted_provider_ack()

        async def mock_eval_js(expression: str) -> Any:
            if "findCard" in expression:
                return {"success": True}
            if "preConfirmCheck" in expression:
                return {
                    "error": "Countdown below final threshold",
                    "errorCode": "PRECONFIRM_COUNTDOWN_BELOW_4",
                    "countdown": 3,
                    "tableShoe": "26445",
                    "tableRound": 12,
                    "placedAmount": "500",
                }
            if "currentShoeRound" in expression:
                return {
                    "loaded": True,
                    "countdown": 5,
                    "rawText": "5",
                    "isDealing": False,
                    "isShuffling": False,
                    "tableShoe": "26445",
                    "tableRound": 12,
                }
            if "countdownTime" in expression and "betBoxPlayer" in expression:
                return True
            if "chipsToClick" in expression:
                return {"success": True, "placedAmount": "500", "confirmed": True}
            if "goHome2" in expression:
                return "clicked_home"
            return {}

        with patch("asyncio.sleep", return_value=None):
            result = await bettor._execute_single_order(
                order,
                mock_eval_js,
                None,
                audit_events.append,
                ack_waiter=should_not_run,
            )

        assert result.reason_code == "PRECONFIRM_COUNTDOWN_BELOW_4"
        assert ack_called is False
        assert not any(event["stage"] == "CONFIRM_CLICKED" for event in audit_events)

    asyncio.run(_run())


def test_invert_bet_side() -> None:
    # Test auto_bettor invert_bet_side
    assert invert_bet_side("P") == "B"
    assert invert_bet_side("PLAYER") == "B"
    assert invert_bet_side("Con") == "B"
    assert invert_bet_side("B") == "P"
    assert invert_bet_side("BANKER") == "P"
    assert invert_bet_side("Cái") == "P"
    assert invert_bet_side(BetSide.PLAYER) == "B"
    assert invert_bet_side(BetSide.BANKER) == "P"


def test_resolve_bet_side() -> None:
    # Forward mode returns original side ("P" or "B")
    assert resolve_bet_side("P", "forward") == "P"
    assert resolve_bet_side("PLAYER", "forward") == "P"
    assert resolve_bet_side(BetSide.PLAYER, "forward") == "P"
    assert resolve_bet_side("B", "forward") == "B"
    assert resolve_bet_side("BANKER", "forward") == "B"
    assert resolve_bet_side(BetSide.BANKER, "forward") == "B"

    # Default mode is forward
    assert resolve_bet_side("P") == "P"
    assert resolve_bet_side("B") == "B"

    # Inverse mode flips the side
    assert resolve_bet_side("P", "inverse") == "B"
    assert resolve_bet_side(BetSide.PLAYER, "inverse") == "B"
    assert resolve_bet_side("PLAYER", "inverse") == "B"
    assert resolve_bet_side("Con", "inverse") == "B"

    assert resolve_bet_side("B", "inverse") == "P"
    assert resolve_bet_side(BetSide.BANKER, "inverse") == "P"
    assert resolve_bet_side("BANKER", "inverse") == "P"
    assert resolve_bet_side("Cái", "inverse") == "P"

    # Variants of inverse string
    assert resolve_bet_side("P", "nguoc") == "B"
    assert resolve_bet_side("P", "ngược") == "B"
    assert resolve_bet_side("P", "reverse") == "B"
    assert resolve_bet_side("P", "Đánh Ngược") == "B"
    assert resolve_bet_side("B", "Đánh Ngược") == "P"
    assert resolve_bet_side("B", "nguoc") == "P"

