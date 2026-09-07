import base64
import json
import time
import unittest

from ae_baccarat_workbench.models import Outcome
from ae_baccarat_workbench.monitor.cdp import (
    DOM_DATA_SCRIPT,
    AeSexyCdpMonitor,
    _cdp_body_text,
    _is_relevant_raw_target,
    _is_relevant_refresh_url,
    _looks_like_bootstrap_resource,
    _looks_interesting,
    _may_contain_text_payload,
)


class CdpMonitorTests(unittest.TestCase):
    def test_payload_filter_accepts_road_list_without_road_info(self) -> None:
        payload = {
            "tableID": 1006,
            "gameShoe": 9,
            "roadList": [0, 1, 2],
        }

        self.assertTrue(_looks_interesting(json.dumps(payload)))

    def test_handle_payload_emits_road_list_snapshot(self) -> None:
        snapshots = []
        statuses = []
        payload = {
            "tableID": 1006,
            "gameShoe": 9,
            "roadList": [0, 1, 2],
        }
        monitor = AeSexyCdpMonitor(
            "http://unused",
            on_snapshot=snapshots.append,
            on_status=statuses.append,
        )

        monitor._handle_payload(json.dumps(payload), source="ws")
        monitor._handle_payload(json.dumps(payload), source="ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C06")
        self.assertEqual(snapshots[0].source, "cdp-ws")
        self.assertEqual([event.outcome for event in snapshots[0].rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])
        self.assertEqual(len(statuses), 1)
        self.assertIn("Live update #1", statuses[0])

    def test_handle_payload_emits_prefixed_base64_game_info_snapshot(self) -> None:
        snapshots = []
        payload = {
            "status": "200",
            "messageType": "GameInfo",
            "message": {
                "tableID": 1012,
                "gameShoe": 20539,
                "gameRound": 55,
                "eventType": "GP_WINNER",
                "bankerHandValue": 7,
                "playerHandValue": 9,
                "winner": 2,
            },
        }
        encoded = "2gdb" + base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
        monitor = AeSexyCdpMonitor("http://unused", on_snapshot=snapshots.append)

        self.assertTrue(_looks_interesting(encoded))
        monitor._handle_payload(encoded, source="ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C12")
        self.assertEqual(snapshots[0].latest_round.outcome, Outcome.PLAYER)

    def test_raw_target_filter_accepts_live_iframes(self) -> None:
        self.assertTrue(
            _is_relevant_raw_target(
                {
                    "type": "iframe",
                    "title": "Sexy",
                    "url": "https://sfcdf.mhuxu.com/player/webMain.jsp",
                }
            )
        )
        self.assertTrue(
            _is_relevant_raw_target(
                {
                    "type": "iframe",
                    "title": "https://sfcdf.mex777.com/player/webMain.jsp",
                    "url": "https://sfcdf.mex777.com/player/webMain.jsp;jsessionid=abc?dm=1",
                }
            )
        )
        self.assertFalse(
            _is_relevant_raw_target(
                {
                    "type": "service_worker",
                    "title": "Supabase",
                    "url": "https://ss.supabase.com/_/service_worker/sw.js",
                }
            )
        )

    def test_cdp_body_text_decodes_base64_json(self) -> None:
        payload = json.dumps({"tableID": 1006, "gameShoe": 9, "roadList": [0, 1, 2]})
        encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")

        self.assertEqual(_cdp_body_text({"body": encoded, "base64Encoded": True}), payload)

    def test_dom_probe_searches_secondary_road_keys(self) -> None:
        for needle in ("roadList", "gameResults", "tableID", "tableName"):
            self.assertIn(needle, DOM_DATA_SCRIPT)

    def test_bootstrap_urls_are_treated_as_possible_history_payloads(self) -> None:
        url = "https://prod.vbgames88.com/play;sid=abc/api/portals/sx~~lobby~baccarat/sessions/1/gateways/10001/init"

        self.assertTrue(_may_contain_text_payload(url, ""))
        self.assertTrue(_looks_like_bootstrap_resource(url))
        self.assertFalse(
            _is_relevant_raw_target(
                {
                    "type": "iframe",
                    "title": "Casino Live Dealer ad",
                    "url": "https://asia.adform.net/serving/container/?PageName=Casino+Live+Dealer",
                }
            )
        )

    def test_refresh_target_url_accepts_live_domains_only(self) -> None:
        self.assertTrue(_is_relevant_refresh_url("https://sfcdf.mhuxu.com/player/webMain.jsp"))
        self.assertTrue(_is_relevant_refresh_url("https://sfcdf.tgmeq.com/player/webMain.jsp;jsessionid=abc?dm=1"))
        self.assertTrue(_is_relevant_refresh_url("https://www.dafabet.com/vn/live-dealer/"))
        self.assertTrue(_is_relevant_refresh_url("https://www.dafabet.com/live-dealer/sexy-casino"))
        self.assertFalse(_is_relevant_refresh_url("https://www.google.com/search?q=baccarat"))

    def test_raw_target_accepts_sfcdf_provider_shards(self) -> None:
        self.assertTrue(
            _is_relevant_raw_target(
                {
                    "type": "iframe",
                    "title": "https://sfcdf.tgmeq.com/player/webMain.jsp",
                    "url": "https://sfcdf.tgmeq.com/player/webMain.jsp;jsessionid=abc?dm=1",
                }
            )
        )

    def test_finished_raw_target_can_be_attached_again_with_the_same_id(self) -> None:
        monitor = AeSexyCdpMonitor("http://unused", on_snapshot=lambda snapshot: None)
        task = object()
        monitor._seen_raw_targets.add("provider-target")
        monitor._raw_tasks.add(task)  # type: ignore[arg-type]

        monitor._raw_target_finished("provider-target", task)  # type: ignore[arg-type]

        self.assertNotIn("provider-target", monitor._seen_raw_targets)
        self.assertNotIn(task, monitor._raw_tasks)


class CdpMonitorRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_reloads_relevant_page_after_live_silence(self) -> None:
        statuses = []
        monitor = AeSexyCdpMonitor(
            "http://unused",
            on_snapshot=lambda snapshot: None,
            on_status=statuses.append,
            auto_refresh_seconds=30,
        )
        page = _FakePage(
            "https://example.com/holder",
            frames=[_FakeFrame("https://sfcdf.mhuxu.com/player/webMain.jsp")],
        )
        monitor._last_live_snapshot_at = time.monotonic() - 31

        await monitor._refresh_page_if_due(page)

        self.assertEqual(page.reload_count, 1)
        self.assertTrue(any("Watchdog refreshed silent live tab" in status for status in statuses))

    async def test_watchdog_reloads_outer_live_dealer_page_after_silence(self) -> None:
        monitor = AeSexyCdpMonitor(
            "http://unused",
            on_snapshot=lambda snapshot: None,
            auto_refresh_seconds=30,
        )
        page = _FakePage("https://www.dafabet.com/vn/live-dealer/")
        monitor._last_live_snapshot_at = time.monotonic() - 31

        await monitor._refresh_page_if_due(page)

        self.assertEqual(page.reload_count, 1)

    async def test_watchdog_does_not_reload_irrelevant_page(self) -> None:
        monitor = AeSexyCdpMonitor(
            "http://unused",
            on_snapshot=lambda snapshot: None,
            auto_refresh_seconds=30,
        )
        page = _FakePage("https://www.google.com/search?q=baccarat")
        monitor._last_live_snapshot_at = time.monotonic() - 31

        await monitor._refresh_page_if_due(page)

        self.assertEqual(page.reload_count, 0)

    async def test_watchdog_does_not_reload_while_live_snapshots_are_fresh(self) -> None:
        monitor = AeSexyCdpMonitor(
            "http://unused",
            on_snapshot=lambda snapshot: None,
            auto_refresh_seconds=30,
        )
        page = _FakePage(
            "https://example.com/holder",
            frames=[_FakeFrame("https://sfcdf.mhuxu.com/player/webMain.jsp")],
        )
        monitor._last_live_snapshot_at = time.monotonic()

        await monitor._refresh_page_if_due(page)

        self.assertEqual(page.reload_count, 0)


class _FakeFrame:
    def __init__(self, url: str) -> None:
        self.url = url


class _FakePage:
    def __init__(self, url: str, *, frames: list[_FakeFrame] | None = None, title: str = "") -> None:
        self.url = url
        self.frames = frames or []
        self._title = title
        self.reload_count = 0

    async def reload(self, **kwargs) -> None:
        self.reload_count += 1

    async def title(self) -> str:
        return self._title


if __name__ == "__main__":
    unittest.main()
