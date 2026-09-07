import json
import base64
import unittest

from ae_baccarat_workbench.ae_decode import (
    iter_payload_snapshots,
    parse_manual_sequence,
    table_id_to_name,
    table_name_to_ids,
)
from ae_baccarat_workbench.models import Outcome


class AeDecodeTests(unittest.TestCase):
    def test_table_name_and_id_helpers(self) -> None:
        self.assertEqual(table_name_to_ids("Baccarat C03"), [1003])
        self.assertEqual(table_id_to_name(1008), "Baccarat C08")

    def test_parse_manual_sequence_supports_vietnamese_labels(self) -> None:
        snapshot = parse_manual_sequence("B P T Cai Con Hoa", "Baccarat C01")

        self.assertEqual(snapshot.table_name, "Baccarat C01")
        self.assertEqual(snapshot.table_id, 1001)
        self.assertEqual([event.outcome for event in snapshot.rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])

    def test_parse_manual_sequence_supports_accented_vietnamese_labels(self) -> None:
        snapshot = parse_manual_sequence("C\u00e1i Con H\u00f2a", "Baccarat C02")

        self.assertEqual([event.outcome for event in snapshot.rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])

    def test_iter_payload_snapshots_reads_nested_road_info_once(self) -> None:
        payload = {
            "messageType": "GameHallInfo",
            "tableID": 1003,
            "message": {
                "roadInfo": {
                    "tableID": 1003,
                    "gameShoe": 12,
                    "markerRoads": [
                        {"road": 0, "round": 1},
                        {"road": 1, "round": 2},
                        {"road": 2, "round": 3},
                    ],
                }
            },
        }

        snapshots = iter_payload_snapshots(json.dumps(payload))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C03")
        self.assertEqual(snapshots[0].shoe, 12)
        self.assertEqual([event.outcome for event in snapshots[0].rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])

    def test_iter_payload_snapshots_reads_socket_io_road_list(self) -> None:
        payload = '42["road", {"roadInfo": {"tableID": 1004, "gameShoe": 7, "roadList": [0, 1, 2]}}]'

        snapshots = iter_payload_snapshots(payload, source="cdp-ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C04")
        self.assertEqual(snapshots[0].source, "cdp-ws")
        self.assertEqual([event.outcome for event in snapshots[0].rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])
        self.assertTrue(all(event.source == "cdp-ws" for event in snapshots[0].rounds))

    def test_iter_payload_snapshots_reads_nested_string_payload(self) -> None:
        nested = {
            "roadInfo": {
                "tableID": 1005,
                "gameShoe": 8,
                "markerRoads": [
                    {"winner": "Banker", "round": 1},
                    {"winner": "Player", "round": 2},
                ],
            }
        }
        payload = {"event": "cache", "data": json.dumps(nested)}

        snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-dom")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C05")
        self.assertEqual(snapshots[0].source, "cdp-dom")
        self.assertEqual([event.outcome for event in snapshots[0].rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
        ])

    def test_iter_payload_snapshots_reads_nested_road_list_string_without_road_info(self) -> None:
        nested = {
            "tableID": 1007,
            "gameShoe": 3,
            "roadList": [0, 1, 2],
        }
        payload = {"event": "cache", "data": json.dumps(nested)}

        snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-dom")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C07")
        self.assertEqual(snapshots[0].shoe, 3)
        self.assertEqual([event.outcome for event in snapshots[0].rounds], [
            Outcome.BANKER,
            Outcome.PLAYER,
            Outcome.TIE,
        ])

    def test_iter_payload_snapshots_reads_prefixed_base64_game_info(self) -> None:
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

        snapshots = iter_payload_snapshots(encoded, source="cdp-ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C12")
        self.assertEqual(snapshots[0].shoe, 20539)
        self.assertEqual(snapshots[0].latest_round.round_no, 55)
        self.assertEqual(snapshots[0].latest_round.outcome, Outcome.PLAYER)

    def test_single_latest_road_uses_current_game_round(self) -> None:
        payload = {
            "tableID": 1002,
            "tableName": "C02",
            "currentGameShoe": 27722,
            "currentGameRound": 32,
            "roads": [{"winner": 2}],
        }

        snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Baccarat C02")
        self.assertEqual(snapshots[0].shoe, 27722)
        self.assertEqual(snapshots[0].latest_round.round_no, 32)
        self.assertEqual(snapshots[0].latest_round.outcome, Outcome.PLAYER)

    def test_full_history_prefers_bead_road_over_big_road(self) -> None:
        payload = {
            "tableID": 8,
            "gameShoe": 47275,
            "currentGameRound": 6,
            "bigRoads": [
                {"road": 0, "round": 1},
                {"road": 2, "round": 2},
                {"road": 0, "round": 3},
                {"road": 0, "round": 4},
                {"road": 0, "round": 5},
                {"road": 2, "round": 6},
            ],
            "beadRoads": [
                {"winner": "Banker", "round": 1},
                {"winner": "Player", "round": 2},
                {"winner": "Banker", "round": 3},
                {"winner": "Banker", "round": 4},
                {"winner": "Tie", "round": 5},
                {"winner": "Player", "round": 6},
            ],
        }

        snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].table_name, "Table 8")
        self.assertEqual(
            [event.outcome for event in snapshots[0].rounds],
            [
                Outcome.BANKER,
                Outcome.PLAYER,
                Outcome.BANKER,
                Outcome.BANKER,
                Outcome.TIE,
                Outcome.PLAYER,
            ],
        )

    def test_big_road_uses_big_road_tie_count_decode_rule(self) -> None:
        payload = {
            "tableID": 1001,
            "gameShoe": 123,
            "gameRound": 12,
            "bigRoads": [{"road": 4, "count": 1}],
        }

        snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-ws")

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].latest_round.round_no, 12)
        self.assertEqual(snapshots[0].latest_round.outcome, Outcome.TIE)

    def test_big_road_pair_bits_do_not_change_banker_or_player_winner(self) -> None:
        expected = {
            0: Outcome.BANKER,
            1: Outcome.BANKER,
            2: Outcome.BANKER,
            3: Outcome.BANKER,
            8: Outcome.PLAYER,
            9: Outcome.PLAYER,
            10: Outcome.PLAYER,
            11: Outcome.PLAYER,
        }

        for road, outcome in expected.items():
            with self.subTest(road=road):
                payload = {
                    "tableID": 1004,
                    "gameShoe": 456,
                    "gameRound": 18,
                    "bigRoads": [{"road": road, "count": 0}],
                }
                snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-ws")

                self.assertEqual(len(snapshots), 1)
                self.assertEqual(snapshots[0].latest_round.outcome, outcome)

    def test_big_road_tie_bit_covers_pair_variants(self) -> None:
        for road in (4, 5, 6, 7, 12, 13, 14, 15):
            with self.subTest(road=road):
                payload = {
                    "tableID": 1004,
                    "gameShoe": 456,
                    "gameRound": 19,
                    "bigRoads": [{"road": road, "count": 1}],
                }
                snapshots = iter_payload_snapshots(json.dumps(payload), source="cdp-ws")

                self.assertEqual(len(snapshots), 1)
                self.assertEqual(snapshots[0].latest_round.outcome, Outcome.TIE)


if __name__ == "__main__":
    unittest.main()
