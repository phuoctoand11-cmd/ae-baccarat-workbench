import unittest

from ae_baccarat_workbench.models import Outcome, RoundEvent, TableSnapshot


class ModelTests(unittest.TestCase):
    def test_snapshot_uses_round_number_for_shoe_position(self) -> None:
        snapshot = TableSnapshot(
            table_name="Baccarat C01",
            table_id=1001,
            shoe="s1",
            rounds=(
                RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="s1", round_no=55),
            ),
        )

        self.assertEqual(snapshot.observed_rounds, 1)
        self.assertEqual(snapshot.current_round_no, 55)
        self.assertEqual(snapshot.total_rounds, 55)
        self.assertEqual(snapshot.known_missing_rounds, 54)
        self.assertAlmostEqual(snapshot.shoe_position_ratio(expected_rounds=72), 55 / 72)

    def test_current_shoe_road_uses_only_latest_shoe(self) -> None:
        snapshot = TableSnapshot(
            table_name="Baccarat C01",
            table_id=1001,
            shoe="2",
            rounds=(
                RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="1", round_no=1),
                RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="1", round_no=2),
                RoundEvent("Baccarat C01", Outcome.PLAYER, table_id=1001, shoe="2", round_no=1),
                RoundEvent("Baccarat C01", Outcome.BANKER, table_id=1001, shoe="2", round_no=2),
            ),
        )

        self.assertEqual(snapshot.current_shoe_road(), "P B")
        self.assertEqual(snapshot.current_shoe_road(limit=1), "B")

    def test_invert_and_resolve_bet_side(self) -> None:
        from ae_baccarat_workbench.models import BetSide, invert_bet_side, resolve_bet_side

        # invert_bet_side
        self.assertEqual(invert_bet_side("P"), "B")
        self.assertEqual(invert_bet_side("PLAYER"), "B")
        self.assertEqual(invert_bet_side("Con"), "B")
        self.assertEqual(invert_bet_side("B"), "P")
        self.assertEqual(invert_bet_side("BANKER"), "P")
        self.assertEqual(invert_bet_side(BetSide.PLAYER), "B")
        self.assertEqual(invert_bet_side(BetSide.BANKER), "P")

        # resolve_bet_side forward
        self.assertEqual(resolve_bet_side("P", "forward"), "P")
        self.assertEqual(resolve_bet_side("B", "forward"), "B")
        self.assertEqual(resolve_bet_side(BetSide.PLAYER), "P")
        self.assertEqual(resolve_bet_side(BetSide.BANKER), "B")

        # resolve_bet_side inverse
        self.assertEqual(resolve_bet_side("P", "inverse"), "B")
        self.assertEqual(resolve_bet_side("B", "inverse"), "P")
        self.assertEqual(resolve_bet_side("P", "Đánh Ngược"), "B")
        self.assertEqual(resolve_bet_side("B", "nguoc"), "P")


if __name__ == "__main__":
    unittest.main()
