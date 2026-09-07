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


if __name__ == "__main__":
    unittest.main()
