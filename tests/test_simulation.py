import unittest

from ae_baccarat_workbench.simulation import run_x3_simulation


def _row(index: int, table: str, side: str, outcome: str, pnl_delta: float) -> dict[str, object]:
    timestamp = f"2026-01-01T00:00:{index:02d}+00:00"
    return {
        "id": index,
        "created_at": timestamp,
        "settled_at": timestamp,
        "table_name": table,
        "strategy_id": "ml_strategy",
        "side": side,
        "stake": 10,
        "signal_fingerprint": f"{table}-{index}",
        "outcome": outcome,
        "pnl_delta": pnl_delta,
        "reason": "ML pass: win probability 60.0% >= threshold 55.0%",
    }


class X3SimulationTests(unittest.TestCase):
    def test_w8_reverse_cycle_triples_until_ml_loses(self) -> None:
        rows = [
            *[_row(index, "Table A", "B", "B", 9.5) for index in range(1, 9)],
            _row(9, "Table A", "B", "B", 9.5),
            _row(10, "Table A", "P", "P", 10),
            _row(11, "Table A", "B", "P", -10),
        ]

        report = run_x3_simulation(rows, banker_commission=0.05)

        self.assertEqual(report.rows_used, 11)
        self.assertEqual(len(report.cycles), 1)
        self.assertEqual(report.summary_all.open, 0)
        cycle = report.cycles[0]
        self.assertEqual(cycle.mode, "reverse")
        self.assertEqual(cycle.trigger, "W8")
        self.assertEqual(cycle.start_row_id, 9)
        self.assertEqual(cycle.bets, 3)
        self.assertEqual(cycle.misses_before_win, 2)
        self.assertEqual(cycle.pnl, 50)
        self.assertEqual(cycle.max_stake, 90)
        self.assertEqual(cycle.max_capital_at_risk, 130)
        self.assertEqual(cycle.max_drawdown, -40)
        self.assertEqual(report.summary_reverse.pnl, 50)

    def test_l6_follow_cycle_keeps_stake_on_push_and_stops_when_ml_wins(self) -> None:
        rows = [
            *[_row(index, "Table B", "B", "P", -10) for index in range(1, 7)],
            _row(7, "Table B", "B", "P", -10),
            _row(8, "Table B", "P", "T", 0),
            _row(9, "Table B", "B", "B", 9.5),
        ]

        report = run_x3_simulation(rows, banker_commission=0.05)

        self.assertEqual(len(report.cycles), 1)
        cycle = report.cycles[0]
        self.assertEqual(cycle.mode, "follow")
        self.assertEqual(cycle.trigger, "L6")
        self.assertEqual(cycle.start_row_id, 7)
        self.assertEqual(cycle.bets, 3)
        self.assertEqual(cycle.pushes, 1)
        self.assertEqual(cycle.misses_before_win, 1)
        self.assertEqual(cycle.current_stake, 30)
        self.assertEqual(cycle.max_stake, 30)
        self.assertEqual(cycle.max_capital_at_risk, 40)
        self.assertAlmostEqual(cycle.pnl, 18.5)
        self.assertEqual(report.summary_follow.pushes_closed, 1)
        self.assertEqual(report.summary_follow.max_misses_before_win, 1)


if __name__ == "__main__":
    unittest.main()
