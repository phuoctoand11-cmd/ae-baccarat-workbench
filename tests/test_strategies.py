import unittest

from ae_baccarat_workbench.ae_decode import parse_manual_sequence
from ae_baccarat_workbench.models import BetSide, StrategyAction
from ae_baccarat_workbench.strategies import RunLengthStrategy, ScccCsssStrategy, StrategyContext


class StrategyTests(unittest.TestCase):
    def test_csss_pattern_follows_latest_side(self) -> None:
        context = StrategyContext(parse_manual_sequence("B P P P P", "Baccarat C01"))

        signal = ScccCsssStrategy().evaluate(context)

        self.assertEqual(signal.action, StrategyAction.BET)
        self.assertEqual(signal.side, BetSide.PLAYER)
        self.assertEqual(signal.features["pattern"], "CSSS")

    def test_sccc_pattern_uses_context_filter_before_betting(self) -> None:
        context = StrategyContext(parse_manual_sequence("B B P B P", "Baccarat C01"))

        signal = ScccCsssStrategy().evaluate(context)

        self.assertEqual(signal.action, StrategyAction.BET)
        self.assertEqual(signal.side, BetSide.BANKER)
        self.assertEqual(signal.features["pattern"], "SCCC")

    def test_run_length_follows_long_current_run(self) -> None:
        context = StrategyContext(parse_manual_sequence("P B B B B", "Baccarat C01"))

        signal = RunLengthStrategy().evaluate(context)

        self.assertEqual(signal.action, StrategyAction.BET)
        self.assertEqual(signal.side, BetSide.BANKER)
        self.assertEqual(signal.features["run_length"], 4)


if __name__ == "__main__":
    unittest.main()
