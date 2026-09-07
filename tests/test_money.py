import unittest

from ae_baccarat_workbench.models import BetSide, MoneyConfig, Outcome
from ae_baccarat_workbench.money import MoneyManager


class MoneyManagerTests(unittest.TestCase):
    def test_loss_up_win_reset_and_banker_commission(self) -> None:
        manager = MoneyManager(
            MoneyConfig(
                stake_chain=(10, 20, 30),
                progression_mode="loss_up_win_reset",
                stop_loss=500,
                take_profit=500,
                group_take_profit=500,
                group_stop_loss=500,
            )
        )

        first = manager.quote()
        self.assertEqual(first.stake, 10)

        loss = manager.apply_result(BetSide.BANKER, Outcome.PLAYER, first.stake)
        self.assertEqual(loss, -10)
        self.assertEqual(manager.quote().stake, 20)

        win = manager.apply_result(BetSide.BANKER, Outcome.BANKER, 20)
        self.assertEqual(win, 19)
        self.assertEqual(manager.quote().stake, 10)
        self.assertEqual(manager.state.pnl, 9)

    def test_tie_is_push(self) -> None:
        manager = MoneyManager(MoneyConfig(stake_chain=(10,), progression_mode="flat"))

        delta = manager.apply_result(BetSide.PLAYER, Outcome.TIE, 10)

        self.assertEqual(delta, 0)
        self.assertEqual(manager.state.pnl, 0)


if __name__ == "__main__":
    unittest.main()
