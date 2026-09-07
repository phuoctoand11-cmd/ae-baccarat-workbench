from __future__ import annotations

from .models import BetSide, MoneyConfig, MoneyQuote, MoneyState, Outcome


class MoneyManager:
    def __init__(self, config: MoneyConfig | None = None) -> None:
        self.config = config or MoneyConfig()
        self.state = MoneyState()

    def quote(self) -> MoneyQuote:
        chain = self.config.stake_chain
        index = min(self.state.index, len(chain) - 1)
        stake = float(chain[index])
        if self.state.closed:
            return MoneyQuote(0.0, index, self.config.progression_mode, self.state.close_reason)
        if self.state.pnl <= -abs(self.config.stop_loss):
            return MoneyQuote(0.0, index, self.config.progression_mode, "Dừng vì chạm stop-loss")
        if self.state.pnl >= abs(self.config.take_profit):
            return MoneyQuote(0.0, index, self.config.progression_mode, "Dừng vì chạm take-profit")
        return MoneyQuote(stake, index, self.config.progression_mode, "Sẵn sàng paper trade")

    def apply_result(self, side: BetSide, outcome: Outcome, stake: float) -> float:
        pnl_delta = self._pnl_delta(side, outcome, stake)
        before_group = self.state.group_pnl
        self.state.pnl += pnl_delta
        self.state.group_pnl += pnl_delta
        self._advance_index(pnl_delta, before_group)
        self._apply_group_limits()
        return pnl_delta

    def reset_group(self) -> None:
        self.state.index = 0
        self.state.group_pnl = 0.0
        self.state.loss_count = 0
        self.state.closed = False
        self.state.close_reason = ""

    def reset_all(self) -> None:
        self.state = MoneyState()

    def _pnl_delta(self, side: BetSide, outcome: Outcome, stake: float) -> float:
        if stake <= 0:
            return 0.0
        if outcome is Outcome.TIE:
            return 0.0
        if side.outcome is not outcome:
            return -stake
        if side is BetSide.BANKER:
            return stake * (1.0 - self.config.banker_commission)
        return stake

    def _advance_index(self, pnl_delta: float, before_group: float) -> None:
        mode = self.config.progression_mode
        if mode == "flat":
            self.state.index = 0
            return
        if pnl_delta < 0:
            self.state.loss_count += 1
        max_index = len(self.config.stake_chain) - 1
        if pnl_delta == 0:
            return
        if mode == "loss_up_win_reset":
            self.state.index = min(max_index, self.state.index + 1) if pnl_delta < 0 else 0
            if pnl_delta > 0:
                self.state.loss_count = 0
        elif mode == "win_up_loss_reset":
            self.state.index = min(max_index, self.state.index + 1) if pnl_delta > 0 else 0
            if pnl_delta <= 0:
                self.state.loss_count = 0
        elif mode == "both_up":
            self.state.index = min(max_index, self.state.index + 1)
        elif mode == "win_up_loss_hold":
            if pnl_delta > 0:
                self.state.index = min(max_index, self.state.index + 1)
        elif mode == "profit_lock_loss_up":
            if pnl_delta < 0:
                self.state.index = min(max_index, self.state.index + 1)
            elif before_group + pnl_delta > 0:
                self.state.index = 0
                self.state.loss_count = 0
            else:
                self.state.index = min(max_index, self.state.index + 1)
        else:
            self.state.index = 0

    def _apply_group_limits(self) -> None:
        if self.state.group_pnl >= abs(self.config.group_take_profit):
            self.state.close_reason = "Chốt nhóm vì đạt group take-profit"
            self.state.closed = True
        elif self.state.group_pnl <= -abs(self.config.group_stop_loss):
            self.state.close_reason = "Dừng nhóm vì chạm group stop-loss"
            self.state.closed = True

