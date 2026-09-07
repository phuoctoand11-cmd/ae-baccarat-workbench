from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import BetSide, Outcome, StrategyAction, StrategySignal, TableSnapshot


def side_from_outcome(outcome: Outcome) -> BetSide | None:
    if outcome is Outcome.BANKER:
        return BetSide.BANKER
    if outcome is Outcome.PLAYER:
        return BetSide.PLAYER
    return None


def opposite_side(side: BetSide) -> BetSide:
    return BetSide.PLAYER if side is BetSide.BANKER else BetSide.BANKER


def signs_from_outcomes(outcomes: list[Outcome]) -> list[str]:
    non_tie = [o for o in outcomes if o is not Outcome.TIE]
    signs: list[str] = []
    for prev, current in zip(non_tie, non_tie[1:]):
        signs.append("-" if prev is current else "+")
    return signs


def current_run_length(outcomes: list[Outcome]) -> int:
    non_tie = [o for o in outcomes if o is not Outcome.TIE]
    if not non_tie:
        return 0
    last = non_tie[-1]
    length = 0
    for value in reversed(non_tie):
        if value is last:
            length += 1
        else:
            break
    return length


@dataclass(frozen=True)
class StrategyContext:
    table: TableSnapshot

    @property
    def outcomes(self) -> list[Outcome]:
        return self.table.outcomes(skip_tie=False)

    @property
    def bp_outcomes(self) -> list[Outcome]:
        return self.table.outcomes(skip_tie=True)

    @property
    def latest_bp_side(self) -> BetSide | None:
        if not self.bp_outcomes:
            return None
        return side_from_outcome(self.bp_outcomes[-1])

    @property
    def round_fingerprint(self) -> str:
        return self.table.latest_fingerprint()


class BetStrategy(Protocol):
    strategy_id: str
    name: str

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        ...


class BaseStrategy:
    strategy_id = "base"
    name = "Base"

    def skip(self, context: StrategyContext, reason: str, **features: object) -> StrategySignal:
        return StrategySignal(
            table_name=context.table.table_name,
            strategy_id=self.strategy_id,
            action=StrategyAction.SKIP,
            side=None,
            confidence=0.0,
            reason=reason,
            round_fingerprint=context.round_fingerprint,
            features=dict(features),
        )

    def bet(
        self,
        context: StrategyContext,
        side: BetSide,
        confidence: float,
        reason: str,
        **features: object,
    ) -> StrategySignal:
        return StrategySignal(
            table_name=context.table.table_name,
            strategy_id=self.strategy_id,
            action=StrategyAction.BET,
            side=side,
            confidence=max(0.0, min(1.0, confidence)),
            reason=reason,
            round_fingerprint=context.round_fingerprint,
            features=dict(features),
        )


class ScccCsssStrategy(BaseStrategy):
    strategy_id = "csss_sccc"
    name = "CSSS/SCCC"

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        bp = context.bp_outcomes
        if len(bp) < 5:
            return self.skip(context, "Cần ít nhất 5 kết quả B/P để đọc SCCC/CSSS")
        signs = signs_from_outcomes(bp)
        last4 = "".join(signs[-4:])
        latest = context.latest_bp_side
        if latest is None:
            return self.skip(context, "Chưa có cửa B/P gần nhất")
        run = current_run_length(context.outcomes)
        shoe_pos = context.table.shoe_position_ratio()
        late_boost = 0.02 if shoe_pos >= 0.66 else 0.0

        if last4 == "+---":
            confidence = 0.506 + late_boost
            if run >= 5:
                confidence = max(confidence, 0.62)
            return self.bet(
                context,
                latest,
                confidence,
                "CSSS (+---): ưu tiên giữ streak, đặc biệt khi run dài hoặc cuối shoe",
                pattern="CSSS",
                signs=last4,
                run_length=run,
                shoe_position=round(shoe_pos, 3),
            )

        if last4 == "-+++":
            predicted = opposite_side(latest)
            confidence = 0.500 + late_boost
            if run == 1:
                confidence = 0.52 + late_boost
            if 0.40 <= shoe_pos <= 0.95:
                confidence += 0.01
            if confidence <= 0.505:
                return self.skip(
                    context,
                    "SCCC (-+++) xuất hiện nhưng điều kiện phụ chưa đủ mạnh",
                    pattern="SCCC",
                    signs=last4,
                    run_length=run,
                    shoe_position=round(shoe_pos, 3),
                )
            return self.bet(
                context,
                predicted,
                min(confidence, 0.56),
                "SCCC (-+++): chỉ lấy khi ngữ cảnh nghiêng về chop tiếp diễn",
                pattern="SCCC",
                signs=last4,
                run_length=run,
                shoe_position=round(shoe_pos, 3),
            )

        return self.skip(context, "Không thấy pattern CSSS/SCCC ở 4 dấu cuối", signs=last4)


class RunLengthStrategy(BaseStrategy):
    strategy_id = "run_length"
    name = "Run length"

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        bp = context.bp_outcomes
        latest = context.latest_bp_side
        if latest is None or len(bp) < 4:
            return self.skip(context, "Cần thêm lịch sử để đọc run")
        run = current_run_length(context.outcomes)
        if run < 3:
            return self.skip(context, "Run hiện tại chưa đủ dài", run_length=run)
        confidence = min(0.58, 0.51 + (run - 3) * 0.015)
        return self.bet(
            context,
            latest,
            confidence,
            f"Run {run} tay: paper-follow cửa đang chạy",
            run_length=run,
        )


class ShoeProfileStrategy(BaseStrategy):
    strategy_id = "shoe_profile"
    name = "Shoe profile"

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        bp = context.bp_outcomes
        if len(bp) < 18:
            return self.skip(context, "Cần ít nhất 18 tay B/P để phân loại shoe")
        banker = sum(1 for outcome in bp if outcome is Outcome.BANKER)
        player = sum(1 for outcome in bp if outcome is Outcome.PLAYER)
        total = banker + player
        banker_rate = banker / total if total else 0.0
        player_rate = player / total if total else 0.0
        if banker_rate > 0.54:
            return self.bet(
                context,
                BetSide.BANKER,
                min(0.60, 0.50 + (banker_rate - 0.50)),
                "Shoe đang nghiêng Cai theo tỷ lệ B/P nội bộ",
                banker_rate=round(banker_rate, 3),
                player_rate=round(player_rate, 3),
            )
        if player_rate > 0.54:
            return self.bet(
                context,
                BetSide.PLAYER,
                min(0.60, 0.50 + (player_rate - 0.50)),
                "Shoe đang nghiêng Con theo tỷ lệ B/P nội bộ",
                banker_rate=round(banker_rate, 3),
                player_rate=round(player_rate, 3),
            )
        return self.skip(
            context,
            "Shoe đang cân bằng, chưa ưu tiên cửa theo profile",
            banker_rate=round(banker_rate, 3),
            player_rate=round(player_rate, 3),
        )


class SequenceFollowStrategy(BaseStrategy):
    strategy_id = "sequence_follow"
    name = "Sequence follow"

    def __init__(self, sequence: str = "BPP") -> None:
        cleaned = [c for c in sequence.upper() if c in ("B", "P")]
        self.sequence = "".join(cleaned) or "BPP"

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        bp = context.bp_outcomes
        if len(bp) < 3:
            return self.skip(context, "Cần thêm lịch sử để bám chuỗi")
        next_token = self.sequence[len(bp) % len(self.sequence)]
        side = BetSide.BANKER if next_token == "B" else BetSide.PLAYER
        return self.bet(
            context,
            side,
            0.505,
            f"Bám chuỗi tham chiếu {self.sequence}, tín hiệu chỉ dùng để so sánh",
            sequence=self.sequence,
            offset=len(bp) % len(self.sequence),
        )


class EnsembleMajorityStrategy(BaseStrategy):
    strategy_id = "ensemble_majority"
    name = "Ensemble majority"

    def __init__(self, strategies: list[BetStrategy]) -> None:
        self.strategies = strategies

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        votes: dict[BetSide, list[StrategySignal]] = {BetSide.BANKER: [], BetSide.PLAYER: []}
        for strategy in self.strategies:
            signal = strategy.evaluate(context)
            if signal.is_actionable and signal.side:
                votes[signal.side].append(signal)
        banker_weight = sum(s.confidence for s in votes[BetSide.BANKER])
        player_weight = sum(s.confidence for s in votes[BetSide.PLAYER])
        if banker_weight == 0 and player_weight == 0:
            return self.skip(context, "Chưa có chuyên gia nào tạo tín hiệu")
        side = BetSide.BANKER if banker_weight >= player_weight else BetSide.PLAYER
        selected = votes[side]
        confidence = min(0.68, 0.50 + abs(banker_weight - player_weight) / max(1, len(self.strategies)))
        return self.bet(
            context,
            side,
            confidence,
            f"Đa số chuyên gia nghiêng {side.vi_label}",
            banker_weight=round(banker_weight, 3),
            player_weight=round(player_weight, 3),
            voters=[s.strategy_id for s in selected],
        )


def default_atomic_strategies() -> list[BetStrategy]:
    return [
        ScccCsssStrategy(),
        RunLengthStrategy(),
        ShoeProfileStrategy(),
        SequenceFollowStrategy("BPP"),
    ]


def default_strategies() -> list[BetStrategy]:
    atomic = default_atomic_strategies()
    return [*atomic, EnsembleMajorityStrategy(atomic)]

