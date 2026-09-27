from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import BetSide, Outcome, StrategyAction, StrategySignal, TableSnapshot


def side_from_outcome(outcome: Outcome) -> BetSide | None:
    if outcome == Outcome.BANKER:
        return BetSide.BANKER
    if outcome == Outcome.PLAYER:
        return BetSide.PLAYER
    return None


def opposite_side(side: BetSide) -> BetSide:
    return BetSide.PLAYER if side == BetSide.BANKER else BetSide.BANKER


def signs_from_outcomes(outcomes: list[Outcome]) -> list[str]:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    signs: list[str] = []
    for prev, current in zip(non_tie, non_tie[1:]):
        signs.append("-" if prev == current else "+")
    return signs


def current_run_length(outcomes: list[Outcome]) -> int:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if not non_tie:
        return 0
    last = non_tie[-1]
    length = 0
    for value in reversed(non_tie):
        if value == last:
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
        banker = sum(1 for outcome in bp if outcome == Outcome.BANKER)
        player = sum(1 for outcome in bp if outcome == Outcome.PLAYER)
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


def calculate_chop_rate(outcomes: list[Outcome], window_size: int = 15) -> float:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if len(non_tie) < 2:
        return 0.5
    recent = non_tie[-window_size:]
    if len(recent) < 2:
        return 0.5
    chops = sum(1 for prev, curr in zip(recent, recent[1:]) if prev != curr)
    return chops / (len(recent) - 1)


def detect_ping_pong_pattern(outcomes: list[Outcome]) -> BetSide | None:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if len(non_tie) < 3:
        return None
    last3 = non_tie[-3:]
    if last3 == [Outcome.BANKER, Outcome.PLAYER, Outcome.BANKER]:
        return BetSide.PLAYER
    if last3 == [Outcome.PLAYER, Outcome.BANKER, Outcome.PLAYER]:
        return BetSide.BANKER
    return None


def detect_double_pair_pattern(outcomes: list[Outcome]) -> BetSide | None:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if len(non_tie) < 3:
        return None
    last3 = non_tie[-3:]
    if last3 == [Outcome.BANKER, Outcome.BANKER, Outcome.PLAYER]:
        return BetSide.PLAYER
    if last3 == [Outcome.PLAYER, Outcome.PLAYER, Outcome.BANKER]:
        return BetSide.BANKER
    if len(non_tie) >= 4:
        last4 = non_tie[-4:]
        if last4 == [Outcome.BANKER, Outcome.BANKER, Outcome.PLAYER, Outcome.PLAYER]:
            return BetSide.BANKER
        if last4 == [Outcome.PLAYER, Outcome.PLAYER, Outcome.BANKER, Outcome.BANKER]:
            return BetSide.PLAYER
    return None


def detect_anti_banker_3(outcomes: list[Outcome]) -> BetSide | None:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if len(non_tie) < 3:
        return None
    last3 = non_tie[-3:]
    if last3 == [Outcome.BANKER, Outcome.BANKER, Outcome.BANKER]:
        if len(non_tie) >= 4 and non_tie[-4] == Outcome.BANKER:
            return None
        return BetSide.PLAYER
    return None


def detect_late_run_pattern(outcomes: list[Outcome]) -> tuple[BetSide | None, int]:
    non_tie = [o for o in outcomes if o != Outcome.TIE]
    if not non_tie:
        return None, 0
    run = current_run_length(outcomes)
    if run < 4:
        return None, run
    latest = side_from_outcome(non_tie[-1])
    return latest, run


class AdaptiveRegimeStrategy(BaseStrategy):
    strategy_id = "adaptive_regime"
    name = "Đa cầu thích ứng"

    def evaluate(self, context: StrategyContext) -> StrategySignal:
        bp = context.bp_outcomes
        if len(bp) < 15:
            return self.skip(context, "Cần tối thiểu 15 ván B/P để phân loại trạng thái bàn")
        chop_rate = calculate_chop_rate(bp, window_size=15)
        run = current_run_length(context.outcomes)

        # Pha Nhảy / Chop Mode: chop_rate >= 0.55
        if chop_rate >= 0.55:
            # 1. Kiểm tra Cầu 1-1
            pp_side = detect_ping_pong_pattern(bp)
            if pp_side is not None:
                confidence = 0.55
                return self.bet(
                    context,
                    pp_side,
                    confidence,
                    f"Pha Nhảy ({chop_rate:.0%}): Cầu nhảy 1-1 -> Đánh {pp_side.vi_label}",
                    regime_mode="chop",
                    road_pattern="ping_pong",
                    chop_rate=round(chop_rate, 3),
                    run_length=run,
                )

            # 2. Kiểm tra Cầu 2-2
            dp_side = detect_double_pair_pattern(bp)
            if dp_side is not None:
                confidence = 0.54
                return self.bet(
                    context,
                    dp_side,
                    confidence,
                    f"Pha Nhảy ({chop_rate:.0%}): Cầu đôi 2-2 -> Đánh {dp_side.vi_label}",
                    regime_mode="chop",
                    road_pattern="double_pair",
                    chop_rate=round(chop_rate, 3),
                    run_length=run,
                )

            # 3. Kiểm tra Bẻ bệt Cái ở cây 4
            ab_side = detect_anti_banker_3(bp)
            if ab_side is not None and run == 3:
                confidence = 0.545
                return self.bet(
                    context,
                    ab_side,
                    confidence,
                    f"Pha Nhảy ({chop_rate:.0%}): B-B-B -> Bẻ sang {ab_side.vi_label} ở cây 4",
                    regime_mode="chop",
                    road_pattern="anti_banker_3",
                    chop_rate=round(chop_rate, 3),
                    run_length=run,
                )

            return self.skip(
                context,
                f"Pha Nhảy ({chop_rate:.0%}): Chưa xuất hiện thế cầu 1-1 / 2-2 rõ nét",
                regime_mode="chop",
                chop_rate=round(chop_rate, 3),
                run_length=run,
            )

        # Pha Bệt / Trend Mode: chop_rate <= 0.40
        if chop_rate <= 0.40:
            lr_side, run_len = detect_late_run_pattern(bp)
            if lr_side is not None and run_len >= 4:
                confidence = min(0.60, 0.53 + (run_len - 4) * 0.015)
                return self.bet(
                    context,
                    lr_side,
                    confidence,
                    f"Pha Bệt ({chop_rate:.0%}): Bệt {run_len} tay đã qua cây 4 -> Bám tiếp {lr_side.vi_label}",
                    regime_mode="trend",
                    road_pattern="late_run",
                    chop_rate=round(chop_rate, 3),
                    run_length=run_len,
                )
            return self.skip(
                context,
                f"Pha Bệt ({chop_rate:.0%}): Run {run} tay chưa đạt chuẩn bệt muộn (cần >= 4 tay)",
                regime_mode="trend",
                chop_rate=round(chop_rate, 3),
                run_length=run,
            )

        # Vùng trung tính (0.40 < chop_rate < 0.55)
        return self.skip(
            context,
            f"Vùng trung tính ({chop_rate:.0%}): Chop rate 40%-55%, đứng ngoài an toàn",
            regime_mode="neutral",
            chop_rate=round(chop_rate, 3),
            run_length=run,
        )


def default_strategies() -> list[BetStrategy]:
    atomic = default_atomic_strategies()
    return [*atomic, EnsembleMajorityStrategy(atomic), AdaptiveRegimeStrategy()]


