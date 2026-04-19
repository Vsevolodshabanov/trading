from __future__ import annotations

from dataclasses import dataclass

from tbank_trader.core.execution import ExecutionConstraints


@dataclass(slots=True)
class RiskDecision:
    approved: bool
    reason: str


class RiskEngine:
    def __init__(
        self,
        max_position_per_symbol: int,
        max_position_notional_rub: float = 0.0,
        allow_short_positions: bool = True,
    ) -> None:
        self.max_position_per_symbol = max_position_per_symbol
        self.max_position_notional_rub = max_position_notional_rub
        self.allow_short_positions = allow_short_positions

    def evaluate(
        self,
        *,
        paused: bool,
        symbol: str,
        side: str,
        quantity: int,
        current_position: int,
        price: float = 0.0,
        lot: int = 1,
        constraints: ExecutionConstraints | None = None,
    ) -> RiskDecision:
        max_position_per_symbol = (
            constraints.max_position_per_symbol if constraints else self.max_position_per_symbol
        )
        max_position_notional_rub = (
            constraints.max_position_notional_rub if constraints else self.max_position_notional_rub
        )
        allow_short_positions = (
            constraints.allow_short_positions if constraints else self.allow_short_positions
        )

        if paused:
            return RiskDecision(False, "system_paused")
        if quantity <= 0:
            return RiskDecision(False, "non_positive_quantity")

        signed_quantity = quantity if side == "buy" else -quantity
        next_position = current_position + signed_quantity
        if not allow_short_positions and next_position < 0:
            return RiskDecision(False, f"short_positions_disabled:{symbol}:{next_position}")
        if abs(next_position) > max_position_per_symbol:
            return RiskDecision(
                False,
                f"max_position_exceeded:{symbol}:{next_position}",
            )
        if max_position_notional_rub > 0 and price > 0 and lot > 0:
            next_notional = abs(next_position) * price * lot
            if next_notional > max_position_notional_rub:
                return RiskDecision(
                    False,
                    f"max_position_notional_exceeded:{symbol}:{next_notional:.2f}",
                )

        return RiskDecision(True, "approved")
