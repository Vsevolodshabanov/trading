from __future__ import annotations

from dataclasses import dataclass

from tbank_trader.broker.base import BrokerInstrument
from tbank_trader.config import AppSettings


@dataclass(slots=True)
class ExecutionConstraints:
    asset_class: str
    target_order_notional_rub: float
    max_order_notional_rub: float
    max_position_notional_rub: float
    max_order_lots: int
    max_position_per_symbol: int
    allow_short_positions: bool
    allow_sell_without_inventory: bool


@dataclass(slots=True)
class OrderSizingDecision:
    approved: bool
    quantity: int
    notional_rub: float
    reason: str


def classify_asset_class(instrument: BrokerInstrument) -> str:
    if instrument.instrument_type == "share":
        return "share"
    if instrument.instrument_type == "bond":
        return "bond"
    if instrument.instrument_type == "currency" or instrument.class_code == "CETS":
        return "fx"
    return "other"


def build_execution_constraints(
    *,
    settings: AppSettings,
    instrument: BrokerInstrument,
    broker_mode: str,
) -> ExecutionConstraints:
    asset_class = classify_asset_class(instrument)
    allow_short_positions = broker_mode == "simulated" and asset_class != "bond"

    if asset_class == "share":
        return ExecutionConstraints(
            asset_class=asset_class,
            target_order_notional_rub=settings.share_target_order_notional_rub,
            max_order_notional_rub=settings.share_max_order_notional_rub,
            max_position_notional_rub=settings.share_max_position_notional_rub,
            max_order_lots=settings.share_max_order_lots,
            max_position_per_symbol=settings.share_max_position_per_symbol,
            allow_short_positions=allow_short_positions,
            allow_sell_without_inventory=allow_short_positions,
        )

    if asset_class == "bond":
        return ExecutionConstraints(
            asset_class=asset_class,
            target_order_notional_rub=settings.bond_target_order_notional_rub,
            max_order_notional_rub=settings.bond_max_order_notional_rub,
            max_position_notional_rub=settings.bond_max_position_notional_rub,
            max_order_lots=settings.bond_max_order_lots,
            max_position_per_symbol=settings.bond_max_position_per_symbol,
            allow_short_positions=False,
            allow_sell_without_inventory=False,
        )

    if asset_class == "fx":
        return ExecutionConstraints(
            asset_class=asset_class,
            target_order_notional_rub=settings.fx_target_order_notional_rub,
            max_order_notional_rub=settings.fx_max_order_notional_rub,
            max_position_notional_rub=settings.fx_max_position_notional_rub,
            max_order_lots=settings.fx_max_order_lots,
            max_position_per_symbol=settings.fx_max_position_per_symbol,
            allow_short_positions=allow_short_positions,
            allow_sell_without_inventory=allow_short_positions,
        )

    return ExecutionConstraints(
        asset_class=asset_class,
        target_order_notional_rub=settings.target_order_notional_rub,
        max_order_notional_rub=settings.max_order_notional_rub,
        max_position_notional_rub=settings.max_position_notional_rub,
        max_order_lots=settings.max_order_lots,
        max_position_per_symbol=settings.max_position_per_symbol,
        allow_short_positions=allow_short_positions,
        allow_sell_without_inventory=allow_short_positions,
    )


class OrderSizer:
    def __init__(
        self,
        *,
        default_order_size: int,
    ) -> None:
        self.default_order_size = default_order_size

    def plan(
        self,
        *,
        symbol: str,
        side: str,
        instrument: BrokerInstrument,
        constraints: ExecutionConstraints,
        price: float,
        current_position: int,
    ) -> OrderSizingDecision:
        if price <= 0:
            return OrderSizingDecision(False, 0, 0.0, f"invalid_price:{symbol}")
        if instrument.lot <= 0:
            return OrderSizingDecision(False, 0, 0.0, f"invalid_lot:{symbol}")
        if side == "sell" and current_position <= 0 and not constraints.allow_sell_without_inventory:
            return OrderSizingDecision(False, 0, 0.0, f"no_inventory_to_sell:{symbol}")

        lot_notional = price * instrument.lot
        if constraints.target_order_notional_rub <= 0:
            quantity = self.default_order_size
        else:
            quantity = max(1, int(constraints.target_order_notional_rub // lot_notional))
            if quantity == 0:
                quantity = 1

        if constraints.max_order_lots > 0:
            quantity = min(quantity, constraints.max_order_lots)

        if constraints.max_order_notional_rub > 0:
            max_by_notional = int(constraints.max_order_notional_rub // lot_notional)
            if max_by_notional <= 0:
                return OrderSizingDecision(
                    False,
                    0,
                    0.0,
                    f"lot_notional_above_cap:{symbol}:{lot_notional:.2f}",
                )
            quantity = min(quantity, max_by_notional)

        if side == "sell" and current_position > 0 and not constraints.allow_short_positions:
            quantity = min(quantity, current_position)

        if quantity <= 0:
            return OrderSizingDecision(False, 0, 0.0, f"non_positive_quantity:{symbol}")

        return OrderSizingDecision(
            True,
            quantity,
            quantity * lot_notional,
            f"asset:{constraints.asset_class}|qty:{quantity}|lot:{instrument.lot}|notional:{quantity * lot_notional:.2f}",
        )
