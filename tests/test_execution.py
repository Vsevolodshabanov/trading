from tbank_trader.broker.base import BrokerInstrument
from tbank_trader.config import AppSettings
from tbank_trader.core.execution import OrderSizer, build_execution_constraints, classify_asset_class


def test_order_sizer_respects_notional_and_lot_caps() -> None:
    sizer = OrderSizer(default_order_size=1)
    instrument = BrokerInstrument(
        symbol="SBER",
        lot=1,
        instrument_type="share",
        class_code="TQBR",
    )
    constraints = build_execution_constraints(
        settings=AppSettings(),
        instrument=instrument,
        broker_mode="tbank",
    )

    decision = sizer.plan(
        symbol="SBER",
        side="buy",
        instrument=instrument,
        constraints=constraints,
        price=316.5,
        current_position=0,
    )

    assert decision.approved is True
    assert decision.quantity == 10
    assert decision.notional_rub == 3165.0


def test_order_sizer_rejects_when_single_lot_is_above_cap() -> None:
    sizer = OrderSizer(default_order_size=1)
    instrument = BrokerInstrument(
        symbol="USD000UTSTOM",
        lot=1000,
        instrument_type="currency",
        class_code="CETS",
    )
    settings = AppSettings(
        fx_target_order_notional_rub=30_000,
        fx_max_order_notional_rub=50_000,
        fx_max_order_lots=1,
    )
    constraints = build_execution_constraints(
        settings=settings,
        instrument=instrument,
        broker_mode="tbank",
    )

    decision = sizer.plan(
        symbol="USD000UTSTOM",
        side="buy",
        instrument=instrument,
        constraints=constraints,
        price=76.9724,
        current_position=0,
    )

    assert decision.approved is False
    assert "lot_notional_above_cap" in decision.reason


def test_order_sizer_rejects_sell_without_inventory_for_long_only_assets() -> None:
    sizer = OrderSizer(default_order_size=1)
    instrument = BrokerInstrument(
        symbol="GAZP",
        lot=10,
        instrument_type="share",
        class_code="TQBR",
    )
    constraints = build_execution_constraints(
        settings=AppSettings(),
        instrument=instrument,
        broker_mode="tbank",
    )

    decision = sizer.plan(
        symbol="GAZP",
        side="sell",
        instrument=instrument,
        constraints=constraints,
        price=128.25,
        current_position=0,
    )

    assert decision.approved is False
    assert decision.reason == "no_inventory_to_sell:GAZP"


def test_build_execution_constraints_uses_fx_limits() -> None:
    instrument = BrokerInstrument(
        symbol="USD000UTSTOM",
        lot=1000,
        instrument_type="currency",
        class_code="CETS",
    )
    settings = AppSettings(
        fx_max_position_per_symbol=1,
        fx_max_position_notional_rub=80_000,
        fx_target_order_notional_rub=30_000,
        fx_max_order_notional_rub=80_000,
        fx_max_order_lots=1,
    )

    constraints = build_execution_constraints(
        settings=settings,
        instrument=instrument,
        broker_mode="tbank",
    )

    assert classify_asset_class(instrument) == "fx"
    assert constraints.asset_class == "fx"
    assert constraints.max_order_lots == 1
    assert constraints.max_order_notional_rub == 80_000
    assert constraints.allow_short_positions is False
