from pathlib import Path

from tbank_trader.broker.base import BrokerPositionSnapshot
from tbank_trader.config import AppSettings
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.models import PositionModel
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    build_dashboard,
    start_new_run,
    record_filled_order,
    record_signal,
    sync_positions,
)


def build_test_session_factory(database_path: Path):
    database_url = f"sqlite:///{database_path}"
    engine = build_engine(database_url)
    init_database(engine)
    return build_session_factory(database_url)


def test_sync_positions_zeroes_missing_symbols(tmp_path: Path) -> None:
    session_factory = build_test_session_factory(tmp_path / "positions.db")
    settings = AppSettings(
        database_url=f"sqlite:///{tmp_path / 'positions.db'}",
        redis_url=None,
        symbols=["SBER", "GAZP"],
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        gazp = session.get(PositionModel, "GAZP")
        assert gazp is not None
        gazp.quantity = 3
        gazp.avg_price = 125.25
        gazp.market_price = 126.10
        session.commit()

    with session_factory() as session:
        sync_positions(
            session,
            [
                BrokerPositionSnapshot(
                    symbol="SBER",
                    quantity=1,
                    avg_price=316.20,
                    market_price=317.10,
                )
            ],
        )
        sber = session.get(PositionModel, "SBER")
        gazp = session.get(PositionModel, "GAZP")

        assert sber is not None
        assert sber.quantity == 1
        assert sber.avg_price == 316.20
        assert sber.market_price == 317.10

        assert gazp is not None
        assert gazp.quantity == 0
        assert gazp.avg_price == 0.0
        assert gazp.market_price == 126.10


def test_dashboard_shows_only_current_run_orders_and_signals(tmp_path: Path) -> None:
    session_factory = build_test_session_factory(tmp_path / "dashboard.db")
    settings = AppSettings(
        database_url=f"sqlite:///{tmp_path / 'dashboard.db'}",
        redis_url=None,
        symbols=["SBER"],
    )

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        old_signal = record_signal(
            session,
            strategy_name="moving_average_cross",
            symbol="SBER",
            side="buy",
            price=300.0,
            confidence=1.0,
            reason="old-signal",
            status="approved",
        )
        record_filled_order(
            session,
            signal_id=old_signal.id,
            symbol="SBER",
            side="buy",
            quantity=1,
            price=300.0,
            broker_mode="simulated",
            broker_order_id="sim-old",
            reason="old-order",
        )
        session.commit()

        start_new_run(session, broker_mode="tbank")

        new_signal = record_signal(
            session,
            strategy_name="moving_average_cross",
            symbol="SBER",
            side="sell",
            price=316.5,
            confidence=1.4,
            reason="new-signal",
            status="approved",
        )
        record_filled_order(
            session,
            signal_id=new_signal.id,
            symbol="SBER",
            side="sell",
            quantity=1,
            price=316.5,
            broker_mode="tbank",
            broker_order_id="real-new",
            reason="new-order",
        )
        session.commit()

        payload = build_dashboard(session)

    assert payload["system"]["broker_mode"] == "tbank"
    assert payload["system"]["order_count"] == 1
    assert payload["system"]["signal_count"] == 1
    assert payload["system"]["strategy_mode"] == "portfolio_momentum"
    assert len(payload["orders"]) == 1
    assert len(payload["signals"]) == 1
    assert payload["orders"][0]["broker_mode"] == "tbank"
    assert payload["orders"][0]["broker_order_id"] == "real-new"
    assert payload["signals"][0]["reason"] == "new-signal"
