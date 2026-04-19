from pathlib import Path

from tbank_trader.broker.base import BrokerInstrument, BrokerPositionSnapshot
from tbank_trader.config import AppSettings
from tbank_trader.core.execution import build_execution_constraints
from tbank_trader.core.portfolio_momentum import build_rebalance_plan, compute_portfolio_selection
from tbank_trader.engine.runner import (
    PortfolioRuntimeState,
    build_constraints,
    create_broker_adapter,
    run_portfolio_iteration,
    run_shadow_portfolio_simulation,
)
from tbank_trader.core.risk import RiskEngine
from tbank_trader.services.event_bus import EventBus
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    build_dashboard,
    set_state,
    start_new_run,
    sync_positions,
)


def test_compute_portfolio_selection_prefers_strongest_symbols() -> None:
    history_by_symbol = {
        "AAA": [100 + step for step in range(150)],
        "BBB": [100 + step * 0.5 for step in range(150)],
        "CCC": [180 - step * 0.3 for step in range(150)],
    }

    selection = compute_portfolio_selection(
        history_by_symbol=history_by_symbol,
        momentum_periods=[30, 60, 90],
        top_percentile=80,
        min_positions=1,
        max_positions=2,
        regime_filter_enabled=True,
        regime_symbol="AAA",
        regime_ma_window=20,
    )

    assert selection.regime_on is True
    assert selection.selected_symbols[0] == "AAA"
    assert "AAA" in selection.target_weights
    assert "CCC" not in selection.target_weights


def test_build_rebalance_plan_sells_before_buys_under_cash_constraint() -> None:
    settings = AppSettings()
    instruments = {
        symbol: BrokerInstrument(
            symbol=symbol,
            lot=1,
            instrument_type="share",
            class_code="TQBR",
        )
        for symbol in ["AAA", "BBB"]
    }
    constraints = {
        symbol: build_execution_constraints(
            settings=settings,
            instrument=instrument,
            broker_mode="simulated",
        )
        for symbol, instrument in instruments.items()
    }

    plan = build_rebalance_plan(
        prices_by_symbol={"AAA": 100.0, "BBB": 200.0},
        current_positions={"AAA": 5, "BBB": 0},
        cash_rub=0.0,
        instruments=instruments,
        constraints_by_symbol=constraints,
        target_weights={"BBB": 1.0},
        scores={"AAA": 10.0, "BBB": 99.0},
    )

    assert plan.actions[0].symbol == "AAA"
    assert plan.actions[0].side == "sell"
    assert any(action.symbol == "BBB" and action.side == "buy" for action in plan.actions)
    assert plan.available_cash_rub >= 0


def test_shadow_portfolio_simulation_updates_dashboard_state(tmp_path: Path) -> None:
    database_path = tmp_path / "shadow-portfolio.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        strategy_mode="portfolio_momentum",
        symbols=["SBER", "GAZP", "LKOH"],
        portfolio_shadow_enabled=True,
        portfolio_regime_filter_enabled=False,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
    )
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)
    broker = create_broker_adapter(settings)
    instruments = broker.get_instruments()
    constraints_by_symbol = build_constraints(settings, instruments)

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        start_new_run(session, broker_mode=settings.broker_mode)
        prices_by_symbol = broker.get_prices(settings.symbols)
        run_shadow_portfolio_simulation(
            session=session,
            settings=settings,
            instruments=instruments,
            constraints_by_symbol=constraints_by_symbol,
            broker=broker,
            prices_by_symbol=prices_by_symbol,
        )
        payload = build_dashboard(session)

    assert payload["shadow_portfolio"]["enabled"] is True
    assert payload["shadow_portfolio"]["regime_state"] == "on"
    assert payload["shadow_portfolio"]["equity_rub"] > 0
    assert len(payload["shadow_trades"]) >= 1
    assert payload["shadow_trades"][0]["strategy_name"] == "portfolio_momentum_shadow"
    assert len(payload["shadow_rebalances"]) >= 1
    assert payload["shadow_rebalances"][0]["status"] == "executed"


def test_live_portfolio_iteration_updates_dashboard_state(tmp_path: Path) -> None:
    database_path = tmp_path / "live-portfolio.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        strategy_mode="portfolio_momentum",
        portfolio_live_enabled=True,
        portfolio_shadow_enabled=False,
        symbols=["SBER", "GAZP", "LKOH"],
        portfolio_regime_filter_enabled=False,
        portfolio_min_positions=1,
        portfolio_max_positions=2,
    )
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)
    broker = create_broker_adapter(settings)
    instruments = broker.get_instruments()
    constraints_by_symbol = build_constraints(settings, instruments)
    risk_engine = RiskEngine(
        settings.max_position_per_symbol,
        max_position_notional_rub=settings.max_position_notional_rub,
        allow_short_positions=True,
    )
    runtime_state = PortfolioRuntimeState()

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        start_new_run(session, broker_mode=settings.broker_mode)
        run_portfolio_iteration(
            session=session,
            settings=settings,
            instruments=instruments,
            constraints_by_symbol=constraints_by_symbol,
            risk_engine=risk_engine,
            broker=broker,
            broker_mode=settings.broker_mode,
            event_bus=EventBus(None),
            runtime_state=runtime_state,
            strategy_name="portfolio_momentum_live",
            state_prefix="portfolio",
        )
        payload = build_dashboard(session)

    assert payload["portfolio_live"]["enabled"] is True
    assert payload["portfolio_live"]["regime_state"] == "on"
    assert len(payload["portfolio_trades"]) >= 1
    assert payload["portfolio_trades"][0]["strategy_name"] == "portfolio_momentum_live"
    assert len(payload["portfolio_rebalances"]) >= 1


def test_live_portfolio_uses_strategy_book_for_risk_limits(tmp_path: Path) -> None:
    database_path = tmp_path / "live-portfolio-strategy-book.db"
    settings = AppSettings(
        database_url=f"sqlite:///{database_path}",
        redis_url=None,
        broker_mode="simulated",
        strategy_mode="portfolio_momentum",
        portfolio_live_enabled=True,
        portfolio_shadow_enabled=False,
        symbols=["SBER"],
        portfolio_regime_filter_enabled=False,
        portfolio_min_positions=1,
        portfolio_max_positions=1,
        share_max_position_per_symbol=20,
    )
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)
    broker = create_broker_adapter(settings)
    instruments = broker.get_instruments()
    constraints_by_symbol = build_constraints(settings, instruments)
    risk_engine = RiskEngine(
        settings.max_position_per_symbol,
        max_position_notional_rub=settings.max_position_notional_rub,
        allow_short_positions=True,
    )
    runtime_state = PortfolioRuntimeState()

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        start_new_run(session, broker_mode=settings.broker_mode)
        sync_positions(
            session,
            [
                BrokerPositionSnapshot(
                    symbol="SBER",
                    quantity=11,
                    avg_price=300.0,
                    market_price=300.0,
                )
            ],
        )
        set_state(session, "portfolio.positions", "SBER:10")
        set_state(session, "portfolio.strategy_cash_rub", "97000")
        session.commit()

        run_portfolio_iteration(
            session=session,
            settings=settings,
            instruments=instruments,
            constraints_by_symbol=constraints_by_symbol,
            risk_engine=risk_engine,
            broker=broker,
            broker_mode=settings.broker_mode,
            event_bus=EventBus(None),
            runtime_state=runtime_state,
            strategy_name="portfolio_momentum_live",
            state_prefix="portfolio",
            use_strategy_book=True,
        )
        payload = build_dashboard(session)

    assert payload["portfolio_live"]["enabled"] is True
    assert len(payload["portfolio_trades"]) >= 1
    assert payload["portfolio_trades"][0]["symbol"] == "SBER"
    assert payload["signals"][0]["status"] == "approved"
