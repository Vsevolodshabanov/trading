from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class InstrumentStateModel(Base):
    __tablename__ = "instrument_state"

    symbol: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_price: Mapped[float] = mapped_column(Float, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstrumentCatalogModel(Base):
    __tablename__ = "instrument_catalog"

    instrument_uid: Mapped[str] = mapped_column(String(64), primary_key=True)
    figi: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    class_code: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    instrument_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    lot: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    exchange: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    country_of_risk: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    buy_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sell_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    api_trade_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    for_iis_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    for_qual_investor_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    weekend_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    otc_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    active_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstrumentEligibilityModel(Base):
    __tablename__ = "instrument_eligibility"

    instrument_uid: Mapped[str] = mapped_column(
        ForeignKey("instrument_catalog.instrument_uid"),
        primary_key=True,
    )
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    reason_codes: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class HistoricalCandleModel(Base):
    __tablename__ = "historical_candles"

    instrument_uid: Mapped[str] = mapped_column(
        ForeignKey("instrument_catalog.instrument_uid"),
        primary_key=True,
    )
    interval: Mapped[str] = mapped_column(String(32), primary_key=True)
    candle_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open_price: Mapped[float] = mapped_column(Float, nullable=False)
    high_price: Mapped[float] = mapped_column(Float, nullable=False)
    low_price: Mapped[float] = mapped_column(Float, nullable=False)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    turnover_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class DividendEventModel(Base):
    __tablename__ = "dividend_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    instrument_uid: Mapped[str] = mapped_column(
        ForeignKey("instrument_catalog.instrument_uid"),
        nullable=False,
        index=True,
    )
    record_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    payment_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    declared_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_buy_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at_event: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dividend_type: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    regularity: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    currency: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    dividend_net: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    close_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    yield_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstrumentResearchStatusModel(Base):
    __tablename__ = "instrument_research_status"

    instrument_uid: Mapped[str] = mapped_column(
        ForeignKey("instrument_catalog.instrument_uid"),
        primary_key=True,
    )
    trading_status: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    buy_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sell_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    api_trade_available_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    otc_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    blocked_tca_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    first_1min_candle_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_1day_candle_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class InstrumentHistoryQualityModel(Base):
    __tablename__ = "instrument_history_quality"

    instrument_uid: Mapped[str] = mapped_column(
        ForeignKey("instrument_catalog.instrument_uid"),
        primary_key=True,
    )
    interval: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    completed_candles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    median_turnover_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    latest_candle_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    history_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    reason_codes: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class HistoricalSimulationRunModel(Base):
    __tablename__ = "historical_simulation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    instruments_considered: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    instruments_with_history: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rebalance_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_rebalances: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executed_actions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    turnover_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    initial_cash_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    final_cash_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    final_equity_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_return_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_drawdown_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    latest_selected_symbols: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    latest_target_weights: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    note: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class HistoricalSimulationRebalanceModel(Base):
    __tablename__ = "historical_simulation_rebalances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("historical_simulation_runs.id"),
        nullable=False,
        index=True,
    )
    rebalance_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    regime_state: Mapped[str] = mapped_column(String(16), nullable=False)
    selected_symbols: Mapped[str] = mapped_column(String(255), nullable=False)
    target_weights: Mapped[str] = mapped_column(String(255), nullable=False)
    positions_before: Mapped[str] = mapped_column(String(255), nullable=False)
    positions_after: Mapped[str] = mapped_column(String(255), nullable=False)
    planned_actions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executed_actions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    turnover_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    equity_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cash_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class PositionModel(Base):
    __tablename__ = "positions"

    symbol: Mapped[str] = mapped_column(String(64), primary_key=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    market_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class SignalModel(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class OrderModel(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[int | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    broker_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    broker_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ShadowTradeModel(Base):
    __tablename__ = "shadow_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    notional_rub: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ShadowRebalanceModel(Base):
    __tablename__ = "shadow_rebalances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    regime_state: Mapped[str] = mapped_column(String(16), nullable=False)
    selected_symbols: Mapped[str] = mapped_column(String(255), nullable=False)
    target_weights: Mapped[str] = mapped_column(String(255), nullable=False)
    positions_before: Mapped[str] = mapped_column(String(255), nullable=False)
    positions_after: Mapped[str] = mapped_column(String(255), nullable=False)
    planned_actions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executed_actions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    equity_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cash_rub: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class AppStateModel(Base):
    __tablename__ = "app_state"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
