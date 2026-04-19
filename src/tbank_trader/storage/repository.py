from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from tbank_trader.config import AppSettings, get_settings
from tbank_trader.broker.base import BrokerPositionSnapshot
from tbank_trader.storage.models import (
    AppStateModel,
    DividendEventModel,
    HistoricalCandleModel,
    HistoricalSimulationRebalanceModel,
    HistoricalSimulationRunModel,
    InstrumentCatalogModel,
    InstrumentEligibilityModel,
    InstrumentHistoryQualityModel,
    InstrumentResearchStatusModel,
    InstrumentStateModel,
    OrderModel,
    PositionModel,
    ShadowRebalanceModel,
    ShadowTradeModel,
    SignalModel,
)

if TYPE_CHECKING:
    from tbank_trader.services.instrument_catalog import InstrumentEligibilitySnapshot
    from tbank_trader.services.tbank_client import CatalogInstrument


def bootstrap_defaults(session: Session, settings: AppSettings) -> None:
    set_state(session, "system.paused", get_state(session, "system.paused", "false"))
    set_state(session, "strategy.mode", settings.strategy_mode)
    set_state(session, "strategy.portfolio_live_enabled", str(settings.portfolio_live_enabled).lower())
    set_state(session, "strategy.shadow_portfolio_enabled", str(settings.portfolio_shadow_enabled).lower())
    set_state(session, "strategy.profile", settings.strategy_profile)
    set_state(session, "strategy.candle_interval", settings.strategy_candle_interval)
    set_state(session, "portfolio.top_percentile", str(settings.portfolio_top_percentile))
    set_state(session, "portfolio.regime_filter_enabled", str(settings.portfolio_regime_filter_enabled).lower())
    set_state(session, "portfolio.regime_symbol", settings.portfolio_regime_symbol or settings.symbols[0])
    set_state(session, "portfolio.regime_state", get_state(session, "portfolio.regime_state", "n/a"))
    set_state(session, "portfolio.regime_reason", get_state(session, "portfolio.regime_reason", "n/a"))
    set_state(session, "portfolio.selected_symbols", get_state(session, "portfolio.selected_symbols", ""))
    set_state(session, "portfolio.target_weights", get_state(session, "portfolio.target_weights", ""))
    set_state(session, "portfolio.positions", get_state(session, "portfolio.positions", ""))
    set_state(session, "portfolio.last_rebalance_at", get_state(session, "portfolio.last_rebalance_at", "n/a"))
    set_state(session, "portfolio.next_rebalance_at", get_state(session, "portfolio.next_rebalance_at", "n/a"))
    set_state(session, "portfolio.last_equity_rub", get_state(session, "portfolio.last_equity_rub", "0"))
    set_state(session, "portfolio.last_cash_rub", get_state(session, "portfolio.last_cash_rub", "0"))
    set_state(session, "portfolio.strategy_cash_rub", get_state(session, "portfolio.strategy_cash_rub", f"{settings.portfolio_live_initial_cash_rub:.2f}"))
    set_state(session, "portfolio.last_plan_actions", get_state(session, "portfolio.last_plan_actions", "0"))
    set_state(session, "portfolio.last_executed_actions", get_state(session, "portfolio.last_executed_actions", ""))
    set_state(session, "portfolio.last_waiting_fingerprint", get_state(session, "portfolio.last_waiting_fingerprint", ""))
    set_state(session, "shadow.portfolio.regime_state", get_state(session, "shadow.portfolio.regime_state", "n/a"))
    set_state(session, "shadow.portfolio.regime_reason", get_state(session, "shadow.portfolio.regime_reason", "n/a"))
    set_state(session, "shadow.portfolio.selected_symbols", get_state(session, "shadow.portfolio.selected_symbols", ""))
    set_state(session, "shadow.portfolio.target_weights", get_state(session, "shadow.portfolio.target_weights", ""))
    set_state(session, "shadow.portfolio.positions", get_state(session, "shadow.portfolio.positions", ""))
    set_state(session, "shadow.portfolio.last_rebalance_at", get_state(session, "shadow.portfolio.last_rebalance_at", "n/a"))
    set_state(session, "shadow.portfolio.next_rebalance_at", get_state(session, "shadow.portfolio.next_rebalance_at", "n/a"))
    set_state(session, "shadow.portfolio.cash_rub", get_state(session, "shadow.portfolio.cash_rub", f"{settings.portfolio_shadow_initial_cash_rub:.2f}"))
    set_state(session, "shadow.portfolio.equity_rub", get_state(session, "shadow.portfolio.equity_rub", f"{settings.portfolio_shadow_initial_cash_rub:.2f}"))
    set_state(session, "shadow.portfolio.last_plan_actions", get_state(session, "shadow.portfolio.last_plan_actions", "0"))
    set_state(session, "shadow.portfolio.last_executed_actions", get_state(session, "shadow.portfolio.last_executed_actions", ""))
    set_state(session, "shadow.portfolio.last_waiting_fingerprint", get_state(session, "shadow.portfolio.last_waiting_fingerprint", ""))
    set_state(session, "execution.share_target_order_notional_rub", f"{settings.share_target_order_notional_rub:.2f}")
    set_state(session, "execution.bond_target_order_notional_rub", f"{settings.bond_target_order_notional_rub:.2f}")
    set_state(session, "execution.fx_target_order_notional_rub", f"{settings.fx_target_order_notional_rub:.2f}")
    set_state(session, "risk.share_max_position_notional_rub", f"{settings.share_max_position_notional_rub:.2f}")
    set_state(session, "risk.bond_max_position_notional_rub", f"{settings.bond_max_position_notional_rub:.2f}")
    set_state(session, "risk.fx_max_position_notional_rub", f"{settings.fx_max_position_notional_rub:.2f}")
    set_state(session, "history.backfill_cursor", get_state(session, "history.backfill_cursor", "0"))
    set_state(session, "history.backfill_last_batch_at", get_state(session, "history.backfill_last_batch_at", "n/a"))
    set_state(session, "history.backfill_last_batch_size", get_state(session, "history.backfill_last_batch_size", "0"))
    set_state(session, "history.backfill_last_batch_written", get_state(session, "history.backfill_last_batch_written", "0"))
    set_state(session, "history.backfill_cycle_completed", get_state(session, "history.backfill_cycle_completed", "false"))
    for index, symbol in enumerate(settings.symbols):
        instrument = session.get(InstrumentStateModel, symbol)
        if instrument is None:
            session.add(
                InstrumentStateModel(
                    symbol=symbol,
                    last_price=100.0 + index * 17.0,
                )
            )
        position = session.get(PositionModel, symbol)
        if position is None:
            session.add(
                PositionModel(
                    symbol=symbol,
                    quantity=0,
                    avg_price=0.0,
                    market_price=100.0 + index * 17.0,
                )
            )
    session.commit()


def get_state(session: Session, key: str, default: str | None = None) -> str | None:
    row = session.get(AppStateModel, key)
    return row.value if row else default


def set_state(session: Session, key: str, value: str) -> None:
    row = session.get(AppStateModel, key)
    if row is None:
        session.add(AppStateModel(key=key, value=value))
    else:
        row.value = value


def is_paused(session: Session) -> bool:
    return get_state(session, "system.paused", "false") == "true"


def update_heartbeat(session: Session, *, broker_mode: str, iteration: int) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    set_state(session, "engine.last_heartbeat", timestamp)
    set_state(session, "engine.iteration", str(iteration))
    set_state(session, "engine.broker_mode", broker_mode)
    session.commit()


def start_new_run(session: Session, *, broker_mode: str) -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    max_order_id = session.scalar(select(func.max(OrderModel.id))) or 0
    max_signal_id = session.scalar(select(func.max(SignalModel.id))) or 0
    max_shadow_trade_id = session.scalar(select(func.max(ShadowTradeModel.id))) or 0
    max_shadow_rebalance_id = session.scalar(select(func.max(ShadowRebalanceModel.id))) or 0
    set_state(session, "engine.started_at", started_at)
    set_state(session, "engine.order_offset", str(max_order_id))
    set_state(session, "engine.signal_offset", str(max_signal_id))
    set_state(session, "engine.shadow_trade_offset", str(max_shadow_trade_id))
    set_state(session, "engine.shadow_rebalance_offset", str(max_shadow_rebalance_id))
    set_state(session, "engine.broker_mode", broker_mode)
    session.commit()


def get_run_offsets(session: Session) -> tuple[int, int]:
    order_offset = int(get_state(session, "engine.order_offset", "0") or "0")
    signal_offset = int(get_state(session, "engine.signal_offset", "0") or "0")
    return order_offset, signal_offset


def get_strategy_ledger_offsets(session: Session) -> tuple[int, int]:
    shadow_trade_offset = int(get_state(session, "engine.shadow_trade_offset", "0") or "0")
    shadow_rebalance_offset = int(get_state(session, "engine.shadow_rebalance_offset", "0") or "0")
    return shadow_trade_offset, shadow_rebalance_offset


def sync_positions(session: Session, snapshots: list[BrokerPositionSnapshot]) -> None:
    by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
    rows = session.scalars(select(PositionModel)).all()
    rows_by_symbol = {row.symbol: row for row in rows}
    for row in rows:
        snapshot = by_symbol.get(row.symbol)
        if snapshot is None:
            row.quantity = 0
            row.avg_price = 0.0
            continue
        row.quantity = snapshot.quantity
        row.avg_price = snapshot.avg_price
        row.market_price = snapshot.market_price

    for symbol, snapshot in by_symbol.items():
        if symbol in rows_by_symbol:
            continue
        session.add(
            PositionModel(
                symbol=symbol,
                quantity=snapshot.quantity,
                avg_price=snapshot.avg_price,
                market_price=snapshot.market_price,
            )
        )
    session.commit()


def upsert_instrument_catalog(
    session: Session,
    instruments: list[Any],
) -> None:
    for instrument in instruments:
        row = session.get(InstrumentCatalogModel, instrument.instrument_uid)
        if row is None:
            row = InstrumentCatalogModel(
                instrument_uid=instrument.instrument_uid,
                figi=instrument.figi,
                ticker=instrument.ticker,
                class_code=instrument.class_code,
                instrument_type=instrument.instrument_type,
                name=instrument.name,
                lot=instrument.lot,
                currency=instrument.currency,
                exchange=instrument.exchange,
                country_of_risk=instrument.country_of_risk,
                buy_available_flag=instrument.buy_available_flag,
                sell_available_flag=instrument.sell_available_flag,
                api_trade_available_flag=instrument.api_trade_available_flag,
                for_iis_flag=instrument.for_iis_flag,
                for_qual_investor_flag=instrument.for_qual_investor_flag,
                weekend_flag=instrument.weekend_flag,
                otc_flag=instrument.otc_flag,
                active_flag=instrument.active_flag,
            )
            session.add(row)
            continue

        row.figi = instrument.figi
        row.ticker = instrument.ticker
        row.class_code = instrument.class_code
        row.instrument_type = instrument.instrument_type
        row.name = instrument.name
        row.lot = instrument.lot
        row.currency = instrument.currency
        row.exchange = instrument.exchange
        row.country_of_risk = instrument.country_of_risk
        row.buy_available_flag = instrument.buy_available_flag
        row.sell_available_flag = instrument.sell_available_flag
        row.api_trade_available_flag = instrument.api_trade_available_flag
        row.for_iis_flag = instrument.for_iis_flag
        row.for_qual_investor_flag = instrument.for_qual_investor_flag
        row.weekend_flag = instrument.weekend_flag
        row.otc_flag = instrument.otc_flag
        row.active_flag = instrument.active_flag


def upsert_instrument_eligibility(
    session: Session,
    snapshots: list[Any],
) -> None:
    for snapshot in snapshots:
        row = session.get(InstrumentEligibilityModel, snapshot.instrument_uid)
        if row is None:
            session.add(
                InstrumentEligibilityModel(
                    instrument_uid=snapshot.instrument_uid,
                    eligible=snapshot.eligible,
                    reason_codes=snapshot.reason_codes,
                )
            )
            continue
        row.eligible = snapshot.eligible
        row.reason_codes = snapshot.reason_codes


def count_instrument_catalog(session: Session) -> int:
    return int(session.scalar(select(func.count(InstrumentCatalogModel.instrument_uid))) or 0)


def count_eligible_instruments(session: Session) -> int:
    return int(
        session.scalar(
            select(func.count(InstrumentEligibilityModel.instrument_uid)).where(
                InstrumentEligibilityModel.eligible.is_(True)
            )
        )
        or 0
    )


def get_eligible_instruments_for_backfill(session: Session) -> list[dict[str, object]]:
    rows = session.execute(
        select(InstrumentCatalogModel, InstrumentEligibilityModel)
        .join(
            InstrumentEligibilityModel,
            InstrumentEligibilityModel.instrument_uid == InstrumentCatalogModel.instrument_uid,
        )
        .where(InstrumentEligibilityModel.eligible.is_(True))
        .order_by(
            InstrumentCatalogModel.instrument_type.asc(),
            InstrumentCatalogModel.ticker.asc(),
            InstrumentCatalogModel.class_code.asc(),
        )
    ).all()
    return [
        {
            "instrument_uid": catalog.instrument_uid,
            "figi": catalog.figi,
            "ticker": catalog.ticker,
            "class_code": catalog.class_code,
            "instrument_type": catalog.instrument_type,
            "name": catalog.name,
            "lot": catalog.lot,
            "currency": catalog.currency,
            "exchange": catalog.exchange,
            "country_of_risk": catalog.country_of_risk,
        }
        for catalog, _eligibility in rows
    ]


def get_eligible_russian_shares(session: Session) -> list[dict[str, object]]:
    rows = session.execute(
        select(InstrumentCatalogModel, InstrumentEligibilityModel)
        .join(
            InstrumentEligibilityModel,
            InstrumentEligibilityModel.instrument_uid == InstrumentCatalogModel.instrument_uid,
        )
        .where(InstrumentEligibilityModel.eligible.is_(True))
        .where(InstrumentCatalogModel.instrument_type == "share")
        .where(InstrumentCatalogModel.country_of_risk == "RU")
        .order_by(
            InstrumentCatalogModel.ticker.asc(),
            InstrumentCatalogModel.class_code.asc(),
        )
    ).all()
    return [
        {
            "instrument_uid": catalog.instrument_uid,
            "figi": catalog.figi,
            "ticker": catalog.ticker,
            "class_code": catalog.class_code,
            "instrument_type": catalog.instrument_type,
            "name": catalog.name,
            "lot": catalog.lot,
            "currency": catalog.currency,
            "exchange": catalog.exchange,
            "country_of_risk": catalog.country_of_risk,
        }
        for catalog, _eligibility in rows
    ]


def upsert_historical_candles(
    session: Session,
    candles: list[dict[str, object]],
) -> None:
    for candle in candles:
        row = session.get(
            HistoricalCandleModel,
            (
                candle["instrument_uid"],
                candle["interval"],
                candle["candle_time"],
            ),
        )
        if row is None:
            session.add(
                HistoricalCandleModel(
                    instrument_uid=str(candle["instrument_uid"]),
                    interval=str(candle["interval"]),
                    candle_time=candle["candle_time"],
                    open_price=float(candle["open_price"]),
                    high_price=float(candle["high_price"]),
                    low_price=float(candle["low_price"]),
                    close_price=float(candle["close_price"]),
                    volume=float(candle["volume"]),
                    turnover_rub=float(candle["turnover_rub"]),
                    is_complete=bool(candle["is_complete"]),
                )
            )
            continue
        row.open_price = float(candle["open_price"])
        row.high_price = float(candle["high_price"])
        row.low_price = float(candle["low_price"])
        row.close_price = float(candle["close_price"])
        row.volume = float(candle["volume"])
        row.turnover_rub = float(candle["turnover_rub"])
        row.is_complete = bool(candle["is_complete"])


def upsert_dividend_events(
    session: Session,
    events: list[dict[str, object]],
) -> None:
    for event in events:
        row = session.get(DividendEventModel, str(event["event_id"]))
        if row is None:
            session.add(
                DividendEventModel(
                    event_id=str(event["event_id"]),
                    instrument_uid=str(event["instrument_uid"]),
                    record_date=event.get("record_date"),
                    payment_date=event.get("payment_date"),
                    declared_date=event.get("declared_date"),
                    last_buy_date=event.get("last_buy_date"),
                    created_at_event=event.get("created_at_event"),
                    dividend_type=str(event.get("dividend_type", "")),
                    regularity=str(event.get("regularity", "")),
                    currency=str(event.get("currency", "")),
                    dividend_net=float(event.get("dividend_net", 0.0) or 0.0),
                    close_price=float(event.get("close_price", 0.0) or 0.0),
                    yield_value=float(event.get("yield_value", 0.0) or 0.0),
                )
            )
            continue
        row.record_date = event.get("record_date")
        row.payment_date = event.get("payment_date")
        row.declared_date = event.get("declared_date")
        row.last_buy_date = event.get("last_buy_date")
        row.created_at_event = event.get("created_at_event")
        row.dividend_type = str(event.get("dividend_type", ""))
        row.regularity = str(event.get("regularity", ""))
        row.currency = str(event.get("currency", ""))
        row.dividend_net = float(event.get("dividend_net", 0.0) or 0.0)
        row.close_price = float(event.get("close_price", 0.0) or 0.0)
        row.yield_value = float(event.get("yield_value", 0.0) or 0.0)


def count_dividend_events(session: Session) -> int:
    return int(session.scalar(select(func.count(DividendEventModel.event_id))) or 0)


def load_dividend_events_for_instruments(
    session: Session,
    *,
    instrument_uids: list[str],
) -> list[dict[str, object]]:
    if not instrument_uids:
        return []

    rows = session.scalars(
        select(DividendEventModel)
        .where(DividendEventModel.instrument_uid.in_(instrument_uids))
        .order_by(
            DividendEventModel.instrument_uid.asc(),
            DividendEventModel.record_date.asc(),
        )
    ).all()
    return [
        {
            "event_id": row.event_id,
            "instrument_uid": row.instrument_uid,
            "record_date": row.record_date,
            "payment_date": row.payment_date,
            "declared_date": row.declared_date,
            "last_buy_date": row.last_buy_date,
            "created_at_event": row.created_at_event,
            "dividend_type": row.dividend_type,
            "regularity": row.regularity,
            "currency": row.currency,
            "dividend_net": row.dividend_net,
            "close_price": row.close_price,
            "yield_value": row.yield_value,
        }
        for row in rows
    ]


def upsert_instrument_research_status(
    session: Session,
    snapshots: list[dict[str, object]],
) -> None:
    for snapshot in snapshots:
        row = session.get(InstrumentResearchStatusModel, str(snapshot["instrument_uid"]))
        if row is None:
            session.add(
                InstrumentResearchStatusModel(
                    instrument_uid=str(snapshot["instrument_uid"]),
                    trading_status=str(snapshot.get("trading_status", "")),
                    buy_available_flag=bool(snapshot.get("buy_available_flag", False)),
                    sell_available_flag=bool(snapshot.get("sell_available_flag", False)),
                    api_trade_available_flag=bool(snapshot.get("api_trade_available_flag", False)),
                    otc_flag=bool(snapshot.get("otc_flag", False)),
                    blocked_tca_flag=bool(snapshot.get("blocked_tca_flag", False)),
                    first_1min_candle_date=snapshot.get("first_1min_candle_date"),
                    first_1day_candle_date=snapshot.get("first_1day_candle_date"),
                )
            )
            continue
        row.trading_status = str(snapshot.get("trading_status", ""))
        row.buy_available_flag = bool(snapshot.get("buy_available_flag", False))
        row.sell_available_flag = bool(snapshot.get("sell_available_flag", False))
        row.api_trade_available_flag = bool(snapshot.get("api_trade_available_flag", False))
        row.otc_flag = bool(snapshot.get("otc_flag", False))
        row.blocked_tca_flag = bool(snapshot.get("blocked_tca_flag", False))
        row.first_1min_candle_date = snapshot.get("first_1min_candle_date")
        row.first_1day_candle_date = snapshot.get("first_1day_candle_date")


def load_instrument_research_status_for_instruments(
    session: Session,
    *,
    instrument_uids: list[str],
) -> list[dict[str, object]]:
    if not instrument_uids:
        return []

    rows = session.scalars(
        select(InstrumentResearchStatusModel)
        .where(InstrumentResearchStatusModel.instrument_uid.in_(instrument_uids))
        .order_by(InstrumentResearchStatusModel.instrument_uid.asc())
    ).all()
    return [
        {
            "instrument_uid": row.instrument_uid,
            "trading_status": row.trading_status,
            "buy_available_flag": row.buy_available_flag,
            "sell_available_flag": row.sell_available_flag,
            "api_trade_available_flag": row.api_trade_available_flag,
            "otc_flag": row.otc_flag,
            "blocked_tca_flag": row.blocked_tca_flag,
            "first_1min_candle_date": row.first_1min_candle_date,
            "first_1day_candle_date": row.first_1day_candle_date,
            "snapshot_at": row.snapshot_at,
        }
        for row in rows
    ]


def count_historical_candles(session: Session, *, interval: str | None = None) -> int:
    query = select(func.count(HistoricalCandleModel.instrument_uid))
    if interval is not None:
        query = query.where(HistoricalCandleModel.interval == interval)
    return int(session.scalar(query) or 0)


def count_instruments_with_historical_candles(session: Session, *, interval: str | None = None) -> int:
    query = select(func.count(func.distinct(HistoricalCandleModel.instrument_uid)))
    if interval is not None:
        query = query.where(HistoricalCandleModel.interval == interval)
    return int(session.scalar(query) or 0)


def get_latest_historical_candle_at(session: Session, *, interval: str | None = None) -> str | None:
    query = select(func.max(HistoricalCandleModel.candle_time))
    if interval is not None:
        query = query.where(HistoricalCandleModel.interval == interval)
    latest = session.scalar(query)
    return latest.isoformat() if latest is not None else None


def upsert_instrument_history_quality(
    session: Session,
    snapshots: list[dict[str, object]],
) -> None:
    for snapshot in snapshots:
        row = session.get(InstrumentHistoryQualityModel, str(snapshot["instrument_uid"]))
        if row is None:
            session.add(
                InstrumentHistoryQualityModel(
                    instrument_uid=str(snapshot["instrument_uid"]),
                    interval=str(snapshot["interval"]),
                    completed_candles=int(snapshot["completed_candles"]),
                    median_turnover_rub=float(snapshot["median_turnover_rub"]),
                    latest_candle_at=snapshot["latest_candle_at"],
                    history_ready=bool(snapshot["history_ready"]),
                    reason_codes=str(snapshot["reason_codes"]),
                )
            )
            continue
        row.interval = str(snapshot["interval"])
        row.completed_candles = int(snapshot["completed_candles"])
        row.median_turnover_rub = float(snapshot["median_turnover_rub"])
        row.latest_candle_at = snapshot["latest_candle_at"]
        row.history_ready = bool(snapshot["history_ready"])
        row.reason_codes = str(snapshot["reason_codes"])


def count_history_ready_instruments(session: Session, *, interval: str | None = None) -> int:
    query = select(func.count(InstrumentHistoryQualityModel.instrument_uid)).where(
        InstrumentHistoryQualityModel.history_ready.is_(True)
    )
    if interval is not None:
        query = query.where(InstrumentHistoryQualityModel.interval == interval)
    return int(session.scalar(query) or 0)


def get_history_quality_by_uid(session: Session) -> dict[str, InstrumentHistoryQualityModel]:
    return {
        row.instrument_uid: row
        for row in session.scalars(select(InstrumentHistoryQualityModel)).all()
    }


def get_eligible_instruments_with_history(
    session: Session,
    *,
    interval: str,
    limit: int | None = None,
) -> list[dict[str, object]]:
    query = (
        select(InstrumentCatalogModel)
        .join(
            InstrumentEligibilityModel,
            InstrumentEligibilityModel.instrument_uid == InstrumentCatalogModel.instrument_uid,
        )
        .join(
            HistoricalCandleModel,
            HistoricalCandleModel.instrument_uid == InstrumentCatalogModel.instrument_uid,
        )
        .where(InstrumentEligibilityModel.eligible.is_(True))
        .where(HistoricalCandleModel.interval == interval)
        .group_by(InstrumentCatalogModel.instrument_uid)
        .order_by(
            InstrumentCatalogModel.instrument_type.asc(),
            InstrumentCatalogModel.ticker.asc(),
            InstrumentCatalogModel.class_code.asc(),
        )
    )
    if limit is not None:
        query = query.limit(limit)

    rows = session.scalars(query).all()
    return [
        {
            "instrument_uid": row.instrument_uid,
            "figi": row.figi,
            "ticker": row.ticker,
            "class_code": row.class_code,
            "instrument_type": row.instrument_type,
            "name": row.name,
            "lot": row.lot,
            "currency": row.currency,
        }
        for row in rows
    ]


def load_historical_candles_for_instruments(
    session: Session,
    *,
    instrument_uids: list[str],
    interval: str,
) -> list[dict[str, object]]:
    if not instrument_uids:
        return []

    rows = session.scalars(
        select(HistoricalCandleModel)
        .where(HistoricalCandleModel.instrument_uid.in_(instrument_uids))
        .where(HistoricalCandleModel.interval == interval)
        .order_by(
            HistoricalCandleModel.instrument_uid.asc(),
            HistoricalCandleModel.candle_time.asc(),
        )
    ).all()
    return [
        {
            "instrument_uid": row.instrument_uid,
            "interval": row.interval,
            "candle_time": row.candle_time,
            "open_price": row.open_price,
            "high_price": row.high_price,
            "low_price": row.low_price,
            "close_price": row.close_price,
            "volume": row.volume,
            "turnover_rub": row.turnover_rub,
            "is_complete": row.is_complete,
        }
        for row in rows
    ]


def get_latest_catalog_sync_at(session: Session) -> str | None:
    latest = session.scalar(select(func.max(InstrumentCatalogModel.updated_at)))
    return latest.isoformat() if latest is not None else None


def upsert_instrument_price(session: Session, symbol: str, last_price: float) -> None:
    row = session.get(InstrumentStateModel, symbol)
    if row is None:
        row = InstrumentStateModel(symbol=symbol, last_price=last_price)
        session.add(row)
    else:
        row.last_price = last_price

    position = session.get(PositionModel, symbol)
    if position is None:
        session.add(
            PositionModel(
                symbol=symbol,
                quantity=0,
                avg_price=0.0,
                market_price=last_price,
            )
        )
    else:
        position.market_price = last_price


def record_signal(
    session: Session,
    *,
    strategy_name: str,
    symbol: str,
    side: str,
    price: float,
    confidence: float,
    reason: str,
    status: str,
) -> SignalModel:
    signal = SignalModel(
        strategy_name=strategy_name,
        symbol=symbol,
        side=side,
        price=price,
        confidence=confidence,
        reason=reason,
        status=status,
    )
    session.add(signal)
    session.flush()
    return signal


def get_position_quantity(session: Session, symbol: str) -> int:
    row = session.get(PositionModel, symbol)
    return row.quantity if row else 0


def record_filled_order(
    session: Session,
    *,
    signal_id: int | None,
    symbol: str,
    side: str,
    quantity: int,
    price: float,
    broker_mode: str,
    broker_order_id: str,
    reason: str,
) -> OrderModel:
    order = OrderModel(
        signal_id=signal_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        status="filled",
        broker_mode=broker_mode,
        broker_order_id=broker_order_id,
        reason=reason,
    )
    session.add(order)

    position = session.get(PositionModel, symbol)
    if position is None:
        position = PositionModel(
            symbol=symbol,
            quantity=0,
            avg_price=0.0,
            market_price=price,
        )
        session.add(position)
        session.flush()

    delta = quantity if side == "buy" else -quantity
    new_quantity = position.quantity + delta

    if position.quantity == 0 or (position.quantity > 0 and new_quantity > position.quantity) or (position.quantity < 0 and new_quantity < position.quantity):
        total_abs = abs(position.quantity) + abs(delta)
        if total_abs == 0:
            position.avg_price = 0.0
        else:
            weighted_value = (abs(position.quantity) * position.avg_price) + (abs(delta) * price)
            position.avg_price = weighted_value / total_abs
    elif new_quantity == 0:
        position.avg_price = 0.0
    elif position.quantity > 0 > new_quantity or position.quantity < 0 < new_quantity:
        position.avg_price = price

    position.quantity = new_quantity
    position.market_price = price
    session.flush()
    return order


def record_shadow_trade(
    session: Session,
    *,
    strategy_name: str,
    symbol: str,
    side: str,
    quantity: int,
    price: float,
    notional_rub: float,
    status: str,
    reason: str,
) -> ShadowTradeModel:
    row = ShadowTradeModel(
        strategy_name=strategy_name,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        notional_rub=notional_rub,
        status=status,
        reason=reason,
    )
    session.add(row)
    session.flush()
    return row


def record_shadow_rebalance(
    session: Session,
    *,
    strategy_name: str,
    status: str,
    regime_state: str,
    selected_symbols: str,
    target_weights: str,
    positions_before: str,
    positions_after: str,
    planned_actions: int,
    executed_actions: int,
    equity_rub: float,
    cash_rub: float,
    reason: str,
) -> ShadowRebalanceModel:
    row = ShadowRebalanceModel(
        strategy_name=strategy_name,
        status=status,
        regime_state=regime_state,
        selected_symbols=selected_symbols,
        target_weights=target_weights,
        positions_before=positions_before,
        positions_after=positions_after,
        planned_actions=planned_actions,
        executed_actions=executed_actions,
        equity_rub=equity_rub,
        cash_rub=cash_rub,
        reason=reason,
    )
    session.add(row)
    session.flush()
    return row


def serialize_positions(session: Session) -> list[dict[str, object]]:
    rows = session.scalars(
        select(PositionModel).order_by(PositionModel.symbol.asc())
    ).all()
    payload: list[dict[str, object]] = []
    for row in rows:
        unrealized = (row.market_price - row.avg_price) * row.quantity if row.quantity else 0.0
        payload.append(
            {
                "symbol": row.symbol,
                "quantity": row.quantity,
                "avg_price": round(row.avg_price, 4),
                "market_price": round(row.market_price, 4),
                "unrealized_pnl": round(unrealized, 4),
            }
        )
    return payload


def serialize_orders(session: Session, limit: int = 25, min_id: int = 0) -> list[dict[str, object]]:
    rows = session.scalars(
        select(OrderModel)
        .where(OrderModel.id > min_id)
        .order_by(desc(OrderModel.created_at), desc(OrderModel.id))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "price": row.price,
            "status": row.status,
            "broker_mode": row.broker_mode,
            "broker_order_id": row.broker_order_id,
            "reason": row.reason,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def serialize_instrument_universe(
    session: Session,
    *,
    limit: int = 200,
    eligible_only: bool = False,
) -> list[dict[str, object]]:
    query = select(InstrumentCatalogModel)
    if eligible_only:
        query = query.join(
            InstrumentEligibilityModel,
            InstrumentEligibilityModel.instrument_uid == InstrumentCatalogModel.instrument_uid,
        ).where(InstrumentEligibilityModel.eligible.is_(True))

    rows = session.scalars(
        query.order_by(
            InstrumentCatalogModel.instrument_type.asc(),
            InstrumentCatalogModel.ticker.asc(),
            InstrumentCatalogModel.class_code.asc(),
        ).limit(limit)
    ).all()
    eligibility_by_uid = {
        row.instrument_uid: row
        for row in session.scalars(select(InstrumentEligibilityModel)).all()
    }
    history_quality_by_uid = get_history_quality_by_uid(session)

    payload: list[dict[str, object]] = []
    for row in rows:
        eligibility = eligibility_by_uid.get(row.instrument_uid)
        history_quality = history_quality_by_uid.get(row.instrument_uid)
        if eligible_only and (eligibility is None or not eligibility.eligible):
            continue
        payload.append(
            {
                "instrument_uid": row.instrument_uid,
                "figi": row.figi,
                "ticker": row.ticker,
                "class_code": row.class_code,
                "instrument_type": row.instrument_type,
                "name": row.name,
                "lot": row.lot,
                "currency": row.currency,
                "exchange": row.exchange,
                "country_of_risk": row.country_of_risk,
                "buy_available_flag": row.buy_available_flag,
                "sell_available_flag": row.sell_available_flag,
                "api_trade_available_flag": row.api_trade_available_flag,
                "for_iis_flag": row.for_iis_flag,
                "for_qual_investor_flag": row.for_qual_investor_flag,
                "weekend_flag": row.weekend_flag,
                "otc_flag": row.otc_flag,
                "active_flag": row.active_flag,
                "eligible": eligibility.eligible if eligibility is not None else False,
                "reason_codes": eligibility.reason_codes if eligibility is not None else "missing_snapshot",
                "history_ready": history_quality.history_ready if history_quality is not None else False,
                "history_reason_codes": history_quality.reason_codes if history_quality is not None else "missing_history",
                "completed_candles": history_quality.completed_candles if history_quality is not None else 0,
                "median_turnover_rub": round(history_quality.median_turnover_rub, 4) if history_quality is not None else 0.0,
                "latest_history_candle_at": history_quality.latest_candle_at.isoformat() if history_quality is not None and history_quality.latest_candle_at is not None else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
        )
    return payload


def serialize_signals(session: Session, limit: int = 25, min_id: int = 0) -> list[dict[str, object]]:
    rows = session.scalars(
        select(SignalModel)
        .where(SignalModel.id > min_id)
        .order_by(desc(SignalModel.created_at), desc(SignalModel.id))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "strategy_name": row.strategy_name,
            "symbol": row.symbol,
            "side": row.side,
            "price": row.price,
            "confidence": row.confidence,
            "reason": row.reason,
            "status": row.status,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def serialize_shadow_trades(session: Session, limit: int = 25) -> list[dict[str, object]]:
    return serialize_strategy_ledger_trades(session, strategy_name="portfolio_momentum_shadow", limit=limit)


def serialize_strategy_ledger_trades(
    session: Session,
    *,
    strategy_name: str,
    limit: int = 25,
    min_id: int = 0,
) -> list[dict[str, object]]:
    rows = session.scalars(
        select(ShadowTradeModel)
        .where(ShadowTradeModel.strategy_name == strategy_name)
        .where(ShadowTradeModel.id > min_id)
        .order_by(desc(ShadowTradeModel.created_at), desc(ShadowTradeModel.id))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "strategy_name": row.strategy_name,
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "price": row.price,
            "notional_rub": row.notional_rub,
            "status": row.status,
            "reason": row.reason,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def serialize_shadow_rebalances(session: Session, limit: int = 25) -> list[dict[str, object]]:
    return serialize_strategy_rebalances(session, strategy_name="portfolio_momentum_shadow", limit=limit)


def serialize_strategy_rebalances(
    session: Session,
    *,
    strategy_name: str,
    limit: int = 25,
    min_id: int = 0,
) -> list[dict[str, object]]:
    rows = session.scalars(
        select(ShadowRebalanceModel)
        .where(ShadowRebalanceModel.strategy_name == strategy_name)
        .where(ShadowRebalanceModel.id > min_id)
        .order_by(desc(ShadowRebalanceModel.created_at), desc(ShadowRebalanceModel.id))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "strategy_name": row.strategy_name,
            "status": row.status,
            "regime_state": row.regime_state,
            "selected_symbols": row.selected_symbols,
            "target_weights": row.target_weights,
            "positions_before": row.positions_before,
            "positions_after": row.positions_after,
            "planned_actions": row.planned_actions,
            "executed_actions": row.executed_actions,
            "equity_rub": row.equity_rub,
            "cash_rub": row.cash_rub,
            "reason": row.reason,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def record_historical_simulation_run(
    session: Session,
    *,
    strategy_name: str,
    interval: str,
    status: str,
    instruments_considered: int,
    instruments_with_history: int,
    rebalance_points: int,
    completed_rebalances: int,
    executed_actions: int,
    turnover_rub: float,
    initial_cash_rub: float,
    final_cash_rub: float,
    final_equity_rub: float,
    total_return_pct: float,
    max_drawdown_pct: float,
    latest_selected_symbols: str,
    latest_target_weights: str,
    note: str,
) -> HistoricalSimulationRunModel:
    row = HistoricalSimulationRunModel(
        strategy_name=strategy_name,
        interval=interval,
        status=status,
        instruments_considered=instruments_considered,
        instruments_with_history=instruments_with_history,
        rebalance_points=rebalance_points,
        completed_rebalances=completed_rebalances,
        executed_actions=executed_actions,
        turnover_rub=turnover_rub,
        initial_cash_rub=initial_cash_rub,
        final_cash_rub=final_cash_rub,
        final_equity_rub=final_equity_rub,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        latest_selected_symbols=latest_selected_symbols,
        latest_target_weights=latest_target_weights,
        note=note,
    )
    session.add(row)
    session.flush()
    return row


def record_historical_simulation_rebalance(
    session: Session,
    *,
    run_id: int,
    rebalance_time: datetime,
    status: str,
    regime_state: str,
    selected_symbols: str,
    target_weights: str,
    positions_before: str,
    positions_after: str,
    planned_actions: int,
    executed_actions: int,
    turnover_rub: float,
    equity_rub: float,
    cash_rub: float,
    reason: str,
) -> HistoricalSimulationRebalanceModel:
    row = HistoricalSimulationRebalanceModel(
        run_id=run_id,
        rebalance_time=rebalance_time,
        status=status,
        regime_state=regime_state,
        selected_symbols=selected_symbols,
        target_weights=target_weights,
        positions_before=positions_before,
        positions_after=positions_after,
        planned_actions=planned_actions,
        executed_actions=executed_actions,
        turnover_rub=turnover_rub,
        equity_rub=equity_rub,
        cash_rub=cash_rub,
        reason=reason,
    )
    session.add(row)
    session.flush()
    return row


def get_latest_historical_simulation_run_id(session: Session) -> int | None:
    return session.scalar(select(func.max(HistoricalSimulationRunModel.id)))


def serialize_latest_historical_simulation(session: Session) -> dict[str, object] | None:
    row = session.scalar(
        select(HistoricalSimulationRunModel)
        .order_by(desc(HistoricalSimulationRunModel.id))
        .limit(1)
    )
    if row is None:
        return None
    return {
        "id": row.id,
        "strategy_name": row.strategy_name,
        "interval": row.interval,
        "status": row.status,
        "instruments_considered": row.instruments_considered,
        "instruments_with_history": row.instruments_with_history,
        "rebalance_points": row.rebalance_points,
        "completed_rebalances": row.completed_rebalances,
        "executed_actions": row.executed_actions,
        "turnover_rub": round(row.turnover_rub, 4),
        "initial_cash_rub": round(row.initial_cash_rub, 4),
        "final_cash_rub": round(row.final_cash_rub, 4),
        "final_equity_rub": round(row.final_equity_rub, 4),
        "total_return_pct": round(row.total_return_pct, 4),
        "max_drawdown_pct": round(row.max_drawdown_pct, 4),
        "latest_selected_symbols": row.latest_selected_symbols,
        "latest_target_weights": row.latest_target_weights,
        "note": row.note,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def serialize_historical_simulation_rebalances(
    session: Session,
    *,
    run_id: int,
    limit: int = 50,
) -> list[dict[str, object]]:
    rows = session.scalars(
        select(HistoricalSimulationRebalanceModel)
        .where(HistoricalSimulationRebalanceModel.run_id == run_id)
        .order_by(
            desc(HistoricalSimulationRebalanceModel.rebalance_time),
            desc(HistoricalSimulationRebalanceModel.id),
        )
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "run_id": row.run_id,
            "rebalance_time": row.rebalance_time.isoformat() if row.rebalance_time else None,
            "status": row.status,
            "regime_state": row.regime_state,
            "selected_symbols": row.selected_symbols,
            "target_weights": row.target_weights,
            "positions_before": row.positions_before,
            "positions_after": row.positions_after,
            "planned_actions": row.planned_actions,
            "executed_actions": row.executed_actions,
            "turnover_rub": round(row.turnover_rub, 4),
            "equity_rub": round(row.equity_rub, 4),
            "cash_rub": round(row.cash_rub, 4),
            "reason": row.reason,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


def get_shadow_history_counts(session: Session) -> tuple[int, int]:
    trades_count = session.scalar(select(func.count(ShadowTradeModel.id))) or 0
    rebalances_count = session.scalar(select(func.count(ShadowRebalanceModel.id))) or 0
    return int(trades_count), int(rebalances_count)


def build_dashboard(
    session: Session,
    *,
    settings: AppSettings | None = None,
) -> dict[str, object]:
    resolved_settings = settings or get_settings()
    paused = is_paused(session)
    heartbeat = get_state(session, "engine.last_heartbeat")
    iteration = get_state(session, "engine.iteration", "0")
    broker_mode = get_state(session, "engine.broker_mode", "unknown")
    started_at = get_state(session, "engine.started_at")
    strategy_mode = get_state(session, "strategy.mode", "portfolio_momentum")
    strategy_portfolio_live_enabled = (
        get_state(session, "strategy.portfolio_live_enabled", "false") == "true"
    )
    strategy_shadow_portfolio_enabled = (
        get_state(session, "strategy.shadow_portfolio_enabled", "false") == "true"
    )
    strategy_profile = get_state(session, "strategy.profile", "balanced")
    candle_interval = get_state(session, "strategy.candle_interval", "n/a")
    portfolio_top_percentile = int(get_state(session, "portfolio.top_percentile", "95") or "95")
    portfolio_regime_filter_enabled = (
        get_state(session, "portfolio.regime_filter_enabled", "true") == "true"
    )
    portfolio_regime_symbol = get_state(session, "portfolio.regime_symbol", "n/a")
    portfolio_regime_state = get_state(session, "portfolio.regime_state", "n/a")
    portfolio_regime_reason = get_state(session, "portfolio.regime_reason", "n/a")
    portfolio_selected_symbols = get_state(session, "portfolio.selected_symbols", "") or ""
    portfolio_target_weights = get_state(session, "portfolio.target_weights", "") or ""
    portfolio_positions = get_state(session, "portfolio.positions", "") or ""
    portfolio_last_rebalance_at = get_state(session, "portfolio.last_rebalance_at", "n/a")
    portfolio_next_rebalance_at = get_state(session, "portfolio.next_rebalance_at", "n/a")
    portfolio_last_equity_rub = float(get_state(session, "portfolio.last_equity_rub", "0") or "0")
    portfolio_last_cash_rub = float(get_state(session, "portfolio.last_cash_rub", "0") or "0")
    portfolio_last_plan_actions = int(get_state(session, "portfolio.last_plan_actions", "0") or "0")
    portfolio_last_executed_actions = get_state(session, "portfolio.last_executed_actions", "") or ""
    shadow_portfolio_regime_state = get_state(session, "shadow.portfolio.regime_state", "n/a")
    shadow_portfolio_regime_reason = get_state(session, "shadow.portfolio.regime_reason", "n/a")
    shadow_portfolio_selected_symbols = get_state(session, "shadow.portfolio.selected_symbols", "") or ""
    shadow_portfolio_target_weights = get_state(session, "shadow.portfolio.target_weights", "") or ""
    shadow_portfolio_positions = get_state(session, "shadow.portfolio.positions", "") or ""
    shadow_portfolio_last_rebalance_at = get_state(session, "shadow.portfolio.last_rebalance_at", "n/a")
    shadow_portfolio_next_rebalance_at = get_state(session, "shadow.portfolio.next_rebalance_at", "n/a")
    shadow_portfolio_cash_rub = float(get_state(session, "shadow.portfolio.cash_rub", "0") or "0")
    shadow_portfolio_equity_rub = float(get_state(session, "shadow.portfolio.equity_rub", "0") or "0")
    shadow_portfolio_last_plan_actions = int(get_state(session, "shadow.portfolio.last_plan_actions", "0") or "0")
    shadow_portfolio_last_executed_actions = (
        get_state(session, "shadow.portfolio.last_executed_actions", "") or ""
    )
    share_target_order_notional_rub = float(
        get_state(session, "execution.share_target_order_notional_rub", "0") or "0"
    )
    bond_target_order_notional_rub = float(
        get_state(session, "execution.bond_target_order_notional_rub", "0") or "0"
    )
    fx_target_order_notional_rub = float(
        get_state(session, "execution.fx_target_order_notional_rub", "0") or "0"
    )
    share_max_position_notional_rub = float(
        get_state(session, "risk.share_max_position_notional_rub", "0") or "0"
    )
    bond_max_position_notional_rub = float(
        get_state(session, "risk.bond_max_position_notional_rub", "0") or "0"
    )
    fx_max_position_notional_rub = float(
        get_state(session, "risk.fx_max_position_notional_rub", "0") or "0"
    )
    universe_catalog_size = count_instrument_catalog(session)
    universe_eligible_size = count_eligible_instruments(session)
    universe_last_sync_at = get_latest_catalog_sync_at(session)
    historical_candle_count = count_historical_candles(session, interval=candle_interval)
    historical_covered_instruments = count_instruments_with_historical_candles(
        session,
        interval=candle_interval,
    )
    history_ready_instruments = count_history_ready_instruments(session, interval=candle_interval)
    historical_last_candle_at = get_latest_historical_candle_at(session, interval=candle_interval)
    history_backfill_cursor = int(get_state(session, "history.backfill_cursor", "0") or "0")
    history_backfill_last_batch_at = get_state(session, "history.backfill_last_batch_at", "n/a")
    history_backfill_last_batch_size = int(get_state(session, "history.backfill_last_batch_size", "0") or "0")
    history_backfill_last_batch_written = int(get_state(session, "history.backfill_last_batch_written", "0") or "0")
    history_backfill_cycle_completed = get_state(session, "history.backfill_cycle_completed", "false") == "true"
    latest_simulation = serialize_latest_historical_simulation(session)
    order_offset, signal_offset = get_run_offsets(session)
    shadow_trade_offset, shadow_rebalance_offset = get_strategy_ledger_offsets(session)
    order_count = session.scalar(
        select(func.count(OrderModel.id)).where(OrderModel.id > order_offset)
    ) or 0
    signal_count = session.scalar(
        select(func.count(SignalModel.id)).where(SignalModel.id > signal_offset)
    ) or 0
    prices = session.scalars(
        select(InstrumentStateModel).order_by(InstrumentStateModel.symbol.asc())
    ).all()

    return {
        "system": {
            "paused": paused,
            "engine_last_heartbeat": heartbeat,
            "engine_iteration": int(iteration),
            "broker_mode": broker_mode,
            "run_started_at": started_at,
            "strategy_mode": strategy_mode,
            "strategy_portfolio_live_enabled": strategy_portfolio_live_enabled,
            "strategy_shadow_portfolio_enabled": strategy_shadow_portfolio_enabled,
            "strategy_profile": strategy_profile,
            "strategy_candle_interval": candle_interval,
            "portfolio_top_percentile": portfolio_top_percentile,
            "portfolio_regime_filter_enabled": portfolio_regime_filter_enabled,
            "portfolio_regime_symbol": portfolio_regime_symbol,
            "portfolio_regime_state": portfolio_regime_state,
            "portfolio_regime_reason": portfolio_regime_reason,
            "portfolio_selected_symbols": portfolio_selected_symbols,
            "portfolio_target_weights": portfolio_target_weights,
            "portfolio_last_rebalance_at": portfolio_last_rebalance_at,
            "portfolio_next_rebalance_at": portfolio_next_rebalance_at,
            "portfolio_last_equity_rub": portfolio_last_equity_rub,
            "portfolio_last_cash_rub": portfolio_last_cash_rub,
            "portfolio_last_plan_actions": portfolio_last_plan_actions,
            "portfolio_last_executed_actions": portfolio_last_executed_actions,
            "historical_regime_mode": resolved_settings.historical_regime_mode,
            "historical_regime_spx_ticker": resolved_settings.historical_regime_spx_ticker,
            "historical_regime_vix_ticker": resolved_settings.historical_regime_vix_ticker,
            "historical_regime_vix_threshold": resolved_settings.historical_regime_vix_threshold,
            "historical_ranking_price_role": "daily_close_prev_day_raw_placeholder_until_dividend_adjustment",
            "historical_execution_price_role": "daily_open_from_intraday",
            "share_target_order_notional_rub": share_target_order_notional_rub,
            "bond_target_order_notional_rub": bond_target_order_notional_rub,
            "fx_target_order_notional_rub": fx_target_order_notional_rub,
            "share_max_position_notional_rub": share_max_position_notional_rub,
            "bond_max_position_notional_rub": bond_max_position_notional_rub,
            "fx_max_position_notional_rub": fx_max_position_notional_rub,
            "universe_catalog_size": universe_catalog_size,
            "universe_eligible_size": universe_eligible_size,
            "universe_last_sync_at": universe_last_sync_at,
            "historical_candle_count": historical_candle_count,
            "historical_covered_instruments": historical_covered_instruments,
            "history_ready_instruments": history_ready_instruments,
            "historical_last_candle_at": historical_last_candle_at,
            "history_backfill_cursor": history_backfill_cursor,
            "history_backfill_last_batch_at": history_backfill_last_batch_at,
            "history_backfill_last_batch_size": history_backfill_last_batch_size,
            "history_backfill_last_batch_written": history_backfill_last_batch_written,
            "history_backfill_cycle_completed": history_backfill_cycle_completed,
            "simulation_last_run_id": latest_simulation["id"] if latest_simulation is not None else None,
            "simulation_last_status": latest_simulation["status"] if latest_simulation is not None else "n/a",
            "simulation_last_return_pct": latest_simulation["total_return_pct"] if latest_simulation is not None else None,
            "simulation_last_max_drawdown_pct": latest_simulation["max_drawdown_pct"] if latest_simulation is not None else None,
            "simulation_last_rebalances": latest_simulation["completed_rebalances"] if latest_simulation is not None else 0,
            "simulation_last_created_at": latest_simulation["created_at"] if latest_simulation is not None else None,
            "order_count": order_count,
            "signal_count": signal_count,
        },
        "shadow_portfolio": {
            "enabled": strategy_shadow_portfolio_enabled,
            "regime_state": shadow_portfolio_regime_state,
            "regime_reason": shadow_portfolio_regime_reason,
            "selected_symbols": shadow_portfolio_selected_symbols,
            "target_weights": shadow_portfolio_target_weights,
            "positions": shadow_portfolio_positions,
            "last_rebalance_at": shadow_portfolio_last_rebalance_at,
            "next_rebalance_at": shadow_portfolio_next_rebalance_at,
            "cash_rub": shadow_portfolio_cash_rub,
            "equity_rub": shadow_portfolio_equity_rub,
            "last_plan_actions": shadow_portfolio_last_plan_actions,
            "last_executed_actions": shadow_portfolio_last_executed_actions,
        },
        "portfolio_live": {
            "enabled": strategy_portfolio_live_enabled,
            "regime_state": portfolio_regime_state,
            "regime_reason": portfolio_regime_reason,
            "selected_symbols": portfolio_selected_symbols,
            "target_weights": portfolio_target_weights,
            "positions": portfolio_positions,
            "last_rebalance_at": portfolio_last_rebalance_at,
            "next_rebalance_at": portfolio_next_rebalance_at,
            "cash_rub": portfolio_last_cash_rub,
            "equity_rub": portfolio_last_equity_rub,
            "last_plan_actions": portfolio_last_plan_actions,
            "last_executed_actions": portfolio_last_executed_actions,
        },
        "prices": [
            {
                "symbol": row.symbol,
                "last_price": round(row.last_price, 4),
            }
            for row in prices
        ],
        "positions": serialize_positions(session),
        "orders": serialize_orders(session, limit=10, min_id=order_offset),
        "signals": serialize_signals(session, limit=10, min_id=signal_offset),
        "shadow_trades": serialize_strategy_ledger_trades(
            session,
            strategy_name="portfolio_momentum_shadow",
            limit=15,
            min_id=shadow_trade_offset,
        ),
        "shadow_rebalances": serialize_strategy_rebalances(
            session,
            strategy_name="portfolio_momentum_shadow",
            limit=20,
            min_id=shadow_rebalance_offset,
        ),
        "portfolio_trades": serialize_strategy_ledger_trades(
            session,
            strategy_name="portfolio_momentum_live",
            limit=20,
            min_id=shadow_trade_offset,
        ),
        "portfolio_rebalances": serialize_strategy_rebalances(
            session,
            strategy_name="portfolio_momentum_live",
            limit=20,
            min_id=shadow_rebalance_offset,
        ),
        "historical_simulation": latest_simulation,
    }
