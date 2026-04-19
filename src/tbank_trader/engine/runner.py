from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import time

from sqlalchemy.orm import Session

from tbank_trader.broker.base import BrokerAdapter, BrokerInstrument
from tbank_trader.broker.simulated import SimulatedBrokerAdapter
from tbank_trader.broker.tbank import TBankBrokerAdapter
from tbank_trader.config import AppSettings, get_settings
from tbank_trader.core.portfolio_momentum import (
    build_rebalance_plan,
    compute_portfolio_selection,
)
from tbank_trader.core.execution import ExecutionConstraints, OrderSizer, build_execution_constraints
from tbank_trader.core.risk import RiskEngine
from tbank_trader.core.strategy import BaseSignalGenerator, build_strategy_generator
from tbank_trader.services.event_bus import EventBus
from tbank_trader.services.tbank_client import quotation_to_float
from tbank_trader.storage.db import build_engine, build_session_factory, init_database
from tbank_trader.storage.repository import (
    bootstrap_defaults,
    get_shadow_history_counts,
    get_state,
    get_position_quantity,
    is_paused,
    record_filled_order,
    record_shadow_rebalance,
    record_shadow_trade,
    record_signal,
    start_new_run,
    set_state,
    sync_positions,
    update_heartbeat,
    upsert_instrument_price,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PortfolioRuntimeState:
    last_rebalance_at: datetime | None = None


@dataclass(slots=True)
class ShadowPortfolioState:
    cash_rub: float
    positions: dict[str, int]


def create_broker_adapter(settings: AppSettings) -> BrokerAdapter:
    if settings.broker_mode == "simulated":
        return SimulatedBrokerAdapter(settings.symbols, settings.random_seed)
    return TBankBrokerAdapter(settings)


def build_strategies(
    settings: AppSettings,
    instruments: dict[str, BrokerInstrument],
) -> dict[str, BaseSignalGenerator]:
    return {
        symbol: build_strategy_generator(
            instrument_type=instruments[symbol].instrument_type,
            profile=settings.strategy_profile,
            short_window=settings.short_window,
            long_window=settings.long_window,
            threshold_bps=settings.signal_threshold_bps,
        )
        for symbol in settings.symbols
    }


def build_constraints(
    settings: AppSettings,
    instruments: dict[str, BrokerInstrument],
) -> dict[str, ExecutionConstraints]:
    return {
        symbol: build_execution_constraints(
            settings=settings,
            instrument=instruments[symbol],
            broker_mode=settings.broker_mode,
        )
        for symbol in settings.symbols
    }


def warmup_tbank_strategies(
    settings: AppSettings,
    broker: TBankBrokerAdapter,
    strategies: dict[str, BaseSignalGenerator],
) -> None:
    now = datetime.now(timezone.utc)
    from_ = now - timedelta(hours=settings.strategy_warmup_lookback_hours)

    for symbol, strategy in strategies.items():
        required = max(strategy.required_history - 1, 0)
        if required <= 0:
            continue

        instrument = broker.instruments[symbol]
        try:
            candles = broker.client.get_candles(
                instrument_id=instrument.instrument_uid,
                from_=from_,
                to=now,
                interval=settings.strategy_candle_interval,
                limit=required + settings.strategy_warmup_extra_bars,
            )
            closes = [
                quotation_to_float(candle.get("close"))
                for candle in candles
                if candle.get("isComplete")
            ]
            warmup_prices = closes[-required:]
            if len(warmup_prices) < required:
                logger.warning(
                    "Warmup candles are shorter than required for %s: have=%s need=%s",
                    symbol,
                    len(warmup_prices),
                    required,
                )
            strategy.warmup(warmup_prices)
            logger.info(
                "Warmup loaded for %s: strategy=%s candles=%s interval=%s",
                symbol,
                strategy.strategy_name,
                len(warmup_prices),
                settings.strategy_candle_interval,
            )
        except Exception:
            logger.exception("Warmup failed for %s", symbol)


def run_event_driven_iteration(
    *,
    session: Session,
    strategies: dict[str, BaseSignalGenerator],
    instruments: dict[str, BrokerInstrument],
    constraints_by_symbol: dict[str, ExecutionConstraints],
    risk_engine: RiskEngine,
    order_sizer: OrderSizer,
    broker: BrokerAdapter,
    broker_mode: str,
    event_bus: EventBus,
) -> dict[str, float]:
    paused = is_paused(session)
    symbols = list(strategies)

    try:
        prices_by_symbol = broker.get_prices(symbols)
    except Exception:
        logger.exception("Failed to fetch market prices")
        return {}

    for symbol, strategy in strategies.items():
        price = prices_by_symbol.get(symbol)
        if price is None:
            logger.error("Missing market price in batch response for %s", symbol)
            continue
        upsert_instrument_price(session, symbol, price)
        signal = strategy.on_price(price)
        if signal is None:
            continue

        instrument = instruments[symbol]
        constraints = constraints_by_symbol[symbol]
        current_position = get_position_quantity(session, symbol)
        sizing = order_sizer.plan(
            symbol=symbol,
            side=signal.side,
            instrument=instrument,
            constraints=constraints,
            price=price,
            current_position=current_position,
        )
        if not sizing.approved:
            record_signal(
                session,
                strategy_name=strategy.strategy_name,
                symbol=symbol,
                side=signal.side,
                price=price,
                confidence=signal.confidence,
                reason=f"{signal.reason}|size:{sizing.reason}",
                status="rejected",
            )
            logger.info("Signal rejected by sizing: %s %s %s", symbol, signal.side, sizing.reason)
            continue

        decision = risk_engine.evaluate(
            paused=paused,
            symbol=symbol,
            side=signal.side,
            quantity=sizing.quantity,
            current_position=current_position,
            price=price,
            lot=instrument.lot,
            constraints=constraints,
        )
        signal_row = record_signal(
            session,
            strategy_name=strategy.strategy_name,
            symbol=symbol,
            side=signal.side,
            price=price,
            confidence=signal.confidence,
            reason=f"{signal.reason}|size:{sizing.reason}|risk:{decision.reason}",
            status="approved" if decision.approved else "rejected",
        )

        if not decision.approved:
            logger.info("Signal rejected: %s %s %s", symbol, signal.side, decision.reason)
            continue

        try:
            broker_order = broker.place_order(
                symbol=symbol,
                side=signal.side,
                quantity=sizing.quantity,
                price=price,
            )
        except Exception as exc:
            signal_row.status = "broker_error"
            signal_row.reason = f"{signal_row.reason}|broker:{exc}"
            logger.exception("Broker order failed for %s %s", symbol, signal.side)
            continue
        order_row = record_filled_order(
            session,
            signal_id=signal_row.id,
            symbol=symbol,
            side=signal.side,
            quantity=sizing.quantity,
            price=price,
            broker_mode=broker_mode,
            broker_order_id=broker_order.broker_order_id,
            reason=f"{signal.reason}|size:{sizing.reason}",
        )
        event_bus.publish(
            "trader.events",
            {
                "type": "order_filled",
                "symbol": symbol,
                "side": signal.side,
                "quantity": sizing.quantity,
                "price": price,
                "order_id": order_row.id,
            },
        )
        logger.info(
            "Filled order %s %s qty=%s at %.4f",
            symbol,
            signal.side,
            sizing.quantity,
            price,
        )

    session.commit()
    if isinstance(broker, TBankBrokerAdapter):
        try:
            sync_positions(session, broker.get_position_snapshots())
        except Exception:
            logger.exception("Failed to sync broker positions")
    return prices_by_symbol


def _format_weight_summary(target_weights: dict[str, float]) -> str:
    if not target_weights:
        return ""
    ordered = sorted(target_weights.items(), key=lambda item: item[1], reverse=True)
    return ",".join(f"{symbol}:{weight:.3f}" for symbol, weight in ordered)


def _format_position_summary(positions: dict[str, int]) -> str:
    active_positions = [
        (symbol, quantity)
        for symbol, quantity in sorted(positions.items())
        if quantity != 0
    ]
    return ",".join(f"{symbol}:{quantity}" for symbol, quantity in active_positions)


def _parse_position_summary(payload: str) -> dict[str, int]:
    positions: dict[str, int] = {}
    if not payload:
        return positions
    for item in payload.split(","):
        symbol, _, quantity = item.partition(":")
        if not symbol or not quantity:
            continue
        try:
            positions[symbol] = int(quantity)
        except ValueError:
            continue
    return positions


def _mark_to_market_equity(
    *,
    cash_rub: float,
    positions: dict[str, int],
    prices_by_symbol: dict[str, float],
    instruments: dict[str, BrokerInstrument],
) -> float:
    equity = cash_rub
    for symbol, quantity in positions.items():
        price = prices_by_symbol.get(symbol)
        instrument = instruments.get(symbol)
        if price is None or instrument is None:
            continue
        equity += quantity * price * instrument.lot
    return equity


def _build_history_selection(
    *,
    settings: AppSettings,
    symbols: list[str],
    broker: BrokerAdapter,
) -> tuple[object, dict[str, list[float]]]:
    history_limit = max(settings.portfolio_history_bars, max(settings.portfolio_momentum_periods) + 1)
    history_by_symbol: dict[str, list[float]] = {}
    for symbol in symbols:
        try:
            closes = broker.get_historical_closes(
                symbol=symbol,
                limit=history_limit,
                interval=settings.strategy_candle_interval,
            )
        except Exception:
            logger.exception("Failed to fetch history for %s", symbol)
            continue
        if closes:
            history_by_symbol[symbol] = closes

    regime_symbol = settings.portfolio_regime_symbol or symbols[0]
    selection = compute_portfolio_selection(
        history_by_symbol=history_by_symbol,
        momentum_periods=settings.portfolio_momentum_periods,
        top_percentile=settings.portfolio_top_percentile,
        min_positions=settings.portfolio_min_positions,
        max_positions=settings.portfolio_max_positions,
        regime_filter_enabled=settings.portfolio_regime_filter_enabled,
        regime_symbol=regime_symbol,
        regime_ma_window=settings.portfolio_regime_ma_window,
    )
    return selection, history_by_symbol


def _selection_has_insufficient_context(selection: object) -> bool:
    regime_reason = getattr(selection, "regime_reason", "")
    return regime_reason in {"empty_universe", "insufficient_history"} or regime_reason.startswith(
        "benchmark_history_short"
    )


def run_shadow_portfolio_simulation(
    *,
    session: Session,
    settings: AppSettings,
    instruments: dict[str, BrokerInstrument],
    constraints_by_symbol: dict[str, ExecutionConstraints],
    broker: BrokerAdapter,
    prices_by_symbol: dict[str, float],
) -> None:
    if not settings.portfolio_shadow_enabled:
        return

    now = datetime.now(timezone.utc)
    positions = _parse_position_summary(
        get_state(session, "shadow.portfolio.positions", "") or ""
    )
    positions_before = _format_position_summary(positions)
    cash_rub = float(
        get_state(
            session,
            "shadow.portfolio.cash_rub",
            f"{settings.portfolio_shadow_initial_cash_rub:.2f}",
        )
        or settings.portfolio_shadow_initial_cash_rub
    )
    equity_rub = _mark_to_market_equity(
        cash_rub=cash_rub,
        positions=positions,
        prices_by_symbol=prices_by_symbol,
        instruments=instruments,
    )
    shadow_trades_count, shadow_rebalances_count = get_shadow_history_counts(session)
    last_executed_actions_raw = (
        get_state(session, "shadow.portfolio.last_executed_actions", "") or ""
    )
    if (shadow_trades_count == 0 or shadow_rebalances_count == 0) and last_executed_actions_raw:
        bootstrap_actions = [item for item in last_executed_actions_raw.split(",") if item]
        for item in bootstrap_actions:
            side, _, tail = item.partition(":")
            symbol, _, quantity_raw = tail.partition(":")
            if not side or not symbol or not quantity_raw:
                continue
            try:
                quantity = int(quantity_raw)
            except ValueError:
                continue
            price = prices_by_symbol.get(symbol, 0.0)
            lot = instruments.get(symbol).lot if instruments.get(symbol) is not None else 1
            if shadow_trades_count == 0 and price > 0:
                record_shadow_trade(
                    session,
                    strategy_name="portfolio_momentum_shadow",
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price,
                    notional_rub=quantity * price * lot,
                    status="filled",
                    reason="bootstrap_from_shadow_state",
                )
        if shadow_rebalances_count == 0:
            record_shadow_rebalance(
                session,
                strategy_name="portfolio_momentum_shadow",
                status="executed",
                regime_state=get_state(session, "shadow.portfolio.regime_state", "n/a") or "n/a",
                selected_symbols=get_state(session, "shadow.portfolio.selected_symbols", "") or "",
                target_weights=get_state(session, "shadow.portfolio.target_weights", "") or "",
                positions_before="",
                positions_after=positions_before,
                planned_actions=len(bootstrap_actions),
                executed_actions=len(bootstrap_actions),
                equity_rub=equity_rub,
                cash_rub=cash_rub,
                reason="bootstrap_from_shadow_state",
            )
        session.commit()

    set_state(session, "shadow.portfolio.cash_rub", f"{cash_rub:.2f}")
    set_state(session, "shadow.portfolio.equity_rub", f"{equity_rub:.2f}")

    last_rebalance_raw = get_state(session, "shadow.portfolio.last_rebalance_at")
    last_rebalance_at: datetime | None = None
    if last_rebalance_raw and last_rebalance_raw not in {"", "n/a"}:
        try:
            last_rebalance_at = datetime.fromisoformat(last_rebalance_raw)
        except ValueError:
            last_rebalance_at = None

    if last_rebalance_at is not None:
        next_rebalance_at = last_rebalance_at + timedelta(
            seconds=settings.portfolio_rebalance_cooldown_seconds
        )
        set_state(session, "shadow.portfolio.next_rebalance_at", next_rebalance_at.isoformat())
        if now < next_rebalance_at:
            set_state(session, "shadow.portfolio.positions", _format_position_summary(positions))
            session.commit()
            return

    symbols = list(instruments)
    selection, _ = _build_history_selection(
        settings=settings,
        symbols=symbols,
        broker=broker,
    )
    if _selection_has_insufficient_context(selection):
        waiting_fingerprint = f"{selection.regime_reason}|{positions_before}"
        last_waiting_fingerprint = (
            get_state(session, "shadow.portfolio.last_waiting_fingerprint", "") or ""
        )
        set_state(session, "shadow.portfolio.regime_state", "waiting")
        set_state(session, "shadow.portfolio.regime_reason", selection.regime_reason[:255])
        set_state(session, "shadow.portfolio.selected_symbols", "")
        set_state(session, "shadow.portfolio.target_weights", "")
        set_state(session, "shadow.portfolio.positions", positions_before)
        set_state(session, "shadow.portfolio.last_plan_actions", "0")
        set_state(session, "shadow.portfolio.last_executed_actions", "")
        if waiting_fingerprint != last_waiting_fingerprint:
            record_shadow_rebalance(
                session,
                strategy_name="portfolio_momentum_shadow",
                status="waiting",
                regime_state="waiting",
                selected_symbols="",
                target_weights="",
                positions_before=positions_before,
                positions_after=positions_before,
                planned_actions=0,
                executed_actions=0,
                equity_rub=equity_rub,
                cash_rub=cash_rub,
                reason=selection.regime_reason[:255],
            )
            set_state(session, "shadow.portfolio.last_waiting_fingerprint", waiting_fingerprint[:255])
        session.commit()
        return

    plan = build_rebalance_plan(
        prices_by_symbol=prices_by_symbol,
        current_positions=positions,
        cash_rub=cash_rub,
        instruments=instruments,
        constraints_by_symbol=constraints_by_symbol,
        target_weights=selection.target_weights,
        scores=selection.scores,
    )
    executed_actions: list[str] = []
    simulated_positions = dict(positions)
    simulated_cash_rub = cash_rub

    for action in plan.actions:
        lot_notional = action.quantity * action.price * instruments[action.symbol].lot
        if action.side == "buy":
            simulated_cash_rub -= lot_notional
            simulated_positions[action.symbol] = simulated_positions.get(action.symbol, 0) + action.quantity
        else:
            simulated_cash_rub += lot_notional
            simulated_positions[action.symbol] = simulated_positions.get(action.symbol, 0) - action.quantity
        executed_actions.append(f"{action.side}:{action.symbol}:{action.quantity}")
        record_shadow_trade(
            session,
            strategy_name="portfolio_momentum_shadow",
            symbol=action.symbol,
            side=action.side,
            quantity=action.quantity,
            price=action.price,
            notional_rub=lot_notional,
            status="filled",
            reason=(
                f"{action.reason}|target_weight:{action.target_weight:.3f}|"
                f"target_qty:{action.target_quantity}|score:{action.score:.2f}"
            )[:255],
        )

    shadow_equity_rub = _mark_to_market_equity(
        cash_rub=simulated_cash_rub,
        positions=simulated_positions,
        prices_by_symbol=prices_by_symbol,
        instruments=instruments,
    )
    positions_after = _format_position_summary(simulated_positions)
    rebalance_status = "executed"
    if not plan.actions and not selection.regime_on:
        rebalance_status = "regime_off"
    elif not plan.actions:
        rebalance_status = "hold"

    record_shadow_rebalance(
        session,
        strategy_name="portfolio_momentum_shadow",
        status=rebalance_status,
        regime_state="on" if selection.regime_on else "off",
        selected_symbols=",".join(selection.selected_symbols),
        target_weights=_format_weight_summary(selection.target_weights),
        positions_before=positions_before,
        positions_after=positions_after,
        planned_actions=len(plan.actions),
        executed_actions=len(executed_actions),
        equity_rub=shadow_equity_rub,
        cash_rub=simulated_cash_rub,
        reason=selection.regime_reason[:255],
    )
    set_state(session, "shadow.portfolio.regime_state", "on" if selection.regime_on else "off")
    set_state(session, "shadow.portfolio.regime_reason", selection.regime_reason[:255])
    set_state(session, "shadow.portfolio.selected_symbols", ",".join(selection.selected_symbols))
    set_state(session, "shadow.portfolio.target_weights", _format_weight_summary(selection.target_weights))
    set_state(session, "shadow.portfolio.positions", positions_after)
    set_state(session, "shadow.portfolio.last_plan_actions", str(len(plan.actions)))
    set_state(session, "shadow.portfolio.last_executed_actions", ",".join(executed_actions)[:255])
    set_state(session, "shadow.portfolio.cash_rub", f"{simulated_cash_rub:.2f}")
    set_state(session, "shadow.portfolio.equity_rub", f"{shadow_equity_rub:.2f}")
    set_state(session, "shadow.portfolio.last_rebalance_at", now.isoformat())
    set_state(session, "shadow.portfolio.last_waiting_fingerprint", "")
    set_state(
        session,
        "shadow.portfolio.next_rebalance_at",
        (now + timedelta(seconds=settings.portfolio_rebalance_cooldown_seconds)).isoformat(),
    )
    session.commit()


def run_portfolio_iteration(
    *,
    session: Session,
    settings: AppSettings,
    instruments: dict[str, BrokerInstrument],
    constraints_by_symbol: dict[str, ExecutionConstraints],
    risk_engine: RiskEngine,
    broker: BrokerAdapter,
    broker_mode: str,
    event_bus: EventBus,
    runtime_state: PortfolioRuntimeState,
    strategy_name: str = "portfolio_momentum_live",
    state_prefix: str = "portfolio",
    use_strategy_book: bool = False,
) -> dict[str, float]:
    paused = is_paused(session)
    symbols = list(instruments)
    try:
        prices_by_symbol = broker.get_prices(symbols)
    except Exception:
        logger.exception("Failed to fetch market prices")
        return {}

    for symbol, price in prices_by_symbol.items():
        upsert_instrument_price(session, symbol, price)

    now = datetime.now(timezone.utc)
    if runtime_state.last_rebalance_at is not None:
        next_rebalance_at = runtime_state.last_rebalance_at + timedelta(
            seconds=settings.portfolio_rebalance_cooldown_seconds
        )
        set_state(session, f"{state_prefix}.next_rebalance_at", next_rebalance_at.isoformat())
        if now < next_rebalance_at:
            session.commit()
            if isinstance(broker, TBankBrokerAdapter):
                try:
                    sync_positions(session, broker.get_position_snapshots())
                except Exception:
                    logger.exception("Failed to sync broker positions")
            return prices_by_symbol

    selection, _ = _build_history_selection(
        settings=settings,
        symbols=symbols,
        broker=broker,
    )
    insufficient_context = _selection_has_insufficient_context(selection)

    if use_strategy_book:
        current_positions = _parse_position_summary(
            get_state(session, f"{state_prefix}.positions", "") or ""
        )
        cash_rub = float(
            get_state(
                session,
                f"{state_prefix}.strategy_cash_rub",
                f"{settings.portfolio_live_initial_cash_rub:.2f}",
            )
            or settings.portfolio_live_initial_cash_rub
        )
    else:
        current_positions = {
            symbol: get_position_quantity(session, symbol)
            for symbol in symbols
        }
        cash_rub = broker.get_cash_balance_rub()
    if insufficient_context:
        waiting_fingerprint = f"{selection.regime_reason}|{','.join(sorted(current_positions))}"
        last_waiting_fingerprint = (
            get_state(session, f"{state_prefix}.last_waiting_fingerprint", "") or ""
        )
        if waiting_fingerprint != last_waiting_fingerprint:
            record_shadow_rebalance(
                session,
                strategy_name=strategy_name,
                status="waiting",
                regime_state="waiting",
                selected_symbols="",
                target_weights="",
                positions_before=",".join(f"{symbol}:{qty}" for symbol, qty in current_positions.items() if qty != 0),
                positions_after=",".join(f"{symbol}:{qty}" for symbol, qty in current_positions.items() if qty != 0),
                planned_actions=0,
                executed_actions=0,
                equity_rub=0.0,
                cash_rub=cash_rub,
                reason=selection.regime_reason[:255],
            )
            set_state(session, f"{state_prefix}.last_waiting_fingerprint", waiting_fingerprint[:255])
        set_state(session, f"{state_prefix}.regime_state", "waiting")
        set_state(session, f"{state_prefix}.regime_reason", selection.regime_reason[:255])
        set_state(session, f"{state_prefix}.selected_symbols", "")
        set_state(session, f"{state_prefix}.target_weights", "")
        current_equity = _mark_to_market_equity(
            cash_rub=cash_rub,
            positions=current_positions,
            prices_by_symbol=prices_by_symbol,
            instruments=instruments,
        )
        set_state(session, f"{state_prefix}.positions", ",".join(f"{symbol}:{qty}" for symbol, qty in current_positions.items() if qty != 0))
        set_state(session, f"{state_prefix}.last_equity_rub", f"{current_equity:.2f}")
        set_state(session, f"{state_prefix}.last_cash_rub", f"{cash_rub:.2f}")
        if use_strategy_book:
            set_state(session, f"{state_prefix}.strategy_cash_rub", f"{cash_rub:.2f}")
        set_state(session, f"{state_prefix}.last_plan_actions", "0")
        set_state(session, f"{state_prefix}.last_executed_actions", "")
        session.commit()
        try:
            sync_positions(session, broker.get_position_snapshots())
        except Exception:
            logger.exception("Failed to sync broker positions")
        return prices_by_symbol

    plan = build_rebalance_plan(
        prices_by_symbol=prices_by_symbol,
        current_positions=current_positions,
        cash_rub=cash_rub,
        instruments=instruments,
        constraints_by_symbol=constraints_by_symbol,
        target_weights=selection.target_weights,
        scores=selection.scores,
    )
    positions_before = ",".join(
        f"{symbol}:{quantity}"
        for symbol, quantity in sorted(current_positions.items())
        if quantity != 0
    )
    executed_actions: list[str] = []
    strategy_positions = dict(current_positions)
    strategy_cash_rub = cash_rub

    set_state(session, f"{state_prefix}.regime_state", "on" if selection.regime_on else "off")
    set_state(session, f"{state_prefix}.regime_reason", selection.regime_reason[:255])
    set_state(session, f"{state_prefix}.selected_symbols", ",".join(selection.selected_symbols))
    set_state(session, f"{state_prefix}.target_weights", _format_weight_summary(selection.target_weights))
    set_state(session, f"{state_prefix}.positions", positions_before)
    set_state(session, f"{state_prefix}.last_equity_rub", f"{plan.total_equity_rub:.2f}")
    set_state(session, f"{state_prefix}.last_cash_rub", f"{cash_rub:.2f}")
    set_state(session, f"{state_prefix}.last_plan_actions", str(len(plan.actions)))
    set_state(session, f"{state_prefix}.next_rebalance_at", (now + timedelta(seconds=settings.portfolio_rebalance_cooldown_seconds)).isoformat())

    for action in plan.actions:
        current_quantity = (
            strategy_positions.get(action.symbol, 0)
            if use_strategy_book
            else get_position_quantity(session, action.symbol)
        )
        constraints = constraints_by_symbol[action.symbol]
        decision = risk_engine.evaluate(
            paused=paused,
            symbol=action.symbol,
            side=action.side,
            quantity=action.quantity,
            current_position=current_quantity,
            price=action.price,
            lot=instruments[action.symbol].lot,
            constraints=constraints,
        )
        reason = (
            f"{action.reason}|target_weight:{action.target_weight:.3f}|"
            f"target_qty:{action.target_quantity}|score:{action.score:.2f}|risk:{decision.reason}"
        )
        signal_row = record_signal(
            session,
            strategy_name=strategy_name,
            symbol=action.symbol,
            side=action.side,
            price=action.price,
            confidence=max(action.score / 20.0, 1.0),
            reason=reason[:255],
            status="approved" if decision.approved else "rejected",
        )
        if not decision.approved:
            logger.info("Portfolio action rejected: %s %s %s", action.symbol, action.side, decision.reason)
            continue

        try:
            broker_order = broker.place_order(
                symbol=action.symbol,
                side=action.side,
                quantity=action.quantity,
                price=action.price,
            )
        except Exception as exc:
            signal_row.status = "broker_error"
            signal_row.reason = f"{signal_row.reason}|broker:{exc}"[:255]
            logger.exception("Broker order failed for %s %s", action.symbol, action.side)
            continue

        order_row = record_filled_order(
            session,
            signal_id=signal_row.id,
            symbol=action.symbol,
            side=action.side,
            quantity=action.quantity,
            price=action.price,
            broker_mode=broker_mode,
            broker_order_id=broker_order.broker_order_id,
            reason=reason[:255],
        )
        lot_notional = action.quantity * action.price * instruments[action.symbol].lot
        if use_strategy_book:
            if action.side == "buy":
                strategy_cash_rub -= lot_notional
                strategy_positions[action.symbol] = strategy_positions.get(action.symbol, 0) + action.quantity
            else:
                strategy_cash_rub += lot_notional
                strategy_positions[action.symbol] = strategy_positions.get(action.symbol, 0) - action.quantity
        executed_actions.append(f"{action.side}:{action.symbol}:{action.quantity}")
        record_shadow_trade(
            session,
            strategy_name=strategy_name,
            symbol=action.symbol,
            side=action.side,
            quantity=action.quantity,
            price=action.price,
            notional_rub=lot_notional,
            status="filled",
            reason=reason[:255],
        )
        event_bus.publish(
            "trader.events",
            {
                "type": "order_filled",
                "symbol": action.symbol,
                "side": action.side,
                "quantity": action.quantity,
                "price": action.price,
                "order_id": order_row.id,
            },
        )

    if use_strategy_book:
        positions_after = ",".join(
            f"{symbol}:{quantity}"
            for symbol, quantity in sorted(strategy_positions.items())
            if quantity != 0
        )
        portfolio_cash_rub = strategy_cash_rub
        portfolio_equity_rub = _mark_to_market_equity(
            cash_rub=strategy_cash_rub,
            positions=strategy_positions,
            prices_by_symbol=prices_by_symbol,
            instruments=instruments,
        )
    else:
        positions_after = ",".join(
            f"{symbol}:{quantity}"
            for symbol, quantity in sorted(
                {
                    symbol: get_position_quantity(session, symbol)
                    for symbol in symbols
                }.items()
            )
            if quantity != 0
        )
        portfolio_cash_rub = cash_rub
        portfolio_equity_rub = plan.total_equity_rub
    rebalance_status = "executed"
    if not plan.actions and not selection.regime_on:
        rebalance_status = "regime_off"
    elif not plan.actions:
        rebalance_status = "hold"
    record_shadow_rebalance(
        session,
        strategy_name=strategy_name,
        status=rebalance_status,
        regime_state="on" if selection.regime_on else "off",
        selected_symbols=",".join(selection.selected_symbols),
        target_weights=_format_weight_summary(selection.target_weights),
        positions_before=positions_before,
        positions_after=positions_after,
        planned_actions=len(plan.actions),
        executed_actions=len(executed_actions),
        equity_rub=portfolio_equity_rub,
        cash_rub=portfolio_cash_rub,
        reason=selection.regime_reason[:255],
    )
    runtime_state.last_rebalance_at = now
    set_state(session, f"{state_prefix}.positions", positions_after)
    set_state(session, f"{state_prefix}.last_rebalance_at", now.isoformat())
    set_state(session, f"{state_prefix}.last_equity_rub", f"{portfolio_equity_rub:.2f}")
    set_state(session, f"{state_prefix}.last_cash_rub", f"{portfolio_cash_rub:.2f}")
    if use_strategy_book:
        set_state(session, f"{state_prefix}.strategy_cash_rub", f"{portfolio_cash_rub:.2f}")
    set_state(session, f"{state_prefix}.last_executed_actions", ",".join(executed_actions)[:255])
    set_state(session, f"{state_prefix}.last_waiting_fingerprint", "")
    session.commit()
    try:
        sync_positions(session, broker.get_position_snapshots())
    except Exception:
        logger.exception("Failed to sync broker positions")
    return prices_by_symbol


def main() -> None:
    settings = get_settings()
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)
    event_bus = EventBus(settings.redis_url)
    risk_engine = RiskEngine(
        settings.max_position_per_symbol,
        max_position_notional_rub=settings.max_position_notional_rub,
        allow_short_positions=settings.broker_mode == "simulated",
    )
    broker = create_broker_adapter(settings)
    instruments = broker.get_instruments()
    constraints_by_symbol = build_constraints(settings, instruments)
    portfolio_runtime = PortfolioRuntimeState()

    with session_factory() as session:
        bootstrap_defaults(session, settings)
        start_new_run(session, broker_mode=settings.broker_mode)
        if isinstance(broker, TBankBrokerAdapter):
            sync_positions(session, broker.get_position_snapshots())
            set_state(session, "broker.account_id", broker.account_id)
            set_state(session, "broker.sandbox", str(settings.tbank_use_sandbox).lower())
            session.commit()

    logger.info(
        "Engine started in %s mode strategy=%s for symbols=%s",
        settings.broker_mode,
        "portfolio_momentum_live",
        settings.symbols,
    )
    iteration = 0
    while True:
        iteration += 1
        with session_factory() as session:
            update_heartbeat(
                session,
                broker_mode=settings.broker_mode,
                iteration=iteration,
            )
            run_portfolio_iteration(
                session=session,
                settings=settings,
                instruments=instruments,
                constraints_by_symbol=constraints_by_symbol,
                risk_engine=risk_engine,
                broker=broker,
                broker_mode=settings.broker_mode,
                event_bus=event_bus,
                runtime_state=portfolio_runtime,
                strategy_name="portfolio_momentum_live",
                state_prefix="portfolio",
                use_strategy_book=True,
            )
        time.sleep(settings.engine_poll_seconds)


if __name__ == "__main__":
    main()
