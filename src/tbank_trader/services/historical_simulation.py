from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from tbank_trader.broker.base import BrokerInstrument
from tbank_trader.config import AppSettings
from tbank_trader.core.execution import ExecutionConstraints, build_execution_constraints
from tbank_trader.core.portfolio_momentum import (
    PortfolioSelection,
    build_rebalance_plan,
    compute_portfolio_selection,
)
from tbank_trader.services.benchmark_regime import RegimeSnapshot, load_spx_vix_regime_history
from tbank_trader.storage.repository import (
    get_eligible_instruments_with_history,
    load_historical_candles_for_instruments,
    record_historical_simulation_rebalance,
    record_historical_simulation_run,
)


@dataclass(slots=True)
class HistoricalSimulationSummary:
    run_id: int
    instruments_considered: int
    instruments_with_history: int
    rebalance_points: int
    completed_rebalances: int
    executed_actions: int
    turnover_rub: float
    initial_cash_rub: float
    final_cash_rub: float
    final_equity_rub: float
    total_return_pct: float
    max_drawdown_pct: float
    status: str
    note: str


@dataclass(slots=True)
class DailyPriceBar:
    timestamp: datetime
    open_price: float
    close_price: float


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


def _selection_has_insufficient_context(selection: PortfolioSelection) -> bool:
    return selection.regime_reason in {"empty_universe", "insufficient_history"} or selection.regime_reason.startswith(
        "benchmark_history_short"
    )


def _symbol_key(
    *,
    ticker: str,
    class_code: str,
    instrument_uid: str,
    seen: set[str],
) -> str:
    base = ticker.strip() or instrument_uid[:8]
    if base not in seen:
        seen.add(base)
        return base

    with_class_code = f"{base}@{class_code or instrument_uid[:8]}"
    if with_class_code not in seen:
        seen.add(with_class_code)
        return with_class_code

    with_uid = f"{base}@{instrument_uid[:8]}"
    seen.add(with_uid)
    return with_uid


def _max_drawdown_pct(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0

    peak = equity_curve[0]
    max_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak <= 0:
            continue
        drawdown = (equity / peak) - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown * 100.0


def _build_daily_price_bars(
    intraday_series_by_symbol: dict[str, list[tuple[datetime, float, float]]],
) -> dict[str, list[DailyPriceBar]]:
    daily_series_by_symbol: dict[str, list[DailyPriceBar]] = {}
    for symbol, points in intraday_series_by_symbol.items():
        by_day: dict[datetime.date, list[tuple[datetime, float, float]]] = defaultdict(list)
        for candle_time, open_price, close_price in points:
            by_day[candle_time.date()].append((candle_time, open_price, close_price))

        daily_points: list[DailyPriceBar] = []
        for day in sorted(by_day):
            ordered = sorted(by_day[day], key=lambda item: item[0])
            first_candle_time, first_open, first_close = ordered[0]
            last_candle_time, _last_open, last_close = ordered[-1]
            daily_open = first_open if first_open > 0 else first_close
            daily_points.append(
                DailyPriceBar(
                    timestamp=last_candle_time,
                    open_price=daily_open,
                    close_price=last_close,
                )
            )
        if daily_points:
            daily_series_by_symbol[symbol] = daily_points
    return daily_series_by_symbol


def _generate_rebalance_dates(
    timestamps: list[datetime],
    *,
    frequency: str,
    min_required_history: int,
) -> list[datetime]:
    if len(timestamps) < min_required_history:
        return []

    eligible_timestamps = timestamps[min_required_history - 1 :]
    groups: dict[tuple[int, int], list[datetime]] = defaultdict(list)
    if frequency == "M":
        for timestamp in eligible_timestamps:
            groups[(timestamp.year, timestamp.month)].append(timestamp)
    else:
        for timestamp in eligible_timestamps:
            iso = timestamp.isocalendar()
            groups[(iso.year, iso.week)].append(timestamp)

    rebalance_dates: list[datetime] = []
    for group_key in sorted(groups):
        rebalance_dates.append(sorted(groups[group_key])[0])
    return rebalance_dates


def _resolve_external_regime_snapshot(
    *,
    rebalance_time: datetime,
    regime_dates: list[datetime.date],
    regime_history: dict[datetime.date, RegimeSnapshot],
) -> RegimeSnapshot | None:
    rebalance_date = rebalance_time.date()
    history_index = bisect_right(regime_dates, rebalance_date) - 1
    if history_index < 0:
        return None
    resolved_date = regime_dates[history_index]
    return regime_history.get(resolved_date)


def run_historical_portfolio_simulation(
    session: Session,
    *,
    settings: AppSettings,
    instrument_limit: int | None = None,
    strategy_name: str = "portfolio_momentum_backtest",
) -> HistoricalSimulationSummary:
    instrument_rows = get_eligible_instruments_with_history(
        session,
        interval=settings.strategy_candle_interval,
        limit=instrument_limit,
    )
    initial_cash_rub = settings.portfolio_live_initial_cash_rub

    if not instrument_rows:
        run = record_historical_simulation_run(
            session,
            strategy_name=strategy_name,
            interval=settings.strategy_candle_interval,
            status="failed",
            instruments_considered=0,
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            latest_selected_symbols="",
            latest_target_weights="",
            note="no_eligible_instruments_with_history",
        )
        session.commit()
        return HistoricalSimulationSummary(
            run_id=run.id,
            instruments_considered=0,
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            status="failed",
            note="no_eligible_instruments_with_history",
        )

    seen_symbols: set[str] = set()
    uid_to_symbol: dict[str, str] = {}
    symbol_to_ticker: dict[str, str] = {}
    instruments: dict[str, BrokerInstrument] = {}
    constraints_by_symbol: dict[str, ExecutionConstraints] = {}
    for row in instrument_rows:
        symbol = _symbol_key(
            ticker=str(row["ticker"]),
            class_code=str(row["class_code"]),
            instrument_uid=str(row["instrument_uid"]),
            seen=seen_symbols,
        )
        uid_to_symbol[str(row["instrument_uid"])] = symbol
        symbol_to_ticker[symbol] = str(row["ticker"])
        instrument = BrokerInstrument(
            symbol=symbol,
            lot=max(int(row["lot"]), 1),
            instrument_type=str(row["instrument_type"]),
            class_code=str(row["class_code"]),
            instrument_uid=str(row["instrument_uid"]),
            figi=str(row["figi"]),
        )
        instruments[symbol] = instrument
        constraints_by_symbol[symbol] = build_execution_constraints(
            settings=settings,
            instrument=instrument,
            broker_mode="simulated",
        )

    candle_rows = load_historical_candles_for_instruments(
        session,
        instrument_uids=list(uid_to_symbol),
        interval=settings.strategy_candle_interval,
    )
    if not candle_rows:
        run = record_historical_simulation_run(
            session,
            strategy_name=strategy_name,
            interval=settings.strategy_candle_interval,
            status="failed",
            instruments_considered=len(instrument_rows),
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            latest_selected_symbols="",
            latest_target_weights="",
            note="no_historical_candles_loaded",
        )
        session.commit()
        return HistoricalSimulationSummary(
            run_id=run.id,
            instruments_considered=len(instrument_rows),
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            status="failed",
            note="no_historical_candles_loaded",
        )

    intraday_series_by_symbol: dict[str, list[tuple[datetime, float, float]]] = {}
    for row in candle_rows:
        if not bool(row["is_complete"]):
            continue
        symbol = uid_to_symbol.get(str(row["instrument_uid"]))
        if symbol is None:
            continue
        intraday_series_by_symbol.setdefault(symbol, []).append(
            (
                row["candle_time"],
                float(row["open_price"]),
                float(row["close_price"]),
            )
        )

    daily_bars_by_symbol = _build_daily_price_bars(intraday_series_by_symbol)

    longest_momentum = max(settings.portfolio_momentum_periods) + 1
    min_required_history = max(longest_momentum, settings.portfolio_regime_ma_window)
    min_required_timeline_points = min_required_history + 1
    daily_bars_by_symbol = {
        symbol: points
        for symbol, points in daily_bars_by_symbol.items()
        if len(points) >= min_required_timeline_points
    }
    if not daily_bars_by_symbol:
        run = record_historical_simulation_run(
            session,
            strategy_name=strategy_name,
            interval=settings.strategy_candle_interval,
            status="failed",
            instruments_considered=len(instrument_rows),
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            latest_selected_symbols="",
            latest_target_weights="",
            note="insufficient_history_for_simulation",
        )
        session.commit()
        return HistoricalSimulationSummary(
            run_id=run.id,
            instruments_considered=len(instrument_rows),
            instruments_with_history=0,
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            status="failed",
            note="insufficient_history_for_simulation",
        )

    filtered_instruments = {
        symbol: instrument
        for symbol, instrument in instruments.items()
        if symbol in daily_bars_by_symbol
    }
    filtered_constraints = {
        symbol: constraints
        for symbol, constraints in constraints_by_symbol.items()
        if symbol in daily_bars_by_symbol
    }
    timestamps = sorted({point.timestamp for points in daily_bars_by_symbol.values() for point in points})

    if len(timestamps) < min_required_timeline_points:
        run = record_historical_simulation_run(
            session,
            strategy_name=strategy_name,
            interval=settings.strategy_candle_interval,
            status="failed",
            instruments_considered=len(instrument_rows),
            instruments_with_history=len(daily_bars_by_symbol),
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            latest_selected_symbols="",
            latest_target_weights="",
            note="timeline_too_short",
        )
        session.commit()
        return HistoricalSimulationSummary(
            run_id=run.id,
            instruments_considered=len(instrument_rows),
            instruments_with_history=len(daily_bars_by_symbol),
            rebalance_points=0,
            completed_rebalances=0,
            executed_actions=0,
            turnover_rub=0.0,
            initial_cash_rub=initial_cash_rub,
            final_cash_rub=initial_cash_rub,
            final_equity_rub=initial_cash_rub,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            status="failed",
            note="timeline_too_short",
        )

    ranking_time_index_by_symbol = {
        symbol: [point.timestamp for point in points]
        for symbol, points in daily_bars_by_symbol.items()
    }
    ranking_close_index_by_symbol = {
        symbol: [point.close_price for point in points]
        for symbol, points in daily_bars_by_symbol.items()
    }
    execution_time_index_by_symbol = {
        symbol: [point.timestamp for point in points]
        for symbol, points in daily_bars_by_symbol.items()
    }
    execution_open_index_by_symbol = {
        symbol: [point.open_price for point in points]
        for symbol, points in daily_bars_by_symbol.items()
    }
    mark_close_index_by_symbol = {
        symbol: [point.close_price for point in points]
        for symbol, points in daily_bars_by_symbol.items()
    }

    preferred_regime_symbol = settings.portfolio_regime_symbol
    regime_symbol = next(
        (
            symbol
            for symbol, ticker in symbol_to_ticker.items()
            if symbol in daily_bars_by_symbol and ticker == preferred_regime_symbol
        ),
        sorted(daily_bars_by_symbol)[0],
    )

    rebalance_times = _generate_rebalance_dates(
        timestamps,
        frequency=settings.portfolio_rebalance_frequency,
        min_required_history=min_required_timeline_points,
    )
    effective_regime_mode = (
        settings.historical_regime_mode
        if settings.portfolio_regime_filter_enabled
        else "disabled"
    )
    external_regime_history: dict[datetime.date, RegimeSnapshot] = {}
    external_regime_dates: list[datetime.date] = []
    use_external_regime = (
        settings.portfolio_regime_filter_enabled
        and settings.historical_regime_mode == "spx_vix"
        and bool(rebalance_times)
    )
    if use_external_regime:
        try:
            external_regime_history = load_spx_vix_regime_history(
                start_at=rebalance_times[0],
                end_at=rebalance_times[-1],
                spx_ticker=settings.historical_regime_spx_ticker,
                vix_ticker=settings.historical_regime_vix_ticker,
                spx_ma_window=settings.portfolio_regime_ma_window,
                vix_threshold=settings.historical_regime_vix_threshold,
            )
            external_regime_dates = sorted(external_regime_history)
        except Exception:
            use_external_regime = False
            effective_regime_mode = "local_ma_fallback_after_spx_vix_load_failure"
        else:
            if not external_regime_dates:
                use_external_regime = False
                effective_regime_mode = "local_ma_fallback_after_empty_spx_vix_history"

    cash_rub = initial_cash_rub
    positions: dict[str, int] = {}
    turnover_rub = 0.0
    executed_actions_total = 0
    equity_curve: list[float] = [initial_cash_rub]
    rebalance_rows: list[dict[str, object]] = []
    latest_selected_symbols = ""
    latest_target_weights = ""

    for rebalance_time in rebalance_times:
        history_by_symbol: dict[str, list[float]] = {}
        execution_prices_by_symbol: dict[str, float] = {}
        mark_prices_by_symbol: dict[str, float] = {}

        for symbol, time_points in ranking_time_index_by_symbol.items():
            history_end = bisect_right(time_points, rebalance_time) - 1
            if history_end < min_required_history:
                continue
            closes = ranking_close_index_by_symbol[symbol][:history_end]
            history_by_symbol[symbol] = closes

            execution_time_points = execution_time_index_by_symbol[symbol]
            execution_idx = bisect_right(execution_time_points, rebalance_time) - 1
            if execution_idx < 0:
                continue
            if execution_time_points[execution_idx].date() != rebalance_time.date():
                continue
            execution_prices_by_symbol[symbol] = execution_open_index_by_symbol[symbol][execution_idx]
            mark_prices_by_symbol[symbol] = mark_close_index_by_symbol[symbol][execution_idx]

        positions_before = _format_position_summary(positions)
        if not history_by_symbol:
            equity = _mark_to_market_equity(
                cash_rub=cash_rub,
                positions=positions,
                prices_by_symbol=mark_prices_by_symbol,
                instruments=filtered_instruments,
            )
            equity_curve.append(equity)
            rebalance_rows.append(
                {
                    "rebalance_time": rebalance_time,
                    "status": "waiting",
                    "regime_state": "waiting",
                    "selected_symbols": "",
                    "target_weights": "",
                    "positions_before": positions_before,
                    "positions_after": positions_before,
                    "planned_actions": 0,
                    "executed_actions": 0,
                    "turnover_rub": 0.0,
                    "equity_rub": equity,
                    "cash_rub": cash_rub,
                    "reason": "selection_history_short_or_missing",
                }
            )
            continue

        if not use_external_regime and regime_symbol not in history_by_symbol:
            equity = _mark_to_market_equity(
                cash_rub=cash_rub,
                positions=positions,
                prices_by_symbol=mark_prices_by_symbol,
                instruments=filtered_instruments,
            )
            equity_curve.append(equity)
            rebalance_rows.append(
                {
                    "rebalance_time": rebalance_time,
                    "status": "waiting",
                    "regime_state": "waiting",
                    "selected_symbols": "",
                    "target_weights": "",
                    "positions_before": positions_before,
                    "positions_after": positions_before,
                    "planned_actions": 0,
                    "executed_actions": 0,
                    "turnover_rub": 0.0,
                    "equity_rub": equity,
                    "cash_rub": cash_rub,
                    "reason": "benchmark_history_short_or_missing",
                }
            )
            continue

        regime_on_override: bool | None = None
        regime_reason_override: str | None = None
        if use_external_regime:
            external_regime = _resolve_external_regime_snapshot(
                rebalance_time=rebalance_time,
                regime_dates=external_regime_dates,
                regime_history=external_regime_history,
            )
            if external_regime is None:
                equity = _mark_to_market_equity(
                    cash_rub=cash_rub,
                    positions=positions,
                    prices_by_symbol=mark_prices_by_symbol,
                    instruments=filtered_instruments,
                )
                equity_curve.append(equity)
                rebalance_rows.append(
                    {
                        "rebalance_time": rebalance_time,
                        "status": "waiting",
                        "regime_state": "waiting",
                        "selected_symbols": "",
                        "target_weights": "",
                        "positions_before": positions_before,
                        "positions_after": positions_before,
                        "planned_actions": 0,
                        "executed_actions": 0,
                        "turnover_rub": 0.0,
                        "equity_rub": equity,
                        "cash_rub": cash_rub,
                        "reason": "external_regime_history_short_or_missing",
                    }
                )
                continue
            regime_on_override = external_regime.is_on
            regime_reason_override = external_regime.reason

        selection = compute_portfolio_selection(
            history_by_symbol=history_by_symbol,
            momentum_periods=settings.portfolio_momentum_periods,
            top_percentile=settings.portfolio_top_percentile,
            min_positions=settings.portfolio_min_positions,
            max_positions=settings.portfolio_max_positions,
            regime_filter_enabled=settings.portfolio_regime_filter_enabled,
            regime_symbol=regime_symbol,
            regime_ma_window=settings.portfolio_regime_ma_window,
            regime_on_override=regime_on_override,
            regime_reason_override=regime_reason_override,
        )

        if _selection_has_insufficient_context(selection):
            equity = _mark_to_market_equity(
                cash_rub=cash_rub,
                positions=positions,
                prices_by_symbol=mark_prices_by_symbol,
                instruments=filtered_instruments,
            )
            equity_curve.append(equity)
            rebalance_rows.append(
                {
                    "rebalance_time": rebalance_time,
                    "status": "waiting",
                    "regime_state": "waiting",
                    "selected_symbols": "",
                    "target_weights": "",
                    "positions_before": positions_before,
                    "positions_after": positions_before,
                    "planned_actions": 0,
                    "executed_actions": 0,
                    "turnover_rub": 0.0,
                    "equity_rub": equity,
                    "cash_rub": cash_rub,
                    "reason": selection.regime_reason[:255],
                }
            )
            continue

        plan = build_rebalance_plan(
            prices_by_symbol=execution_prices_by_symbol,
            current_positions=positions,
            cash_rub=cash_rub,
            instruments=filtered_instruments,
            constraints_by_symbol=filtered_constraints,
            target_weights=selection.target_weights,
            scores=selection.scores,
        )

        turnover_this_rebalance = 0.0
        executed_actions = 0
        for action in plan.actions:
            instrument = filtered_instruments[action.symbol]
            notional = action.quantity * action.price * instrument.lot
            turnover_this_rebalance += notional
            if action.side == "buy":
                cash_rub -= notional
                positions[action.symbol] = positions.get(action.symbol, 0) + action.quantity
            else:
                cash_rub += notional
                positions[action.symbol] = positions.get(action.symbol, 0) - action.quantity
            executed_actions += 1

        turnover_rub += turnover_this_rebalance
        executed_actions_total += executed_actions
        latest_selected_symbols = ",".join(selection.selected_symbols)
        latest_target_weights = _format_weight_summary(selection.target_weights)
        positions_after = _format_position_summary(positions)
        equity = _mark_to_market_equity(
            cash_rub=cash_rub,
            positions=positions,
            prices_by_symbol=mark_prices_by_symbol,
            instruments=filtered_instruments,
        )
        equity_curve.append(equity)

        rebalance_status = "executed"
        if not plan.actions and not selection.regime_on:
            rebalance_status = "regime_off"
        elif not plan.actions:
            rebalance_status = "hold"

        rebalance_rows.append(
            {
                "rebalance_time": rebalance_time,
                "status": rebalance_status,
                "regime_state": "on" if selection.regime_on else "off",
                "selected_symbols": latest_selected_symbols,
                "target_weights": latest_target_weights,
                "positions_before": positions_before,
                "positions_after": positions_after,
                "planned_actions": len(plan.actions),
                "executed_actions": executed_actions,
                "turnover_rub": turnover_this_rebalance,
                "equity_rub": equity,
                "cash_rub": cash_rub,
                "reason": selection.regime_reason[:255],
            }
        )

    final_prices = {
        symbol: close_points[-1]
        for symbol, close_points in mark_close_index_by_symbol.items()
    }
    final_equity_rub = _mark_to_market_equity(
        cash_rub=cash_rub,
        positions=positions,
        prices_by_symbol=final_prices,
        instruments=filtered_instruments,
    )
    if not rebalance_rows:
        equity_curve.append(final_equity_rub)

    total_return_pct = ((final_equity_rub / initial_cash_rub) - 1.0) * 100.0 if initial_cash_rub > 0 else 0.0
    max_drawdown_pct = _max_drawdown_pct(equity_curve)
    completed_rebalances = len(
        [row for row in rebalance_rows if row["status"] != "waiting"]
    )
    note_prefix = "ok" if rebalance_rows else "no_rebalance_points_executed"
    note = (
        f"{note_prefix}:regime_mode={effective_regime_mode}:"
        "price_roles=ranking_daily_close_prev_day_execution_daily_open_mark_daily_close"
    )
    status = "completed" if rebalance_rows else "failed"

    run = record_historical_simulation_run(
        session,
        strategy_name=strategy_name,
        interval=settings.strategy_candle_interval,
        status=status,
        instruments_considered=len(instrument_rows),
        instruments_with_history=len(daily_bars_by_symbol),
        rebalance_points=len(rebalance_rows),
        completed_rebalances=completed_rebalances,
        executed_actions=executed_actions_total,
        turnover_rub=turnover_rub,
        initial_cash_rub=initial_cash_rub,
        final_cash_rub=cash_rub,
        final_equity_rub=final_equity_rub,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        latest_selected_symbols=latest_selected_symbols,
        latest_target_weights=latest_target_weights,
        note=note,
    )
    for row in rebalance_rows:
        record_historical_simulation_rebalance(
            session,
            run_id=run.id,
            rebalance_time=row["rebalance_time"],
            status=str(row["status"]),
            regime_state=str(row["regime_state"]),
            selected_symbols=str(row["selected_symbols"]),
            target_weights=str(row["target_weights"]),
            positions_before=str(row["positions_before"]),
            positions_after=str(row["positions_after"]),
            planned_actions=int(row["planned_actions"]),
            executed_actions=int(row["executed_actions"]),
            turnover_rub=float(row["turnover_rub"]),
            equity_rub=float(row["equity_rub"]),
            cash_rub=float(row["cash_rub"]),
            reason=str(row["reason"]),
        )
    session.commit()

    return HistoricalSimulationSummary(
        run_id=run.id,
        instruments_considered=len(instrument_rows),
        instruments_with_history=len(daily_bars_by_symbol),
        rebalance_points=len(rebalance_rows),
        completed_rebalances=completed_rebalances,
        executed_actions=executed_actions_total,
        turnover_rub=turnover_rub,
        initial_cash_rub=initial_cash_rub,
        final_cash_rub=cash_rub,
        final_equity_rub=final_equity_rub,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        status=status,
        note=note,
    )


def main() -> None:
    from tbank_trader.config import get_settings
    from tbank_trader.storage.db import build_engine, build_session_factory, init_database

    settings = get_settings()
    engine = build_engine(settings.database_url)
    init_database(engine)
    session_factory = build_session_factory(settings.database_url)

    with session_factory() as session:
        summary = run_historical_portfolio_simulation(
            session,
            settings=settings,
        )

    print(
        f"Historical simulation finished: run_id={summary.run_id} status={summary.status} "
        f"instruments={summary.instruments_with_history}/{summary.instruments_considered} "
        f"rebalances={summary.completed_rebalances}/{summary.rebalance_points} "
        f"actions={summary.executed_actions} return_pct={summary.total_return_pct:.2f} "
        f"max_dd_pct={summary.max_drawdown_pct:.2f} note={summary.note}"
    )


if __name__ == "__main__":
    main()
