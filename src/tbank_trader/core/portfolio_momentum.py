from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from tbank_trader.broker.base import BrokerInstrument
from tbank_trader.core.execution import ExecutionConstraints


@dataclass(slots=True)
class PortfolioSelection:
    target_weights: dict[str, float]
    scores: dict[str, float]
    selected_symbols: list[str]
    regime_on: bool
    regime_reason: str


@dataclass(slots=True)
class RebalanceAction:
    symbol: str
    side: str
    quantity: int
    price: float
    current_quantity: int
    target_quantity: int
    target_weight: float
    score: float
    reason: str


@dataclass(slots=True)
class PortfolioRebalancePlan:
    actions: list[RebalanceAction]
    target_quantities: dict[str, int]
    total_equity_rub: float
    available_cash_rub: float
    estimated_notional_rub: float


def _percentile_scores(values_by_symbol: dict[str, float]) -> dict[str, float]:
    valid = {
        symbol: value
        for symbol, value in values_by_symbol.items()
        if value is not None
    }
    if not valid:
        return {}

    population = list(valid.values())
    total = len(population)
    return {
        symbol: sum(1 for candidate in population if candidate <= value) / total * 100.0
        for symbol, value in valid.items()
    }


def _normalize_weights(scores: dict[str, float]) -> dict[str, float]:
    total = sum(scores.values())
    if total <= 0:
        return {}
    return {
        symbol: score / total
        for symbol, score in scores.items()
    }


def compute_portfolio_selection(
    *,
    history_by_symbol: dict[str, list[float]],
    momentum_periods: list[int],
    top_percentile: int,
    min_positions: int,
    max_positions: int,
    regime_filter_enabled: bool,
    regime_symbol: str,
    regime_ma_window: int,
    regime_on_override: bool | None = None,
    regime_reason_override: str | None = None,
) -> PortfolioSelection:
    if not history_by_symbol:
        return PortfolioSelection({}, {}, [], True, "empty_universe")

    trailing_returns_by_period: dict[int, dict[str, float]] = {}
    for period in momentum_periods:
        trailing_returns: dict[str, float] = {}
        for symbol, closes in history_by_symbol.items():
            if len(closes) <= period:
                continue
            start_price = closes[-period - 1]
            end_price = closes[-1]
            if start_price <= 0:
                continue
            trailing_returns[symbol] = (end_price / start_price) - 1.0
        trailing_returns_by_period[period] = trailing_returns

    ranked_periods = [
        _percentile_scores(returns_by_symbol)
        for returns_by_symbol in trailing_returns_by_period.values()
        if returns_by_symbol
    ]
    if not ranked_periods:
        return PortfolioSelection({}, {}, [], True, "insufficient_history")

    aggregate_scores: dict[str, list[float]] = {}
    for ranked_period in ranked_periods:
        for symbol, score in ranked_period.items():
            aggregate_scores.setdefault(symbol, []).append(score)

    averaged_scores = {
        symbol: mean(values)
        for symbol, values in aggregate_scores.items()
        if values
    }
    final_scores = _percentile_scores(averaged_scores)
    ranked_symbols = sorted(final_scores.items(), key=lambda item: item[1], reverse=True)

    selected = [symbol for symbol, score in ranked_symbols if score >= top_percentile]
    if len(selected) < min_positions:
        selected = [symbol for symbol, _ in ranked_symbols[:min_positions]]
    if max_positions > 0:
        selected = selected[:max_positions]

    selected_scores = {
        symbol: final_scores[symbol]
        for symbol in selected
    }

    regime_on = True
    regime_reason = "disabled"
    if regime_filter_enabled:
        if regime_on_override is not None:
            regime_on = regime_on_override
            regime_reason = regime_reason_override or "override"
        else:
            benchmark_history = history_by_symbol.get(regime_symbol, [])
            if len(benchmark_history) < regime_ma_window:
                regime_on = False
                regime_reason = f"benchmark_history_short:{regime_symbol}"
            else:
                moving_average = sum(benchmark_history[-regime_ma_window:]) / regime_ma_window
                last_price = benchmark_history[-1]
                regime_on = last_price >= moving_average
                relation = "above" if regime_on else "below"
                regime_reason = (
                    f"benchmark_{relation}_ma:{regime_symbol}:{last_price:.4f}:{moving_average:.4f}"
                )

    target_weights = _normalize_weights(selected_scores) if regime_on else {}
    selected_symbols = list(target_weights) if regime_on else []

    return PortfolioSelection(
        target_weights=target_weights,
        scores=final_scores,
        selected_symbols=selected_symbols,
        regime_on=regime_on,
        regime_reason=regime_reason,
    )


def build_rebalance_plan(
    *,
    prices_by_symbol: dict[str, float],
    current_positions: dict[str, int],
    cash_rub: float,
    instruments: dict[str, BrokerInstrument],
    constraints_by_symbol: dict[str, ExecutionConstraints],
    target_weights: dict[str, float],
    scores: dict[str, float],
) -> PortfolioRebalancePlan:
    all_symbols = sorted(set(prices_by_symbol) | set(current_positions) | set(target_weights))
    lot_values = {
        symbol: prices_by_symbol[symbol] * instruments[symbol].lot
        for symbol in all_symbols
        if symbol in prices_by_symbol and symbol in instruments and prices_by_symbol[symbol] > 0
    }
    total_equity_rub = cash_rub + sum(
        current_positions.get(symbol, 0) * lot_values.get(symbol, 0.0)
        for symbol in all_symbols
    )

    target_quantities: dict[str, int] = {}
    for symbol in all_symbols:
        lot_notional = lot_values.get(symbol)
        instrument = instruments.get(symbol)
        constraints = constraints_by_symbol.get(symbol)
        if not lot_notional or instrument is None or constraints is None:
            target_quantities[symbol] = current_positions.get(symbol, 0)
            continue

        target_weight = target_weights.get(symbol, 0.0)
        raw_target = int((target_weight * total_equity_rub) // lot_notional) if total_equity_rub > 0 else 0
        quantity = max(0, raw_target)
        if constraints.max_position_per_symbol > 0:
            quantity = min(quantity, constraints.max_position_per_symbol)
        if constraints.max_position_notional_rub > 0:
            quantity = min(quantity, int(constraints.max_position_notional_rub // lot_notional))
        target_quantities[symbol] = max(0, quantity)

    actions: list[RebalanceAction] = []
    available_cash_rub = cash_rub

    sell_symbols = [
        symbol
        for symbol in all_symbols
        if current_positions.get(symbol, 0) > target_quantities.get(symbol, 0)
    ]
    for symbol in sell_symbols:
        current_quantity = current_positions.get(symbol, 0)
        target_quantity = target_quantities.get(symbol, 0)
        quantity = current_quantity - target_quantity
        if quantity <= 0:
            continue
        lot_notional = lot_values.get(symbol, 0.0)
        available_cash_rub += quantity * lot_notional
        actions.append(
            RebalanceAction(
                symbol=symbol,
                side="sell",
                quantity=quantity,
                price=prices_by_symbol[symbol],
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                target_weight=target_weights.get(symbol, 0.0),
                score=scores.get(symbol, 0.0),
                reason="rebalance_sell",
            )
        )

    buy_candidates = sorted(
        [
            symbol
            for symbol in all_symbols
            if target_quantities.get(symbol, 0) > current_positions.get(symbol, 0)
        ],
        key=lambda symbol: (target_weights.get(symbol, 0.0), scores.get(symbol, 0.0)),
        reverse=True,
    )
    for symbol in buy_candidates:
        current_quantity = current_positions.get(symbol, 0)
        target_quantity = target_quantities.get(symbol, 0)
        desired_quantity = target_quantity - current_quantity
        if desired_quantity <= 0:
            continue

        constraints = constraints_by_symbol[symbol]
        lot_notional = lot_values.get(symbol, 0.0)
        if lot_notional <= 0:
            continue

        quantity = desired_quantity
        if constraints.max_order_lots > 0:
            quantity = min(quantity, constraints.max_order_lots)
        if constraints.max_order_notional_rub > 0:
            quantity = min(quantity, int(constraints.max_order_notional_rub // lot_notional))
        quantity = min(quantity, int(available_cash_rub // lot_notional))
        if quantity <= 0:
            continue

        available_cash_rub -= quantity * lot_notional
        actions.append(
            RebalanceAction(
                symbol=symbol,
                side="buy",
                quantity=quantity,
                price=prices_by_symbol[symbol],
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                target_weight=target_weights.get(symbol, 0.0),
                score=scores.get(symbol, 0.0),
                reason="rebalance_buy",
            )
        )

    estimated_notional_rub = sum(
        action.quantity * prices_by_symbol[action.symbol] * instruments[action.symbol].lot
        for action in actions
    )
    return PortfolioRebalancePlan(
        actions=actions,
        target_quantities=target_quantities,
        total_equity_rub=total_equity_rub,
        available_cash_rub=available_cash_rub,
        estimated_notional_rub=estimated_notional_rub,
    )
