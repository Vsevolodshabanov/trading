"""Execution-based backtester with transaction costs and integer share constraints."""

from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
import numpy as np


def _normalize_lot_sizes(lot_size, tickers) -> pd.Series:
    """Return per-ticker lot sizes as a positive integer Series."""
    if isinstance(lot_size, pd.Series):
        lot_sizes = lot_size.copy()
    elif isinstance(lot_size, dict):
        lot_sizes = pd.Series(lot_size)
    else:
        lot_sizes = pd.Series(int(lot_size), index=pd.Index(tickers))

    lot_sizes = pd.to_numeric(lot_sizes, errors='coerce')
    lot_sizes = lot_sizes.reindex(pd.Index(tickers)).fillna(1).astype(int)
    lot_sizes = lot_sizes.clip(lower=1)
    return lot_sizes


def _prepare_price_frame(prices_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a clean, sorted price frame with numeric values."""
    frame = prices_df.copy()
    frame.index = pd.to_datetime(frame.index)
    frame = frame.sort_index()
    frame = frame[~frame.index.duplicated(keep="last")]
    for column in frame.columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _update_last_prices(last_prices: pd.Series, prices_today_raw: pd.Series) -> pd.Series:
    """Carry forward the last known price for mark-to-market purposes."""
    if last_prices is None:
        last_prices = pd.Series(np.nan, index=prices_today_raw.index, dtype=float)
    prices_today_raw = pd.to_numeric(prices_today_raw, errors="coerce")
    last_prices = last_prices.reindex(last_prices.index.union(prices_today_raw.index), fill_value=np.nan)
    available = prices_today_raw.dropna()
    if not available.empty:
        last_prices.loc[available.index] = available
    return last_prices


def _split_trade_and_mark_prices(
    prices_today_raw: pd.Series,
    last_prices: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return current tradable prices, updated last prices, and valuation prices."""
    prices_today_raw = pd.to_numeric(prices_today_raw, errors="coerce")
    updated_last_prices = _update_last_prices(last_prices, prices_today_raw)
    trade_prices = prices_today_raw.dropna()
    valuation_prices = updated_last_prices.copy()
    return trade_prices, updated_last_prices, valuation_prices


@dataclass
class PortfolioState:
    """Represents portfolio state with cash and integer share holdings.
    
    Attributes
    ----------
    cash : float
        Cash balance (USD)
    shares : pd.Series
        Integer shares held for each ticker (index=ticker, values=int shares)
    """
    cash: float
    shares: pd.Series
    
    def __post_init__(self):
        """Ensure shares are integers."""
        if not isinstance(self.shares, pd.Series):
            self.shares = pd.Series(self.shares, dtype=int)
        else:
            self.shares = self.shares.astype(int)
    
    def value(self, prices: pd.Series) -> float:
        """Calculate total portfolio value (cash + holdings).
        
        Parameters
        ----------
        prices : pd.Series
            Current prices for tickers
            
        Returns
        -------
        float
            Total portfolio equity
        """
        holdings_value = self.position_values(prices).sum()
        return self.cash + holdings_value
    
    def position_values(self, prices: pd.Series) -> pd.Series:
        """Calculate value of each position.
        
        Parameters
        ----------
        prices : pd.Series
            Current prices for tickers
            
        Returns
        -------
        pd.Series
            Value in USD for each ticker position
        """
        # Align tickers between shares and prices
        aligned_shares = self.shares.reindex(prices.index, fill_value=0)
        return aligned_shares * prices
    
    def weights(self, prices: pd.Series) -> pd.Series:
        """Calculate portfolio weights including cash.
        
        Parameters
        ----------
        prices : pd.Series
            Current prices for tickers
            
        Returns
        -------
        pd.Series
            Portfolio weights (sum = 1.0, includes 'CASH' entry)
        """
        equity = self.value(prices)
        if equity == 0:
            # Avoid division by zero
            return pd.Series({'CASH': 1.0})
        
        position_vals = self.position_values(prices)
        weights = position_vals / equity
        
        # Add cash weight
        cash_weight = self.cash / equity
        weights = pd.concat([weights, pd.Series({'CASH': cash_weight})])
        
        return weights


class ExecutionModel:
    """Transaction cost model.
    
    Cost = TC_FIXED + TC_PCT * notional
    
    Parameters
    ----------
    tc_fixed : float
        Fixed cost per order (USD). Must be non-negative.
    tc_pct : float
        Proportional cost as fraction (e.g., 0.0005 = 5 bps). Must be non-negative.
    
    Raises
    ------
    ValueError
        If tc_fixed or tc_pct are negative, NaN, or infinite.
    """
    
    def __init__(self, tc_fixed: float, tc_pct: float):
        # Validate inputs
        if not isinstance(tc_fixed, (int, float)) or np.isnan(tc_fixed) or np.isinf(tc_fixed):
            raise ValueError(f"tc_fixed must be a finite number, got {tc_fixed}")
        if tc_fixed < 0:
            raise ValueError(f"tc_fixed must be non-negative, got {tc_fixed}")
        
        if not isinstance(tc_pct, (int, float)) or np.isnan(tc_pct) or np.isinf(tc_pct):
            raise ValueError(f"tc_pct must be a finite number, got {tc_pct}")
        if tc_pct < 0:
            raise ValueError(f"tc_pct must be non-negative, got {tc_pct}")
        
        self.tc_fixed = tc_fixed
        self.tc_pct = tc_pct
    
    def cost(self, notional: float) -> float:
        """Calculate transaction cost for given notional.
        
        Parameters
        ----------
        notional : float
            Trade notional value (USD)
            
        Returns
        -------
        float
            Transaction cost (USD)
        """
        return self.tc_fixed + self.tc_pct * notional


def rebalance_portfolio_plan_a(
    state: PortfolioState,
    target_weights: pd.Series,
    prices: pd.Series,
    valuation_prices: pd.Series | None,
    exec_model: ExecutionModel,
    lot_size,
    date: pd.Timestamp
) -> tuple[PortfolioState, list[dict]]:
    """Rebalance portfolio using Plan A: sell first, then buy with cash constraint.
    
    Plan A Strategy:
    1. Execute all SELLS first (frees up cash)
    2. Execute BUYS in priority order (highest target weight first)
    3. Allow partial fills if cash insufficient
    4. Cash never goes negative
    
    Parameters
    ----------
    state : PortfolioState
        Current portfolio state (will be modified in place)
    target_weights : pd.Series
        Target weights for each ticker (should sum to 1 or 0)
    prices : pd.Series
        Current prices available for execution
    valuation_prices : pd.Series | None
        Mark-to-market prices used for total portfolio equity. If None, falls back to `prices`.
    exec_model : ExecutionModel
        Transaction cost model
    lot_size : int | pd.Series | dict
        Minimum tradeable unit. Can be scalar or per-ticker mapping.
    date : pd.Timestamp
        Rebalance date (for trade logging)
        
    Returns
    -------
    state : PortfolioState
        Updated portfolio state
    trades : list[dict]
        List of executed trades with keys: date, ticker, side, shares, price, notional, cost
    """
    trades = []
    
    # Step 1: Universe alignment
    # Union of tickers in shares, weights, and prices
    valuation_prices = prices if valuation_prices is None else valuation_prices
    all_tickers = (
        state.shares.index
        .union(target_weights.index)
        .union(prices.index)
        .union(valuation_prices.index)
    )
    
    # Fill missing values
    current_shares = state.shares.reindex(all_tickers, fill_value=0).astype(int)
    current_shares_all = current_shares.copy()
    target_weights_aligned = target_weights.reindex(all_tickers, fill_value=0.0)
    prices_aligned = prices.reindex(all_tickers, fill_value=np.nan)
    valuation_prices_aligned = pd.to_numeric(valuation_prices.reindex(all_tickers, fill_value=np.nan), errors="coerce")
    lot_sizes_aligned = _normalize_lot_sizes(lot_size, all_tickers)
    
    # Remove tickers with missing prices (can't trade)
    valid_tickers = prices_aligned.notna()
    current_shares = current_shares[valid_tickers]
    target_weights_aligned = target_weights_aligned[valid_tickers]
    prices_aligned = prices_aligned[valid_tickers]
    lot_sizes_aligned = lot_sizes_aligned[valid_tickers]
    
    # Warn if positions are excluded due to missing prices
    excluded_tickers = state.shares[~state.shares.index.isin(prices_aligned.index)]
    excluded_tickers = excluded_tickers[excluded_tickers > 0]
    if len(excluded_tickers) > 0:
        excluded_list = ', '.join([f"{ticker}({shares})" for ticker, shares in excluded_tickers.items()])
        print(f"  ⚠️  Warning: {len(excluded_tickers)} position(s) excluded from rebalance (no price data): {excluded_list}")
    
    # Step 2: Compute current equity from tradeable positions only
    # Use explicit calculation to make it clear we're only valuing priced positions
    equity = state.cash + (current_shares_all * valuation_prices_aligned).sum()
    
    if equity <= 0:
        # No equity to rebalance
        return state, trades
    
    # Step 3: Compute target shares (rounded to lot_size)
    target_values = target_weights_aligned * equity
    raw_target_shares = target_values / prices_aligned
    target_shares = (np.floor(raw_target_shares / lot_sizes_aligned) * lot_sizes_aligned).astype(int)
    
    # Step 4: Compute deltas
    delta_shares = target_shares - current_shares
    
    # Step 5: Execute SELLS first (delta < 0)
    sells = delta_shares[delta_shares < 0].sort_index()
    
    for ticker in sells.index:
        shares_exec = -delta_shares[ticker]  # Positive number of shares to sell
        price = prices_aligned[ticker]
        notional = shares_exec * price
        cost = exec_model.cost(notional)
        
        # Execute sell
        state.shares[ticker] = state.shares.get(ticker, 0) - shares_exec
        state.cash += notional - cost
        
        # Log trade
        trades.append({
            'date': date,
            'ticker': ticker,
            'side': 'SELL',
            'shares': shares_exec,
            'price': price,
            'notional': notional,
            'cost': cost
        })
    
    # Step 6: Execute BUYS (delta > 0), cash constrained
    # Priority: highest target weight first
    buys = delta_shares[delta_shares > 0].copy()
    buy_priorities = target_weights_aligned[buys.index].sort_values(ascending=False)
    
    for ticker in buy_priorities.index:
        shares_desired = delta_shares[ticker]
        price = prices_aligned[ticker]
        ticker_lot_size = int(lot_sizes_aligned[ticker])
        
        # Full order cost
        notional_full = shares_desired * price
        cost_full = exec_model.cost(notional_full)
        required_full = notional_full + cost_full
        
        if state.cash >= required_full:
            # Execute full order
            shares_exec = shares_desired
            notional = notional_full
            cost = cost_full
        else:
            # Attempt partial fill
            if state.cash <= exec_model.tc_fixed:
                # Can't afford even the fixed fee
                continue
            
            # Compute maximum affordable shares
            effective_price = price * (1 + exec_model.tc_pct)
            shares_max = np.floor((state.cash - exec_model.tc_fixed) / effective_price / ticker_lot_size) * ticker_lot_size
            shares_max = int(shares_max)
            
            shares_exec = min(shares_desired, shares_max)
            
            if shares_exec <= 0:
                # Can't afford any shares
                continue
            
            notional = shares_exec * price
            cost = exec_model.cost(notional)
        
        # Execute buy
        state.shares[ticker] = state.shares.get(ticker, 0) + shares_exec
        state.cash -= (notional + cost)
        
        # Log trade
        trades.append({
            'date': date,
            'ticker': ticker,
            'side': 'BUY',
            'shares': shares_exec,
            'price': price,
            'notional': notional,
            'cost': cost
        })
    
    # Step 7: Enforce invariants
    # Clamp tiny negative cash to zero
    if state.cash < 0 and state.cash >= -1e-9:
        state.cash = 0.0
    
    # Assert cash is non-negative
    assert state.cash >= -1e-9, f"Cash went negative: {state.cash}"
    
    # Ensure all shares are integers and multiples of lot_size
    state.shares = state.shares.astype(int)
    lot_sizes_for_state = _normalize_lot_sizes(lot_size, state.shares.index)
    assert ((state.shares % lot_sizes_for_state) == 0).all(), "Shares not multiples of lot size"
    
    return state, trades


def run_execution_backtest(
    prices_df: pd.DataFrame,
    target_weights_df: pd.DataFrame,
    rebalance_dates: list,
    initial_capital: float,
    lot_size,
    tc_fixed: float,
    tc_pct: float,
    cash_return_rate: float = 0.0,
    apply_cash_yield: bool = True,
    start_date: pd.Timestamp = None,
    management_fee_monthly: float = 0.0
) -> tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    """Run execution-based backtest with transaction costs and cash constraints.
    
    Parameters
    ----------
    prices_df : pd.DataFrame
        Daily prices (index=dates, columns=tickers)
    target_weights_df : pd.DataFrame
        Target weights on rebalance dates only (index=rebalance_dates, columns=tickers)
    rebalance_dates : list
        List of dates when rebalancing is allowed
    initial_capital : float
        Starting cash (USD)
    lot_size : int | pd.Series | dict
        Minimum tradeable unit. Can be scalar or per-ticker mapping.
    tc_fixed : float
        Fixed cost per order (USD)
    tc_pct : float
        Proportional cost as fraction (e.g., 0.0005 = 5 bps)
    cash_return_rate : float
        Annual return rate on uninvested cash (e.g., 0.04 = 4%)
    apply_cash_yield : bool
        Whether to apply daily cash yield
    start_date : pd.Timestamp, optional
        Start date for the backtest. If provided, only dates >= start_date are used.
        If not provided, uses all dates from prices_df.
    management_fee_monthly : float
        Monthly management fee as fraction (e.g., 0.001 = 0.1% per month).
        Accrued daily from portfolio equity.
        
    Returns
    -------
    equity_curve : pd.Series
        Daily portfolio equity values
    daily_returns : pd.Series
        Daily returns (fraction)
    snapshots_df : pd.DataFrame
        Daily snapshots: date, equity, cash, holdings_value, num_positions, costs_today
    trades_df : pd.DataFrame
        All trades: date, ticker, side, shares, price, notional, cost
    """
    # Filter prices by start_date if provided
    prices_df = _prepare_price_frame(prices_df)
    target_weights_df = _prepare_price_frame(target_weights_df)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        prices_df = prices_df[prices_df.index >= start_date].copy()
        # Also filter rebalance_dates
        rebalance_dates = [d for d in rebalance_dates if pd.to_datetime(d) >= start_date]
    
    # Initialize state
    all_tickers = prices_df.columns.tolist()
    state = PortfolioState(
        cash=initial_capital,
        shares=pd.Series(0, index=all_tickers, dtype=int)
    )
    
    # Create execution model
    exec_model = ExecutionModel(tc_fixed=tc_fixed, tc_pct=tc_pct)
    
    # Calculate daily cash return
    daily_cash_return = (1 + cash_return_rate) ** (1/252) - 1 if apply_cash_yield else 0.0

    # Calculate daily management fee (monthly fee converted to daily, assuming 21 trading days/month)
    daily_mgmt_fee_rate = management_fee_monthly / 21 if management_fee_monthly > 0 else 0.0

    # Accumulators
    all_trades = []
    snapshots = []
    total_mgmt_fees = 0.0
    rebalance_dates_set = set(pd.to_datetime(rebalance_dates))
    last_prices = pd.Series(np.nan, index=all_tickers, dtype=float)
    
    print(f"Running execution backtest...")
    print(f"  Start date: {prices_df.index[0].strftime('%Y-%m-%d')}")
    print(f"  End date: {prices_df.index[-1].strftime('%Y-%m-%d')}")
    print(f"  Trading days: {len(prices_df)}")
    print(f"  Rebalance dates: {len(rebalance_dates)}")
    print(f"  Initial capital: ${initial_capital:,.0f}")
    print(f"  Transaction costs: ${tc_fixed} + {tc_pct*10000:.1f} bps")
    if management_fee_monthly > 0:
        print(f"  Management fee: {management_fee_monthly*100:.2f}% monthly ({management_fee_monthly*12*100:.2f}% annual)")
    
    # Main loop: iterate through all trading dates
    for i, date in enumerate(prices_df.index):
        prices_today_raw = prices_df.loc[date]
        trade_prices, last_prices, valuation_prices = _split_trade_and_mark_prices(
            prices_today_raw=prices_today_raw,
            last_prices=last_prices,
        )
        
        # Check if rebalance date
        if date in rebalance_dates_set:
            # Get target weights for this date
            if date in target_weights_df.index:
                target_weights = target_weights_df.loc[date]
                target_weights = pd.to_numeric(target_weights, errors='coerce').fillna(0.0)
                
                # Execute rebalance
                state, trades = rebalance_portfolio_plan_a(
                    state=state,
                    target_weights=target_weights,
                    prices=trade_prices,
                    valuation_prices=valuation_prices,
                    exec_model=exec_model,
                    lot_size=lot_size,
                    date=date
                )
                
                # Record trades
                all_trades.extend(trades)
        
        # Apply cash yield FIRST (before calculating equity snapshot)
        if apply_cash_yield and daily_cash_return > 0:
            state.cash *= (1 + daily_cash_return)
        
        # Calculate equity BEFORE management fee deduction (fee is based on AUM)
        equity_before_fee = state.value(valuation_prices)
        
        # Apply daily management fee (accrued from total portfolio equity)
        mgmt_fee_today = 0.0
        if daily_mgmt_fee_rate > 0 and equity_before_fee > 0:
            mgmt_fee_today = equity_before_fee * daily_mgmt_fee_rate
            state.cash -= mgmt_fee_today
            total_mgmt_fees += mgmt_fee_today
        
        # Mark-to-market: calculate equity value (AFTER yield and fees applied)
        equity = state.value(valuation_prices)
        holdings_value = state.position_values(valuation_prices).sum()
        num_positions = (state.shares > 0).sum()
        
        # Calculate transaction costs incurred today
        costs_today = sum(t['cost'] for t in all_trades if t['date'] == date)
        
        # Record snapshot
        snapshots.append({
            'date': date,
            'equity': equity,
            'cash': state.cash,
            'holdings_value': holdings_value,
            'num_positions': num_positions,
            'costs_today': costs_today,
            'mgmt_fee_today': mgmt_fee_today
        })
        
        # Progress indicator
        if (i + 1) % 100 == 0 or i == len(prices_df) - 1:
            print(f"  Processed {i+1}/{len(prices_df)} days, equity: ${equity:,.0f}")
    
    # Create output DataFrames
    snapshots_df = pd.DataFrame(snapshots).set_index('date')
    equity_curve = snapshots_df['equity']
    daily_returns = equity_curve.pct_change().fillna(0)
    trades_df = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()
    
    # Print summary
    print(f"\n=== Backtest Complete ===")
    print(f"Final equity: ${equity_curve.iloc[-1]:,.2f}")
    print(f"Total return: {(equity_curve.iloc[-1] / initial_capital - 1)*100:.2f}%")
    print(f"Total trades: {len(trades_df)}")
    print(f"Total transaction costs: ${snapshots_df['costs_today'].sum():,.2f}")
    print(f"Total management fees: ${snapshots_df['mgmt_fee_today'].sum():,.2f}")
    print(f"Total all costs: ${snapshots_df['costs_today'].sum() + snapshots_df['mgmt_fee_today'].sum():,.2f}")
    print(f"Cash never went negative: ✓" if snapshots_df['cash'].min() >= -1e-6 else "✗ WARNING")
    
    return equity_curve, daily_returns, snapshots_df, trades_df


def liquidate_all_positions(
    state: PortfolioState,
    prices: pd.Series,
    exec_model: ExecutionModel,
    date: pd.Timestamp
) -> tuple[PortfolioState, list[dict]]:
    """Sell all positions and go to 100% cash.
    
    Parameters
    ----------
    state : PortfolioState
        Current portfolio state
    prices : pd.Series
        Current prices for execution
    exec_model : ExecutionModel
        Transaction cost model
    date : pd.Timestamp
        Date for trade logging
        
    Returns
    -------
    state : PortfolioState
        Updated portfolio state (all cash)
    trades : list[dict]
        List of sell trades executed
    """
    trades = []
    
    # Sell all positions with shares > 0
    positions_to_sell = state.shares[state.shares > 0]
    
    for ticker, shares in positions_to_sell.items():
        if ticker not in prices.index or pd.isna(prices[ticker]):
            continue
        
        price = prices[ticker]
        notional = shares * price
        cost = exec_model.cost(notional)
        
        # Execute sell
        state.shares[ticker] = 0
        state.cash += notional - cost
        
        trades.append({
            'date': date,
            'ticker': ticker,
            'side': 'SELL',
            'shares': shares,
            'price': price,
            'notional': notional,
            'cost': cost,
            'reason': 'REGIME_OFF'
        })
    
    return state, trades


def run_execution_backtest_with_filters(
    prices_df: pd.DataFrame,
    benchmark_prices_df: pd.DataFrame,
    target_weights_df: pd.DataFrame,
    rebalance_dates: list,
    initial_capital: float,
    lot_size,
    tc_fixed: float,
    tc_pct: float,
    cash_return_rate: float = 0.0,
    apply_cash_yield: bool = True,
    start_date: pd.Timestamp = None,
    management_fee_monthly: float = 0.0,
    regime_filter = None
) -> tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    """Run execution-based backtest with dynamic regime filters.
    
    Key differences from run_execution_backtest:
    1. Calculates rolling metrics daily for strategy AND benchmarks
    2. Evaluates regime filters using previous day's metrics (no lookahead)
    3. When regime turns OFF: sells all positions immediately
    4. When regime turns ON: waits for next scheduled rebalance
    
    Parameters
    ----------
    prices_df : pd.DataFrame
        Daily prices for strategy tickers (index=dates, columns=tickers)
    benchmark_prices_df : pd.DataFrame
        Daily prices for benchmarks (index=dates, columns=benchmark tickers)
    target_weights_df : pd.DataFrame
        Target weights on rebalance dates only (index=rebalance_dates, columns=tickers)
    rebalance_dates : list
        List of dates when rebalancing is allowed
    initial_capital : float
        Starting cash (USD)
    lot_size : int | pd.Series | dict
        Minimum tradeable unit. Can be scalar or per-ticker mapping.
    tc_fixed : float
        Fixed cost per order (USD)
    tc_pct : float
        Proportional cost as fraction (e.g., 0.0005 = 5 bps)
    cash_return_rate : float
        Annual return rate on uninvested cash (e.g., 0.04 = 4%)
    apply_cash_yield : bool
        Whether to apply daily cash yield
    start_date : pd.Timestamp, optional
        Start date for the backtest
    management_fee_monthly : float
        Monthly management fee as fraction
    regime_filter : CombinedRegimeFilter, optional
        Combined regime filter instance. If None, no filtering applied.
        
    Returns
    -------
    equity_curve : pd.Series
        Daily portfolio equity values
    daily_returns : pd.Series
        Daily returns (fraction)
    snapshots_df : pd.DataFrame
        Daily snapshots with filter states and rolling metrics
    trades_df : pd.DataFrame
        All trades including reason (REBALANCE or REGIME_OFF)
    """
    from rolling_metrics import RollingMetricsTracker
    
    # Filter prices by start_date if provided
    prices_df = _prepare_price_frame(prices_df)
    benchmark_prices_df = _prepare_price_frame(benchmark_prices_df)
    target_weights_df = _prepare_price_frame(target_weights_df)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        prices_df = prices_df[prices_df.index >= start_date].copy()
        benchmark_prices_df = benchmark_prices_df[benchmark_prices_df.index >= start_date].copy()
        rebalance_dates = [d for d in rebalance_dates if pd.to_datetime(d) >= start_date]
    
    # Initialize state
    all_tickers = prices_df.columns.tolist()
    state = PortfolioState(
        cash=initial_capital,
        shares=pd.Series(0, index=all_tickers, dtype=int)
    )
    
    # Create execution model
    exec_model = ExecutionModel(tc_fixed=tc_fixed, tc_pct=tc_pct)
    
    # Calculate daily rates
    daily_cash_return = (1 + cash_return_rate) ** (1/252) - 1 if apply_cash_yield else 0.0
    daily_mgmt_fee_rate = management_fee_monthly / 21 if management_fee_monthly > 0 else 0.0
    
    # Initialize rolling metrics tracker
    metrics_tracker = RollingMetricsTracker(windows=[30, 60, 252])
    
    # Load SPX/VIX data if filter uses it
    if regime_filter is not None:
        for f in regime_filter.filters:
            if hasattr(f, 'load_data'):
                f.load_data(start_date=start_date, end_date=prices_df.index[-1])
    
    # Accumulators
    all_trades = []
    snapshots = []
    rebalance_dates_set = set(pd.to_datetime(rebalance_dates))
    last_prices = pd.Series(np.nan, index=all_tickers, dtype=float)
    benchmark_last_prices = pd.Series(np.nan, index=benchmark_prices_df.columns, dtype=float)
    benchmark_base_prices: dict[str, float] = {}
    
    # Track regime state
    regime_is_on = True  # Start with regime ON
    regime_off_count = 0
    
    print(f"Running execution backtest with dynamic filters...")
    print(f"  Start date: {prices_df.index[0].strftime('%Y-%m-%d')}")
    print(f"  End date: {prices_df.index[-1].strftime('%Y-%m-%d')}")
    print(f"  Trading days: {len(prices_df)}")
    print(f"  Rebalance dates: {len(rebalance_dates)}")
    print(f"  Initial capital: ${initial_capital:,.0f}")
    if regime_filter is not None:
        print(f"  Active filters: {regime_filter.get_enabled_filter_names()}")
        print(f"  Filter logic: {regime_filter.logic}")
    
    # Main loop
    for i, date in enumerate(prices_df.index):
        prices_today_raw = prices_df.loc[date]
        trade_prices, last_prices, valuation_prices = _split_trade_and_mark_prices(
            prices_today_raw=prices_today_raw,
            last_prices=last_prices,
        )
        benchmark_today_raw = benchmark_prices_df.loc[date] if date in benchmark_prices_df.index else pd.Series(dtype=float)
        benchmark_last_prices = _update_last_prices(benchmark_last_prices, benchmark_today_raw)
        
        # Calculate current equity (before any actions)
        equity_now = state.value(valuation_prices)
        
        # Update metrics tracker with current equity
        metrics_tracker.update('STRATEGY', date, equity_now)
        for ticker in benchmark_prices_df.columns:
            current_price = benchmark_last_prices.get(ticker, np.nan)
            if pd.isna(current_price):
                continue
            if ticker not in benchmark_base_prices and pd.notna(benchmark_today_raw.get(ticker, np.nan)):
                benchmark_base_prices[ticker] = float(benchmark_today_raw[ticker])
            base_price = benchmark_base_prices.get(ticker)
            if base_price is None or base_price <= 0:
                continue
            bench_equity = initial_capital * (current_price / base_price)
            metrics_tracker.update(ticker, date, bench_equity)
        
        # Evaluate regime filters (using previous day's metrics)
        filter_states = {}
        new_regime_is_on = True
        
        if regime_filter is not None and i > 0:  # Skip first day (no previous data)
            new_regime_is_on, filter_states = regime_filter.get_signal(date, metrics_tracker)
        
        # Handle regime transitions
        trades_today = []
        
        if regime_is_on and not new_regime_is_on:
            # Regime just turned OFF - liquidate all positions
            state, liquidation_trades = liquidate_all_positions(
                state=state,
                prices=trade_prices,
                exec_model=exec_model,
                date=date
            )
            trades_today.extend(liquidation_trades)
            regime_off_count += 1
            if i < 50 or regime_off_count <= 5:  # Limit log spam
                print(f"  📉 {date.strftime('%Y-%m-%d')}: Regime OFF - liquidated {len(liquidation_trades)} positions")
        
        regime_is_on = new_regime_is_on
        
        # Rebalance only if regime is ON and it's a rebalance date
        if regime_is_on and date in rebalance_dates_set:
            if date in target_weights_df.index:
                target_weights = pd.to_numeric(target_weights_df.loc[date], errors='coerce').fillna(0.0)
                
                state, rebalance_trades = rebalance_portfolio_plan_a(
                    state=state,
                    target_weights=target_weights,
                    prices=trade_prices,
                    valuation_prices=valuation_prices,
                    exec_model=exec_model,
                    lot_size=lot_size,
                    date=date
                )
                
                # Add reason to trades
                for t in rebalance_trades:
                    t['reason'] = 'REBALANCE'
                trades_today.extend(rebalance_trades)
        
        all_trades.extend(trades_today)
        
        # Apply cash yield
        if apply_cash_yield and daily_cash_return > 0:
            state.cash *= (1 + daily_cash_return)
        
        # Apply management fee
        equity_before_fee = state.value(valuation_prices)
        mgmt_fee_today = 0.0
        if daily_mgmt_fee_rate > 0 and equity_before_fee > 0:
            mgmt_fee_today = equity_before_fee * daily_mgmt_fee_rate
            state.cash -= mgmt_fee_today
        
        # Final equity for snapshot
        equity = state.value(valuation_prices)
        holdings_value = state.position_values(valuation_prices).sum()
        num_positions = (state.shares > 0).sum()
        costs_today = sum(t['cost'] for t in trades_today)
        
        # Get rolling metrics for snapshot
        strat_metrics_30 = metrics_tracker.get_metrics('STRATEGY', 30)
        
        # Build snapshot with filter info
        snapshot = {
            'date': date,
            'equity': equity,
            'cash': state.cash,
            'holdings_value': holdings_value,
            'num_positions': num_positions,
            'costs_today': costs_today,
            'mgmt_fee_today': mgmt_fee_today,
            'regime_on': regime_is_on,
            'rolling_dd_30d': strat_metrics_30.get('drawdown'),
            'rolling_vol_30d': strat_metrics_30.get('volatility'),
        }
        
        # Add filter states
        for fname, fstate in filter_states.items():
            snapshot[f'filter_{fname}_on'] = fstate.is_on
            snapshot[f'filter_{fname}_value'] = fstate.value
        
        snapshots.append(snapshot)
        
        # Progress indicator
        if (i + 1) % 100 == 0 or i == len(prices_df) - 1:
            regime_str = "ON" if regime_is_on else "OFF"
            print(f"  Processed {i+1}/{len(prices_df)} days, equity: ${equity:,.0f}, regime: {regime_str}")
    
    # Create output DataFrames
    snapshots_df = pd.DataFrame(snapshots).set_index('date')
    equity_curve = snapshots_df['equity']
    daily_returns = equity_curve.pct_change().fillna(0)
    trades_df = pd.DataFrame(all_trades) if all_trades else pd.DataFrame()
    
    # Summary statistics
    regime_on_days = snapshots_df['regime_on'].sum()
    regime_off_days = len(snapshots_df) - regime_on_days
    
    print(f"\n=== Backtest Complete ===")
    print(f"Final equity: ${equity_curve.iloc[-1]:,.2f}")
    print(f"Total return: {(equity_curve.iloc[-1] / initial_capital - 1)*100:.2f}%")
    print(f"Total trades: {len(trades_df)}")
    print(f"Total transaction costs: ${snapshots_df['costs_today'].sum():,.2f}")
    print(f"Total management fees: ${snapshots_df['mgmt_fee_today'].sum():,.2f}")
    print(f"Regime ON days: {regime_on_days} ({regime_on_days/len(snapshots_df)*100:.1f}%)")
    print(f"Regime OFF days: {regime_off_days} ({regime_off_days/len(snapshots_df)*100:.1f}%)")
    print(f"Regime OFF events: {regime_off_count}")
    
    return equity_curve, daily_returns, snapshots_df, trades_df


def run_benchmark_backtest(
    benchmark_tickers: list,
    prices_df: pd.DataFrame,
    initial_capital: float,
    lot_size,
    tc_fixed: float,
    tc_pct: float,
    cash_return_rate: float = 0.0,
    apply_cash_yield: bool = True,
    start_date: pd.Timestamp = None
) -> dict:
    """Run buy-and-hold backtests for benchmark tickers with execution constraints.
    
    Each benchmark is purchased on day 1 with integer shares and held until end.
    Uninvested cash grows at cash_return_rate.
    
    Parameters
    ----------
    benchmark_tickers : list
        List of benchmark tickers to backtest separately
    prices_df : pd.DataFrame
        Daily prices (index=dates, columns=tickers)
    initial_capital : float
        Starting cash (USD)
    lot_size : int | pd.Series | dict
        Minimum tradeable unit. Can be scalar or per-ticker mapping.
    tc_fixed : float
        Fixed cost per order (USD)
    tc_pct : float
        Proportional cost as fraction (e.g., 0.0005 = 5 bps)
    cash_return_rate : float
        Annual return rate on uninvested cash (e.g., 0.04 = 4%)
    apply_cash_yield : bool
        Whether to apply daily cash yield
    start_date : pd.Timestamp, optional
        Start date for the backtest. If provided, only dates >= start_date are used.
        If not provided, uses all dates from prices_df.
        
    Returns
    -------
    results : dict
        Dictionary with benchmark ticker as key, each containing:
        - equity_curve : pd.Series (daily portfolio values)
        - daily_returns : pd.Series (daily returns)
        - shares : int (number of shares purchased)
        - cash : float (final cash balance)
        - cost : float (transaction cost paid)
    """
    # Filter prices by start_date if provided
    prices_df = _prepare_price_frame(prices_df)

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        prices_df = prices_df[prices_df.index >= start_date].copy()
    
    results = {}
    
    # Calculate daily cash return
    daily_cash_return = (1 + cash_return_rate) ** (1/252) - 1 if apply_cash_yield else 0.0
    
    exec_model = ExecutionModel(tc_fixed=tc_fixed, tc_pct=tc_pct)
    
    print(f"\nRunning buy-and-hold benchmarks...")
    print(f"  Benchmarks: {benchmark_tickers}")
    print(f"  Initial capital: ${initial_capital:,.0f}")
    print(f"  Transaction costs: ${tc_fixed} + {tc_pct*10000:.1f} bps")
    
    for ticker in benchmark_tickers:
        if ticker not in prices_df.columns:
            print(f"  ⚠️  {ticker}: Not found in price data, skipping")
            continue
        ticker_lot_size = int(_normalize_lot_sizes(lot_size, [ticker]).iloc[0])
        
        # Get price series for this ticker
        ticker_prices = prices_df[ticker].dropna()
        
        if len(ticker_prices) == 0:
            print(f"  ⚠️  {ticker}: No price data available, skipping")
            continue
        
        # Day 1: Buy as many shares as possible
        first_date = ticker_prices.index[0]
        first_price = ticker_prices.iloc[0]
        
        # Calculate maximum affordable shares (exact calculation accounting for transaction costs)
        # Cost formula: total_needed = notional + tc_fixed + tc_pct * notional
        #            = shares * price + tc_fixed + tc_pct * (shares * price)
        #            = shares * price * (1 + tc_pct) + tc_fixed
        # 
        # We need: shares * price * (1 + tc_pct) + tc_fixed <= initial_capital
        # 
        # Use exact calculation by iterating down from max possible lots
        max_possible_lots = int(initial_capital / first_price / ticker_lot_size)
        shares = 0
        
        for num_lots in range(max_possible_lots, 0, -1):
            test_shares = num_lots * ticker_lot_size
            notional = test_shares * first_price
            cost = exec_model.cost(notional)
            if notional + cost <= initial_capital:
                shares = test_shares
                break
        
        if shares <= 0:
            print(f"  ⚠️  {ticker}: Cannot afford any shares at ${first_price:.2f}, skipping")
            continue
        
        # Execute purchase
        notional = shares * first_price
        cost = exec_model.cost(notional)
        cash = initial_capital - notional - cost
        
        # Build equity curve
        equity_curve = []
        cash_balance = cash
        
        for date in ticker_prices.index:
            price = ticker_prices.loc[date]
            holdings_value = shares * price
            equity = cash_balance + holdings_value
            equity_curve.append(equity)
            
            # Apply cash yield
            if apply_cash_yield and daily_cash_return > 0:
                cash_balance *= (1 + daily_cash_return)
        
        equity_series = pd.Series(equity_curve, index=ticker_prices.index)
        daily_returns = equity_series.pct_change().fillna(0)
        
        results[ticker] = {
            'equity_curve': equity_series,
            'daily_returns': daily_returns,
            'shares': shares,
            'cash': cash_balance,
            'cost': cost,
            'first_price': first_price,
            'final_value': equity_series.iloc[-1]
        }
        
        total_return = (equity_series.iloc[-1] / initial_capital - 1) * 100
        print(f"  ✓ {ticker}: {shares} shares @ ${first_price:.2f}, Final: ${equity_series.iloc[-1]:,.2f} ({total_return:.2f}%)")
    
    return results
