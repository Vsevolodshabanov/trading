"""Portfolio position sizing for client portfolios."""

import os
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


def calculate_positions(target_weights, prices, portfolio_size, lot_size=1, rounding_rule='floor'):
    """Calculate actual portfolio positions with lot sizing constraints.
    
    Converts target weights into actual positions considering:
    - No fractional shares (configurable rounding)
    - Lot size requirements
    - Portfolio size constraints
    - Positions that can't be opened are skipped
    
    Parameters
    ----------
    target_weights : pd.Series or dict
        Target weights for each ticker (ticker: weight)
        Weights should sum to <= 1.0
    prices : pd.Series or dict
        Current prices for each ticker (ticker: price)
    portfolio_size : float
        Total portfolio value in USD
    lot_size : int | pd.Series | dict, default 1
        Minimum lot size (number of shares per lot). Can be scalar or per-ticker mapping.
    rounding_rule : str, default 'floor'
        Rounding rule for calculating number of shares
        Currently only 'floor' (round down) is supported
        Other rules ('round', 'ceil') can be added in future
        
    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - ticker: Stock ticker
        - target_weight: Target weight from strategy
        - price: Current price
        - shares: Number of shares to buy (whole shares only)
        - position_value: Actual dollar value of position
        - actual_weight: Actual weight in portfolio
        
    dict
        Summary with:
        - total_invested: Total dollar amount invested
        - cash_remaining: Uninvested cash
        - actual_weight_sum: Sum of actual weights
    """
    # Convert to Series if needed
    if isinstance(target_weights, dict):
        target_weights = pd.Series(target_weights)
    if isinstance(prices, dict):
        prices = pd.Series(prices)
    
    # Remove NaN weights and ensure we have prices for all tickers
    target_weights = target_weights.dropna()
    common_tickers = target_weights.index.intersection(prices.index)
    target_weights = target_weights[common_tickers]
    prices = prices[common_tickers]
    lot_sizes = _normalize_lot_sizes(lot_size, common_tickers)
    
    # Sort by target weight descending (allocate larger positions first)
    target_weights = target_weights.sort_values(ascending=False)
    
    results = []
    cash_remaining = portfolio_size
    
    for ticker in target_weights.index:
        target_weight = target_weights[ticker]
        price = prices[ticker]
        
        # Skip if price is NaN or zero
        if pd.isna(price) or price <= 0:
            continue
        
        # Calculate target dollar amount
        target_value = target_weight * portfolio_size
        ticker_lot_size = int(lot_sizes[ticker])
        
        # Calculate number of lots based on rounding rule
        if rounding_rule != 'floor':
            raise ValueError(f"Only 'floor' rounding rule is currently supported. Got: {rounding_rule}")
        
        num_lots_exact = target_value / (price * ticker_lot_size)
        num_lots = int(np.floor(num_lots_exact))
        shares = num_lots * ticker_lot_size
        
        # Skip if we can't afford even one lot
        if shares == 0:
            continue
        
        # Calculate actual position value
        position_value = shares * price
        
        # Check if we have enough cash remaining
        if position_value > cash_remaining:
            # Recalculate with available cash
            num_lots_exact = cash_remaining / (price * ticker_lot_size)
            num_lots = int(np.floor(num_lots_exact))
            shares = num_lots * ticker_lot_size
            position_value = shares * price
            
            # Skip if still can't afford
            if shares == 0:
                continue
        
        # Update cash remaining
        cash_remaining -= position_value
        
        # Calculate actual weight
        actual_weight = position_value / portfolio_size
        
        results.append({
            'ticker': ticker,
            'target_weight': target_weight,
            'price': price,
            'shares': shares,
            'position_value': position_value,
            'actual_weight': actual_weight
        })
    
    # Create DataFrame
    positions_df = pd.DataFrame(results)
    
    # Calculate summary
    if len(positions_df) > 0:
        total_invested = positions_df['position_value'].sum()
        actual_weight_sum = positions_df['actual_weight'].sum()
    else:
        total_invested = 0
        actual_weight_sum = 0
    
    summary = {
        'total_invested': total_invested,
        'cash_remaining': portfolio_size - total_invested,
        'actual_weight_sum': actual_weight_sum
    }
    
    return positions_df, summary


def build_client_portfolio_positions(
    weights_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    portfolio_size: float,
    portfolio_date=None,
    lot_size: int = 1,
    rounding_rule: str = 'floor',
    save_to_excel: bool = False,
    output_dir: str = 'data',
    verbose: bool = True,
    tc_fixed: float = 1.0,
    tc_pct: float = 0.0005
):
    """Build client portfolio positions for a given date and optionally save to Excel.

    Parameters
    ----------
    weights_df : pd.DataFrame
        Weights table with dates as index and tickers as columns.
    prices_df : pd.DataFrame
        Price table with dates as index and tickers as columns.
    portfolio_size : float
        Portfolio size in USD.
    portfolio_date : str | pd.Timestamp | None
        Target date (e.g., '2025-01-31') or None for latest available date.
    lot_size : int | pd.Series | dict, default 1
        Minimum tradeable unit (shares). Can be scalar or per-ticker mapping.
    rounding_rule : str, default 'floor'
        Rounding rule for share calculations.
    save_to_excel : bool, default False
        If True, save positions to Excel.
    output_dir : str, default 'data'
        Output directory for Excel export.

    Returns
    -------
    positions_df : pd.DataFrame
    summary : dict
    target_date : pd.Timestamp
    output_path : str | None
    """
    if portfolio_date is not None:
        target_date = pd.Timestamp(portfolio_date)
        available_dates = weights_df.index[weights_df.index <= target_date]
        if len(available_dates) == 0:
            raise ValueError(f"No weights available before {portfolio_date}")
        target_date = available_dates[-1]
    else:
        target_date = weights_df.index[-1]

    target_weights = weights_df.loc[target_date].dropna()
    target_prices = prices_df.loc[target_date][target_weights.index]

    positions_df, summary = calculate_positions(
        target_weights=target_weights,
        prices=target_prices,
        portfolio_size=portfolio_size,
        lot_size=lot_size,
        rounding_rule=rounding_rule
    )

    output_path = None
    if save_to_excel:
        os.makedirs(output_dir, exist_ok=True)
        date_str = target_date.strftime('%Y-%m-%d')
        output_path = os.path.join(output_dir, f"{date_str}_{int(portfolio_size)}.xlsx")
        positions_df.to_excel(output_path, index=False)

    if verbose:
        print("=== Client Portfolio Calculation ===")
        print(f"Target date: {target_date.strftime('%Y-%m-%d')}")
        print(f"Portfolio size: ${portfolio_size:,.0f}")
        if output_path:
            print(f"Saved positions to: {output_path}")

        num_positions = len(positions_df[positions_df['shares'] > 0])
        total_notional = positions_df['position_value'].sum()
        estimated_tc_fixed = num_positions * tc_fixed
        estimated_tc_pct = total_notional * tc_pct
        estimated_total_costs = estimated_tc_fixed + estimated_tc_pct

        print(f"\n--- Position Summary ---")
        print(f"Number of positions: {num_positions}")
        print(f"Total invested (before costs): ${summary['total_invested']:,.2f}")
        print(f"Cash remaining (before costs): ${summary['cash_remaining']:,.2f}")
        print(f"Actual weight sum: {summary['actual_weight_sum']:.2%}")

        print(f"\n--- Estimated Transaction Costs ---")
        print(f"Fixed costs ({num_positions} orders × ${tc_fixed}): ${estimated_tc_fixed:,.2f}")
        print(f"Proportional costs ({tc_pct*100:.2f}% × ${total_notional:,.0f}): ${estimated_tc_pct:,.2f}")
        print(f"Total estimated costs: ${estimated_total_costs:,.2f}")

        print(f"\n--- Net Summary ---")
        net_invested = summary['total_invested'] + estimated_total_costs
        net_cash = portfolio_size - net_invested
        print(f"Net invested (with costs): ${net_invested:,.2f}")
        print(f"Net cash remaining: ${net_cash:,.2f}")

        print(f"\nList of positions:")

    return positions_df, summary, target_date, output_path
