from __future__ import annotations

"""Plotly-based visualization functions for backtest analysis."""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ModuleNotFoundError:
    go = None
    make_subplots = None


def _require_plotly() -> None:
    if go is None or make_subplots is None:
        raise ModuleNotFoundError(
            "plotly is required for visualization. Install it with `pip install plotly`."
        )


def plot_equity_comparison(
    frictionless_equity: pd.Series,
    execution_equity: pd.Series,
    rebalance_dates: List[pd.Timestamp],
    initial_investment: float,
    title: str = "Equity Curves: Frictionless vs Execution-Based"
) -> go.Figure:
    """
    Plot equity curves comparison between frictionless and execution-based backtests.
    
    Parameters
    ----------
    frictionless_equity : pd.Series
        Equity curve from frictionless backtest
    execution_equity : pd.Series
        Equity curve from execution-based backtest
    rebalance_dates : List[pd.Timestamp]
        List of rebalance dates to mark
    initial_investment : float
        Initial investment amount for return calculation
        
    Returns
    -------
    go.Figure
        Plotly figure object
    """
    _require_plotly()
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=(title, "Performance Impact of Costs & Fees (%)")
    )
    
    # Top panel: Equity curves
    fig.add_trace(
        go.Scatter(
            x=frictionless_equity.index,
            y=frictionless_equity.values,
            mode='lines',
            name='Frictionless',
            line=dict(width=2, color='blue')
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=execution_equity.index,
            y=execution_equity.values,
            mode='lines',
            name='With Costs & Fees',
            line=dict(width=2, color='orange')
        ),
        row=1, col=1
    )
    
    # Add rebalance markers
    rebalance_in_backtest = [d for d in rebalance_dates if d in execution_equity.index]
    rebalance_values = [execution_equity.loc[d] for d in rebalance_in_backtest]
    
    fig.add_trace(
        go.Scatter(
            x=rebalance_in_backtest,
            y=rebalance_values,
            mode='markers',
            name='Rebalance',
            marker=dict(size=8, color='red', symbol='circle', line=dict(width=1, color='darkred'))
        ),
        row=1, col=1
    )
    
    # Bottom panel: Return difference
    return_diff = (frictionless_equity / frictionless_equity.iloc[0] - 
                   execution_equity / execution_equity.iloc[0]) * 100
    
    fig.add_trace(
        go.Scatter(
            x=return_diff.index,
            y=return_diff.values,
            mode='lines',
            name='Return Diff',
            line=dict(width=2, color='red'),
            showlegend=False
        ),
        row=2, col=1
    )
    
    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="black", opacity=0.5, row=2, col=1)
    
    # Update layout
    fig.update_layout(
        height=800,
        margin=dict(t=80, b=80, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
        hovermode='x unified',
        hoverlabel=dict(namelength=-1, font=dict(size=12))
    )
    
    fig.update_yaxes(title_text="Portfolio Value ($)", tickformat="$,.0f", row=1, col=1)
    fig.update_yaxes(title_text="Return Difference (%)", tickformat=".2f", ticksuffix="%", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    
    # Add range slider to bottom x-axis
    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.05),
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=1.0, x=0.0
        ),
        row=2, col=1
    )
    
    return fig


def plot_strategy_vs_benchmarks(
    strategy_equity: pd.Series,
    benchmark_results: Dict[str, dict],
    rebalance_dates: List[pd.Timestamp],
    title: str = "Strategy vs Benchmarks (Execution-Based with Transaction Costs)"
) -> go.Figure:
    """
    Plot strategy equity curve vs benchmark equity curves.
    
    Parameters
    ----------
    strategy_equity : pd.Series
        Strategy equity curve
    benchmark_results : Dict[str, dict]
        Dictionary of benchmark results with 'equity_curve' key
    rebalance_dates : List[pd.Timestamp]
        List of rebalance dates to mark
        
    Returns
    -------
    go.Figure
        Plotly figure object
    """
    _require_plotly()
    fig = go.Figure()
    
    colors = ['orange', 'green', 'purple', 'brown', 'pink']
    
    # Plot strategy
    fig.add_trace(
        go.Scatter(
            x=strategy_equity.index,
            y=strategy_equity.values,
            mode='lines',
            name='Strategy (Exec)',
            line=dict(width=2.5, color='blue')
        )
    )
    
    # Plot benchmarks
    for i, (ticker, result) in enumerate(benchmark_results.items()):
        equity = result['equity_curve']
        fig.add_trace(
            go.Scatter(
                x=equity.index,
                y=equity.values,
                mode='lines',
                name=f"{ticker} (Buy & Hold)",
                line=dict(width=2, color=colors[i % len(colors)])
            )
        )
    
    # Add rebalance markers
    rebalance_in_backtest = [d for d in rebalance_dates if d in strategy_equity.index]
    rebalance_values = [strategy_equity.loc[d] for d in rebalance_in_backtest]
    
    fig.add_trace(
        go.Scatter(
            x=rebalance_in_backtest,
            y=rebalance_values,
            mode='markers',
            name='Rebalance',
            marker=dict(size=8, color='red', symbol='circle', line=dict(width=1, color='darkred'))
        )
    )
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        xaxis_title="Date",
        yaxis_title="Portfolio Value ($)",
        height=700,
        margin=dict(t=80, b=80, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
        hovermode='x unified',
        hoverlabel=dict(namelength=-1, font=dict(size=12))
    )
    
    # Add currency format to y-axis
    fig.update_yaxes(tickformat="$,.0f")
    
    # Add range slider and selector
    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.08),
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=1.02, x=0.0
        )
    )
    
    return fig


def plot_strategy_equity(
    strategy_equity: pd.Series,
    rebalance_dates: List[pd.Timestamp],
    title: str = "Strategy Equity Curve",
    snapshots_df: Optional[pd.DataFrame] = None
) -> go.Figure:
    """
    Plot strategy equity curve with rebalance date markers and optional regime state.

    Parameters
    ----------
    strategy_equity : pd.Series
        Strategy equity curve
    rebalance_dates : List[pd.Timestamp]
        List of rebalance dates to mark
    title : str
        Plot title
    snapshots_df : pd.DataFrame, optional
        Snapshots dataframe with 'regime_on' column to show strategy ON/OFF state

    Returns
    -------
    go.Figure
        Plotly figure object
    """
    _require_plotly()
    # Check if we have regime data to display
    show_regime = (snapshots_df is not None and 
                   'regime_on' in snapshots_df.columns)
    
    if show_regime:
        # Create subplots with regime state panel
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.8, 0.2],
            subplot_titles=(title, "Strategy State (1=ON, 0=OFF)")
        )
        equity_row = 1
    else:
        fig = go.Figure()
        equity_row = None

    # Equity curve trace
    equity_trace = go.Scatter(
        x=strategy_equity.index,
        y=strategy_equity.values,
        mode='lines',
        name='Strategy',
        line=dict(width=2.5, color='blue')
    )
    
    if show_regime:
        fig.add_trace(equity_trace, row=1, col=1)
    else:
        fig.add_trace(equity_trace)

    # Rebalance markers
    rebalance_in_backtest = [d for d in rebalance_dates if d in strategy_equity.index]
    rebalance_values = [strategy_equity.loc[d] for d in rebalance_in_backtest]

    rebalance_trace = go.Scatter(
        x=rebalance_in_backtest,
        y=rebalance_values,
        mode='markers',
        name='Rebalance',
        marker=dict(size=8, color='red', symbol='circle', line=dict(width=1, color='darkred'))
    )
    
    if show_regime:
        fig.add_trace(rebalance_trace, row=1, col=1)
    else:
        fig.add_trace(rebalance_trace)

    # Add regime state panel if available
    if show_regime:
        regime_series = snapshots_df['regime_on'].astype(int)
        
        fig.add_trace(
            go.Scatter(
                x=snapshots_df.index,
                y=regime_series.values,
                mode='lines',
                name='Strategy ON',
                line=dict(width=1, color='green', shape='hv'),
                fill='tozeroy',
                fillcolor='rgba(0, 128, 0, 0.3)',
                showlegend=False
            ),
            row=2, col=1
        )
        
        # Update layout for subplots
        fig.update_layout(
            height=800,
            margin=dict(t=120, b=100, l=60, r=40),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
            hovermode='x unified',
            hoverlabel=dict(namelength=-1, font=dict(size=12))
        )
        
        fig.update_yaxes(title_text="Portfolio Value ($)", tickformat="$,.0f", row=1, col=1)
        fig.update_yaxes(title_text="State", range=[-0.1, 1.1], row=2, col=1)
        
        fig.update_xaxes(
            rangeslider=dict(visible=True, thickness=0.08),
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=3, label="3M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(step="all", label="All")
                ]),
                y=1.08, x=0.0
            ),
            hoverformat="%Y-%m-%d",
            row=2, col=1
        )
        fig.update_xaxes(hoverformat="%Y-%m-%d", row=1, col=1)
    else:
        # Original layout without regime panel
        fig.update_layout(
            title=dict(text=title, font=dict(size=14)),
            xaxis_title="Date",
            yaxis_title="Portfolio Value ($)",
            height=700,
            margin=dict(t=80, b=80, l=60, r=40),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
            hovermode='x unified',
            hoverlabel=dict(namelength=-1, font=dict(size=12))
        )

        fig.update_yaxes(tickformat="$,.0f")

        fig.update_xaxes(
            rangeslider=dict(visible=True, thickness=0.08),
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=3, label="3M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(step="all", label="All")
                ]),
                y=1.02, x=0.0
            )
        )

    return fig


def plot_strategy_metrics(
    metrics_df: pd.DataFrame,
    drawdown_series: pd.Series,
    vol_window: int = 252,
    sharpe_window: int = 252,
    title: str = "Strategy Metrics (Rolling)"
) -> go.Figure:
    """
    Plot rolling strategy metrics (CAGR, volatility, Sharpe, drawdown).

    Parameters
    ----------
    metrics_df : pd.DataFrame
        Output from calculate_all_rolling_metrics
    drawdown_series : pd.Series
        Drawdown series (negative values) aligned to dates
    vol_window : int
        Window for volatility column (e.g., 252)
    sharpe_window : int
        Window for Sharpe column (e.g., 252)
    title : str
        Plot title

    Returns
    -------
    go.Figure
        Plotly figure object
    """
    vol_col = f"vol_{vol_window}d"
    sharpe_col = f"sharpe_{sharpe_window}d"

    fig = make_subplots(
        rows=2, cols=2,
        shared_xaxes=True,
        vertical_spacing=0.1,
        horizontal_spacing=0.08,
        subplot_titles=(
            "CAGR (since inception)",
            f"Volatility ({vol_window}d)",
            f"Sharpe ({sharpe_window}d)",
            "Drawdown (to date)"
        )
    )

    fig.add_trace(
        go.Scatter(
            x=metrics_df.index,
            y=metrics_df['cagr'],
            mode='lines',
            name='CAGR',
            line=dict(width=2, color='blue')
        ),
        row=1, col=1
    )

    if vol_col in metrics_df.columns:
        fig.add_trace(
            go.Scatter(
                x=metrics_df.index,
                y=metrics_df[vol_col],
                mode='lines',
                name='Volatility',
                line=dict(width=2, color='orange')
            ),
            row=1, col=2
        )

    if sharpe_col in metrics_df.columns:
        fig.add_trace(
            go.Scatter(
                x=metrics_df.index,
                y=metrics_df[sharpe_col],
                mode='lines',
                name='Sharpe',
                line=dict(width=2, color='green')
            ),
            row=2, col=1
        )

    fig.add_trace(
        go.Scatter(
            x=drawdown_series.index,
            y=drawdown_series.values,
            mode='lines',
            name='Drawdown',
            line=dict(width=2, color='red')
        ),
        row=2, col=2
    )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        height=800,
        margin=dict(t=80, b=80, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
        hovermode='x unified',
        hoverlabel=dict(namelength=-1, font=dict(size=12))
    )

    fig.update_yaxes(tickformat=".1%", row=1, col=1)
    fig.update_yaxes(tickformat=".1%", row=1, col=2)
    fig.update_yaxes(tickformat=".2f", row=2, col=1)
    fig.update_yaxes(tickformat=".1%", row=2, col=2)

    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.05),
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=1.02, x=0.0
        ),
        row=2, col=1
    )

    return fig


def plot_filtered_backtest(
    equity_unfiltered: pd.Series,
    equity_filtered: pd.Series,
    snapshots_filtered: pd.DataFrame,
    rebalance_dates: List[pd.Timestamp],
    drawdown_threshold: float = -0.10,
    initial_investment: float = 100000,
    title: str = "Equity Curves: With vs Without Dynamic Regime Filters"
) -> go.Figure:
    """
    Plot filtered vs unfiltered backtest with regime state and rolling metrics.
    
    Parameters
    ----------
    equity_unfiltered : pd.Series
        Equity curve without filters
    equity_filtered : pd.Series
        Equity curve with filters
    snapshots_filtered : pd.DataFrame
        Snapshots dataframe with regime_on, rolling_dd_30d, rolling_vol_30d columns
    rebalance_dates : List[pd.Timestamp]
        List of rebalance dates to mark
    drawdown_threshold : float
        Drawdown threshold for horizontal line
    initial_investment : float
        Initial investment for return calculation
        
    Returns
    -------
    go.Figure
        Plotly figure object
    """
    _require_plotly()
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.75, 0.25],
        subplot_titles=(
            title,
            "Regime State (1=Invested, 0=Cash)"
        )
    )
    
    # Panel 1: Equity curves
    fig.add_trace(
        go.Scatter(
            x=equity_unfiltered.index,
            y=equity_unfiltered.values,
            mode='lines',
            name='Without Filters',
            line=dict(width=2, color='blue'),
            opacity=0.7
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=equity_filtered.index,
            y=equity_filtered.values,
            mode='lines',
            name='With Dynamic Filters',
            line=dict(width=2, color='orange')
        ),
        row=1, col=1
    )
    
    # Rebalance markers
    rebalance_in_backtest = [d for d in rebalance_dates if d in equity_filtered.index]
    rebalance_values = [equity_filtered.loc[d] for d in rebalance_in_backtest]
    
    fig.add_trace(
        go.Scatter(
            x=rebalance_in_backtest,
            y=rebalance_values,
            mode='markers',
            name='Rebalance',
            marker=dict(size=8, color='red', symbol='circle', line=dict(width=1, color='darkred'))
        ),
        row=1, col=1
    )
    
    # Panel 2: Regime state
    regime_series = snapshots_filtered['regime_on'].astype(int)
    
    fig.add_trace(
        go.Scatter(
            x=snapshots_filtered.index,
            y=regime_series.values,
            mode='lines',
            name='Regime ON',
            line=dict(width=1, color='green', shape='hv'),
            fill='tozeroy',
            fillcolor='rgba(0, 128, 0, 0.3)',
            showlegend=False
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=700,
        margin=dict(t=80, b=100, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=11)),
        hovermode='x unified',
        hoverlabel=dict(namelength=-1, font=dict(size=12))
    )
    
    fig.update_yaxes(title_text="Portfolio Value ($)", tickformat="$,.0f", row=1, col=1)
    fig.update_yaxes(title_text="Regime ON", range=[-0.1, 1.1], row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    
    # Add range slider and selector to bottom x-axis (below the chart)
    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.05),
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=-0.25, x=0.5, xanchor='center'
        ),
        row=2, col=1
    )
    
    return fig


def plot_regime_signal_plotly(
    regime_signal: pd.Series,
    spx: pd.Series,
    spx_ma: pd.Series,
    vix: pd.Series,
    vix_threshold: float,
    spx_ma_period: int
) -> go.Figure:
    """
    Plot regime signal with SPX and VIX indicators.
    
    Parameters
    ----------
    regime_signal : pd.Series
        Boolean series indicating regime ON/OFF
    spx : pd.Series
        SPX price series
    spx_ma : pd.Series
        SPX moving average series
    vix : pd.Series
        VIX price series
    vix_threshold : float
        VIX threshold value
    spx_ma_period : int
        Moving average period for label
        
    Returns
    -------
    go.Figure
        Plotly figure object
    """
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.4, 0.3, 0.3],
        subplot_titles=(
            "SPX Price vs Moving Average",
            "VIX Level",
            "Regime Signal (1=Invested, 0=Cash)"
        )
    )
    
    # Panel 1: SPX and MA
    fig.add_trace(
        go.Scatter(x=spx.index, y=spx.values, mode='lines', name='SPX', line=dict(width=1.5, color='blue')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=spx_ma.index, y=spx_ma.values, mode='lines', name=f'SPX {spx_ma_period}d MA', 
                   line=dict(width=1.5, color='orange', dash='dash')),
        row=1, col=1
    )
    
    # Panel 2: VIX
    fig.add_trace(
        go.Scatter(x=vix.index, y=vix.values, mode='lines', name='VIX', line=dict(width=1.5, color='red')),
        row=2, col=1
    )
    fig.add_hline(y=vix_threshold, line_dash="dash", line_color="green", 
                  annotation_text=f"VIX Threshold ({vix_threshold})", row=2, col=1)
    
    # Panel 3: Regime signal
    regime_int = regime_signal.astype(int)
    fig.add_trace(
        go.Scatter(x=regime_signal.index, y=regime_int.values, mode='lines', name='Regime ON',
                   line=dict(width=1, color='green', shape='hv'), fill='tozeroy',
                   fillcolor='rgba(0, 128, 0, 0.3)', showlegend=False),
        row=3, col=1
    )
    
    fig.update_layout(
        height=700,
        hovermode='x unified',
        hoverlabel=dict(namelength=-1, font=dict(size=12))
    )
    fig.update_yaxes(title_text="SPX Price", tickformat=",.0f", row=1, col=1)
    fig.update_yaxes(title_text="VIX", tickformat=".1f", row=2, col=1)
    fig.update_yaxes(title_text="Regime", range=[-0.1, 1.1], row=3, col=1)
    fig.update_xaxes(title_text="Date", row=3, col=1)
    
    # Add range slider to bottom x-axis
    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.05),
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=1.0, x=0.0
        ),
        row=3, col=1
    )
    
    return fig


def print_performance_comparison(
    frictionless_equity: pd.Series,
    execution_equity: pd.Series,
    snapshots_df: pd.DataFrame,
    initial_investment: float
) -> None:
    """Print performance comparison metrics."""
    print("=== Performance Comparison ===")
    print(f"\nFrictionless Backtest:")
    print(f"  Final Value: ${frictionless_equity.iloc[-1]:,.2f}")
    print(f"  Total Return: {(frictionless_equity.iloc[-1] / frictionless_equity.iloc[0] - 1) * 100:.2f}%")

    print(f"\nExecution Backtest (with costs & fees):")
    print(f"  Final Value: ${execution_equity.iloc[-1]:,.2f}")
    print(f"  Total Return: {(execution_equity.iloc[-1] / execution_equity.iloc[0] - 1) * 100:.2f}%")

    print(f"\nCost Breakdown:")
    print(f"  Transaction Costs: ${snapshots_df['costs_today'].sum():,.2f}")
    print(f"  Management Fees: ${snapshots_df['mgmt_fee_today'].sum():,.2f}")
    print(f"  Total Costs: ${snapshots_df['costs_today'].sum() + snapshots_df['mgmt_fee_today'].sum():,.2f}")

    return_diff = (frictionless_equity.iloc[-1] / frictionless_equity.iloc[0] - 
                   execution_equity.iloc[-1] / execution_equity.iloc[0]) * 100
    print(f"\nImpact of Costs & Fees:")
    print(f"  Return Difference: {return_diff:.2f}%")


def print_strategy_vs_benchmarks_table(
    strategy_equity: pd.Series,
    benchmark_results: Dict[str, dict],
    trades_df: pd.DataFrame,
    initial_investment: float,
    rolling_window: int = 252
) -> None:
    """Print comparison table for strategy vs benchmarks with both expanding and rolling metrics.
    
    Parameters
    ----------
    strategy_equity : pd.Series
        Strategy equity curve
    benchmark_results : Dict[str, dict]
        Results from run_benchmark_backtest
    trades_df : pd.DataFrame
        Strategy trades
    initial_investment : float
        Initial capital
    rolling_window : int, default 252
        Rolling window for the second table (e.g., 252 for 1-year rolling).
    """
    from rolling_metrics import calculate_expanding_metrics, calculate_all_rolling_metrics
    
    # ==================== TABLE 1: EXPANDING (SINCE INCEPTION) ====================
    print(f"\n=== Performance Comparison (Since Inception, All with Transaction Costs) ===")
    print(f"\n{'Strategy':<20} {'Final Value':<14} {'Return':<9} {'CAGR':<8} {'MDD':<10} {'Sharpe':<10} {'Vol':<10} {'Trades':<10}")
    print("-" * 95)

    # Strategy expanding metrics
    strategy_exp = calculate_expanding_metrics(strategy_equity)
    strategy_return_exp = (strategy_equity.iloc[-1] / initial_investment - 1) * 100
    strategy_cagr_exp = strategy_exp['cagr'].iloc[-1] * 100 if not strategy_exp.empty else 0
    strategy_mdd_exp = strategy_exp['mdd'].iloc[-1] * 100 if not strategy_exp.empty else 0
    strategy_sharpe_exp = strategy_exp['sharpe'].iloc[-1] if not strategy_exp.empty else 0
    strategy_vol_exp = strategy_exp['vol'].iloc[-1] * 100 if not strategy_exp.empty else 0
    
    print(f"{'Momentum Strategy':<20} ${strategy_equity.iloc[-1]:>12,.2f} {strategy_return_exp:>7.2f}% {strategy_cagr_exp:>6.2f}% {strategy_mdd_exp:>7.2f}% {strategy_sharpe_exp:>7.2f} {strategy_vol_exp:>6.2f}% {len(trades_df):>7}")

    for ticker, result in benchmark_results.items():
        bench_equity = result['equity_curve']
        bench_exp = calculate_expanding_metrics(bench_equity)
        
        bench_return_exp = (result['final_value'] / initial_investment - 1) * 100
        bench_cagr_exp = bench_exp['cagr'].iloc[-1] * 100 if not bench_exp.empty else 0
        bench_mdd_exp = bench_exp['mdd'].iloc[-1] * 100 if not bench_exp.empty else 0
        bench_sharpe_exp = bench_exp['sharpe'].iloc[-1] if not bench_exp.empty else 0
        bench_vol_exp = bench_exp['vol'].iloc[-1] * 100 if not bench_exp.empty else 0
        
        print(f"{ticker + ' (B&H)':<20} ${result['final_value']:>12,.2f} {bench_return_exp:>7.2f}% {bench_cagr_exp:>6.2f}% {bench_mdd_exp:>7.2f}% {bench_sharpe_exp:>7.2f} {bench_vol_exp:>6.2f}% {'1':>7}")

    # ==================== TABLE 2: ROLLING WINDOW ====================
    print(f"\n=== Performance Comparison ({rolling_window}d Rolling, All with Transaction Costs) ===")
    print(f"\n{'Strategy':<20} {'Final Value':<14} {'Return':<9} {'CAGR':<8} {'MDD':<9} {'Sharpe':<8} {'Vol':<8} {'Trades':<8}")
    print("-" * 95)

    # Strategy rolling metrics
    strategy_roll = calculate_all_rolling_metrics(strategy_equity, windows=[rolling_window])
    strategy_return_roll = strategy_roll[f'return_{rolling_window}d'].iloc[-1] * 100 if f'return_{rolling_window}d' in strategy_roll else 0
    strategy_cagr_roll = strategy_roll['cagr'].iloc[-1] * 100 if 'cagr' in strategy_roll else 0
    strategy_mdd_roll = strategy_roll[f'dd_{rolling_window}d'].iloc[-1] * 100 if f'dd_{rolling_window}d' in strategy_roll else 0
    strategy_sharpe_roll = strategy_roll[f'sharpe_{rolling_window}d'].iloc[-1] if f'sharpe_{rolling_window}d' in strategy_roll else 0
    strategy_vol_roll = strategy_roll[f'vol_{rolling_window}d'].iloc[-1] * 100 if f'vol_{rolling_window}d' in strategy_roll else 0
    
    print(f"{'Momentum Strategy':<20} ${strategy_equity.iloc[-1]:>12,.2f} {strategy_return_roll:>7.2f}% {strategy_cagr_roll:>6.2f}% {strategy_mdd_roll:>7.2f}% {strategy_sharpe_roll:>7.2f} {strategy_vol_roll:>6.2f}% {len(trades_df):>7}")

    for ticker, result in benchmark_results.items():
        bench_equity = result['equity_curve']
        bench_roll = calculate_all_rolling_metrics(bench_equity, windows=[rolling_window])
        
        bench_return_roll = bench_roll[f'return_{rolling_window}d'].iloc[-1] * 100 if f'return_{rolling_window}d' in bench_roll else 0
        bench_cagr_roll = bench_roll['cagr'].iloc[-1] * 100 if 'cagr' in bench_roll else 0
        bench_mdd_roll = bench_roll[f'dd_{rolling_window}d'].iloc[-1] * 100 if f'dd_{rolling_window}d' in bench_roll else 0
        bench_sharpe_roll = bench_roll[f'sharpe_{rolling_window}d'].iloc[-1] if f'sharpe_{rolling_window}d' in bench_roll else 0
        bench_vol_roll = bench_roll[f'vol_{rolling_window}d'].iloc[-1] * 100 if f'vol_{rolling_window}d' in bench_roll else 0
        
        print(f"{ticker + ' (B&H)':<20} ${result['final_value']:>12,.2f} {bench_return_roll:>7.2f}% {bench_cagr_roll:>6.2f}% {bench_mdd_roll:>7.2f}% {bench_sharpe_roll:>7.2f} {bench_vol_roll:>6.2f}% {'1':>7}")


def print_filtered_comparison(
    equity_unfiltered: pd.Series,
    equity_filtered: pd.Series,
    initial_investment: float
) -> None:
    """Print filtered vs unfiltered performance comparison."""
    print("\n=== Performance Comparison ===")
    print(f"Without filters: ${equity_unfiltered.iloc[-1]:,.2f} ({(equity_unfiltered.iloc[-1]/initial_investment-1)*100:.2f}%)")
    print(f"With filters:    ${equity_filtered.iloc[-1]:,.2f} ({(equity_filtered.iloc[-1]/initial_investment-1)*100:.2f}%)")
    print(f"Difference:      ${equity_filtered.iloc[-1] - equity_unfiltered.iloc[-1]:,.2f}")


def plot_expanding_metrics(
    expanding_df: pd.DataFrame,
    title: str = "Expanding Metrics (Since Inception)",
    sharpe_ylim: float = 4.0,
    cagr_ylim: float = None
) -> go.Figure:
    """
    Plot expanding metrics in a 4x1 vertical layout.

    Parameters
    ----------
    expanding_df : pd.DataFrame
        Output from calculate_expanding_metrics with columns:
        cagr, vol, sharpe, drawdown, mdd
    title : str
        Plot title
    sharpe_ylim : float
        Maximum y-axis limit for Sharpe ratio plot
    cagr_ylim : float, optional
        Maximum y-axis limit for CAGR plot (e.g., 0.8 for 80%)

    Returns
    -------
    go.Figure
        Plotly figure object
    """
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=(
            "CAGR (Annualized, Since Inception)",
            "Volatility (Expanding, Annualized)",
            "Sharpe Ratio (Expanding, Annualized)",
            "Drawdown & MDD (Expanding)"
        )
    )

    # CAGR
    fig.add_trace(
        go.Scatter(x=expanding_df.index, y=expanding_df['cagr'], mode='lines',
                   name='CAGR', line=dict(width=2, color='blue')),
        row=1, col=1
    )

    # Volatility
    fig.add_trace(
        go.Scatter(x=expanding_df.index, y=expanding_df['vol'], mode='lines',
                   name='Volatility', line=dict(width=2, color='orange')),
        row=2, col=1
    )

    # Sharpe
    fig.add_trace(
        go.Scatter(x=expanding_df.index, y=expanding_df['sharpe'], mode='lines',
                   name='Sharpe', line=dict(width=2, color='green')),
        row=3, col=1
    )

    # Drawdown & MDD
    fig.add_trace(
        go.Scatter(x=expanding_df.index, y=expanding_df['drawdown'], mode='lines',
                   name='Drawdown', line=dict(width=1.5, color='red')),
        row=4, col=1
    )
    fig.add_trace(
        go.Scatter(x=expanding_df.index, y=expanding_df['mdd'], mode='lines',
                   name='MDD', line=dict(width=2, color='darkred', dash='dash')),
        row=4, col=1
    )

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        height=900,
        margin=dict(t=80, b=60, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=10)),
        hovermode='x unified'
    )

    fig.update_yaxes(
        tickformat=".1%", title_text="CAGR", 
        range=[0, cagr_ylim] if cagr_ylim else None,
        row=1, col=1
    )
    fig.update_yaxes(tickformat=".1%", title_text="Volatility", row=2, col=1)
    fig.update_yaxes(tickformat=".2f", title_text="Sharpe", range=[-sharpe_ylim, sharpe_ylim], row=3, col=1)
    fig.update_yaxes(tickformat=".1%", title_text="Drawdown", row=4, col=1)

    return fig


def plot_rolling_metrics(
    rolling_df: pd.DataFrame,
    window: int,
    volatility_threshold: Optional[float] = None,
    sharpe_ylim: float = 4.0,
    title: Optional[str] = None
) -> go.Figure:
    """
    Plot rolling metrics (drawdown, volatility, sharpe) in a 3x1 vertical layout.

    Parameters
    ----------
    rolling_df : pd.DataFrame
        Output from calculate_all_rolling_metrics with columns:
        dd_{window}d, vol_{window}d, sharpe_{window}d
    window : int
        Rolling window in days (for column name lookup and title)
    volatility_threshold : float, optional
        If provided, adds a horizontal threshold line on volatility plot
    sharpe_ylim : float
        Maximum y-axis limit for Sharpe ratio plot
    title : str, optional
        Plot title (defaults to "Rolling {window}-Day Metrics")

    Returns
    -------
    go.Figure
        Plotly figure object
    """
    if title is None:
        title = f"Rolling {window}-Day Metrics"

    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=(
            f"Rolling {window}-Day Drawdown",
            f"Rolling {window}-Day Volatility (Annualized)",
            f"Rolling {window}-Day Sharpe Ratio"
        )
    )

    # Drawdown
    fig.add_trace(
        go.Scatter(
            x=rolling_df.index,
            y=rolling_df[f'dd_{window}d'],
            mode='lines',
            name='Drawdown',
            line=dict(width=2, color='red'),
            fill='tozeroy',
            fillcolor='rgba(255, 0, 0, 0.1)'
        ),
        row=1, col=1
    )

    # Volatility
    fig.add_trace(
        go.Scatter(
            x=rolling_df.index,
            y=rolling_df[f'vol_{window}d'],
            mode='lines',
            name='Volatility',
            line=dict(width=2, color='orange')
        ),
        row=2, col=1
    )
    # Add threshold line for volatility if provided
    if volatility_threshold is not None:
        fig.add_hline(y=volatility_threshold, line_dash="dash", line_color="gray",
                      annotation_text=f"Threshold ({volatility_threshold:.0%})", row=2, col=1)

    # Sharpe
    fig.add_trace(
        go.Scatter(
            x=rolling_df.index,
            y=rolling_df[f'sharpe_{window}d'],
            mode='lines',
            name='Sharpe',
            line=dict(width=2, color='green')
        ),
        row=3, col=1
    )
    # Add zero line for Sharpe
    fig.add_hline(y=0, line_dash="dot", line_color="gray", row=3, col=1)

    fig.update_layout(
        title=dict(text=title, font=dict(size=14)),
        height=700,
        margin=dict(t=80, b=60, l=60, r=40),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, font=dict(size=10)),
        hovermode='x unified',
        showlegend=True
    )

    fig.update_yaxes(tickformat=".1%", title_text="Drawdown", row=1, col=1)
    fig.update_yaxes(tickformat=".1%", title_text="Volatility", row=2, col=1)
    fig.update_yaxes(tickformat=".2f", title_text="Sharpe", range=[-sharpe_ylim, sharpe_ylim], row=3, col=1)

    # Add range slider to bottom subplot only
    fig.update_xaxes(
        rangeslider=dict(visible=True, thickness=0.05),
        rangeselector=dict(
            buttons=list([
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(count=1, label="YTD", step="year", stepmode="todate"),
                dict(count=1, label="1Y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            y=1.02, x=0.0
        ),
        row=3, col=1
    )

    return fig


# ---------------------------------------------------------------------------
# Efficient frontier: Return vs Volatility scatter + Pareto front
# ---------------------------------------------------------------------------

def plot_efficient_frontier(
    results_df: pd.DataFrame,
    x_col: str = 'volatility',
    y_col: str = 'total_return',
    pareto_df: pd.DataFrame = None,
    benchmark_points: Dict = None,
    title: str = "Filter Optimization: Return vs Volatility",
) -> go.Figure:
    """Scatter plot of sweep results with Pareto front and benchmarks.

    Parameters
    ----------
    results_df : pd.DataFrame
        Full sweep results with at least x_col and y_col columns.
    x_col : str
        Column for X-axis (default: 'volatility').
    y_col : str
        Column for Y-axis (default: 'total_return').
    pareto_df : pd.DataFrame, optional
        Pareto-optimal subset of results_df.
    benchmark_points : dict, optional
        {'SPY B&H': {'volatility': ..., 'total_return': ...}, ...}
    title : str
        Chart title.

    Returns
    -------
    go.Figure
    """
    fig = go.Figure()

    # Classify points by filter type for color coding
    valid = results_df.dropna(subset=[x_col, y_col]).copy()

    def _filter_type(row):
        dd = row.get('dd_enabled', False)
        vol = row.get('vol_enabled', False)
        if not dd and not vol:
            return 'No Filter'
        elif dd and not vol:
            return 'DD Only'
        elif not dd and vol:
            return 'Vol Only'
        else:
            return 'DD + Vol'

    valid['filter_type'] = valid.apply(_filter_type, axis=1)

    colors = {
        'No Filter': '#636EFA',
        'DD Only': '#EF553B',
        'Vol Only': '#00CC96',
        'DD + Vol': '#AB63FA',
    }

    for ftype, color in colors.items():
        subset = valid[valid['filter_type'] == ftype]
        if subset.empty:
            continue

        hover_parts = []
        for _, row in subset.iterrows():
            parts = [f"Return: {row[y_col]:.2%}", f"Vol: {row[x_col]:.2%}"]
            if 'sharpe' in row:
                parts.append(f"Sharpe: {row['sharpe']:.2f}")
            if 'mdd' in row:
                parts.append(f"MDD: {row['mdd']:.2%}")
            if row.get('dd_enabled'):
                parts.append(f"DD: w={int(row['dd_window'])}, th={row['dd_threshold']:.1%}, src={row['dd_source']}")
            if row.get('vol_enabled'):
                parts.append(f"Vol: w={int(row['vol_window'])}, th={row['vol_threshold']:.0%}, src={row['vol_source']}")
            hover_parts.append('<br>'.join(parts))

        fig.add_trace(go.Scattergl(
            x=subset[x_col] * 100,
            y=subset[y_col] * 100,
            mode='markers',
            name=ftype,
            marker=dict(color=color, size=3, opacity=0.4),
            text=hover_parts,
            hoverinfo='text',
        ))

    # Pareto front
    if pareto_df is not None and not pareto_df.empty:
        pareto_sorted = pareto_df.sort_values(x_col)
        fig.add_trace(go.Scatter(
            x=pareto_sorted[x_col] * 100,
            y=pareto_sorted[y_col] * 100,
            mode='lines+markers',
            name='Pareto Front',
            marker=dict(color='gold', size=8, symbol='star',
                        line=dict(color='black', width=1)),
            line=dict(color='gold', width=2, dash='dash'),
        ))

    # Benchmark points (added last so they render on top)
    bench_colors = ['#FF6600', '#0066FF', '#00CC00', '#CC0000']
    if benchmark_points:
        for i, (name, point) in enumerate(benchmark_points.items()):
            bc = bench_colors[i % len(bench_colors)]
            fig.add_trace(go.Scatter(
                x=[point['volatility'] * 100],
                y=[point['total_return'] * 100],
                mode='markers+text',
                name=name,
                marker=dict(size=18, symbol='diamond', color=bc,
                            line=dict(color='black', width=2)),
                text=[name],
                textposition='top center',
                textfont=dict(size=12, color='black'),
            ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        xaxis_title='Strategy Volatility (%)',
        yaxis_title='Total Return (%)',
        template='plotly_white',
        width=1100,
        height=700,
        legend=dict(
            yanchor='top', y=0.99,
            xanchor='left', x=0.01,
            bgcolor='rgba(255,255,255,0.8)',
        ),
        hovermode='closest',
    )

    return fig
