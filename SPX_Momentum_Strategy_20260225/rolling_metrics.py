"""Rolling metrics calculation for strategy and benchmarks.

Calculates daily rolling metrics that can be used for:
1. Dynamic regime filtering (drawdown, volatility thresholds)
2. Performance monitoring and comparison
3. Risk management

All calculations use PREVIOUS day's data to avoid lookahead bias.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union


def calculate_rolling_drawdown(equity_curve: pd.Series, window: int) -> pd.Series:
    """Calculate rolling drawdown from rolling peak.
    
    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio/benchmark values
    window : int
        Rolling window in days
        
    Returns
    -------
    pd.Series
        Rolling drawdown (negative values, e.g., -0.10 = -10%)
    """
    rolling_max = equity_curve.rolling(window=window, min_periods=window).max()
    drawdown = equity_curve / rolling_max - 1.0
    return drawdown


def calculate_rolling_volatility(returns: pd.Series, window: int, annualize: bool = True) -> pd.Series:
    """Calculate rolling volatility (annualized by default).
    
    Parameters
    ----------
    returns : pd.Series
        Daily returns
    window : int
        Rolling window in days
    annualize : bool
        If True, multiply by sqrt(252) for annualized volatility
        
    Returns
    -------
    pd.Series
        Rolling volatility
    """
    rolling_std = returns.rolling(window=window, min_periods=window).std()
    if annualize:
        rolling_std = rolling_std * np.sqrt(252)
    return rolling_std


def calculate_rolling_sharpe(returns: pd.Series, window: int) -> pd.Series:
    """Calculate rolling Sharpe ratio (no risk-free rate).
    
    Parameters
    ----------
    returns : pd.Series
        Daily returns
    window : int
        Rolling window in days
        
    Returns
    -------
    pd.Series
        Rolling Sharpe ratio (annualized)
    """
    rolling_mean = returns.rolling(window=window, min_periods=window).mean()
    rolling_std = returns.rolling(window=window, min_periods=window).std()
    
    # Avoid division by zero
    sharpe = np.where(rolling_std > 0, rolling_mean / rolling_std * np.sqrt(252), np.nan)
    return pd.Series(sharpe, index=returns.index)


def calculate_rolling_return(equity_curve: pd.Series, window: int) -> pd.Series:
    """Calculate rolling return over window.
    
    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio/benchmark values
    window : int
        Rolling window in days
        
    Returns
    -------
    pd.Series
        Rolling return (e.g., 0.05 = 5%)
    """
    return equity_curve.pct_change(periods=window)


def calculate_cagr_since_inception(equity_curve: pd.Series) -> pd.Series:
    """Calculate annualized CAGR from inception to each day.

    CAGR = (P(t)/P(0))^(252/N) - 1
    where N = number of trading days since start

    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio/benchmark values

    Returns
    -------
    pd.Series
        Annualized CAGR from inception
    """
    initial_value = equity_curve.iloc[0]

    # N = number of trading days since start (index position)
    n_days = pd.Series(range(len(equity_curve)), index=equity_curve.index, dtype=float)
    # Avoid division by zero for first day
    n_days = n_days.replace(0, np.nan)

    # Total return from inception
    total_return = equity_curve / initial_value - 1

    # Annualized CAGR: (1 + return)^(252/N) - 1
    cagr = (1 + total_return) ** (252 / n_days) - 1

    return cagr


def calculate_expanding_metrics(equity_curve: pd.Series) -> pd.DataFrame:
    """Calculate expanding (whole-series) metrics up to each day.

    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio/benchmark values (index=dates)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - cagr: CAGR from inception (non-annualized)
        - vol: expanding volatility (annualized)
        - sharpe: expanding Sharpe ratio (annualized)
        - drawdown: drawdown from all-time high to date
        - return: total return from inception
    """
    returns = equity_curve.pct_change().fillna(0)

    expanding_mean = returns.expanding(min_periods=2).mean()
    expanding_std = returns.expanding(min_periods=2).std()

    vol = expanding_std * np.sqrt(252)
    sharpe = np.where(expanding_std > 0, expanding_mean / expanding_std * np.sqrt(252), np.nan)
    sharpe = pd.Series(sharpe, index=returns.index)

    expanding_max = equity_curve.expanding(min_periods=1).max()
    drawdown = equity_curve / expanding_max - 1.0
    mdd = drawdown.expanding(min_periods=1).min()

    cagr = calculate_cagr_since_inception(equity_curve)
    total_return = equity_curve / equity_curve.iloc[0] - 1

    return pd.DataFrame(
        {
            'cagr': cagr,
            'vol': vol,
            'sharpe': sharpe,
            'drawdown': drawdown,
            'mdd': mdd,
            'return': total_return
        },
        index=equity_curve.index
    )


def calculate_rolling_mdd(equity_curve: pd.Series, window: int) -> pd.Series:
    """Calculate rolling maximum drawdown over a window."""
    rolling_max = equity_curve.rolling(window=window, min_periods=window).max()
    drawdown = equity_curve / rolling_max - 1.0
    return drawdown.rolling(window=window, min_periods=window).min()


def calculate_all_rolling_metrics(
    equity_curve: pd.Series,
    windows: List[int] = [30, 60, 252]
) -> pd.DataFrame:
    """Calculate all rolling metrics for a single equity curve.
    
    Parameters
    ----------
    equity_curve : pd.Series
        Daily portfolio/benchmark values (index=dates)
    windows : List[int]
        List of rolling windows to calculate metrics for
        
    Returns
    -------
    pd.DataFrame
        DataFrame with columns for each metric and window combination:
        - dd_{window}d: rolling drawdown
        - vol_{window}d: rolling volatility (annualized)
        - sharpe_{window}d: rolling Sharpe ratio
        - return_{window}d: rolling return
        Plus non-windowed metrics:
        - cagr: CAGR from inception (annualized)
    """
    # Calculate daily returns
    returns = equity_curve.pct_change().fillna(0)
    
    results = {}
    
    # CAGR from inception (not rolling)
    results['cagr'] = calculate_cagr_since_inception(equity_curve)
    
    for window in windows:
        results[f'dd_{window}d'] = calculate_rolling_drawdown(equity_curve, window)
        results[f'vol_{window}d'] = calculate_rolling_volatility(returns, window)
        results[f'sharpe_{window}d'] = calculate_rolling_sharpe(returns, window)
        results[f'return_{window}d'] = calculate_rolling_return(equity_curve, window)
    
    return pd.DataFrame(results, index=equity_curve.index)


def calculate_metrics_for_multiple_series(
    series_dict: Dict[str, pd.Series],
    windows: List[int] = [30, 60, 252]
) -> Dict[str, pd.DataFrame]:
    """Calculate rolling metrics for multiple equity curves (strategy + benchmarks).
    
    Parameters
    ----------
    series_dict : Dict[str, pd.Series]
        Dictionary mapping names to equity curves
        e.g., {'STRATEGY': strategy_equity, 'SPY': spy_equity, 'SPMO': spmo_equity}
    windows : List[int]
        List of rolling windows
        
    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary mapping names to their metrics DataFrames
    """
    return {
        name: calculate_all_rolling_metrics(equity, windows)
        for name, equity in series_dict.items()
    }


def get_metric_value(
    metrics_dict: Dict[str, pd.DataFrame],
    source: str,
    metric_name: str,
    date: pd.Timestamp,
    use_previous_day: bool = True
) -> Optional[float]:
    """Get a specific metric value for a given source and date.
    
    Parameters
    ----------
    metrics_dict : Dict[str, pd.DataFrame]
        Output from calculate_metrics_for_multiple_series
    source : str
        Which series to get metric from ('STRATEGY', 'SPY', etc.)
    metric_name : str
        Which metric (e.g., 'dd_30d', 'vol_30d')
    date : pd.Timestamp
        Date to get metric for
    use_previous_day : bool
        If True, return previous day's value to avoid lookahead bias
        
    Returns
    -------
    float or None
        Metric value, or None if not available
    """
    if source not in metrics_dict:
        return None
    
    df = metrics_dict[source]
    
    if metric_name not in df.columns:
        return None
    
    if use_previous_day:
        # Get the index position of the date
        if date not in df.index:
            return None
        idx = df.index.get_loc(date)
        if idx == 0:
            return None
        prev_date = df.index[idx - 1]
        return df.loc[prev_date, metric_name]
    else:
        if date not in df.index:
            return None
        return df.loc[date, metric_name]


class RollingMetricsTracker:
    """Track rolling metrics incrementally during backtest.
    
    This class allows calculating rolling metrics on-the-fly during a backtest,
    without needing the full equity curve upfront.
    """
    
    def __init__(self, windows: List[int] = [30, 60, 252]):
        """Initialize tracker.
        
        Parameters
        ----------
        windows : List[int]
            Rolling windows to track
        """
        self.windows = windows
        self.max_window = max(windows)
        
        # Storage for each tracked series
        self._data: Dict[str, List[tuple]] = {}  # {name: [(date, value), ...]}
    
    def update(self, name: str, date: pd.Timestamp, value: float):
        """Add a new data point.
        
        Parameters
        ----------
        name : str
            Series name ('STRATEGY', 'SPY', etc.)
        date : pd.Timestamp
            Date
        value : float
            Equity value
        """
        if name not in self._data:
            self._data[name] = []
        
        self._data[name].append((date, value))
        
        # Keep only last max_window + 1 points (need +1 for returns calculation)
        if len(self._data[name]) > self.max_window + 10:
            self._data[name] = self._data[name][-(self.max_window + 10):]
    
    def get_metrics(self, name: str, window: int) -> Dict[str, Optional[float]]:
        """Get current rolling metrics for a series.
        
        Parameters
        ----------
        name : str
            Series name
        window : int
            Rolling window
            
        Returns
        -------
        Dict[str, Optional[float]]
            Dictionary with 'drawdown', 'volatility', 'sharpe', 'return', 'cagr'
            Note: cagr is from inception, not rolling
        """
        if name not in self._data or len(self._data[name]) < 2:
            return {'drawdown': None, 'volatility': None, 'sharpe': None, 'return': None, 'cagr': None}
        
        # Convert to series
        data = self._data[name]
        dates, values = zip(*data)
        equity = pd.Series(values, index=pd.DatetimeIndex(dates))
        
        # Get the available window (might be less than requested at start)
        actual_window = min(window, len(equity))
        
        if actual_window < 2:
            return {'drawdown': None, 'volatility': None, 'sharpe': None, 'return': None, 'cagr': None}
        
        # Use last 'actual_window' points for rolling metrics
        equity_window = equity.iloc[-actual_window:]
        returns_window = equity_window.pct_change().dropna()
        
        # Calculate rolling metrics
        rolling_max = equity_window.max()
        current_value = equity_window.iloc[-1]
        drawdown = current_value / rolling_max - 1.0 if rolling_max > 0 else 0
        
        if len(returns_window) > 1:
            volatility = returns_window.std() * np.sqrt(252)
            mean_return = returns_window.mean()
            sharpe = mean_return / returns_window.std() * np.sqrt(252) if returns_window.std() > 0 else np.nan
        else:
            volatility = None
            sharpe = None
        
        # Total return over window
        total_return = (current_value / equity_window.iloc[0] - 1) if equity_window.iloc[0] > 0 else 0
        
        # Annualized CAGR from INCEPTION: (P(t)/P(0))^(252/N) - 1
        # where N = trading days since start
        initial_value = equity.iloc[0]
        n_days = len(equity) - 1  # Number of trading days since start
        total_return_inception = (current_value / initial_value - 1) if initial_value > 0 else 0
        cagr = (1 + total_return_inception) ** (252 / n_days) - 1 if n_days > 0 else None
        
        return {
            'drawdown': drawdown,
            'volatility': volatility,
            'sharpe': sharpe,
            'return': total_return,
            'cagr': cagr
        }
