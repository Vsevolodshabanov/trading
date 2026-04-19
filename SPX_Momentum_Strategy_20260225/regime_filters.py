"""Configurable regime filters for dynamic risk management.

Supports multiple filter types that can be combined:
1. SPX/VIX filter - market regime based on SPX MA and VIX level
2. Drawdown filter - turns OFF when rolling drawdown exceeds threshold
3. Volatility filter - turns OFF when rolling volatility exceeds threshold

Filters can be based on STRATEGY metrics or BENCHMARK metrics (SPY, SPX, etc.)
"""

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Literal
from dataclasses import dataclass

from rolling_metrics import RollingMetricsTracker


@dataclass
class FilterState:
    """State of a single filter."""
    name: str
    is_on: bool  # True = invested, False = cash
    value: Optional[float]  # Current metric value
    threshold: Optional[float]  # Threshold that triggered
    source: str  # 'STRATEGY', 'SPY', etc.


class BaseRegimeFilter(ABC):
    """Abstract base class for regime filters."""
    
    def __init__(self, name: str, enabled: bool = True):
        self.name = name
        self.enabled = enabled
    
    @abstractmethod
    def get_signal(self, date: pd.Timestamp, metrics_tracker: RollingMetricsTracker) -> FilterState:
        """Get filter signal for a given date.
        
        Parameters
        ----------
        date : pd.Timestamp
            Current date
        metrics_tracker : RollingMetricsTracker
            Tracker with rolling metrics for strategy and benchmarks
            
        Returns
        -------
        FilterState
            Current state of this filter
        """
        pass


class SpxVixFilter(BaseRegimeFilter):
    """Filter based on SPX moving average and VIX level.
    
    Signal is ON when:
    - SPX > SPX MA (uptrend)
    - VIX < threshold (low fear)
    """
    
    def __init__(
        self,
        enabled: bool = True,
        spx_ma_period: int = 200,
        vix_threshold: float = 25.0,
        spx_ticker: str = '^GSPC',
        vix_ticker: str = '^VIX'
    ):
        super().__init__(name='SPX_VIX', enabled=enabled)
        self.spx_ma_period = spx_ma_period
        self.vix_threshold = vix_threshold
        self.spx_ticker = spx_ticker
        self.vix_ticker = vix_ticker
        
        # Pre-loaded data (will be loaded on first use)
        self._spx_data: Optional[pd.Series] = None
        self._vix_data: Optional[pd.Series] = None
        self._spx_ma: Optional[pd.Series] = None
        self._signal: Optional[pd.Series] = None
    
    def load_data(self, start_date: str, end_date):
        """Load SPX and VIX data."""
        import yfinance as yf

        spx = yf.Ticker(self.spx_ticker)
        vix = yf.Ticker(self.vix_ticker)
        
        # Need extra history for MA calculation
        ma_start = pd.to_datetime(start_date) - pd.Timedelta(days=self.spx_ma_period * 2)
        
        spx_raw = spx.history(start=ma_start, end=end_date)['Close']
        vix_raw = vix.history(start=ma_start, end=end_date)['Close']

        # Normalize to date-only index: yfinance returns ^GSPC as America/New_York
        # and ^VIX as America/Chicago (different UTC hours), breaking & alignment
        spx_raw.index = pd.to_datetime(spx_raw.index.date)
        vix_raw.index = pd.to_datetime(vix_raw.index.date)
        self._spx_data = spx_raw
        self._vix_data = vix_raw

        # Calculate MA
        self._spx_ma = self._spx_data.rolling(window=self.spx_ma_period).mean()

        # Pre-calculate signal using previous day's data (no lookahead)
        spx_above_ma = self._spx_data > self._spx_ma
        vix_below_threshold = self._vix_data < self.vix_threshold
        self._signal = (spx_above_ma & vix_below_threshold).shift(1)
    
    def get_signal(self, date: pd.Timestamp, metrics_tracker: RollingMetricsTracker = None) -> FilterState:
        """Get filter signal."""
        if not self.enabled:
            return FilterState(
                name=self.name,
                is_on=True,  # Disabled = always ON
                value=None,
                threshold=None,
                source='N/A'
            )
        
        if self._signal is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        # Normalize date to timezone-naive for comparison (yfinance returns tz-aware)
        date_naive = pd.Timestamp(date).tz_localize(None) if date.tzinfo is None else pd.Timestamp(date).tz_convert(None)
        signal_index_naive = self._signal.index.tz_localize(None) if self._signal.index.tz is not None else self._signal.index
        
        # Find the closest date in our data
        if date_naive in signal_index_naive:
            idx = signal_index_naive.get_loc(date_naive)
            is_on = bool(self._signal.iloc[idx])
        else:
            # Get previous available date
            available_dates = signal_index_naive[signal_index_naive <= date_naive]
            if len(available_dates) == 0:
                is_on = True  # Default to ON if no data
            else:
                idx = signal_index_naive.get_loc(available_dates[-1])
                is_on = bool(self._signal.iloc[idx])
        
        # Get current values for reporting
        vix_value = None
        if self._vix_data is not None:
            vix_index_naive = self._vix_data.index.tz_localize(None) if self._vix_data.index.tz is not None else self._vix_data.index
            if date_naive in vix_index_naive:
                idx = vix_index_naive.get_loc(date_naive)
                vix_value = self._vix_data.iloc[idx]
        
        return FilterState(
            name=self.name,
            is_on=is_on,
            value=vix_value,
            threshold=self.vix_threshold,
            source='VIX'
        )
    
    def get_precomputed_signal(self) -> pd.Series:
        """Return the pre-computed signal series."""
        return self._signal


class DrawdownFilter(BaseRegimeFilter):
    """Filter based on rolling drawdown threshold.
    
    Signal is ON when rolling drawdown > threshold (less negative).
    e.g., threshold=-0.10 means filter turns OFF when DD < -10%
    """
    
    def __init__(
        self,
        enabled: bool = True,
        window: int = 30,
        threshold: float = -0.10,
        source: str = 'STRATEGY'
    ):
        super().__init__(name='DRAWDOWN', enabled=enabled)
        self.window = window
        self.threshold = threshold
        self.source = source
    
    def get_signal(self, date: pd.Timestamp, metrics_tracker: RollingMetricsTracker) -> FilterState:
        """Get filter signal."""
        if not self.enabled:
            return FilterState(
                name=self.name,
                is_on=True,
                value=None,
                threshold=None,
                source='N/A'
            )
        
        metrics = metrics_tracker.get_metrics(self.source, self.window)
        drawdown = metrics['drawdown']
        
        if drawdown is None:
            # Not enough data yet, default to ON
            is_on = True
        else:
            # Signal ON when drawdown is above (less negative than) threshold
            is_on = drawdown > self.threshold
        
        return FilterState(
            name=self.name,
            is_on=is_on,
            value=drawdown,
            threshold=self.threshold,
            source=self.source
        )


class VolatilityFilter(BaseRegimeFilter):
    """Filter based on rolling volatility threshold.
    
    Signal is ON when rolling volatility < threshold.
    e.g., threshold=0.40 means filter turns OFF when vol > 40%
    """
    
    def __init__(
        self,
        enabled: bool = True,
        window: int = 30,
        threshold: float = 0.40,
        source: str = 'STRATEGY'
    ):
        super().__init__(name='VOLATILITY', enabled=enabled)
        self.window = window
        self.threshold = threshold
        self.source = source
    
    def get_signal(self, date: pd.Timestamp, metrics_tracker: RollingMetricsTracker) -> FilterState:
        """Get filter signal."""
        if not self.enabled:
            return FilterState(
                name=self.name,
                is_on=True,
                value=None,
                threshold=None,
                source='N/A'
            )
        
        metrics = metrics_tracker.get_metrics(self.source, self.window)
        volatility = metrics['volatility']
        
        if volatility is None:
            # Not enough data yet, default to ON
            is_on = True
        else:
            # Signal ON when volatility is below threshold
            is_on = volatility < self.threshold
        
        return FilterState(
            name=self.name,
            is_on=is_on,
            value=volatility,
            threshold=self.threshold,
            source=self.source
        )


class CombinedRegimeFilter:
    """Combines multiple filters with configurable logic.
    
    Supports two combination modes:
    - 'all': Signal OFF only if ALL filters are OFF (lenient)
    - 'any': Signal OFF if ANY filter is OFF (strict)
    """
    
    def __init__(self, logic: Literal['all', 'any'] = 'any'):
        """Initialize combined filter.
        
        Parameters
        ----------
        logic : str
            'any' = OFF if ANY filter triggers (default, more protective)
            'all' = OFF only if ALL filters trigger
        """
        self.logic = logic
        self.filters: List[BaseRegimeFilter] = []
    
    def add_filter(self, filter_instance: BaseRegimeFilter):
        """Add a filter to the combination."""
        self.filters.append(filter_instance)
    
    def get_signal(self, date: pd.Timestamp, metrics_tracker: RollingMetricsTracker) -> tuple[bool, Dict[str, FilterState]]:
        """Get combined signal from all filters.
        
        Parameters
        ----------
        date : pd.Timestamp
            Current date
        metrics_tracker : RollingMetricsTracker
            Tracker with rolling metrics
            
        Returns
        -------
        tuple[bool, Dict[str, FilterState]]
            (is_on, {filter_name: FilterState})
            is_on = True means INVESTED, False means CASH
        """
        if not self.filters:
            return True, {}
        
        filter_states = {}
        signals = []
        
        for f in self.filters:
            state = f.get_signal(date, metrics_tracker)
            filter_states[f.name] = state
            if f.enabled:
                signals.append(state.is_on)
        
        if not signals:
            # All filters disabled
            return True, filter_states
        
        # Combine signals
        if self.logic == 'any':
            # OFF if ANY filter is OFF
            combined_signal = all(signals)
        else:  # 'all'
            # OFF only if ALL filters are OFF
            combined_signal = any(signals)
        
        return combined_signal, filter_states
    
    def get_enabled_filter_names(self) -> List[str]:
        """Get names of enabled filters."""
        return [f.name for f in self.filters if f.enabled]


def create_regime_filter_from_config(config) -> CombinedRegimeFilter:
    """Factory function to create CombinedRegimeFilter from config.
    
    Parameters
    ----------
    config : module
        Config module with filter parameters
        
    Returns
    -------
    CombinedRegimeFilter
        Configured filter instance
    """
    # Get combination logic from config (default: 'any')
    logic = getattr(config, 'REGIME_FILTER_LOGIC', 'any')
    combined = CombinedRegimeFilter(logic=logic)
    
    # SPX/VIX Filter
    if getattr(config, 'REGIME_SPX_VIX_ENABLED', False):
        spx_vix = SpxVixFilter(
            enabled=True,
            spx_ma_period=getattr(config, 'SPX_MA_PERIOD', 200),
            vix_threshold=getattr(config, 'VIX_THRESHOLD', 25),
            spx_ticker=getattr(config, 'SPX_TICKER', '^GSPC'),
            vix_ticker=getattr(config, 'VIX_TICKER', '^VIX')
        )
        combined.add_filter(spx_vix)
    
    # Drawdown Filter
    if getattr(config, 'REGIME_DRAWDOWN_ENABLED', False):
        dd_filter = DrawdownFilter(
            enabled=True,
            window=getattr(config, 'DRAWDOWN_WINDOW', 30),
            threshold=getattr(config, 'DRAWDOWN_THRESHOLD', -0.10),
            source=getattr(config, 'DRAWDOWN_SOURCE', 'STRATEGY')
        )
        combined.add_filter(dd_filter)
    
    # Volatility Filter
    if getattr(config, 'REGIME_VOLATILITY_ENABLED', False):
        vol_filter = VolatilityFilter(
            enabled=True,
            window=getattr(config, 'VOLATILITY_WINDOW', 30),
            threshold=getattr(config, 'VOLATILITY_THRESHOLD', 0.40),
            source=getattr(config, 'VOLATILITY_SOURCE', 'STRATEGY')
        )
        combined.add_filter(vol_filter)
    
    return combined
