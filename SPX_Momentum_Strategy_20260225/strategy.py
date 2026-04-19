"""Momentum strategy implementation."""

from __future__ import annotations

import numpy as np
import polars as pl
import matplotlib.pyplot as plt

from polars_bridge import DATE_COL, ensure_pandas_series, ensure_polars_frame


def _asset_columns(frame: pl.DataFrame) -> list[str]:
    return [col for col in frame.columns if col != DATE_COL]


def _percentile_rank_values(values: np.ndarray) -> np.ndarray:
    """Compute percentile ranks for a row while preserving NaNs."""
    arr = np.asarray(values, dtype=float)
    out = np.full(arr.shape, np.nan, dtype=float)
    valid_mask = np.isfinite(arr)
    valid_values = arr[valid_mask]
    n = valid_values.size

    if n == 0:
        return out

    order = np.argsort(valid_values, kind="mergesort")
    sorted_values = valid_values[order]
    sorted_ranks = np.empty(n, dtype=float)

    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_values[j + 1] == sorted_values[i]:
            j += 1
        avg_rank = ((i + 1) + (j + 1)) / 2.0
        sorted_ranks[i : j + 1] = avg_rank / n * 100.0
        i = j + 1

    unsorted_ranks = np.empty(n, dtype=float)
    unsorted_ranks[order] = sorted_ranks
    out[valid_mask] = unsorted_ranks
    return out


def _frame_from_matrix(dates: list, columns: list[str], matrix: np.ndarray) -> pl.DataFrame:
    data = {DATE_COL: dates}
    for idx, column in enumerate(columns):
        data[column] = matrix[:, idx].tolist() if matrix.size else []
    return pl.DataFrame(data)


def momentum_calculate_returns(price_table, periods=[252, 126, 90, 30]):
    """Расчет доходностей за один или несколько определенных периодов."""
    price_frame = ensure_polars_frame(price_table).sort(DATE_COL)
    asset_cols = _asset_columns(price_frame)

    returns = {}
    for period in periods:
        returns[period] = price_frame.select(
            [pl.col(DATE_COL)]
            + [
                (pl.col(column) / pl.col(column).shift(period) - 1.0).alias(column)
                for column in asset_cols
            ]
        )
    return returns


def momentum_get_rebalance_dates(returns_dict, freq="W", momentum_period=None):
    """Вычисление дат ребалансировок."""
    freq_mapping = {"M": "1mo", "W": "1w"}
    every = freq_mapping.get(freq, freq)

    if momentum_period is None:
        momentum_period = list(returns_dict.keys())[0]

    frame = ensure_polars_frame(returns_dict[momentum_period]).sort(DATE_COL)
    rebalance_dates = (
        frame
        .select(DATE_COL)
        .drop_nulls()
        .group_by_dynamic(DATE_COL, every=every, period=every)
        .agg(pl.col(DATE_COL).min().alias("rebalance_date"))
        .drop_nulls("rebalance_date")
        .get_column("rebalance_date")
        .to_list()
    )
    return rebalance_dates


def momentum_rank_score_calc(rebalance_dates, returns_dict):
    """Вычисление процентного ранга бумаг на даты ребалансировок."""
    rank_score_dict = {}
    rebalance_frame = pl.DataFrame({DATE_COL: rebalance_dates})

    for period, returns_df in returns_dict.items():
        frame = ensure_polars_frame(returns_df).sort(DATE_COL)
        asset_cols = _asset_columns(frame)
        selected = rebalance_frame.join(frame, on=DATE_COL, how="left").sort(DATE_COL)
        values = selected.select(asset_cols).to_numpy()
        ranked = np.vstack([_percentile_rank_values(row) for row in values]) if len(values) else np.empty((0, len(asset_cols)))
        rank_score_dict[period] = _frame_from_matrix(
            selected.get_column(DATE_COL).to_list(),
            asset_cols,
            ranked,
        )

    return rank_score_dict


def momentum_hqm_table_calc(rank_score_dict, returns_dict, rank=None):
    """Расчет общего перцентильного ранга за все периоды."""
    if rank is None:
        from config import MOMENTUM_RANK

        rank = MOMENTUM_RANK

    first_period = list(rank_score_dict.keys())[0]
    base_frame = ensure_polars_frame(rank_score_dict[first_period]).sort(DATE_COL)
    asset_cols = _asset_columns(base_frame)

    matrices = [
        ensure_polars_frame(rank_score_dict[period]).sort(DATE_COL).select(asset_cols).to_numpy()
        for period in rank_score_dict
    ]
    stacked = np.stack(matrices)
    valid_counts = np.sum(np.isfinite(stacked), axis=0)
    full_period_mask = valid_counts == len(rank_score_dict)
    averaged = np.divide(
        np.nansum(stacked, axis=0),
        valid_counts,
        out=np.full(valid_counts.shape, np.nan, dtype=float),
        where=full_period_mask,
    )
    reranked = np.vstack([_percentile_rank_values(row) for row in averaged]) if len(averaged) else np.empty((0, len(asset_cols)))
    reranked[reranked <= rank] = np.nan

    return _frame_from_matrix(
        base_frame.get_column(DATE_COL).to_list(),
        asset_cols,
        reranked,
    )


def momentum_weights_table_calc(hqm_table, price_table, freq="W"):
    """Расчет весов позиций."""
    _ = freq
    hqm_frame = ensure_polars_frame(hqm_table).sort(DATE_COL)
    price_frame = ensure_polars_frame(price_table).sort(DATE_COL)
    asset_cols = _asset_columns(price_frame)

    hqm_values = hqm_frame.select(asset_cols).to_numpy()
    row_sums = np.nansum(hqm_values, axis=1)
    row_sums = np.where(row_sums == 0, np.nan, row_sums)
    weights_values = hqm_values / row_sums[:, None] if len(hqm_values) else np.empty((0, len(asset_cols)))

    hqm_weights = _frame_from_matrix(
        hqm_frame.get_column(DATE_COL).to_list(),
        asset_cols,
        weights_values,
    )

    return (
        price_frame
        .select(DATE_COL)
        .join_asof(hqm_weights, on=DATE_COL, strategy="backward")
        .select([DATE_COL] + asset_cols)
    )


def calculate_regime_signal(
    start_date,
    end_date,
    spx_ticker="^GSPC",
    vix_ticker="^VIX",
    spx_ma_period=200,
    vix_threshold=25,
):
    """Calculate regime filter signal based on SPX and VIX."""
    import pandas as pd
    import yfinance as yf

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    download_start = start_dt - pd.Timedelta(days=spx_ma_period + 301)

    spx_data = yf.download(spx_ticker, start=download_start, end=end_date, progress=False)
    vix_data = yf.download(vix_ticker, start=download_start, end=end_date, progress=False)

    spx = spx_data["Close"]
    vix = vix_data["Close"]

    if hasattr(spx, "iloc") and getattr(spx, "ndim", 1) > 1:
        spx = spx.iloc[:, 0]
    if hasattr(vix, "iloc") and getattr(vix, "ndim", 1) > 1:
        vix = vix.iloc[:, 0]

    spx_ma = spx.rolling(window=spx_ma_period, min_periods=spx_ma_period).mean()
    regime_signal_full = (spx > spx_ma) & (vix < vix_threshold)
    regime_signal = regime_signal_full[start_dt:end_dt]
    return regime_signal, spx, spx_ma, vix


def plot_regime_signal(regime_signal, spx, spx_ma, vix, vix_threshold, spx_ma_period=200):
    """Plot regime signal visualization with SPX, MA, and VIX."""
    regime_signal = ensure_pandas_series(regime_signal, name="regime_signal")
    spx = ensure_pandas_series(spx, name="spx")
    spx_ma = ensure_pandas_series(spx_ma, name="spx_ma")
    vix = ensure_pandas_series(vix, name="vix")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    def add_regime_spans(ax, signal_series, color="lightgreen", alpha=0.3):
        if not signal_series.empty and signal_series.sum() > 0:
            signal_diff = signal_series.astype(int).diff()
            start_dates = signal_series.index[signal_diff == 1]
            end_dates = signal_series.index[signal_diff == -1]

            if len(start_dates) > len(end_dates):
                end_dates = end_dates.tolist() + [signal_series.index[-1]]

            for idx, (start, end) in enumerate(zip(start_dates, end_dates)):
                label = "Strategy ON" if idx == 0 else ""
                ax.axvspan(start, end, color=color, alpha=alpha, label=label, zorder=1)

    add_regime_spans(ax1, regime_signal)
    add_regime_spans(ax2, regime_signal)

    ax1.plot(spx.index, spx.values, label="SPX", color="blue", linewidth=1.5, zorder=3)
    ax1.plot(
        spx_ma.index,
        spx_ma.values,
        label=f"SPX {spx_ma_period}D MA",
        color="orange",
        linewidth=1.5,
        linestyle="--",
        zorder=3,
    )
    ax1.set_ylabel("Price", fontsize=10)
    ax1.set_title("S&P 500 Index and Moving Average", fontsize=12, fontweight="bold")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    ax2.plot(vix.index, vix.values, label="VIX", color="red", linewidth=1.5, zorder=3)
    ax2.axhline(
        y=vix_threshold,
        label=f"Threshold ({vix_threshold})",
        color="darkred",
        linestyle="--",
        linewidth=1.5,
        zorder=3,
    )
    ax2.set_ylabel("VIX Level", fontsize=10)
    ax2.set_xlabel("Date", fontsize=10)
    ax2.set_title("VIX Volatility Index", fontsize=12, fontweight="bold")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


def apply_regime_filter(weights_table, regime_signal):
    """Apply regime filter to weights table."""
    weights_frame = ensure_polars_frame(weights_table).sort(DATE_COL)
    regime_frame = ensure_polars_frame(regime_signal).sort(DATE_COL)
    asset_cols = _asset_columns(weights_frame)
    regime_cols = [col for col in regime_frame.columns if col != DATE_COL]
    if len(regime_cols) != 1:
        raise ValueError("regime_signal must contain exactly one value column")
    regime_col = regime_cols[0]

    aligned = weights_frame.join_asof(regime_frame, on=DATE_COL, strategy="backward")
    aligned = aligned.with_columns(pl.col(regime_col).fill_null(False).cast(pl.Boolean))
    filtered = aligned.with_columns(
        [
            pl.when(pl.col(regime_col))
            .then(pl.col(column))
            .otherwise(0.0)
            .alias(column)
            for column in asset_cols
        ]
    )
    return filtered.select([DATE_COL] + asset_cols)
