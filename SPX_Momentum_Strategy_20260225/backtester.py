"""Backtesting execution engine."""

from __future__ import annotations

import numpy as np
import polars as pl

from config import INITIAL_INVESTMENT, BACKTEST_START, CASH_RETURN_RATE
from polars_bridge import DATE_COL, ensure_polars_frame


def backtest_strategy(price_table, weights, start_date=BACKTEST_START, cash_return_rate=CASH_RETURN_RATE):
    """Backtest strategy and calculate returns."""
    prices = ensure_polars_frame(price_table).sort(DATE_COL)
    weights_frame = ensure_polars_frame(weights).sort(DATE_COL)
    start_dt = np.datetime64(start_date)

    prices = prices.filter(pl.col(DATE_COL) >= start_dt)
    asset_cols = [col for col in prices.columns if col != DATE_COL]

    returns = prices.select(
        [pl.col(DATE_COL)]
        + [
            (pl.col(column) / pl.col(column).shift(1) - 1.0)
            .replace([float("inf"), float("-inf")], None)
            .alias(column)
            for column in asset_cols
        ]
    )

    aligned = (
        returns.join(weights_frame, on=DATE_COL, how="left", suffix="_w")
        .sort(DATE_COL)
    )

    weighted_exprs = [
        (pl.col(column).fill_null(0.0) * pl.col(f"{column}_w").fill_null(0.0)).alias(f"{column}_weighted")
        for column in asset_cols
        if f"{column}_w" in aligned.columns
    ]
    aligned = aligned.with_columns(weighted_exprs)

    weighted_cols = [f"{column}_weighted" for column in asset_cols if f"{column}_weighted" in aligned.columns]
    weight_cols = [f"{column}_w" for column in asset_cols if f"{column}_w" in aligned.columns]
    daily_cash_return = (1 + cash_return_rate) ** (1 / 252) - 1

    aligned = aligned.with_columns(
        pl.sum_horizontal(weighted_cols).fill_null(0.0).alias("Strategy_daily_returns_raw"),
        pl.sum_horizontal(weight_cols).alias("weights_sum"),
    )
    aligned = aligned.with_columns(
        pl.when(pl.col("weights_sum").is_null() | (pl.col("weights_sum") == 0))
        .then(daily_cash_return)
        .otherwise(pl.col("Strategy_daily_returns_raw"))
        .alias("Strategy_daily_returns")
    )
    aligned = aligned.with_columns(
        (pl.col("Strategy_daily_returns") + 1.0).cum_prod().sub(1.0).alias("Strategy_cum_returns")
    )
    aligned = aligned.with_columns(
        ((pl.col("Strategy_cum_returns") + 1.0) * INITIAL_INVESTMENT).alias("Strategy_money_value")
    )

    return aligned.select([DATE_COL, "Strategy_daily_returns", "Strategy_cum_returns", "Strategy_money_value"])
