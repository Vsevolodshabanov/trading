"""Data loading functions for the local T-Bank research dataset."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from config_tbank_dataset import DATASET_DIR, END_DATE, START_DATE
from polars_bridge import DATE_COL


def _resolve_dataset_dir(dataset_dir=None) -> Path:
    return Path(dataset_dir) if dataset_dir is not None else Path(DATASET_DIR)


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_datetime_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    expressions = []
    for column in columns:
        if column in frame.columns:
            expressions.append(
                pl.col(column)
                .str.to_datetime(strict=False, time_zone="UTC")
                .dt.replace_time_zone(None)
                .alias(column)
            )
    return frame.with_columns(expressions) if expressions else frame


def _ensure_wide_order(frame: pl.DataFrame, tickers: list[str]) -> pl.DataFrame:
    available = [ticker for ticker in tickers if ticker in frame.columns]
    missing = [ticker for ticker in tickers if ticker not in frame.columns]
    if missing:
        frame = frame.with_columns([pl.lit(None, dtype=pl.Float64).alias(ticker) for ticker in missing])
    return frame.select([DATE_COL] + tickers)


def load_instruments(dataset_dir=None) -> pl.DataFrame:
    dataset_path = _resolve_dataset_dir(dataset_dir)
    return (
        pl.read_csv(dataset_path / "instruments.csv")
        .sort(["ticker", "class_code"])
    )


def load_tickers(file_name=None, dataset_dir=None) -> list[str]:
    """Load tickers from the local research dataset."""
    _ = file_name
    return load_instruments(dataset_dir=dataset_dir).get_column("ticker").to_list()


def load_lot_sizes(tickers=None, dataset_dir=None) -> dict[str, int]:
    instruments = (
        load_instruments(dataset_dir=dataset_dir)
        .unique(subset=["ticker"], keep="first")
        .select(["ticker", "lot"])
    )
    if tickers is not None:
        instruments = instruments.filter(pl.col("ticker").is_in(list(tickers)))
    return {row["ticker"]: int(row["lot"]) for row in instruments.iter_rows(named=True)}


def load_status_snapshot(dataset_dir=None) -> pl.DataFrame:
    dataset_path = _resolve_dataset_dir(dataset_dir)
    path = dataset_path / "instrument_status.csv"
    if not path.exists():
        return pl.DataFrame()
    frame = pl.read_csv(path)
    return _parse_datetime_columns(
        frame,
        ["first_1min_candle_date", "first_1day_candle_date", "snapshot_at"],
    )


def _load_candles(dataset_dir=None) -> pl.DataFrame:
    dataset_path = _resolve_dataset_dir(dataset_dir)
    candles = pl.read_csv(dataset_path / "daily_candles.csv")
    candles = _parse_datetime_columns(candles, ["candle_time"])
    return candles.sort(["instrument_uid", "candle_time"])


def _load_dividends(dataset_dir=None) -> pl.DataFrame:
    dataset_path = _resolve_dataset_dir(dataset_dir)
    path = dataset_path / "dividends.csv"
    if not path.exists():
        return pl.DataFrame(
            schema={
                "instrument_uid": pl.String,
                "record_date": pl.Datetime,
                "payment_date": pl.Datetime,
                "declared_date": pl.Datetime,
                "last_buy_date": pl.Datetime,
                "dividend_net": pl.Float64,
            }
        )
    frame = pl.read_csv(path)
    return _parse_datetime_columns(
        frame,
        ["record_date", "payment_date", "declared_date", "last_buy_date", "created_at_event"],
    )


def _load_optional_splits(dataset_dir=None) -> pl.DataFrame:
    dataset_path = _resolve_dataset_dir(dataset_dir)
    path = dataset_path / "splits.csv"
    if not path.exists():
        return pl.DataFrame(
            schema={
                "instrument_uid": pl.String,
                "effective_date": pl.Datetime,
                "ratio_num": pl.Float64,
                "ratio_den": pl.Float64,
                "split_factor": pl.Float64,
            }
        )

    splits = pl.read_csv(path)
    splits = _parse_datetime_columns(splits, ["effective_date"])

    if "split_factor" not in splits.columns:
        ratio_num_expr = (
            pl.col("ratio_num").cast(pl.Float64, strict=False)
            if "ratio_num" in splits.columns
            else pl.lit(1.0)
        )
        ratio_den_expr = (
            pl.col("ratio_den").cast(pl.Float64, strict=False)
            if "ratio_den" in splits.columns
            else pl.lit(1.0)
        )
        splits = splits.with_columns(
            pl.when(ratio_den_expr.fill_null(1.0) == 0.0)
            .then(None)
            .otherwise(ratio_num_expr.fill_null(1.0) / ratio_den_expr.fill_null(1.0))
            .fill_null(1.0)
            .alias("split_factor")
        )
    else:
        splits = splits.with_columns(
            pl.col("split_factor").cast(pl.Float64, strict=False).fill_null(1.0)
        )

    return splits


def _apply_split_adjustments(
    price_table: pl.DataFrame,
    instruments: pl.DataFrame,
    splits: pl.DataFrame,
) -> pl.DataFrame:
    if splits.is_empty():
        return price_table

    uid_to_ticker = {
        row["instrument_uid"]: row["ticker"]
        for row in instruments.unique(subset=["instrument_uid"]).select(["instrument_uid", "ticker"]).iter_rows(named=True)
    }

    adjusted = price_table
    for row in splits.iter_rows(named=True):
        ticker = uid_to_ticker.get(row["instrument_uid"])
        effective_date = row.get("effective_date")
        factor = float(row.get("split_factor") or 1.0)
        if (
            ticker is None
            or ticker not in adjusted.columns
            or effective_date is None
            or factor <= 0
        ):
            continue
        adjusted = adjusted.with_columns(
            pl.when(pl.col(DATE_COL) < effective_date)
            .then(pl.col(ticker) / factor)
            .otherwise(pl.col(ticker))
            .alias(ticker)
        )

    return adjusted


def _pivot_prices(
    candles: pl.DataFrame,
    instruments: pl.DataFrame,
    value_col: str,
    tickers: list[str],
) -> pl.DataFrame:
    merged = candles.join(
        instruments.select(["instrument_uid", "ticker"]),
        on="instrument_uid",
        how="inner",
    )
    merged = merged.filter(pl.col("ticker").is_in(tickers))
    table = (
        merged
        .pivot(index="candle_time", on="ticker", values=value_col, aggregate_function="last")
        .rename({"candle_time": DATE_COL})
        .sort(DATE_COL)
    )
    return _ensure_wide_order(table, tickers)


def _build_dividend_cash_table(
    dividends: pl.DataFrame,
    instruments: pl.DataFrame,
    trading_dates: list[datetime],
    tickers: list[str],
) -> pl.DataFrame:
    base = pl.DataFrame({DATE_COL: trading_dates})
    if dividends.is_empty():
        return base.with_columns([pl.lit(0.0).alias(ticker) for ticker in tickers])

    merged = dividends.join(
        instruments.select(["instrument_uid", "ticker"]),
        on="instrument_uid",
        how="inner",
    )
    merged = (
        merged
        .filter(pl.col("ticker").is_in(tickers))
        .with_columns(
            pl.coalesce(["record_date", "last_buy_date"])
            .dt.truncate("1d")
            .alias("event_date")
        )
        .drop_nulls("event_date")
        .group_by(["event_date", "ticker"])
        .agg(pl.col("dividend_net").sum().alias("dividend_net"))
        .pivot(index="event_date", on="ticker", values="dividend_net", aggregate_function="sum")
        .rename({"event_date": DATE_COL})
        .sort(DATE_COL)
    )

    dividend_table = (
        base.join(merged, on=DATE_COL, how="left")
        .with_columns([pl.col(col).fill_null(0.0) for col in merged.columns if col != DATE_COL])
    )
    return _ensure_wide_order(dividend_table, tickers).with_columns(
        [pl.col(ticker).fill_null(0.0) for ticker in tickers]
    )


def _build_dividend_adjusted_close(raw_close: pl.DataFrame, dividend_cash: pl.DataFrame) -> pl.DataFrame:
    dates = raw_close.get_column(DATE_COL).to_list()
    result: dict[str, list[float | None]] = {DATE_COL: dates}
    tickers = [col for col in raw_close.columns if col != DATE_COL]

    for ticker in tickers:
        close_values = np.asarray(raw_close.get_column(ticker).to_list(), dtype=float)
        dividend_values = np.asarray(dividend_cash.get_column(ticker).to_list(), dtype=float)
        adjusted_values = np.full(close_values.shape, np.nan, dtype=float)

        valid_mask = np.isfinite(close_values)
        valid_idx = np.where(valid_mask)[0]
        if valid_idx.size == 0:
            result[ticker] = adjusted_values.tolist()
            continue

        adjusted_values[valid_idx[0]] = close_values[valid_idx[0]]
        for current_idx in valid_idx[1:]:
            prev_idx = valid_idx[valid_idx < current_idx][-1]
            prev_close = close_values[prev_idx]
            if not np.isfinite(prev_close) or prev_close == 0:
                adjusted_values[current_idx] = adjusted_values[prev_idx]
                continue
            price_return = close_values[current_idx] / prev_close - 1.0
            dividend_return = dividend_values[current_idx] / prev_close if np.isfinite(dividend_values[current_idx]) else 0.0
            total_return = price_return + dividend_return
            adjusted_values[current_idx] = adjusted_values[prev_idx] * (1.0 + total_return)

        result[ticker] = adjusted_values.tolist()

    return pl.DataFrame(result)


def _slice_by_date(frame: pl.DataFrame, start, end) -> pl.DataFrame:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    return frame.filter(pl.col(DATE_COL).is_between(start_dt, end_dt, closed="both"))


def filter_frame_to_dates(frame: pl.DataFrame, valid_dates: list[datetime]) -> pl.DataFrame:
    """Keep only a validated trading calendar."""
    return frame.filter(pl.col(DATE_COL).is_in(valid_dates)).sort(DATE_COL)


def select_tickers(frame: pl.DataFrame, tickers: list[str]) -> pl.DataFrame:
    """Select ticker columns from a wide table while preserving date order."""
    return _ensure_wide_order(frame, tickers).sort(DATE_COL)


def get_turnover_table(tickers=None, start=START_DATE, end=END_DATE, dataset_dir=None) -> pl.DataFrame:
    """Load daily RUB turnover in wide format."""
    instruments = load_instruments(dataset_dir=dataset_dir)
    selected_tickers = list(tickers or instruments.get_column("ticker").to_list())
    candles = _load_candles(dataset_dir=dataset_dir)

    turnover = _pivot_prices(candles, instruments, "turnover_rub", selected_tickers)
    return _slice_by_date(turnover, start, end)


def build_market_calendar(
    price_table: pl.DataFrame,
    min_active_coverage_ratio: float = 0.75,
    weekdays_only: bool = True,
) -> tuple[list[datetime], pl.DataFrame]:
    """Infer a sane trading calendar from wide market data coverage."""
    frame = price_table.sort(DATE_COL)
    dates = frame.get_column(DATE_COL).to_list()
    asset_cols = [col for col in frame.columns if col != DATE_COL]
    matrix = np.asarray(frame.select(asset_cols).to_numpy(), dtype=float)

    if matrix.size == 0:
        report = pl.DataFrame(
            {
                DATE_COL: dates,
                "present_count": [],
                "active_count": [],
                "coverage_ratio": [],
                "is_weekday": [],
                "keep": [],
            }
        )
        return [], report

    present_mask = np.isfinite(matrix)
    n_dates, n_assets = present_mask.shape
    first_idx = np.full(n_assets, n_dates, dtype=int)
    last_idx = np.full(n_assets, -1, dtype=int)

    for col_idx in range(n_assets):
        valid_rows = np.where(present_mask[:, col_idx])[0]
        if valid_rows.size:
            first_idx[col_idx] = valid_rows[0]
            last_idx[col_idx] = valid_rows[-1]

    row_idx = np.arange(n_dates)[:, None]
    active_mask = (
        (row_idx >= first_idx[None, :])
        & (row_idx <= last_idx[None, :])
        & (first_idx[None, :] < n_dates)
    )
    active_count = active_mask.sum(axis=1)
    present_count = present_mask.sum(axis=1)
    coverage_ratio = np.divide(
        present_count,
        active_count,
        out=np.zeros(n_dates, dtype=float),
        where=active_count > 0,
    )
    is_weekday = np.array([dt.weekday() < 5 for dt in dates], dtype=bool)

    keep_mask = active_count > 0
    if weekdays_only:
        keep_mask &= is_weekday
    keep_mask &= coverage_ratio >= min_active_coverage_ratio

    report = pl.DataFrame(
        {
            DATE_COL: dates,
            "present_count": present_count.tolist(),
            "active_count": active_count.tolist(),
            "coverage_ratio": coverage_ratio.tolist(),
            "is_weekday": is_weekday.tolist(),
            "keep": keep_mask.tolist(),
        }
    )
    valid_dates = [dates[idx] for idx, keep in enumerate(keep_mask) if keep]
    return valid_dates, report


def build_universe_eligibility(
    price_table: pl.DataFrame,
    turnover_table: pl.DataFrame,
    min_history_days: int,
    liquidity_window: int,
    min_median_turnover_rub: float,
    max_daily_return: float,
    min_daily_return: float,
) -> tuple[list[str], pl.DataFrame]:
    """Build a static eligible universe report for the notebook/backtest."""
    frame = price_table.sort(DATE_COL)
    asset_cols = [col for col in frame.columns if col != DATE_COL]
    price_matrix = np.asarray(frame.select(asset_cols).to_numpy(), dtype=float)
    turnover_matrix = np.asarray(turnover_table.select(asset_cols).to_numpy(), dtype=float)

    history_obs = np.sum(np.isfinite(price_matrix), axis=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        returns_matrix = price_matrix[1:] / price_matrix[:-1] - 1.0

    has_ret_history = np.any(np.isfinite(returns_matrix), axis=0)
    max_observed_return = np.full(len(asset_cols), np.nan, dtype=float)
    min_observed_return = np.full(len(asset_cols), np.nan, dtype=float)
    for col_idx in range(len(asset_cols)):
        if not has_ret_history[col_idx]:
            continue
        finite_returns = returns_matrix[:, col_idx][np.isfinite(returns_matrix[:, col_idx])]
        if finite_returns.size == 0:
            continue
        max_observed_return[col_idx] = float(finite_returns.max())
        min_observed_return[col_idx] = float(finite_returns.min())

    recent_median_turnover = np.full(len(asset_cols), np.nan, dtype=float)
    for col_idx in range(len(asset_cols)):
        turnover_values = turnover_matrix[:, col_idx]
        finite_turnover = turnover_values[np.isfinite(turnover_values)]
        if finite_turnover.size == 0:
            continue
        recent_median_turnover[col_idx] = float(np.median(finite_turnover[-liquidity_window:]))

    passes_history = history_obs >= min_history_days
    passes_corp_action_screen = (
        np.isnan(max_observed_return) | (max_observed_return <= max_daily_return)
    ) & (
        np.isnan(min_observed_return) | (min_observed_return >= min_daily_return)
    )
    passes_liquidity = (
        np.isfinite(recent_median_turnover) & (recent_median_turnover >= min_median_turnover_rub)
    )
    eligible_mask = passes_history & passes_corp_action_screen & passes_liquidity

    report = pl.DataFrame(
        {
            "ticker": asset_cols,
            "history_obs": history_obs.tolist(),
            "max_observed_return": max_observed_return.tolist(),
            "min_observed_return": min_observed_return.tolist(),
            "recent_median_turnover_rub": recent_median_turnover.tolist(),
            "passes_history": passes_history.tolist(),
            "passes_corp_action_screen": passes_corp_action_screen.tolist(),
            "passes_liquidity": passes_liquidity.tolist(),
            "eligible": eligible_mask.tolist(),
        }
    ).sort(["eligible", "ticker"], descending=[True, False])

    eligible_tickers = report.filter(pl.col("eligible")).get_column("ticker").to_list()
    return eligible_tickers, report


def get_raw_close_prices(tickers=None, start=START_DATE, end=END_DATE, dataset_dir=None) -> pl.DataFrame:
    instruments = load_instruments(dataset_dir=dataset_dir)
    selected_tickers = list(tickers or instruments.get_column("ticker").to_list())
    candles = _load_candles(dataset_dir=dataset_dir)
    splits = _load_optional_splits(dataset_dir=dataset_dir)

    raw_close = _pivot_prices(candles, instruments, "close_price", selected_tickers)
    raw_close = _apply_split_adjustments(raw_close, instruments, splits)
    return _slice_by_date(raw_close, start, end)


def fetch_price_data_divs(tickers, start=START_DATE, end=END_DATE, batch_size=100, dataset_dir=None):
    """Load local execution prices and dividend-adjusted research prices."""
    _ = batch_size
    instruments = load_instruments(dataset_dir=dataset_dir)
    selected_tickers = list(dict.fromkeys(tickers))
    candles = _load_candles(dataset_dir=dataset_dir)
    dividends = _load_dividends(dataset_dir=dataset_dir)
    splits = _load_optional_splits(dataset_dir=dataset_dir)

    raw_open = _pivot_prices(candles, instruments, "open_price", selected_tickers)
    raw_close = _pivot_prices(candles, instruments, "close_price", selected_tickers)

    raw_open = _apply_split_adjustments(raw_open, instruments, splits)
    raw_close = _apply_split_adjustments(raw_close, instruments, splits)

    raw_open = _slice_by_date(raw_open, start, end)
    raw_close = _slice_by_date(raw_close, start, end)

    dividend_cash = _build_dividend_cash_table(
        dividends=dividends,
        instruments=instruments,
        trading_dates=raw_close.get_column(DATE_COL).to_list(),
        tickers=selected_tickers,
    )
    dividend_cash = _slice_by_date(dividend_cash, start, end)
    adjusted_close = _build_dividend_adjusted_close(raw_close, dividend_cash)

    return raw_open, adjusted_close, dividend_cash


def get_benchmark_prices(benchmark_tickers_list, start=START_DATE, end=END_DATE, dataset_dir=None) -> pl.DataFrame:
    """Load local benchmark prices from the same dataset."""
    available_tickers = set(load_tickers(dataset_dir=dataset_dir))
    benchmark_tickers = [ticker for ticker in benchmark_tickers_list if ticker in available_tickers]
    if not benchmark_tickers:
        return pl.DataFrame({DATE_COL: []}, schema={DATE_COL: pl.Datetime})

    _, adjusted_close, _ = fetch_price_data_divs(
        benchmark_tickers,
        start=start,
        end=end,
        dataset_dir=dataset_dir,
    )
    return adjusted_close
