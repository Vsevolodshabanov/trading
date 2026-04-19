"""Helpers for moving between polars-first tables and pandas-based legacy code."""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl


DATE_COL = "date"


def is_polars_frame(obj: Any) -> bool:
    return isinstance(obj, pl.DataFrame)


def is_polars_series(obj: Any) -> bool:
    return isinstance(obj, pl.Series)


def is_pandas_frame(obj: Any) -> bool:
    return isinstance(obj, pd.DataFrame)


def is_pandas_series(obj: Any) -> bool:
    return isinstance(obj, pd.Series)


def ensure_polars_frame(obj: Any, date_col: str = DATE_COL) -> pl.DataFrame:
    """Convert a supported tabular object into a polars DataFrame with a date column."""
    if isinstance(obj, pl.DataFrame):
        return obj.clone()

    if isinstance(obj, pl.Series):
        name = obj.name or "value"
        return pl.DataFrame({name: obj})

    if isinstance(obj, pd.Series):
        series_name = obj.name or "value"
        if isinstance(obj.index, pd.DatetimeIndex):
            return pl.from_pandas(
                obj.rename(series_name).reset_index(names=date_col)
            )
        return pl.DataFrame({series_name: obj.to_list()})

    if isinstance(obj, pd.DataFrame):
        frame = obj.copy()
        if isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index(names=date_col)
        elif frame.index.name == date_col:
            frame = frame.reset_index()
        return pl.from_pandas(frame)

    if isinstance(obj, dict):
        return pl.DataFrame(obj)

    raise TypeError(f"Unsupported table type for polars conversion: {type(obj)!r}")


def ensure_pandas_frame(obj: Any, date_col: str = DATE_COL) -> pd.DataFrame:
    """Convert a supported tabular object into a pandas DataFrame with DatetimeIndex when possible."""
    if isinstance(obj, pd.DataFrame):
        return obj.copy()

    if isinstance(obj, pd.Series):
        return obj.to_frame()

    if isinstance(obj, pl.Series):
        return obj.to_pandas().to_frame()

    if isinstance(obj, pl.DataFrame):
        frame = obj.to_pandas()
        if date_col in frame.columns:
            frame[date_col] = pd.to_datetime(frame[date_col])
            frame = frame.set_index(date_col)
        return frame

    if isinstance(obj, dict):
        return pd.DataFrame(obj)

    raise TypeError(f"Unsupported table type for pandas conversion: {type(obj)!r}")


def ensure_pandas_series(obj: Any, name: str | None = None, date_col: str = DATE_COL) -> pd.Series:
    """Convert a supported object into a pandas Series."""
    if isinstance(obj, pd.Series):
        series = obj.copy()
        if name is not None:
            series.name = name
        return series

    if isinstance(obj, pl.Series):
        return obj.to_pandas().rename(name)

    if isinstance(obj, pl.DataFrame):
        if date_col in obj.columns:
            value_cols = [col for col in obj.columns if col != date_col]
            if len(value_cols) != 1:
                raise ValueError("Expected exactly one value column besides date for Series conversion")
            frame = ensure_pandas_frame(obj, date_col=date_col)
            return frame[value_cols[0]].rename(name or value_cols[0])
        if obj.width != 1:
            raise ValueError("Expected a single-column frame for Series conversion")
        col = obj.columns[0]
        return obj[col].to_pandas().rename(name or col)

    if isinstance(obj, pd.DataFrame):
        if date_col in obj.columns:
            value_cols = [col for col in obj.columns if col != date_col]
            if len(value_cols) != 1:
                raise ValueError("Expected exactly one value column besides date for Series conversion")
            frame = ensure_pandas_frame(obj, date_col=date_col)
            return frame[value_cols[0]].rename(name or value_cols[0])
        if obj.shape[1] != 1:
            raise ValueError("Expected a single-column frame for Series conversion")
        return obj.iloc[:, 0].rename(name or obj.columns[0])

    raise TypeError(f"Unsupported series type: {type(obj)!r}")
