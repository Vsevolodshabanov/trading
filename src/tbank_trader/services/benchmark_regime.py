from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import yfinance as yf


@dataclass(slots=True)
class RegimeSnapshot:
    is_on: bool
    reason: str


def _extract_close_series(dataset: pd.DataFrame | pd.Series) -> pd.Series:
    if isinstance(dataset, pd.Series):
        series = dataset
    elif "Close" in dataset:
        series = dataset["Close"]
    else:
        raise ValueError("yfinance payload does not contain Close column")

    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    return series.dropna()


def _normalize_daily_index(series: pd.Series) -> pd.Series:
    normalized = series.copy()
    normalized_index = pd.DatetimeIndex(pd.to_datetime(normalized.index))
    if normalized_index.tz is not None:
        normalized_index = normalized_index.tz_localize(None)
    normalized.index = normalized_index.normalize()
    return normalized[~normalized.index.duplicated(keep="last")]


def _download_close_series_yfinance(
    *,
    ticker: str,
    start_at: datetime,
    end_at: datetime,
) -> pd.Series:
    raw = yf.download(
        ticker,
        start=start_at,
        end=end_at,
        progress=False,
        auto_adjust=False,
    )
    return _extract_close_series(raw)


def _load_yahoo_chart_series(
    *,
    ticker: str,
    start_at: datetime,
    end_at: datetime,
) -> pd.Series:
    start_ts = int(pd.Timestamp(start_at).timestamp())
    end_ts = int(pd.Timestamp(end_at).timestamp())
    encoded_ticker = quote(ticker, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_ticker}"
        f"?period1={start_ts}&period2={end_ts}&interval=1d&includePrePost=false&events=div%2Csplits"
    )
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        raise ValueError(f"Yahoo chart payload is empty for {ticker}")

    timestamps = result[0].get("timestamp") or []
    quotes = ((result[0].get("indicators") or {}).get("quote") or [{}])[0]
    closes = quotes.get("close") or []
    if not timestamps or not closes:
        raise ValueError(f"Yahoo chart series is empty for {ticker}")

    series = pd.Series(
        closes,
        index=pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None),
    )
    return series.dropna()


def load_spx_vix_regime_history(
    *,
    start_at: datetime,
    end_at: datetime,
    spx_ticker: str,
    vix_ticker: str,
    spx_ma_window: int,
    vix_threshold: float,
) -> dict[date, RegimeSnapshot]:
    start_ts = pd.Timestamp(start_at).tz_localize(None)
    end_ts = pd.Timestamp(end_at).tz_localize(None)
    download_start = start_ts - pd.Timedelta(days=spx_ma_window * 3)
    download_end = end_ts + pd.Timedelta(days=1)

    try:
        spx = _normalize_daily_index(
            _download_close_series_yfinance(
                ticker=spx_ticker,
                start_at=download_start.to_pydatetime(),
                end_at=download_end.to_pydatetime(),
            )
        )
    except Exception:
        spx = _normalize_daily_index(
            _load_yahoo_chart_series(
                ticker=spx_ticker,
                start_at=download_start.to_pydatetime(),
                end_at=download_end.to_pydatetime(),
            )
        )

    try:
        vix = _normalize_daily_index(
            _download_close_series_yfinance(
                ticker=vix_ticker,
                start_at=download_start.to_pydatetime(),
                end_at=download_end.to_pydatetime(),
            )
        )
    except Exception:
        vix = _normalize_daily_index(
            _load_yahoo_chart_series(
                ticker=vix_ticker,
                start_at=download_start.to_pydatetime(),
                end_at=download_end.to_pydatetime(),
            )
        )
    benchmark_frame = (
        pd.concat({"spx_close": spx, "vix_close": vix}, axis=1, join="inner")
        .sort_index()
        .dropna()
    )
    if benchmark_frame.empty:
        return {}

    benchmark_frame["spx_ma"] = benchmark_frame["spx_close"].rolling(
        window=spx_ma_window,
        min_periods=spx_ma_window,
    ).mean()
    benchmark_frame["signal"] = (
        (benchmark_frame["spx_close"] > benchmark_frame["spx_ma"])
        & (benchmark_frame["vix_close"] < vix_threshold)
    ).shift(1)

    history: dict[date, RegimeSnapshot] = {}
    for row_date, row in benchmark_frame.iterrows():
        if row_date < start_ts.normalize() or row_date > end_ts.normalize():
            continue
        signal = row["signal"]
        spx_ma = row["spx_ma"]
        if pd.isna(signal) or pd.isna(spx_ma):
            continue

        is_on = bool(signal)
        spx_relation = "above" if float(row["spx_close"]) > float(spx_ma) else "below"
        vix_relation = "below" if float(row["vix_close"]) < vix_threshold else "above"
        reason = (
            f"spx_vix_{'on' if is_on else 'off'}:"
            f"spx_{spx_relation}_ma:{float(row['spx_close']):.4f}:{float(spx_ma):.4f}:"
            f"vix_{vix_relation}_threshold:{float(row['vix_close']):.4f}:{vix_threshold:.4f}"
        )
        history[row_date.date()] = RegimeSnapshot(is_on=is_on, reason=reason[:255])

    return history
