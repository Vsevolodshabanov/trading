from datetime import datetime, timezone

import pandas as pd

from tbank_trader.config import AppSettings
from tbank_trader.services.benchmark_regime import load_spx_vix_regime_history
import tbank_trader.services.benchmark_regime as benchmark_regime


def test_default_historical_regime_mode_is_spx_vix() -> None:
    settings = AppSettings()
    assert settings.historical_regime_mode == "spx_vix"


def test_load_spx_vix_regime_history_falls_back_from_yfinance(monkeypatch) -> None:
    def fail_yfinance(**_: object) -> pd.Series:
        raise RuntimeError("rate_limited")

    def fake_chart_series(*, ticker: str, start_at: datetime, end_at: datetime) -> pd.Series:
        assert ticker in {"^GSPC", "^VIX"}
        assert start_at < end_at
        base = pd.Timestamp("2026-01-01")
        index = pd.date_range(base, periods=8, freq="D")
        if ticker == "^GSPC":
            values = [100.0, 101.0, 102.0, 103.0, 104.0, 103.0, 105.0, 106.0]
        else:
            values = [20.0, 19.0, 18.5, 19.2, 18.8, 18.0, 17.5, 17.0]
        return pd.Series(values, index=index)

    monkeypatch.setattr(benchmark_regime, "_download_close_series_yfinance", fail_yfinance)
    monkeypatch.setattr(benchmark_regime, "_load_yahoo_chart_series", fake_chart_series)

    history = load_spx_vix_regime_history(
        start_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
        end_at=datetime(2026, 1, 8, tzinfo=timezone.utc),
        spx_ticker="^GSPC",
        vix_ticker="^VIX",
        spx_ma_window=3,
        vix_threshold=25.0,
    )

    assert history
    assert any(snapshot.is_on for snapshot in history.values())
    assert all(snapshot.reason.startswith("spx_vix_") for snapshot in history.values())
