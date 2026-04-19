from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TBANK_TRADER_",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "tbank-trader"
    environment: str = "dev"
    database_url: str = "sqlite:///./tbank-trader.db"
    redis_url: str | None = None
    broker_mode: Literal["simulated", "tbank"] = "simulated"
    symbols: Annotated[list[str], NoDecode] = Field(default_factory=lambda: ["SBER", "GAZP"])
    engine_poll_seconds: float = 2.0
    strategy_mode: Literal["portfolio_momentum"] = "portfolio_momentum"
    portfolio_live_enabled: bool = True
    portfolio_live_initial_cash_rub: float = 100_000.0
    portfolio_shadow_enabled: bool = False
    portfolio_shadow_initial_cash_rub: float = 100_000.0
    universe_exclude_qual_only: bool = True
    historical_backfill_days: int = 365
    historical_backfill_limit_per_request: int = 500
    historical_backfill_batch_size: int = 100
    historical_min_candle_count: int = 150
    universe_min_median_turnover_rub: float = 100_000.0
    portfolio_rebalance_frequency: Literal["W", "M"] = "W"
    historical_regime_mode: Literal["local_ma", "spx_vix"] = "spx_vix"
    historical_regime_spx_ticker: str = "^GSPC"
    historical_regime_vix_ticker: str = "^VIX"
    historical_regime_vix_threshold: float = 25.0
    strategy_profile: Literal["balanced", "active", "conservative"] = "balanced"
    strategy_candle_interval: str = "CANDLE_INTERVAL_5_MIN"
    strategy_warmup_lookback_hours: int = 24
    strategy_warmup_extra_bars: int = 4
    short_window: int = 5
    long_window: int = 20
    signal_threshold_bps: int = 20
    portfolio_history_bars: int = 180
    portfolio_rebalance_cooldown_seconds: int = 3600
    portfolio_momentum_periods: Annotated[list[int], NoDecode] = Field(default_factory=lambda: [30, 90, 126])
    portfolio_top_percentile: int = 95
    portfolio_min_positions: int = 1
    portfolio_max_positions: int = 5
    portfolio_regime_filter_enabled: bool = True
    portfolio_regime_symbol: str | None = None
    portfolio_regime_ma_window: int = 50
    max_position_per_symbol: int = 20
    max_position_notional_rub: float = 25_000.0
    target_order_notional_rub: float = 5_000.0
    max_order_notional_rub: float = 10_000.0
    max_order_lots: int = 10
    share_max_position_per_symbol: int = 20
    share_max_position_notional_rub: float = 25_000.0
    share_target_order_notional_rub: float = 5_000.0
    share_max_order_notional_rub: float = 10_000.0
    share_max_order_lots: int = 10
    bond_max_position_per_symbol: int = 20
    bond_max_position_notional_rub: float = 15_000.0
    bond_target_order_notional_rub: float = 3_000.0
    bond_max_order_notional_rub: float = 8_000.0
    bond_max_order_lots: int = 20
    fx_max_position_per_symbol: int = 1
    fx_max_position_notional_rub: float = 80_000.0
    fx_target_order_notional_rub: float = 30_000.0
    fx_max_order_notional_rub: float = 80_000.0
    fx_max_order_lots: int = 1
    order_size: int = 1
    dashboard_refresh_seconds: int = 5
    broker_status_cache_seconds: int = 30
    random_seed: int = 7
    tbank_use_sandbox: bool = True
    tbank_verify_ssl: bool = True
    tbank_timeout_seconds: float = 10.0
    tbank_min_order_interval_seconds: float = 0.75
    tbank_sandbox_auto_create_account: bool = True
    tbank_sandbox_min_rub_balance: int = 100_000
    tbank_token: str | None = None
    tbank_account_id: str | None = None

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, list):
            return value
        return [item.strip() for item in value.split(",") if item.strip()]

    @field_validator("portfolio_momentum_periods", mode="before")
    @classmethod
    def parse_portfolio_momentum_periods(cls, value: str | list[int] | list[str]) -> list[int]:
        if isinstance(value, list):
            return [int(item) for item in value]
        return [int(item.strip()) for item in value.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()
