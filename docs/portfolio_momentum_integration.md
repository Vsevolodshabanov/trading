# Portfolio Momentum Integration

## What Was Ported

The folder `SPX_Momentum_Strategy_20260225` is a research and backtesting package, not a live trading service. The production integration reuses the core ideas that are portable into `tbank-trader`:

- multi-horizon momentum ranking
- percentile-based cohort selection
- benchmark moving-average regime filter
- sell-first / buy-second rebalance sequencing

The following parts were intentionally not copied directly:

- `yfinance` market data loading
- notebook orchestration
- plotting and Excel export
- US-only benchmark assumptions such as hardwired `^GSPC` and `^VIX`

## Current Service Design

The service now supports two strategy modes:

- `event_driven`: existing per-symbol momentum / mean-reversion loop
- `portfolio_momentum`: portfolio-level rebalance logic over the configured `symbols`

`portfolio_momentum` works as follows:

1. Pull historical closes for each configured symbol from the broker adapter.
2. Compute trailing returns on multiple lookback horizons.
3. Rank the universe by percentile on each horizon.
4. Average ranks and re-rank to get a final momentum score.
5. Select the top percentile cohort.
6. Apply a benchmark moving-average regime filter.
7. Convert target weights into target lot quantities.
8. Execute rebalances with `sell -> cash update -> buy`.

## Config Knobs

Relevant environment variables:

- `TBANK_TRADER_STRATEGY_MODE=portfolio_momentum`
- `TBANK_TRADER_PORTFOLIO_HISTORY_BARS=180`
- `TBANK_TRADER_PORTFOLIO_MOMENTUM_PERIODS=30,90,126`
- `TBANK_TRADER_PORTFOLIO_TOP_PERCENTILE=95`
- `TBANK_TRADER_PORTFOLIO_MIN_POSITIONS=1`
- `TBANK_TRADER_PORTFOLIO_MAX_POSITIONS=5`
- `TBANK_TRADER_PORTFOLIO_REBALANCE_COOLDOWN_SECONDS=3600`
- `TBANK_TRADER_PORTFOLIO_REGIME_FILTER_ENABLED=true`
- `TBANK_TRADER_PORTFOLIO_REGIME_SYMBOL=SBER`
- `TBANK_TRADER_PORTFOLIO_REGIME_MA_WINDOW=50`

## Known Limitations

- Regime filter is simplified to benchmark price vs moving average.
- Portfolio construction is cash-aware, but does not yet model explicit commissions or slippage.
- Rebalance sizing respects existing order and position caps, so target weights may be reached over multiple cycles instead of one pass.
- Sandbox execution still depends on the liquidity and instrument availability of the configured symbols.

## Next Experiments

- Add transaction-cost and spread estimates into the rebalance planner.
- Add separate benchmark instruments outside the traded universe.
- Persist portfolio snapshots and target books in dedicated tables instead of `app_state`.
- Add a research runner for offline backtests over T-Invest candle dumps.
- Add a regime layer based on volatility / drawdown, not only benchmark MA.
- Add a hybrid mode where portfolio momentum sets the basket and event-driven logic times entries inside that basket.
