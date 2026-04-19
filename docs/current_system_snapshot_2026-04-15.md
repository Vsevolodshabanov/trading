# TBank Trader: Current System Snapshot

Date: `2026-04-15`

## Purpose

This document fixes the current state of the `tbank-trader` system as of `2026-04-15`.
It is intended as a working snapshot of:

- the live architecture
- the active strategy scope
- the deployed server state
- the implemented research/backtest capabilities
- the current bottlenecks and next development steps

## Project Scope Right Now

The system is no longer a multi-strategy prototype. At the current stage it is focused on a single strategy family:

- `SPX Momentum`

Everything else has effectively been deprioritized from the active roadmap. The current development goal is:

1. make the historical simulation and live service match the reference `SPX_Momentum_Strategy_20260225` as closely as possible
2. expand historical data coverage across the T-Invest universe
3. only after parity and coverage are good enough, improve portfolio construction and research quality

## High-Level Architecture

The system currently runs as a Dockerized service with the following components:

- `api`
  - FastAPI application
  - serves dashboard pages and JSON API
  - exposes system state, positions, orders, simulation results, universe metrics

- `engine`
  - trading loop
  - polls broker and market state
  - computes current live portfolio rebalance actions
  - routes approved actions to broker

- `db`
  - PostgreSQL
  - stores app state, positions, orders, signals, catalog, history quality, candles, historical simulation runs

- `redis`
  - lightweight runtime support and future cache/event layer

## Active Runtime Environment

Current deployed server:

- host: `159.223.213.53`
- deployed path: `/root/tbank-trader`
- public API port: `8010`
- mode: `prod` environment
- broker mode: `tbank`
- broker target: `T-Invest sandbox`

Running containers on the server at the time of this snapshot:

- `tbank-trader-api-1`
- `tbank-trader-engine-1`
- `tbank-trader-db-1`
- `tbank-trader-redis-1`

## Current Strategy State

### Live Strategy

The live engine currently runs one strategy mode only:

- `portfolio_momentum`

In practice this is the current local implementation of the `SPX Momentum` service strategy.

### Historical Simulation Mode

Historical simulation now supports two regime modes:

- `local_ma`
  - old behavior
  - benchmark/regime taken from the local tradable universe

- `spx_vix`
  - new parity-oriented behavior
  - regime is derived from external `SPX` and `VIX` benchmark data
  - this is the mode currently enabled on the server for historical simulation

The `spx_vix` path now has a fallback mechanism:

- first tries `yfinance`
- if `yfinance` is rate-limited, falls back to direct Yahoo chart API

This was required because the pure `yfinance` path was unstable under rate limiting and caused the historical simulation to get stuck in `benchmark_history_short_or_missing`.

## What Is Already Implemented

### 1. Broker and Execution Layer

- T-Invest sandbox connectivity
- broker account discovery
- order placement through the broker adapter
- position synchronization from broker
- lot-aware execution constraints
- cash-constrained sell-first / buy-second rebalance logic
- execution guardrails by instrument class

### 2. State and Persistence

- persistent application state table
- positions, orders, signals tables
- portfolio rebalance history
- historical simulation run and rebalance tables
- instrument catalog and eligibility state
- historical candle storage
- history-quality snapshot data

### 3. Universe Tracking

- full T-Invest instrument catalog sync exists
- instrument metadata is persisted locally
- eligibility rules exist with exclusion reasons
- current eligibility logic already uses fields such as:
  - `for_qual_investor_flag`
  - API trade availability
  - OTC exclusion
  - active/inactive status
  - buy/sell availability

### 4. Historical Data Layer

- historical candle backfill exists
- OHLCV candles are stored locally
- completeness flag is stored
- turnover proxy is stored
- batch backfill with cursor progress is implemented
- history quality metrics exist:
  - completed candle count
  - median turnover proxy
  - `history_ready`

### 5. Historical Simulation

- a historical simulation runner exists
- run-level and rebalance-level outputs are persisted
- rebalance scheduling has already been moved away from fixed intraday stepping
- simulation now uses a daily research timeline and weekly/monthly rebalance grouping
- `SPX/VIX` external regime logic is integrated into historical simulation

### 6. UI and API

The UI/API already expose:

- system heartbeat
- broker mode
- live positions
- live orders
- live signals
- live portfolio rebalances
- historical simulation summary
- universe coverage and history coverage metrics

## Current Server Metrics

The following values were current at the time of this snapshot from the live server API.

### Universe and History

- catalog size: `15940`
- eligible universe size: `1851`
- historical candle count: `44372`
- historically covered instruments: `96`
- history-ready instruments: `0`
- historical backfill cursor: `100`
- last backfill batch size: `100`
- last backfill batch written: `44366`

### Latest Historical Simulation

Latest historical simulation run:

- run id: `4`
- status: `completed`
- instruments considered: `96`
- instruments with sufficient history for simulation: `5`
- rebalance points: `35`
- completed rebalances: `7`
- executed actions: `1`
- turnover: `1798.0 RUB`
- final equity: `100004.2 RUB`
- total return: `0.0042%`
- max drawdown: `0.0%`

Important interpretation:

- the simulation is now alive and no longer fully blocked by benchmark-history issues
- but the usable historical universe is still extremely small
- the main bottleneck has shifted from benchmark regime logic to data coverage and research-quality data preparation

### Live Portfolio State

Current live state at snapshot time:

- live strategy enabled: `true`
- shadow strategy enabled: `false`
- live regime state: `on`
- live selected symbols: `SBER`
- live target weights: `SBER:1.000`
- live portfolio positions summary: `SBER:20`
- portfolio cash: `93582.3 RUB`
- portfolio equity: `99996.7 RUB`

The dashboard also showed real broker-linked order history and rebalance history.

## Major Functional Gaps Still Open

The system is functional, but it is not yet reference-parity complete.

### 1. Data Coverage Is Still Too Small

This is the current main blocker.

- only `96` instruments have historical candles in the local store
- only `5` instruments currently survive the simulation history requirements
- `history_ready_instruments` is still `0`

This means the portfolio simulator is technically running, but it is not yet running on a sufficiently broad and representative universe.

### 2. Ranking Series and Execution Series Are Not Yet Split

The reference strategy expects separate roles for prices:

- dividend-adjusted ranking series
- execution price series

This separation is not yet fully implemented in the service.

### 3. Execution Economics Are Not Yet Reference-Equivalent

The service historical simulator still does not fully reproduce the reference economics:

- fixed transaction cost component
- proportional cost component
- optional cash yield
- optional management fee accrual

### 4. Live Engine Still Uses Local Regime Logic

Historical simulation now supports external `SPX/VIX` regime logic, but the live engine is still driven by the local `portfolio_regime_symbol` logic.

That is currently intentional to avoid destabilizing the live loop while historical parity is still being built.

## Key Files That Define the Current System

Core planning:

- [spx_momentum_strategy_plan.md](/Users/vgshabanov/work/tbank-trader/docs/spx_momentum_strategy_plan.md)
- [system_development_plan.md](/Users/vgshabanov/work/tbank-trader/docs/system_development_plan.md)
- [roadmap.md](/Users/vgshabanov/work/tbank-trader/docs/roadmap.md)

Core implementation:

- [config.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/config.py)
- [portfolio_momentum.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/core/portfolio_momentum.py)
- [historical_simulation.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/services/historical_simulation.py)
- [benchmark_regime.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/services/benchmark_regime.py)
- [runner.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/engine/runner.py)
- [repository.py](/Users/vgshabanov/work/tbank-trader/src/tbank_trader/storage/repository.py)

Reference package:

- [strategy.py](/Users/vgshabanov/work/tbank-trader/SPX_Momentum_Strategy_20260225/strategy.py)
- [regime_filters.py](/Users/vgshabanov/work/tbank-trader/SPX_Momentum_Strategy_20260225/regime_filters.py)
- [execution_backtester.py](/Users/vgshabanov/work/tbank-trader/SPX_Momentum_Strategy_20260225/execution_backtester.py)

## Current Development Priority

The current best next steps are:

1. expand historical backfill depth and breadth so that a materially larger subset of the eligible universe becomes usable
2. implement research-grade price preparation:
   - dividend-aware ranking series
   - execution-price series
3. reproduce reference execution economics
4. only after that, decide whether the live engine should also move from local benchmark logic to external `SPX/VIX` regime logic

## Validation Status

At the time of this snapshot:

- source code compiles successfully
- container test suite on the server passed: `32 passed`
- server deployment is up
- historical simulation in `spx_vix` mode produces real completed rebalances

## Summary

The system is already a functioning Dockerized trading service with:

- broker routing
- persistent state
- instrument universe tracking
- historical candle storage
- portfolio simulation
- monitoring UI

But it is still in the transition from a working sandbox prototype to a serious research-grade SPX Momentum platform.

The most important fact about the current state is this:

- the architectural shell is already in place
- the benchmark-regime parity issue is no longer the primary blocker
- the primary blocker is now research data quality and historical coverage across the investable universe
