# tbank-trader

Standalone MVP for a Docker-based auto-trading system with:

- `FastAPI` control plane and dashboard
- `Postgres` persistence
- `Redis` event bus placeholder
- `engine` service with simulated and `T-Invest sandbox` broker modes
- clear seams for future strategy and reasoning modules

## What is implemented

- event-loop trading engine
- optional `portfolio_momentum` mode adapted from the external `SPX_Momentum_Strategy_20260225` research package
- simulated market feed with strategy/risk parity against sandbox mode
- moving-average momentum for shares/FX and mean-reversion profile for bonds
- cross-sectional ranking across the configured symbol universe with benchmark moving-average regime filtering
- sell-first / buy-second portfolio rebalance planner with cash-aware order sequencing
- candle warmup from `T-Invest` history so signals do not wait for a cold start
- instrument-aware sizing by lot and RUB notional
- per-asset execution rules for `shares / bonds / fx`
- long-only inventory filter for sandbox execution so `sell` without inventory is rejected before routing
- deterministic risk engine with lot and notional caps
- order, signal, position, and heartbeat persistence
- lightweight browser dashboard with 5-second refresh
- broker status panel with sandbox account, portfolio and instrument metadata
- current-run scoped dashboard so old simulated history does not pollute live sandbox monitoring
- batched `GetLastPrices` calls and broker position reconciliation on every engine cycle
- Docker Compose stack for `api`, `engine`, `db`, `redis`
- pytest smoke tests

## Quick start

```bash
docker compose up --build
```

UI:

- `http://localhost:8000`

API:

- `GET /health`
- `GET /api/dashboard`
- `GET /api/orders?scope=current|all`
- `GET /api/signals?scope=current|all`
- `GET /api/positions`
- `GET /api/broker/status`
- `POST /api/system/pause`
- `POST /api/system/resume`

## Run tests

```bash
docker compose run --build --rm test
```

## Notes

- Default mode is `simulated` so the stack is runnable without broker credentials.
- `broker_mode=tbank` now supports sandbox account discovery, pay-in, last prices, and market orders through the REST gateway.
- `broker_mode=tbank` also warms strategies from historical candles via `MarketDataService/GetCandles`.
- `strategy_mode=portfolio_momentum` uses the same market data path but evaluates the whole portfolio on a rebalance cadence instead of emitting one-symbol live crossover signals.
- The dashboard shows only the current engine run by default. Full historical orders and signals remain available through `scope=all`.
- The current strategy is intentionally simple and is meant to validate execution, risk, storage, and UI plumbing first.
- The next implementation stages are captured in [docs/roadmap.md](/Users/vgshabanov/work/tbank-trader/docs/roadmap.md).
