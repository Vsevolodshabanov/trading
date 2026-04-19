# Roadmap

## Current state

- Docker stack is running with `api`, `engine`, `db`, `redis`
- `T-Invest sandbox` adapter is connected through the REST gateway
- broker account discovery, pay-in, last prices, sandbox market orders and position sync are implemented
- dashboard shows the current engine run separately from old history

## Next build stages

1. Market data layer
   - move from pure polling to mixed mode: REST for bootstrapping, streams for quotes/orderbook/trades
   - store short rolling windows in Redis and longer history in Postgres
   - add per-symbol freshness checks and stale-market protection

2. Execution and risk
   - add instrument-aware sizing by lot, currency and notional limits
   - add per-strategy exposure caps, daily loss limits and kill switch rules
   - persist broker executions separately from internal signals for reconciliation

3. Strategy experiments
   - momentum/reversal baseline for liquid shares
   - news reaction pipeline with headline classification and delayed confirmation filters
   - cross-instrument relative value for `share / bond / FX` baskets with explicit carry and liquidity constraints

4. Research workflow
   - offline backtests on historical candles and event datasets
   - paper-trading in sandbox with fixed experiment configs
   - promotion gates: fill quality, drawdown, turnover, slippage sensitivity

## Reasoning agent guidance

- Do not put an LLM or reasoning model into the hot execution loop every `5-10` seconds.
- The trading loop should stay deterministic and fast: prices, risk checks, order routing.
- A reasoning agent is better used asynchronously in one of these roles:
  - regime classification every `1-5` minutes
  - news summarization on event arrival
  - post-trade anomaly review when fills or PnL deviate from expectations
  - generation of experiment proposals, not direct order placement

## Recommended architecture for an agent

- `engine`: deterministic order loop
- `market-data worker`: collects quotes, candles, news, spreads
- `reasoning worker`: consumes aggregated snapshots and writes advisory state
- `risk/execution`: may read advisory state, but only as an additional filter

## First concrete experiments

1. Shares momentum
   - universe: `SBER`, `GAZP`, `LKOH`, `TATN`
   - trigger: short-term breakout above intraday range with volume confirmation
   - controls: max holding time, max spread, no averaging down

2. Bonds carry/reversion
   - universe: liquid OFZs
   - trigger: deviation from short rolling fair value with low spread
   - controls: duration bucket limits, no trading around auctions or low-liquidity windows

3. FX reaction
   - universe: `USDRUB`, `EURRUB`, instrument equivalents on T-Invest
   - trigger: sharp move plus confirmation from correlated assets
   - controls: tighter stop logic, session filters, wider slippage assumptions

## Promotion rule

- A strategy moves from sandbox to the next stage only if it survives a fixed observation window with positive expectancy after fees/slippage and without breaking drawdown or exposure limits.
