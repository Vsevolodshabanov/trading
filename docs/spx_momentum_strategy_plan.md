# SPX Momentum Development Plan

## Goal

Transform the current `SPX Momentum` implementation from a working sandbox prototype into a production-grade portfolio engine that:

- matches the reference `SPX_Momentum_Strategy_20260225` logic as closely as possible in both strategy mechanics and backtest methodology
- tracks the full investable universe available through T-Invest API
- filters instruments into a realistic tradable universe
- selects an optimal basket under a fixed RUB budget
- rebalances with explicit turnover, liquidity, and execution constraints
- is measurable through repeatable experiments and live monitoring

## Current State

The service already has:

- one active strategy only: `SPX Momentum`
- live broker routing through T-Invest sandbox
- lot-aware rebalancing
- regime filter
- strategy-level cash and positions ledger
- UI for portfolio state, trades, rebalances, broker orders, positions

The main limitation right now is not the execution shell. The bottleneck is the strategy layer:

- tiny manually configured universe
- simple percentile selection
- regime filter is simpler than the reference SPX implementation
- backtest timing and execution economics do not yet match the reference SPX package
- no dividend-adjusted ranking series
- no full-market instrument ingestion
- no liquidity model
- no optimizer for fixed-budget portfolio construction
- no structured experiment pipeline over a broad universe

## Progress Tracker

### Done

- `Phase 1`: T-Invest instrument catalog sync exists and persists metadata in the local database.
- `Phase 1`: eligibility snapshot exists with exclusion reasons, including `for_qual_investor_flag`, OTC, API availability, and buy/sell availability.
- `Phase 2`: first baseline tradable-universe filtering layer exists.
- `Phase 2`: first historical marketability snapshot exists via `completed_candles`, `median_turnover_rub`, and `history_ready`.
- `Phase 3`: historical candle store exists with OHLCV, completeness flag, turnover proxy, incremental batch backfill, and cursor-based progress tracking.
- `Phase 3`: historical candle loading now returns full OHLCV rows to downstream services instead of a close-only projection.
- `Phase 3`: historical simulation now separates price roles structurally:
  - ranking history uses previous-day daily close
  - rebalance execution uses same-day daily open
  - mark-to-market uses same-day daily close
- `Phase 3.5`: a first historical portfolio simulation runner exists and stores run-level and rebalance-level outputs in the database.
- `Phase 3.5`: weekly/monthly rebalance calendar generation now exists on top of the daily research timeline instead of fixed intraday bar stepping.
- `Phase 3.5`: `SPX/VIX` external regime mode is now the default parity mode for historical simulation.
- `Phase 3.5`: historical simulation now has a controlled fallback to `local_ma` when external `SPX/VIX` regime history cannot be loaded reliably, and the effective regime mode is exposed in run notes and API/dashboard payloads.
- `Phase 8`: dashboard/API now expose historical price-role metadata so we can see which series are used for ranking and execution.
- `Phase 8`: dashboard/API already expose universe size, eligible universe, historical coverage, history-ready count, latest simulation status, return, and max drawdown.

### In Progress

- `Phase 3`: daily research timeline exists, but dividend-aware ranking data are still not complete.
- `Phase 3`: ranking currently uses previous-day raw daily close as a placeholder until dividend history and adjusted research series are implemented.
- `Phase 3.5`: historical simulator now uses separate ranking/execution price roles plus default `SPX/VIX` parity mode with fallback semantics, but it still differs from the reference package in dividend adjustment and execution economics.
- `Reference Parity`: the current implementation matches the high-level momentum idea, but not the full `SPX_Momentum_Strategy_20260225` mechanics.

### Next

- Add dividend history storage and build a true dividend-adjusted ranking series instead of the current raw-close placeholder.
- Expand historical coverage and `history_ready` so the research universe is materially larger than the current tiny usable subset.
- Restore reference-style cash yield / management fee / fixed-plus-proportional cost semantics.

## Development Principles

1. Universe first, optimizer second.
2. Do not expand live trading logic before we can backtest and compare variants.
3. Every rule added to live execution must have an observable metric behind it.
4. We should separate:
   - market data ingestion
   - instrument eligibility
   - alpha/ranking
   - portfolio construction
   - execution and risk
5. Any new portfolio configuration must pass a historical simulation stage before it is allowed into live broker routing.
6. Reference parity comes before creative extensions. We first reproduce the core `SPX_Momentum_Strategy_20260225` behavior faithfully, then add T-Invest-specific improvements.

## Reference Parity Requirement

Our target is not merely to build a strategy that is "similar in spirit" to the reference package. We want the local service to reproduce the following reference behaviors unless there is a documented market-structure reason not to:

- multi-period cross-sectional momentum ranking using the same HQM-style percentile-of-percentiles logic
- explicit rebalance calendar based on daily data and weekly/monthly schedule, not ad hoc intraday bar stepping
- separate price roles:
  - dividend-adjusted ranking series
  - execution price series
- execution backtest semantics:
  - integer lots
  - sell-first / buy-second cash-constrained rebalance
  - fixed plus proportional transaction costs
  - optional cash yield
  - optional management fee accrual
- regime layer semantics:
  - SPX above moving average
  - VIX below threshold
  - optional drawdown and volatility filters
  - previous-day evaluation to avoid lookahead bias
- simulation outputs:
  - equity curve
  - trade ledger
  - rebalance ledger
  - regime state history
  - drawdown and volatility diagnostics

Any intentional deviation from the reference package must be listed explicitly in the plan, code comments, and experiment output.

## Mandatory Pre-Launch Rule

Before we start any new portfolio version in live mode, we must always do the following:

1. Collect historical data for the full eligible universe.
2. Reconstruct the investable universe as it existed over time.
3. Run a historical portfolio simulation in the style of the reference `SPX Momentum` example.
4. Validate portfolio metrics, turnover, cash usage, drawdown and execution feasibility.
5. Only then promote that portfolio specification into sandbox live routing.

This rule is critical because otherwise we would be launching portfolio construction logic that has never been validated on the same historical universe it is supposed to trade.

## Phase 1: Full T-Invest Universe Tracking

### Objective

Build a persistent local catalog of all instruments accessible through T-Invest API.

### Tasks

- Add a scheduled sync job that loads:
  - shares
  - ETFs / funds if accessible and intended
  - bonds
  - currencies
- Store instrument metadata locally:
  - `uid`
  - `figi`
  - `ticker`
  - `class_code`
  - `instrument_type`
  - `lot`
  - `currency`
  - `country_of_risk`
  - `exchange`
  - `buy_available_flag`
  - `sell_available_flag`
  - `api_trade_available_flag`
  - `for_iis_flag`
  - `for_qual_investor_flag`
  - `weekend_flag`
  - `otc_flag`
  - status / active flag
- Add corporate actions and payout metadata where available:
  - dividends
  - coupon schedule for bonds if we decide to support them in the strategy universe
  - split / merge related events if they can be reconstructed
- Add an eligibility table or cached snapshot with reasons why an instrument is excluded.

### Result

We stop working from `TBANK_TRADER_SYMBOLS` as the primary universe source and switch to a database-backed universe registry.

## Phase 2: Tradable Universe Filter

### Objective

Reduce the raw instrument catalog to a realistic rebalance universe.

### Tasks

- Define baseline eligibility filters:
  - only instruments tradable through API
  - only active instruments
  - exclude `for_qual_investor_flag=true` instruments for the retail/IIS default mode
  - exclude OTC / broken / frozen names
  - exclude instruments without enough price history
  - exclude instruments with missing or abnormal lot metadata
  - exclude instruments with insufficient history for the longest momentum window plus warmup buffer
- Add marketability filters:
  - minimum average daily turnover proxy
  - minimum average number of completed candles
  - minimum median traded notional per candle
  - maximum spread proxy if available
- Use turnover proxy from candles:
  - `turnover_rub ~= volume * close`
  - store this derived metric in the historical layer for future filters and ranking penalties
- Split universe into strategy buckets:
  - equities
  - bonds
  - FX
- Decide whether the first serious version should focus only on equities. That is the cleanest path.

### Result

We get `tradable_universe_v1`, which is stable enough for ranking and backtests.

## Phase 3: Historical Data Store

### Objective

Make ranking and experiments independent from one-off API polling while preserving the same data semantics needed by the reference SPX implementation.

### Tasks

- Build a historical candle ingestion job for eligible instruments.
- Store:
  - OHLCV candles
  - timestamp
  - interval
  - completeness flag
- Store price representations needed for research:
  - raw close/open series
  - dividend-adjusted price series or the data needed to reconstruct it
  - dividend history as a separate time series if T-Invest does not provide ready-made adjusted prices
- Reconstruct the reference data split explicitly:
  - ranking series based on dividend-adjusted data
  - execution series based on executable price points, ideally daily open for parity with the reference package
- Add daily research bars even if live routing continues to use intraday candles:
  - daily adjusted bars for ranking and historical parity
  - intraday bars only as an execution/live extension layer
- Support rolling backfill and incremental updates.
- Add data quality checks:
  - missing bars
  - duplicate bars
  - obvious price spikes
  - stale instruments
  - broken dividend-adjustment windows

### Result

We get a local research-grade price store that supports reproducible experiments.

## Phase 3.5: Historical Portfolio Simulation

### Objective

Make every candidate SPX Momentum portfolio go through a historical simulation workflow before live deployment, following the same spirit as the external reference strategy package.

### Tasks

- Build a backtest/simulation runner that operates on:
  - historical eligible universe snapshots
  - historical candle store
  - historical rebalance schedule matching the reference weekly/monthly cadence
  - fixed-budget portfolio constraints
- Reproduce the core portfolio lifecycle on history:
  - universe filtering
  - ranking
  - regime filter
  - target weight generation
  - lot-level portfolio construction
  - cash evolution
  - rebalance execution assumptions
- Store simulation outputs:
  - equity curve
  - position history
  - rebalance ledger
  - regime state history
  - turnover series
  - drawdown series
  - per-period holdings
- Add model comparison output so multiple variants can be compared on the same historical window.
- Reuse the existing `execution_backtester` logic as the core simulation engine instead of rebuilding execution logic from scratch.
- Add an adapter layer that converts the local historical store into the dataframes / inputs expected by the existing backtester.
- Reproduce reference execution economics before adding new realism layers:
  - fixed cost per order
  - proportional bps cost
  - cash yield
  - management fee accrual
  - regime-off liquidation behavior
- Keep slippage as an extension only after reference parity is reached:
  - fixed bps model as baseline extension
  - spread/liquidity-aware model later

### Reference Parity Checklist

The historical simulator is not considered complete until it matches the reference package on the following axes:

- daily price timeline as the research backbone
- weekly or monthly rebalance dates generated from the research timeline
- HQM ranking logic
- SPX/VIX regime signal
- optional drawdown/volatility filter support
- no-lookahead regime evaluation
- sell-first buy-second execution sequencing
- integer-lot rounding
- fixed plus proportional costs
- cash yield handling
- management fee handling

### Promotion Rule

No new portfolio specification should be allowed into live broker routing unless:

- historical data coverage is sufficient
- historical simulation has been completed
- metrics are saved and reviewable
- the strategy passes predefined acceptance thresholds

### Result

Historical simulation becomes a mandatory gate between strategy design and live deployment.

## Phase 4: Ranking Engine Upgrade

### Objective

Replace the current simple score with a ranking stack that can be tuned and compared.

### Candidate features

- reference HQM score as the locked baseline
- trailing returns over multiple windows
- relative strength percentile vs entire universe
- volatility-adjusted momentum
- drawdown penalty
- trend stability score
- volume / liquidity penalty
- regime-aware score suppression
- modular feature weighting through configuration rather than hard-coded ranking formula

### Experiments

- Compare:
  - pure momentum
  - momentum + volatility scaling
  - momentum + drawdown penalty
  - momentum + liquidity filter
- Run tests by:
  - universe bucket
  - rebalance frequency
  - top percentile
  - max positions

### Result

We get a modular scorer instead of one hard-coded ranking formula, so multiple ranking variants can be configured and compared without rewriting the engine.

## Phase 5: Fixed-Budget Portfolio Constructor

### Objective

Choose the best possible basket for a fixed RUB budget under real lot constraints.

### Tasks

- Define the optimization target:
  - maximize total score
  - respect cash budget
  - respect lot sizes
  - respect max weight / max position limits
  - penalize turnover
  - penalize illiquid names
- Implement a first practical solver:
  - greedy score-per-ruble allocator
- Implement a second better solver:
  - mixed integer or constrained heuristic optimizer
- Add portfolio construction constraints:
  - min and max number of names
  - max weight per instrument
  - max asset-class exposure
  - reserve cash buffer
  - turnover cap per rebalance
  - partial rebalance when the target cannot be reached in one pass under live constraints
- Define turnover-aware controls explicitly:
  - max portfolio turnover per rebalance
  - turnover penalty in the objective
  - preference for keeping near-target holdings instead of full reshuffles
- Make cash buffer dynamic:
  - baseline reserve
  - additional reserve based on expected slippage / commissions / current volatility

### Result

We move from “top names with simple weight normalization” to “best feasible lot-level portfolio under a fixed budget”.

## Phase 6: Execution Model

### Objective

Make live routing realistic enough that research and production do not diverge too much.

### Tasks

- First bring live decision logic to reference parity:
  - same rebalance cadence family as the reference model
  - same regime state semantics
  - same ranking input semantics
- Add strategy-specific execution settings:
  - cash reserve
  - max turnover per rebalance
  - per-instrument max fill notional
  - max participation proxy
- Add execution safety:
  - skip names with stale price
  - skip names with abnormal spread / low liquidity
  - rate limit broker calls
  - partial rebalance support
  - shift rebalance date to the next tradable session if the planned date lands on a holiday or weekend
  - monitor placed orders asynchronously and handle partial fills / unfilled remainder
  - persist raw broker responses for auditability
- Separate these ledgers clearly in UI and storage:
  - broker account positions
  - strategy-owned book
  - target portfolio
  - execution slippage stats

### Result

The engine becomes a real portfolio executor, not just a signal sender.

## Phase 6.5: Risk Management Layer

### Objective

Add explicit portfolio-level risk controls before expanding capital or universe size.

### Tasks

- Add max daily loss protection.
- Add portfolio-level trailing stop / drawdown guard.
- Add circuit breakers for extreme market regimes.
- Add concentration controls:
  - max weight per issuer
  - max sector concentration if sector metadata is available
- Decide which risk controls are:
  - hard live blockers
  - soft warnings
  - research-only diagnostics

### Result

The strategy has a dedicated risk layer instead of relying only on ranking and rebalance constraints.

## Phase 7: Experiment Framework

### Objective

Create a repeatable process for deciding which SPX Momentum variant is actually better.

The very first experiment group is not about invention. It is about parity:

- compare local implementation against the reference package on the same historical window
- explain every residual difference
- do not move on to variant research until the baseline mismatch is understood and documented

### Metrics

- CAGR / annualized return
- Sharpe / Sortino
- max drawdown
- turnover
- average holding period
- exposure ratio
- cash drag
- hit rate
- slippage-adjusted return

### Experiment grid

- rebalance frequency:
  - weekly
  - monthly
  - only after parity: daily / 4h / 1h extensions if justified
- top percentile:
  - 80
  - 90
  - 95
  - 98
- number of names:
  - 5
  - 10
  - 20
- regime filter:
  - on
  - off
- budget:
  - 50k
  - 100k
  - 300k
  - 1m RUB

### Result

We stop tuning by intuition and start ranking candidate models by portfolio outcomes.

## Phase 8: UI and Observability

### Objective

Make the dashboard useful for research and live control of one strategy.

### Add to UI

- universe size and eligible size
- current selected basket
- target vs actual weights
- current cash buffer
- turnover on latest rebalance
- excluded instruments with reasons
- top-ranked names just below the cutoff
- strategy vs broker inventory split
- experiment snapshot / active model version

### Result

The UI becomes a strategy console, not just a trade monitor.

## Technical Implementation Notes

- Prefer asynchronous T-Invest ingestion for large universe jobs.
- Use the T-Invest client in a way that supports batched / scheduled historical backfills.
- Keep the service Docker-first and add a scheduler for recurring sync jobs:
  - external cron on host, or
  - container-native scheduler such as `ofelia`
- Keep data ingestion, simulation, and live trading as separate runnable workflows.

## Known Risks

- T-Invest historical data may be incomplete or have gaps.
- Universe composition can drift over time and must be versioned in historical simulations.
- Corporate actions may distort momentum signals if prices are not adjusted correctly.
- Exact SPX and VIX benchmark parity may require an external benchmark data source even if trading is executed through T-Invest.
- Bond / FX support can complicate the first serious version; equities-only may still be the right production path initially.

## Immediate Build Order

This is the recommended order of implementation from here:

1. Build instrument catalog sync from T-Invest API.
2. Store eligibility flags and exclusion reasons, including non-qual restrictions.
3. Build historical daily data store for the entire eligible universe, including dividend-aware price reconstruction.
4. Build reference-compatible rebalance calendar generation on top of daily data.
5. Reproduce reference SPX ranking logic exactly on local data.
6. Reproduce reference SPX/VIX regime filter and optional drawdown/volatility filters with no-lookahead evaluation.
7. Build mandatory historical portfolio simulation for the full universe by reusing the existing backtester core and restoring reference execution economics.
8. Compare local baseline against `SPX_Momentum_Strategy_20260225` and document any remaining gaps.
9. Switch SPX Momentum live decision logic to the database-backed, reference-compatible implementation.
10. Only after parity is established, add a first fixed-budget greedy portfolio constructor.
11. Only after parity is established, compare 3-5 ranking variants inside the historical simulator.
12. Add turnover, liquidity and drawdown penalties.
13. Add explicit portfolio-level risk management rules.
14. Add target-vs-actual portfolio views in UI.
15. Only after that tighten live execution and increase capital.

## What To Do Next

The next concrete development task should be:

`Implement a T-Invest instrument catalog sync + eligibility snapshot.`

Why this is first:

- without a full universe there is no serious SPX Momentum strategy
- without eligibility metadata there is no clean backtest universe
- without a stable universe definition we cannot build a meaningful budget optimizer

After that, the second task should be:

`Implement historical daily candle backfill plus dividend-aware ranking data for all eligible instruments.`

After that, the third task should be:

`Implement reference-compatible historical portfolio simulation for SPX Momentum over the full eligible universe.`

Only then does it make sense to invest serious effort into “ideal combination under fixed budget”.
