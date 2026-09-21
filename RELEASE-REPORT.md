# Compression Radar 6.1 — recorded-run audit and implemented repairs

Date: 20 September 2026. Input: `compression_radar(6).zip`.

## Verdict

**The supplied run was not collecting sufficiently fresh and complete data to evaluate the strategy's trading edge. Both proposed fixes were real issues. Additional implementation and dashboard defects were found and repaired.**

This deliverable updates the supplied **V6 code and its added dashboard**. It is not another replacement architecture. It remains an experimental public-data paper/shadow system with no exchange order placement. Passing software tests does not establish an accurate or profitable strategy in every market.

The repairs preserve entry, structural stop, reward-after-costs checks, shared account risk limits, partial/final exit accounting, and paper-only execution. They do not loosen stale-data checks to manufacture trades. The compression baseline calculation changes explicitly to comparable-duration windows; this is a strategy definition change that needs a new experiment.

## What your recorded run actually contains

The database was recovered with its supplied WAL and copied using SQLite backup. Integrity check: `ok`. The uploaded archive and original working extraction were retained. The recovered snapshot has **104,700 events**, 59 symbol states, 30,035 decisions, **zero positions**, and **zero outcomes**. Recorded receipt timestamps span **12:06:33–16:04:36 UTC on 20 September 2026** (approximately 17:06–21:04 Pakistan time).

| Observation | Recorded result | Meaning |
|---|---:|---|
| Reconstructed compression episodes | 38 | Historical outputs of the original algorithm, not independently proven accumulation |
| Episodes with no trade events while active | 34 | Tick-level setup confirmation was unavailable for most ranges |
| Trade events | 35,121 | Incomplete subscribed-stream sample |
| Trades rejected as delayed by original logic | 26,291 (74.9%) | Most observed trades were not eligible as current evidence |
| Median trade receipt minus trade timestamp | 203,253 ms | Roughly 203 seconds, far beyond the 3-second guard |
| Median quote delay | 153,328 ms | Dashboard receipt alone was not proof of live prices |
| Median depth delay | 135,378 ms | Depth-based fills could not reliably use these observations |
| Completed setup candidates in original event reconstruction | 0 | No confirmed candidate reached the proposal layer in available inputs |
| Episodes with a later recorded bar crossing either edge within 30 minutes | 26 | Price movement existed; this is not proof of an executable missed trade |

Trade publication timestamps in raw `E` fields also lagged receipt by about 203 seconds at the median. This was not merely the difference between trade time and aggregation publication time. The archive cannot isolate how much came from network/socket buffering, application backpressure, local disk/CPU, or clock behavior. Queue and clock telemetry were missing from the original run. We therefore fix confirmed bottlenecks and add those measurements instead of declaring an unproven sole cause.

### Were trades missed?

**There is no demonstrated completed, valid setup that was incorrectly refused execution in this recording. There is substantial evidence of missed monitoring.** The original observer reconstruction emitted zero candidates. Most ranges had no ticks, and most recorded ticks were delayed. A later candle wick beyond a range cannot establish the order of sweep, reclaim, response, aggression, fresh executable quotes, and affordable risk/reward.

These are the four original episodes with any recorded ticks while active:

| Symbol | Frozen lower–upper | Recorded ticks | Recorded trade-price minimum–maximum |
|---|---|---:|---|
| CHIPUSDT | 0.04087–0.04118 | 39 | 0.04096–0.04114 |
| AKEUSDT | 0.045326–0.054242 | 8,141 | 0.046407–0.05039 |
| CELRUSDT | 0.003901–0.004065 | 499 | 0.00396–0.004008 |
| MARSCOINUSDT | 0.09406–0.09525 | 20 | 0.09467–0.09475 |

Those observed prices stayed inside their corresponding ranges. Other episodes may have had opportunities while not subscribed, but assigning them an entry or PnL would invent unavailable evidence. `audit/episode-coverage.csv` lists all 38 original episodes, range edges, widths, rotations, tick/depth counts, available subsequent bars, and later boundary crossings. Later highs/lows are descriptive observations, not trade returns. Missing bars and observations are exposed rather than filled synthetically.

## Implemented repairs

| Area / file | Confirmed problem | Implemented behavior |
|---|---|---|
| `radar/market.py`, `radar/engine.py` | Only positions were pinned; an active compression could disappear from coarse candidates or live selection | Pending/open positions and unexpired tradable episodes are pinned. New eligible ranges are reconsidered by the monitoring loop, not just the five-minute ranking refresh. Expired ranges lose their pin. |
| `radar/market.py` | A universe change canceled all streams and reset every selected symbol's flow | Per-symbol subscription tasks retain existing sockets. Only added/removed symbols change connections. Real reconnects still emit GAP events. |
| `radar/engine.py` | Every recovery lookup scanned the growing event table | Indexed `(applied, seq)` lookup. Local 104,700-row benchmark: median 17.20 ms before, 0.00237 ms after, 30 repetitions. This is a query benchmark only. |
| `radar/engine.py`, `radar/market.py` | Per-event durability and thread scheduling can amplify backlog | Worker drains up to 32 already-queued events without waiting to fill a batch. Inputs are committed durably, then bounded reducer groups commit atomically. A group failure rolls back all group state/offset effects; recovery cannot skip failed work. |
| `radar/model.py`, `radar/market.py`, `radar/engine.py` | A quote could age in the local queue while its original receipt time was still used as decision time | `processed_ms` is recorded separately; decision time is at least receipt time. Quote source/receipt timestamps stay intact. Queue-delayed entries are canceled or blocked, and exits await valid liquidity. |
| `radar/market.py` | Repeated stale socket messages could continue to drain obsolete history | Repeated over-age observations trigger reconnect and an explicit gap/warmup cycle. Old messages are still journaled; no false fresh timestamps are assigned. |
| `radar/market.py` | Broad REST refresh serialized bar/funding work; a three-bar backfill could miss longer gaps | Independent supervised refresh, bar, funding, clock, timer and reducer tasks. Selected-symbol bar polling catches up based on last closed bar, up to the venue request bound. Any remaining bar gap blocks fresh compression. |
| `radar/market.py` | Clock offset was sampled only at startup; queue/service health was not preserved | Periodic offset/uncertainty measurement, explicit gaps on material offset changes, per-symbol stream times, queue depth/delay, and periodic durable HEALTH records. |
| `radar/strategy.py` | Comparing a 30-minute range against a 90-minute envelope can label ordinary trend movement as contraction | Compare recent range width with the median of prior equal-duration range widths, while retaining rotation and drift checks. Store source interval, baseline width, and contraction ratio with the range. |
| `radar/strategy.py` | Old backfill could create a nominally new current episode | New episodes require a recently closed source candle. Old bars still build history but cannot alone activate a current setup. |
| `radar/strategy.py` | Expiry was skipped on early-return observation types; stale bar context could support entries | Expiry is evaluated before early returns. Stale bar context blocks candidate confirmation. Expired unselected states can be cleaned by timers. |
| `radar/strategy.py`, `radar/engine.py` | Quote/timer processing erased the useful blocker, producing almost exclusively `NO_ELIGIBLE_SETUP` | Preserve meaningful reasons and journal state/reason transitions, with episode and attempt evidence. Timer records retain the blocker. |
| `radar/__main__.py` | Stale quotes or a last candle close were presented as current prices; expired ranges remained active | Use the same quote validity check as execution. Show unavailable live prices, retain the historical close separately, exclude expired ranges and consumed attempts from approach alerts. |
| `radar/__main__.py` | Dashboard used a shared connection without one enclosing snapshot lock | Enrichment runs under the reducer connection lock so related portfolio/state reads are consistent. |
| `radar/__main__.py`, `radar/dashboard.html` | Current-position price repeated entry; unrealized PnL showed booked net | Display fresh executable-side marks. Compute unrealized mark PnL from remaining quantity and entry. Mark PnL excludes hypothetical exit costs; stale marks are unavailable. |
| `radar/dashboard.html` | Slow/erroring fetches could launch again every 100 ms; stale gauge appeared at midpoint | One in-flight fetch, timeout, retry pacing, visible paused/stale status, and no gauge marker without a live price. A real 0% range position is preserved. |
| `radar/dashboard.html` | Hard-coded account caps and a false equity fallback could mislead | Read caps from config; preserve zero equity; display fresh-quote coverage and queue delay. |
| `pyproject.toml` | Installed distributions could omit the added dashboard file | Include `dashboard.html` as package data; installed-package presence verified. |
| `radar/config.py` | Boolean/string values could pass numeric configuration validation | Reject inappropriate field types explicitly. |

### What the range and hot-coin labels mean

Range edges remain the **minimum low and maximum high of the recent closed-bar window**, with alternating visits and limited drift. They are observable boundaries, not a claim that every edge is institutional support/resistance. The equal-duration baseline removes one bias; it does not prove accumulation or predict a large move.

Discovery remains a bounded, turnover-filtered candidate pool ranked initially by absolute 24-hour percentage movement, then by relative volume surprise and the magnitude of OI-growth surprise. It considers both expansion and contraction. **This is not an exhaustive search of every newly heating altcoin.** A coin outside the coarse pool can be missed. Ranking parameters remain experimental; no fitted win-rate or optimal hold time is claimed. Pins preserve monitoring after discovery rather than requiring a coin to remain at the top of a transient ranking.

OI changes measure outstanding contracts; they do not independently identify directional buying, new money inflow, or a forthcoming move. In this implementation they support selection, while a failed-auction setup needs observed price behavior and aggression confirmation. There is no validated hidden-order/iceberg inference, full footprint engine, or automatic discretionary human judgment.

The strategy is stateful and normalized to market observations, but necessarily uses explicit parameters and rules. It should not be described as a rule-free experienced trader. No parameter was optimized to make this short run produce trades.

## Verification and limits

- Original supplied suite: **67 passed** before repairs.
- Updated suite: **89 passed**. The 22 added cases cover pins, socket retention, full feed lifecycle with a controlled client, expiry, same-duration compression, backfill, stale dashboard values, side-specific marks, queue-time freshness, wrong config types, index usage, durable telemetry, batch rollback, and batch/single reduction equivalence.
- Existing tests include mirrored long/short flows, restart/unclean-process recovery, duplicate input/fill effects, concurrent connections, gap/warmup behavior, fees/funding, partial exits, stale/thin/crossed books, and seeded random paths. These are software cases, not a distribution of future market outcomes.
- Full repaired-code replay processed **104,700 recorded events**, leaving **zero pending**, with **zero positions/outcomes**. This is a changed-code replay of the supplied observations; it cannot simulate the additional streams that pinning would have collected or remove original data delays. Reasons now expose delayed trades, no compression, gap warmup, and stale bars instead of concealing them in a generic message.
- JavaScript syntax and installed dashboard asset checked. Dashboard HTTP/JSON behavior tested. No claim of manual visual testing on your Windows machine.
- Direct public Binance connectivity was attempted here and failed at DNS resolution. No network restrictions were bypassed. **Live exchange connectivity and a prolonged real-feed soak remain unverified in this environment.**
- Test success does not measure unseen-market win rate, expectancy, or drawdown. This run has no trades from which to estimate them.

## How to run this update

1. Keep the old project and its `data-paper` directory as the original evidence.
2. Extract the new ZIP to a **new folder**, not over the running application.
3. Install Python 3.11+ if needed, then run `run-paper.bat`.
4. The launcher creates/uses **`data-paper-v61`** and starts a fresh frozen experiment. No existing account positions need migration: this supplied run had none.
5. Open http://127.0.0.1:8780/. Check fresh quote coverage and worker lag, not merely whether the page refreshes. A stale/unavailable display is an operational problem, not an entry signal.
6. If startup fails, run `.venv\Scripts\python -m radar doctor` and retain the error. Stop normally with Ctrl+C before archiving your next run.
7. Run `test-project.bat` to repeat the software tests locally. Run the synthetic `demo` only as a functional check; its outcome is not market evidence.

Do not copy the old DB into the new data directory or alter its frozen hash to force it open. Source changes intentionally start a new experiment. The new ZIP omits the old virtual environment, caches, build output, old test artifacts, and recorded DB from the runtime folder. It contains current source, dashboard, tests, launchers, and this audit. Your original uploaded archive remains the raw evidence source.

## What still needs observation before stronger claims

The next run must first demonstrate continuously usable per-symbol quotes/depth/trades, current bars and OI, and bounded queue delay. Check that every pinned active range receives its streams and that valid constructed setups still pass the pipeline without real data staleness. If your laptop cannot sustain the pinned universe, reduce the candidate/universe workload in a **new experiment**; do not loosen freshness guards.

Collect both candidate and rejected setup evidence. Assess realized paper fills including costs, missed opportunities with complete coverage, parameter sensitivity, and held-out market periods. Compare failure rates across direction, liquidity, volatility, and sessions. Freeze the evaluated variant before judging new data. A longer test should include disconnect/restart and disk-limit behavior. These are remaining evidence requirements, not a promise that sufficient paper testing guarantees future profits.

A broad strong move after a range is not necessarily a spring or upthrust entry. Continuation entries are still outside this release. Full-volume-profile/absorption analytics, multi-exchange corroboration, and real-money execution are also outside the supported scope. This repair does not silently add untested variants to increase activity.

**Release conclusion: repaired and regression-tested paper runtime; not certified profitable, universally accurate, or ready for unattended real-money execution.**

## Evidence files

- `audit/recorded-run-summary.json`: source hashes, recovered-database integrity and latency statistics.
- `audit/episode-coverage.csv`: every original reconstructed episode, boundaries and available observations.
- `audit/original-trace.json`: original observer reconstruction and universe changes.
- `audit/baseline_strategy.py`, `audit/trace_run.py`: the original observer and its reconstruction script.
- `audit/repaired-recorded-replay.json`, `audit/replay_recorded.py`: repaired-code replay results and runner.
- `audit/query-benchmark.json`: before/after pending-event lookup measurements and query plans.
- `audit/tests.txt`, `audit/source-sha256.json`: final tests and source inventory.

Archive SHA-256: `5f5a1b1d016312fa877155d6f70b82531b1105c9d5dc2ad829c4704994cc54fb`.
Recovered SQLite snapshot SHA-256: `4dd1dfd66109851fbff9a04ae79320e62e9c3363396a1d598b668d54f53f0e9d`.
