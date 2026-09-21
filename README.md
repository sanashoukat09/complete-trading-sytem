# Compression Radar 6.1.3 — Spring / Upthrust Live-Data Paper Runtime

**Purpose:** monitor live Binance USDⓈ-M public market data, detect failed-auction **Spring longs** and **Upthrust shorts**, and execute them in an auditable **paper/shadow ledger** with realistic quote/depth, costs, risk and position management.

**It does not place real-money exchange orders.** No API key or account credentials are used. This release is engineered for live public-data paper validation; profitability on unseen markets is not guaranteed or certified.

## Start here

1. Keep every previous 6.1 / 6.1.1 folder and database unchanged as evidence.
2. Extract this ZIP into a **new writable folder**.
3. Install Python **3.11+** with the Windows Python launcher (`py`).
4. Double-click **`run-paper.bat`**.
5. The launcher installs the one runtime dependency, runs `radar doctor`, and only starts the scanner if public Binance REST/WebSocket connectivity succeeds.
6. Open **http://127.0.0.1:8780/**.
7. Leave the terminal running; use **Ctrl+C** for a normal stop.

The fresh experiment directory is **`data-paper-v613`**. Never copy an older radar database into it and never modify a stored source/config hash just to force a restart.

Manual Windows commands:

```powershell
py -3 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m radar doctor
.venv\Scripts\python -m radar paper --config config.json --data-dir data-paper-v613
```

If `doctor` fails, do not run degraded. Fix network/DNS/region access or the reported endpoint problem first.

## What changed after the complete 6.1 forensic run

The complete supplied history contained about **1.34 million events** and no positions. The important discovery was that the zero-trade result was largely an operational observation problem rather than proof that the market offered nothing. Incoming trades were often reasonably fresh at receipt, but the old reducer/worker path accumulated roughly **11 seconds median internal delay** while the strategy correctly rejected evidence older than three seconds. The event queue also reached its former capacity during bursts.

6.1.3 therefore fixes the runtime before changing the trading decision:

- a 50,000-event bounded queue and batched ingestion/reduction;
- state reuse inside reducer transactions instead of repeatedly decoding the same symbol state for every tick;
- targeted flush barriers rather than stopping the whole live feed until a continuously arriving queue empties;
- persistent Binance WebSocket connections with live subscription deltas rather than reconnecting retained symbols after each ranking change;
- current 2026 Binance USDⓈ-M stream lanes: book/depth on `/public`, aggregate trades on `/market`;
- independent OI polling for monitored/pinned symbols, so broad discovery refresh cannot starve a mature setup;
- active balance/attempt/position pinning through ordinary ranking churn;
- lossless compressed journal payloads with backward-compatible replay and a bounded WAL/checkpoint policy;
- durable queue/lag/coverage health reporting.

On the captured event shape, the repaired reducer benchmark processes roughly **4.35k events/sec** versus a worst observed recorded burst of about **836 events/sec**. This is a local code benchmark, not a guarantee for every PC or future market burst; runtime health still blocks stale evidence rather than pretending it is current.

## Trading logic: one setup idea, two mirrored directions

There are only two tradable setup families:

- **Spring long:** downside excursion fails and the auction reclaims the frozen lower boundary.
- **Upthrust short:** upside excursion fails and the auction reclaims the frozen upper boundary.

The engine does not add unrelated breakout/continuation families merely to increase trade count.

A failed-auction attempt progresses through a causal path:

**frozen balance → genuine sweep → outside effort/result → reclaim → effective response → direct entry OR defended retest → executable proposal**.

The competing hypothesis stays alive. Sustained outside acceptance after reclaim, renewed structural extension, missing critical evidence, bad liquidity or broken economics invalidates/waits rather than forcing a trade.

### Experienced-entry behavior

The system is not hard-coded to “enter early” or “wait more.” It chooses the path from the evidence and economics:

- **Direct flow response:** favorable aggressive flow and favorable price progress agree.
- **Price-led failed effort:** adverse flow fails to make adverse progress and price produces a strong directional response; a fixed 55% favorable-flow number cannot veto the market response by itself.
- **Defended retest:** evidence is already valid but the direct entry has become economically poor; instead of chasing or loosening minimum RR, the attempt stays alive for a later defended test and renewed progress.
- **Outside acceptance / structural invalidation:** cancel the failed-auction hypothesis.

This is intentionally different from both permissive trading and textbook over-confirmation.

## Discovery versus decision

Discovery answers **where to spend live-stream resources**; Spring/Upthrust still decides whether a trade exists. 6.1.3 replaces the old 40-coin, single-snapshot attention model with a two-stage broader scan:

1. **Quick scan:** up to 280 eligible liquid USDT perpetuals, diversified across 24h movers and turnover, with closed 1m bars used to detect fresh 5m activity.
2. **Deep scan:** up to 160 contracts receive full compression history plus 5m OI history. Up to 24 qualified symbols are live-streamed, plus any structural/position pins.

Heat is now path-dependent rather than a single 5-minute score. The deep layer compares recent 5m/10m volume, signed 5m/15m OI behavior and how much of an OI impulse remains. It classifies attention as **NEW_IMPULSE, SUSTAINED_HOT, HOT_RETAINED, COOLING, FLUSH_EVENT, NORMAL, or DEAD_BURST**.

- A +OI/volume shock that continues or mostly remains can stay hot even if the latest 5m OI change cools slightly.
- A one-window burst that almost fully reverses while current volume returns to normal becomes **DEAD_BURST** and is not given a live-stream slot.
- A large negative OI event accompanied by exceptional current volume and net deleveraging becomes **FLUSH_EVENT**, because forced liquidation can still be important failed-auction context.
- Once a real Spring/Upthrust attempt starts, discovery cooling cannot evict it. A passive balance can relinquish its pin after a confirmed dead burst, but an active attempt/position remains monitored.

BTC/ETH are excluded from new candidate selection by default but can be configured. A bounded scan can still miss a contract outside the quick/deep caps; the caps are operational safeguards, not claims that exactly 160 coins matter.

## Entry, sizing and risk

Before an `INTENT`, the engine requires:

- current instrument filters;
- fresh quote and, by default, top-20 depth;
- executable-side price plus adverse slippage assumption;
- structural stop beyond the failed excursion;
- credible midpoint/opposite-boundary target provenance;
- fees/funding allowance;
- minimum cost-adjusted reward/risk;
- valid tick/lot/notional geometry;
- per-trade and shared portfolio budget.

Default paper risk is **0.3% of modeled equity per trade**, with a **1.2% total open-risk cap**, **2% modeled daily loss cap**, a notional cap and at most three simultaneous positions. These are experiment defaults, not personalized financial advice.

An authorized paper entry still waits for a subsequent valid quote and is recalculated before fill. If price movement ruins the geometry, it cancels rather than using stale authorization.

## Manager

Paper execution and management use the same durable position state:

- entry fill against valid executable liquidity;
- structural stop;
- TP1 and optional TP2;
- configured partial at TP1;
- after TP1, the remaining position receives a **cost-adjusted breakeven protection level**;
- TP2, structural stop, breakeven protection, semantic invalidation or maximum-hold exit;
- taker fees, adverse slippage assumptions and observed funding accounting;
- stale/thin liquidity causes `WAIT`, never a fictional fill.

A stop is an invalidation trigger, not a guaranteed fill price; gaps can lose more than one modeled R.

## Modes

| Mode | Behavior |
|---|---|
| `collection` | collect/process public data only |
| `shadow` | generate/log eligible proposals; no paper positions |
| `paper` | simulated positions, fills, management and accounting |
| Real-money | **not implemented** |

The shipped `config.json` uses `paper`.

## Dashboard and evidence

The dashboard at `127.0.0.1:8780` shows live-data health, selected/pinned symbols, range/attempt state, decisions, positions and outcomes. A stale quote is shown unavailable; the last candle is not presented as a live price.

The authoritative experiment is `<data-dir>/radar.db` (SQLite WAL). It includes durable events, state, decisions, positions, fills and outcomes. New journal payloads are losslessly compressed; the raw exchange message is retained inside the encoded event for replay/audit.

Default disk budget is 10 GB. The process stops at its configured limit rather than silently deleting evidence. Stop normally before archiving an experiment.

## Replay / status / export

```powershell
.venv\Scripts\python -m radar status --data-dir data-paper-v613
.venv\Scripts\python -m radar export --data-dir data-paper-v613 --output paper-positions.csv
.venv\Scripts\python -m radar research --data-dir data-paper-v613
.venv\Scripts\python -m radar replay --source data-paper-v613\radar.db --destination replay-v613.db
```

Replay uses the same reducer and makes no network calls. Keep this exact release ZIP with the experiment so code/config provenance remains reproducible.

## Verification included with this release

Run:

```powershell
.venv\Scripts\python -m pip install -r requirements-dev.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python -m radar demo --data-dir demo-data
```

The release suite covers long/short mirrored strategy paths, queue/freshness behavior, subscription retention and deltas, reconnect/gap behavior, persistence/replay, entry geometry, duplicate protection, partial/final fills, TP1→breakeven management, TP2, fees/funding, stale/thin/crossed books, disk guards and dashboard/runtime behavior.

The demo is a **synthetic favorable fixture** whose purpose is to prove plumbing from setup through accounting. Its R result is not a historical performance claim.

See:

- `RELEASE-REPORT.md` — what was actually verified and what was not;
- `SPRING-UPTHRUST-UPGRADE.md` — failed-auction decision contract;
- `audit/` — forensic/replay/benchmark/test evidence.

## Important limits

No system can be guaranteed to give the “best” result on every unseen trade. This package is designed to avoid the concrete runtime/logical defects found in 6.1 and to preserve uncertainty rather than fabricate certainty.

The complete old archive could reconstruct trade-price setup paths, but it did **not** persist historical quote/depth events needed for an authentic executable historical fill replay. Therefore reconstructed historical candidates are labelled as such; they are not promoted to fake backtest trades.

Live public Binance connectivity could not be completed from the build workspace because its external DNS resolution is blocked. `doctor` is included specifically so your machine tests current public REST plus the 2026 WebSocket routes before the scanner starts.

Prospective paper performance on future unseen markets remains the economic test. The software release gate demonstrates correct execution mechanics; it does not certify a future win rate or positive expectancy.
