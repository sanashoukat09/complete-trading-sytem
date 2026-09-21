# Compression Radar 6.1.3 — Release Report

Date: 21 September 2026

## Release decision

6.1.3 is the completed **live-public-data / paper-execution** Spring+Upthrust release. It repairs the runtime defects demonstrated by the full 6.1 run, strengthens the failed-auction event contract, verifies the complete paper execution/manager lifecycle in both directions, and updates Binance USDⓈ-M WebSocket routing for the 2026 endpoint migration.

It deliberately contains **no authenticated real-money order transport**. The supported deployment is live market collection plus shadow/paper decisions. No future win rate, expectancy or “best result” is guaranteed.

## What the full 6.1 run proved

The complete supplied history contains **1,340,389 events**, **94,603 decisions**, **0 positions** and **0 outcomes**, spanning 20 Sep 2026 18:18:23 UTC through 21 Sep 2026 01:04:12 UTC.

The key cause of the zero-trade run was operational rather than simply “no setup”:

- TRADE events: **1,257,115**.
- Median exchange/event → local receipt delay: **533 ms**; p95 **2,690 ms**; only **2.32%** exceeded 3 seconds.
- Median local receipt → strategy processing delay: **11,084 ms** overall and **11,074 ms** on trades; p95 trade delay **13,762 ms**.
- Worst recorded received burst: **836 events in one second**, **1,675 across three seconds**.
- Decision funnel included **222 WAIT_BOUNDARY_SWEEP**, **33 WAIT_RECLAIM**, and **7 OUTSIDE_ATTEMPT** cycles, so zero positions did not mean the scanner never approached a setup.

Because the strategy correctly rejected evidence older than its freshness guard, the old engine was frequently making usable evidence stale inside its own queue before the decision layer could use it.

The archive also does **not** contain historical QUOTE or DEPTH rows. They existed live in memory/health, but were not retained in the recovered event journal. Therefore no later analysis can honestly reconstruct exact historical executable bid/ask, depth-weighted fill or slippage from that run.

## Historical setup-path diagnostic

For diagnosis only, 6.1.3 was replayed over symbols that actually formed balances/attempts after removing recorded GAP artifacts and normalizing local receipt timing. Quote/depth were **not invented**. This is therefore a strategy-path counterfactual, not a backtest or executable trade ledger.

After the final genuine-sweep and mixed-response repairs it produces **29 unique Spring/Upthrust candidate paths**: **11 reach the frozen-range midpoint before structural stop and 18 hit structural stop first**. The pre-fix observer produced 37 candidates with the same 11 midpoint-first cases; the quality repair removed eight stop-first micro/noisy paths without deleting those 11 cases.

Examples preserved by the final observer:

| Symbol | Setup | Decision UTC | Trigger | Structural stop | Mid | Far | Recorded first passage |
|---|---|---:|---:|---:|---:|---:|---|
| EGLDUSDT | Upthrust | 2026-09-20 18:28:13.714 | 3.688 | 3.699 | 3.680 | 3.666 | Mid first |
| EGLDUSDT | Upthrust | 2026-09-20 18:46:51.407 | 3.688 | 3.701 | 3.680 | 3.666 | Mid first |
| EGLDUSDT | Spring | 2026-09-20 18:58:09.377 | 3.671 | 3.658 | 3.680 | 3.694 | Mid first |

These trigger/structure values come from recorded trades and the causal strategy state. They are **not claimed fills** because historical book/quote observations are missing.

The original ALGO upside attempt that motivated the forensic check still does not become a valid Upthrust merely because timing is normalized: it did not provide the required failed-auction reclaim/response. This is an important negative control—the fix is not “let more things trade.”

## 6.1.3 persistent-heat discovery upgrade

The Spring/Upthrust decision contract is unchanged from the final 6.1.2 logic; 6.1.3 changes how broadly and intelligently the system decides **what to keep watching**.

The old discovery layer deeply evaluated only 40 contracts and treated the latest standardized volume/OI surprise too independently. A single 5-minute OI/volume burst could therefore look hot even if the entire impulse disappeared immediately afterward. The new discovery pipeline uses:

- a diversified **quick scan of up to 280** eligible liquid USDT perpetuals;
- a **deep scan of up to 160** contracts with compression history and 5-minute OI history;
- up to **24 live-streamed qualified symbols**, plus structural/position pins;
- 5m and 10m volume context;
- signed 5m and 15m OI context;
- OI impulse direction, retained fraction and reversal fraction;
- explicit heat lifecycle states: `NEW_IMPULSE`, `SUSTAINED_HOT`, `HOT_RETAINED`, `COOLING`, `FLUSH_EVENT`, `NORMAL`, and `DEAD_BURST`.

A one-window impulse that almost fully reverses while current volume returns to normal becomes `DEAD_BURST` and receives no live-stream slot. A partial OI pullback that leaves most of the participation intact can remain `HOT_RETAINED`. A large negative OI move with exceptional current volume and net deleveraging becomes `FLUSH_EVENT`, because forced liquidation can still matter for a failed auction.

Passive balances may relinquish their structural pin after confirmed `DEAD_BURST`; once a causal Spring/Upthrust attempt exists, discovery cooling cannot evict it. This keeps resource allocation adaptive without letting ranking changes interrupt a live setup.

A dedicated 60-contract fake-exchange integration test deep-scans 50 symbols end-to-end (above the old 40 cap), ranks the heat states and selects the qualified live-stream set. Unit paths separately prove burst→dead, retained cooling, sustained heat and deleveraging flush behavior.

The normalized 6.1 historical strategy-path replay remains **29 unique Spring/Upthrust candidates**, preserving the same setup behavior. That is expected: discovery coverage changed; the mature setup contract did not.

## Runtime repairs

### Throughput and freshness

- worker queue increased and kept bounded;
- event ingestion and reducer work execute in bounded batches;
- per-symbol state is reused within reducer transactions instead of decoded/encoded per tick;
- targeted flush barriers replace global drain behavior during an always-arriving live stream;
- queue depth/delay/overload telemetry is retained;
- repeated stale data still causes reconnect/gap handling rather than timestamp laundering.

Final benchmark against the real 6.1 event shape: **20,000 events in 4.60 s ≈ 4,350 events/s**, versus the old run's worst observed received burst of 836 events/s. This is measured software headroom in the build environment, not a claim that every machine/market will have the same margin.

### Feed lifecycle

- one persistent **public** socket for `bookTicker` and `depth20@100ms`;
- one persistent **market** socket for `aggTrade`;
- live `SUBSCRIBE` / `UNSUBSCRIBE` deltas when the monitored universe changes;
- active balances, attempts and paper exposure remain pinned through normal ranking churn;
- ordinary book reconnect removes liquidity certainty without erasing continuous trade-auction evidence;
- trade-stream gaps invalidate effort/response evidence and require warmup;
- OI polling is independent for active monitored symbols.

### Storage

New event payloads are losslessly zlib-compressed while retaining the normalized event and original raw venue message. The decoder remains compatible with old plain JSON events. WAL/checkpoint behavior and configured disk limits bound transient growth. A representative depth20 codec sample previously measured ~68% payload-byte reduction; actual full-database reduction varies with event mix and SQLite overhead.

## Spring / Upthrust decision contract

Only Spring long and Upthrust short are tradable.

The mirrored sequence is:

1. frozen causal balance;
2. **genuine sweep** (at least 5% of balance width or the instrument noise floor);
3. outside effort/result observation;
4. directional reclaim;
5. effective response;
6. direct response entry or patient defended retest;
7. fresh executable quote/depth and economic authorization.

Effort/result routes distinguish high-effort poor-result, low-effort exhaustion and mixed evidence. Flow is supporting evidence rather than an absolute fixed-percentage veto. A price-led confirmation must be **strictly stronger** in price progress than the aligned-flow route; a prior mixed-effort logic inversion that could make the “strong” path easier has been removed.

If evidence is valid but the direct quote destroys cost-adjusted R:R, the engine does not loosen the R:R and does not chase. The attempt changes to `RETEST_ONLY`; a later defended revisit plus renewed response can authorize a new proposal. Persistent outside acceptance or new structural extension invalidates the failed-auction hypothesis.

## Execution and management verified

Paper proposals use current metadata, executable side, depth capacity, adverse slippage, fee/funding allowance, structural stop and target provenance. Risk is recomputed before the subsequent quote fill. Bad geometry cancels.

The manager supports:

- entry;
- structural stop;
- TP1 partial;
- cost-adjusted breakeven protection after TP1;
- TP2/final exit;
- time/semantic invalidation exits;
- funding/fee accounting;
- stale/thin liquidity waits instead of fictional fills;
- restart/idempotency tests.

The final deliberately favorable **synthetic** full-engine demo processed 236 events and actually executed the engine ledger from setup through `TP2_HIT`: initial modeled risk **29.9768**, fees **1.9979**, net **128.5364**, net R **4.2879**. This is proof of software execution mechanics only; it is not market-performance evidence.

## Release verification

- `python -m compileall -q radar tests`: **pass**.
- `pytest -q`: **113 passed**.
- long and short Spring/Upthrust full trade lifecycle: pass.
- TP1 → cost-adjusted breakeven protection: pass in both directions.
- TP1 → TP2 close: pass in both directions.
- persistent WebSocket subscription delta: pass.
- current `/public` + `/market` stream-lane assertion: pass.
- lossless raw venue-message journal round-trip: pass.
- clean package install/import: version **6.1.3**, dashboard resource present.
- throughput benchmark: ~**4,350 events/s** on the recorded event shape.

Exact machine outputs are saved under `audit/`.

## What was not possible to verify here

The build container cannot resolve `fapi.binance.com`; `radar doctor` therefore fails here at DNS resolution. No network restriction was bypassed. The Windows launcher runs `doctor` first so your own machine must prove current public REST/WebSocket access before paper mode starts.

The tests and synthetic execution do not prove future profitability. The historical 6.1 archive cannot supply exact executable historical fills because quote/depth history is missing. A clean prospective 6.1.3 live-data paper run is therefore the appropriate economic evaluation.

## Deployment

1. Extract to a new folder.
2. Run `run-paper.bat`.
3. Let `doctor` pass.
4. Run only with fresh `data-paper-v613`.
5. Monitor dashboard queue delay/data freshness as well as setup decisions.
6. Stop with Ctrl+C before copying/archive.

Do not copy the 6.1 database into 6.1.3. Do not disable freshness guards to manufacture trades. No real-money order endpoint exists in this package.
