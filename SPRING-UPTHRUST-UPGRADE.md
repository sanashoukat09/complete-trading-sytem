# Compression Radar 6.1.3 — Spring / Upthrust Upgrade

This branch continues from the repaired Compression Radar 6.1 runtime. All supplied historical run evidence remains attributed to 6.1. This source change must start a new experiment/database; do not overwrite or relabel the frozen 6.1 history.

## Strategy scope

Only two setup families are tradable here:

- Spring long (failed downside auction)
- Upthrust short (failed upside auction)

No continuation, breakout, liquidity-vacuum, trap, or other setup family was added.

## What changed

The failed-auction observer is now a mirrored path-aware state machine:

1. frozen balance and boundary;
2. genuine outside excursion;
3. outside effort/result tracking;
4. directional reclaim;
5. direct response OR patient defended retest;
6. renewed favorable progress;
7. executable entry only if the existing cost/RR/risk checks still pass.

A fixed aggression percentage no longer has sole veto power. Two evidence routes exist inside the SAME Spring/Upthrust family:

- `DIRECT_FLOW_RESPONSE`: favorable flow and price progress agree;
- `DIRECT_PRICE_LED_RESPONSE`: price makes strong favorable progress even while flow is mixed, consistent with adverse effort failing to produce adverse result;
- `DEFENDED_RETEST`: a valid reclaim responds, revisits the boundary without structural failure, then renews favorable progress.

The opposite hypothesis remains live. Persistent outside trading after reclaim becomes `OUTSIDE_ACCEPTANCE_AFTER_RECLAIM` and kills the failed-auction attempt. Meaningful extension beyond the sweep extreme becomes `STRUCTURAL_INVALIDATION`.

## Entry behavior

The strategy no longer treats a tiny revisit of the sweep extreme/boundary as automatic failure. It distinguishes temporary pressure from genuine renewed acceptance. Direct entries can happen earlier when response quality is already sufficient, while late quotes are still rejected by the existing executable-price and minimum-net-RR checks. This preserves sniper behavior without chasing after confirmation destroys the trade geometry.

## Storage

The durable `events.payload` journal now uses lossless zlib compression with a `CRZ1` prefix. The decoder remains backward compatible with old plain-text event payloads, so the audit replay utility can read the original 6.1 event database. The normalized data and the original raw exchange message remain present in the compressed payload.

SQLite WAL checkpointing is tightened and the journal size limit is bounded to reduce transient disk growth.

Representative depth20 codec benchmark (5,000 events):

- uncompressed JSON: 8,538,673 bytes
- compressed payloads: 2,720,994 bytes
- reduction: 68.13%

This is a codec benchmark, not a guarantee that every live run will shrink by the same percentage.

## Verification

The final release test count is recorded in `audit/tests.txt`; the release gate adds complete long/short management, current Binance stream-lane, subscription-delta and lossless-replay tests on top of the earlier failed-auction cases.

The four added failed-auction tests cover:

- controlled Spring retest remains valid;
- persistent post-reclaim outside acceptance invalidates;
- strong price-led response can qualify with ~40% favorable flow instead of requiring 55%;
- Spring and Upthrust are exact directional mirrors on the same evidence path.

All prior repaired-6.1 regression categories remain in the suite.

## Historical-data limitation

The supplied 6.1 run cannot honestly validate this new entry policy economically. The previously audited archive had severe receipt/processing delay and incomplete per-symbol tick coverage; it produced no executable positions/outcomes under the repaired live-freshness rules. It is useful for software regression and identifying operational bottlenecks, not for claiming a new win rate or expectancy.

A clean 6.1.3 paper run records the full funnel: outside attempts, reclaims, direct responses, retests, invalidations, rejected proposals, fills, MFE/MAE, realized R after costs, and missed moves with valid data coverage.


## 6.1.3 runtime hardening

The complete 6.1 history showed that the original strategy was often receiving evidence too late **inside the application**, even when exchange-to-receipt lag was acceptable. 6.1.3 therefore enlarges and batches the bounded queue, reuses per-symbol state inside reducer transactions, removes continuous-feed global drain barriers, keeps OI refreshed independently for monitored symbols, and uses persistent WebSocket subscriptions.

Binance's 2026 USDⓈ-M stream migration is reflected in the runtime: high-frequency book streams use the `/public` lane and `aggTrade` uses `/market`. Ordinary universe changes update subscriptions on the existing socket and do not invalidate retained symbols. Genuine trade-feed gaps still invalidate a causal attempt where appropriate.

The complete old archive did not persist quote/depth rows, so exact historical executable fills cannot be reconstructed from it. Counterfactual replay results in `audit/` are setup-path diagnostics, not claimed historical trades.
