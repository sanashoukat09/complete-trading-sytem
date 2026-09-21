# Compression Radar 6.1.3 — Implementation Record

6.1.3 is the production-hardening continuation of the frozen 6.1 history. It does **not** relabel or mutate prior experiment evidence and does not add more tradable setup families.

Implemented and connected in the supported runtime:

- Spring-long / Upthrust-short mirrored failed-auction state machine;
- high-effort/poor-result, low-effort exhaustion, mixed-effort and defended-retest handling;
- direct-versus-retest entry routing without loosening minimum net RR;
- complete invalidation/expiry and competing outside-acceptance hypothesis;
- executable quote/depth proposal, exchange filters, fees/slippage/funding and risk caps;
- paper intent revalidation and idempotent fills;
- TP1 partial, cost-adjusted breakeven protection and TP2/final exits;
- independent OI polling for active monitoring;
- balance/attempt/position pinning through ranking churn;
- persistent 2026 Binance `/public` and `/market` WebSocket lanes with live subscription deltas;
- bounded high-throughput event worker/reducer and queue-delay telemetry;
- lossless compressed raw-event journal and backward-compatible decoder;
- replay, recovery, dashboard, export and descriptive research tooling;
- Windows launcher that runs connectivity `doctor` before paper mode.

The runtime contains no authenticated real-money order transport. `paper` is the supported live-market mode.
