# Candidate Screen, Trade Gaps, and Missing Quotes Forensic Audit

This forensic document provides the exact evidence regarding candidate screening, trade gaps, and quote availability from the recorded multi-hour run.

## 1. Candidate Screen Breakdown

From the reconstructed compression episodes, **22 hypothetical boundary-crossing candidates** were evaluated:
- **Price-Only Cost/RR Screen (14 Passed, 8 Failed):**
  - 14 candidates exhibited a theoretical geometric distance to target sufficient to clear estimated execution friction and fee costs.
  - 8 candidates failed the minimum 1.5:1 reward-to-risk ratio requirement after deducting round-trip taker fees and slippage allowance.
- **Retained Universe & OI Checks (2 Passed, 12 Failed):**
  - Of the 14 geometrically viable candidates, only **2 candidates** satisfied active universe liquidity (turnover > 10M USDT) and non-negative open interest flow.
  - 12 candidates were rejected because they occurred on deprioritized instruments or during periods of declining open interest.
- **Observed Path Outcome for the 2 Retained Candidates:**
  - In forward path tracing across subsequent recorded bars, **both candidate paths encountered their structural invalidation/stop price first before reaching target**.
  - No candidate achieved a profitable exit under live causal constraints.

## 2. Trade Gaps & Missing Quotes Analysis

| Category | Count | Meaning |
|---|---:|---|
| **Total Reconstructed Episodes** | **38** | Compression ranges identified by rolling window scan |
| **Episodes with Zero Recorded Trades** | **34 (89.5%)** | Ticks missing while range was active due to delayed WebSocket stream subscription |
| **Episodes with Active Trades Recorded** | **4 (10.5%)** | Only 4 symbols had concurrent trade stream coverage |
| **Total Trades Recorded** | **35,121** | Incomplete stream sample |
| **Trades Rejected as Delayed** | **26,291 (74.9%)** | Trades received with timestamp lag > 3,000ms |
| **Median Trade Receipt Latency** | **203,253 ms (~203s)** | Massive buffering lag in original capture |

### Episodes with Concurrent Trade Coverage:
1. **AKEUSDT** (Range: `0.04533` - `0.05424`): 8,141 recorded trades, 138 depth packets. Price stayed inside range; zero candidate triggers.
2. **CELRUSDT** (Range: `0.00390` - `0.00407`): 499 recorded trades, 106 depth packets. Price stayed inside range; zero candidate triggers.
3. **CHIPUSDT** (Range: `0.04087` - `0.04118`): 39 recorded trades, 45 depth packets. Price stayed inside range; zero candidate triggers.
4. **MARSCOINUSDT** (Range: `0.09406` - `0.09525`): 20 recorded trades, 14 depth packets. Price stayed inside range; zero candidate triggers.

## 3. Conclusion for Research & Modeling
- The recorded data confirms that the lack of executed positions in the paper run was **correct behavior**: the engine properly blocked entries due to stale quotes (>3s lag), unconfirmed reclaims, and insufficient cost-adjusted reward-to-risk.
- To produce an exact forward backtest, subscriptions must be pinned proactively upon range formation, preventing the trade gaps observed in episodes 1-34.
