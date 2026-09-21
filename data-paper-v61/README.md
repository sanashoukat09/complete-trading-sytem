# Radar Paper Run Data & Working History

This directory contains the operational history from the multi-hour paper trading run:

- **`radar_portable.db`** (~44.1 MB):
  - Complete record of **94,603 decisions** evaluated over hours of streaming Binance USDT perpetual market data.
  - **59 symbol states** and complete multi-factor universe rankings.
  - Runtime health logs and configuration manifests with code hashes.
  - **15,000 raw market events** sample.
  - Fully compatible with SQLite and all analysis tools.

### Candidate Screening & Trade Gap Forensics
For the detailed breakdown of all 22 hypothetical candidates, the 14 candidates passing the cost/RR screen, the 2 candidates passing universe/OI checks, and the analysis of trade gaps across the 38 episodes, see:
- [`audit/candidate_screen_and_trade_gaps.md`](../audit/candidate_screen_and_trade_gaps.md)
- [`audit/episode-coverage.csv`](../audit/episode-coverage.csv)
- [`RELEASE-REPORT.md`](../RELEASE-REPORT.md)
