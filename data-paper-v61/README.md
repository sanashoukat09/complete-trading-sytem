# Radar Paper Run Data & Working History

This directory contains the operational history from the multi-hour paper trading run:

- **adar_portable.db** (~44.1 MB):
  - Complete record of **94,603 decisions** evaluated over hours of streaming Binance USDT perpetual market data.
  - **59 symbol states** and complete multi-factor universe rankings.
  - Runtime health logs and configuration manifests with code hashes.
  - **15,000 raw market events** sample.
  - Fully compatible with SQLite and all analysis tools.

### Using with Radar
The paper scanner looks for data-paper-v61/radar.db. If you need to inspect or run against this recorded session, you can copy the portable database:

`ash
# Windows
copy data-paper-v61\radar_portable.db data-paper-v61\radar.db

# Linux / Mac
cp data-paper-v61/radar_portable.db data-paper-v61/radar.db
`

*Note: The raw full-stream database (adar.db, 10.48 GB with 10.5 million raw tick-level events) exceeds GitHub's 100 MB per-file upload limit. The full decision log and state history is completely preserved in adar_portable.db.*
