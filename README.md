# Compression Radar 6.1 — recorded-run repairs

**This package runs collection, shadow signals and simulated paper trades on public Binance USDT perpetual data. It cannot place real-money orders. Its trading edge is unverified.**

This release repairs the supplied V6 runtime and preserves its dashboard. V6 was a replacement for the V5 runtime, not a claim that adding more order-flow inputs guarantees profits. The supported program has one journal, one reducer, one paper position manager and one accounting ledger. The old competing runtime paths and backup trees are not shipped on its import path.

## Upgrade from the supplied run

Extract into a **new folder**. Keep your previous project and `data-paper` unchanged. The new launcher uses `data-paper-v61`: changed code must use a fresh frozen experiment. Do not copy the old database into that directory or edit its code hash. Your supplied run had no open positions to transfer. Audit results and per-episode coverage are in `audit/` and `RELEASE-REPORT.md`.

The dashboard shows unavailable values when quotes are stale; a closed candle is never presented as a live price. Range edges are observed window extrema, not guaranteed support or resistance. Unrealized mark PnL excludes hypothetical exit fees and slippage; equity remains on a closed-position accounting basis.

## Start on Windows

1. Install Python **3.11 or newer** with the Python launcher (`py`). Python 3.12 was used for verification.
2. Extract the entire ZIP to a normal writable folder.
3. Double-click **`run-paper.bat`**. On the first run it creates an isolated environment and installs the pinned network dependency. Internet access is needed.
4. Open **http://127.0.0.1:8780/** in your browser.
5. Leave the terminal open. Press **Ctrl+C** there to stop.

No Binance key, exchange account access or deposit is needed. Do not put credentials in this package. The paper balance starts at 10,000 simulated USDT.

If `py` is not recognized, reinstall Python with its launcher enabled or use the manual commands below. The Windows launchers have been inspected; execution tests ran on Linux, not on your Windows laptop.

```powershell
py -3 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m radar doctor
.venv\Scripts\python -m radar paper --config config.json --data-dir data-paper-v61
```

`doctor` checks public REST time/metadata and the current public/market WebSocket routes. If your network or region cannot access those services, it fails with an error. Do not treat missing data as a running scanner.

For Linux/macOS use `python3 -m venv .venv`, activate the environment and use `python` in the same commands.

## Check the pipeline before using network data

```powershell
.venv\Scripts\python -m radar demo --data-dir demo-data
```

This generates a **synthetic** balance, spring, response, paper entry and exits. Its favorable result is deliberately constructed to exercise the pipeline. It is not a historical backtest, win-rate estimate or proof of profit. Use a fresh demo directory each time.

Run the checks with `test-project.bat`, or:

```powershell
.venv\Scripts\python -m pip install -r requirements-dev.txt
.venv\Scripts\python -m pytest -q
```

See `audit/tests.txt` for the actual release test result and `RELEASE-REPORT.md` for scope and limitations.

## Operating modes

| Mode | What happens |
|---|---|
| `collection` | Records and processes public observations; no signals or positions |
| `shadow` | Records eligible proposals and explanations; no simulated positions |
| `paper` | Reserves simulated account risk, then fills against a subsequent valid observed quote/depth snapshot |
| Real-money execution | Not present; there are no authenticated order endpoints or live mode |

Use separate directories for modes, for example `data-shadow` and `data-paper`. Configuration and source hashes are frozen into each database. Changing the strategy, costs, mode or code requires a new experiment directory. Do not delete an active experiment to bypass a mismatch.

## What the program actually does

### Coin selection

It loads authentic contract filters, selects tradable USDT perpetuals with adequate reported turnover, and prioritizes a bounded candidate pool using percentage movement and liquidity. BTC and ETH are excluded from *new candidate selection* by default to focus resources on other coins; this is configurable.

It backfills closed one-minute candles and five-minute OI for the candidate pool. The fine ranking uses standardized relative volume and absolute standardized OI growth surprises, while recording OI direction separately. OI contraction is considered rather than treated as universally bad. The top configured instruments receive trade, best-quote and top-20 depth streams. Instruments with pending/open paper exposure stay monitored.

This is a bounded candidate pool, not tick-level surveillance of every listed coin. A coin outside that pool can be missed. Scores rank attention; they are not probabilities of a large move.

### Compression and failed auctions

Compression needs a smaller recent structural range relative to its historical reference, limited net directional drift and repeated alternating visits to the range edges. The detector uses closed bars only. A balance stays identified through the excursion/reclaim sequence and expires explicitly.

The supported entry families are **spring long** and **upthrust short**. They require an outside excursion, reclaim of the relevant frozen boundary, a subsequent price response beyond an observable reference and supporting aggressive trade participation. Invalidations, missing coverage, insufficient response and opposing flow produce named WAIT reasons. Evidence contains the exact input identities.

These are explicit experimental rules with contextual measurements. They do not reproduce human discretion perfectly. Continuation, iceberg detection, full footprint profiles, multi-exchange order flow and hidden-intent claims are not implemented in this supported release. Top-20 depth informs execution capacity, not a claim that displayed liquidity is permanent or authentic intent.

### Entry and risk

A proposal uses an executable quote, adverse slippage, structural stop beyond the excursion, exchange tick/lot filters and estimated fees/funding. It uses the midpoint target only when its estimated net reward/risk qualifies; otherwise it evaluates the opposite boundary. It does not invent a distant TP to improve the ratio.

Default simulated risk is 0.3% of modeled equity per trade, with shared total/daily/notional caps and at most three positions. These are example experimental controls, not a recommendation for funding an account. All coins share the conservative portfolio cap; the program does not claim they are independent bets.

Paper authorization is durable and idempotent. Entry waits at least 50 ms and for another quote, then revalidates price/capacity. Quantity can decrease to remain inside the committed risk budget. A moved price can cancel the intent.

### Management and costs

Stops, TP1, optional TP2 and time exits are handled by the same manager. Where there are two targets, TP1 takes the configured fraction and TP2 exits the remainder. A one-target trade exits fully. The stop remains structural; this version does not pretend an arbitrary break-even/trailing rule has been optimized.

Paper fills use the displayed top-20 depth with a participation haircut, plus adverse slippage and taker fees. Thin or stale books delay fills and produce explicit WAIT records. A stop gap can lose more than one modeled R. No fill is manufactured at the stop price during an outage.

Actual published historical funding observations are applied to exposure held at the funding timestamp, including delayed observations after close. Outcomes remain excluded from the descriptive research summary until the configured history query covers the holding interval. This is still a paper model: queue position, exchange rejection, liquidation, actual fees and latency can differ in real execution.

## Dashboard and records

The dashboard reports selected instruments, paper exposure, recent decisions and outcomes. `RECEIVING_DATA` means recent feed traffic has been observed. It is **not** a profitability label or a guarantee that every symbol has a valid setup. Read the per-decision WAIT reasons.

The local dashboard binds to `127.0.0.1` only. It does not accept trade instructions or expose credentials.

All authoritative records are in `<data-dir>/radar.db` (SQLite WAL): inputs, reducer states, decisions, positions, executions and outcomes. Input is committed before reduction. Reduction and its side effects commit together. A failing pending event blocks subsequent events until recovery succeeds. Duplicate events cannot create duplicate risk or fills; conflicting duplicates fail visibly.

The journal grows. The default disk budget is 10,000 MB; the process stops on reaching it. Monitor free disk space. Do not delete active journal/database/WAL files. Stop the application before copying the experiment directory. Archive closed experiments rather than silently pruning evidence. A production retention/archival service is not implemented.

## Replay, export and research

Offline replay uses the same code and frozen configuration:

```powershell
.venv\Scripts\python -m radar replay --source data-paper\radar.db --destination replay-paper.db
.venv\Scripts\python -m radar status --data-dir data-paper-v61
.venv\Scripts\python -m radar export --data-dir data-paper-v61 --output paper-positions.csv
.venv\Scripts\python -m radar research --data-dir data-paper-v61
```

Replay needs a fresh destination and the same code revision. It makes no network calls. Preserve the release ZIP alongside its experiment data for future reproducibility.

Research output validates unique, flat completed positions and rejects conflicting duplicates. It reports net paper outcomes and, with sufficient daily blocks, an exploratory uncertainty interval. It deliberately returns `EDGE_NOT_CERTIFIED`. This is not a full strategy-selection correction, a purged research platform, or a promotion gate for real money.

## Configuration and tuning

Every setting is in `config.json`. Rates use dimensionless fractions: `0.0005` is five basis points. Prices/quantities use venue metadata, not fixed four-decimal rounding.

The defaults (30-bar compression, 90-bar reference, 90-minute episode life and four-hour maximum hold) define one experimental intraday family. They have not been optimized on market outcomes. Changing values to make one replay look better is not validation. Use a new directory/config version for each experiment, retain unsuccessful experiments and evaluate prospectively.

## What remains unverified

- Real Binance connectivity in this workspace: blocked by DNS resolution. Run `doctor` locally.
- A sustained live-data soak on your laptop, overload behavior at its peak input rate and long-term disk usage.
- Prospective expectancy after realistic execution uncertainty across regimes and sessions.
- Real-money execution, venue protective orders and account reconciliation: not implemented.
- Feature parity with every module or claim in the old V5 tree: not claimed. This package deliberately has one narrower supported runtime rather than keeping disconnected modules as advertised features.

Start with the demo, tests and doctor, then collect paper evidence. Ordinary valid setups can fail without any news or unusual event; uncertainty is intrinsic to the strategy.
