# Usage Guide

Complete guide to using the Trading Data Downloader CLI.

## Table of Contents

- [Installation](#installation)
- [Command Overview](#command-overview)
- [Downloading Data](#downloading-data)
- [File Organization](#file-organization)
- [Symbol Discovery](#symbol-discovery)
- [Advanced Features](#advanced-features)
- [Common Workflows](#common-workflows)
- [Security & Path Safety](#security--path-safety)
- [Troubleshooting](#troubleshooting)

## Installation

Build from source:

```bash
cargo build --release
```

The binary will be available at `./target/release/trading-data-downloader`.

For convenience, add an alias to your shell:

```bash
alias tdd='./target/release/trading-data-downloader'
```

## Command Overview

```bash
trading-data-downloader <COMMAND>

Commands:
  download    Download trading data (bars, agg-trades, funding)
  sources     List available data sources and symbols
  validate    Validate identifiers or resume state
  help        Print help information
```

### Global Options

```bash
--data-dir <DIR>             Root directory for data files (default: ./data)
--output-format <FORMAT>     Output format: json or human (default: human)
--resume <MODE>              Resume mode: on, off, reset, verify (default: off)
--resume-dir <DIR>           Custom resume state directory
--concurrency <N>            Number of concurrent downloads (default: 1)
--max-retries <N>            Maximum retry attempts (default: 5)
--force                      Force re-download existing data
```

## Downloading Data

### 1. OHLCV Bars

Download candlestick (OHLCV) data for backtesting and analysis.

**Basic Example:**

```bash
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

**Output:** `data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv`

Files are automatically organized by venue and symbol. Multi-month downloads create separate files:
- `BTCUSDT-bars-1m-2024-01.csv` (January data)
- `BTCUSDT-bars-1m-2024-02.csv` (February data)
- etc.

**Supported Intervals:**
- `1m`, `3m`, `5m`, `15m`, `30m` - Minutes
- `1h`, `2h`, `4h`, `6h`, `8h`, `12h` - Hours
- `1d`, `3d` - Days
- `1w` - Week
- `1M` - Month

**Multiple Intervals:**

```bash
# Download 1m, 5m, and 1h bars
for interval in 1m 5m 1h; do
  trading-data-downloader download bars \
    --identifier "BINANCE:BTC/USDT:USDT" \
    --symbol ETHUSDT \
    --interval $interval \
    --start-time 2024-01-01 \
    --end-time 2024-01-31
done
```

**Custom Data Directory:**

```bash
# Save to custom location
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1h \
  --start-time 2024-01-01 \
  --end-time 2024-12-31 \
  --data-dir ./my-trading-data
```

**Output:** `./my-trading-data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1h-2024-01.csv` (and subsequent months)

### 2. Aggregate Trades

Download compressed trade data for market microstructure analysis.

```bash
trading-data-downloader download agg-trades \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --start-time 2024-11-01 \
  --end-time 2024-11-15
```

**Note:** Aggregate trades are downloaded in 1-hour windows and include:
- Trade ID
- Price and quantity
- Timestamp (millisecond precision)
- Buyer/maker flags

### 3. Funding Rates

Download funding rate history for perpetual futures.

```bash
trading-data-downloader download funding \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol ETHUSDT \
  --start-time 2024-01-01 \
  --end-time 2024-12-31
```

**Note:** Funding rates are recorded every 8 hours (00:00, 08:00, 16:00 UTC).

## File Organization

All downloaded files are automatically organized in a hierarchical structure for easy management:

### Directory Structure

```
data/
├── binance_futures_usdt/          # Venue: Exchange + market type
│   ├── BTCUSDT/                   # Symbol folder
│   │   ├── BTCUSDT-bars-1m-2024-01.csv
│   │   ├── BTCUSDT-bars-1m-2024-02.csv
│   │   ├── BTCUSDT-bars-1h-2024-01.csv
│   │   ├── BTCUSDT-aggtrades-2024-01.csv
│   │   └── BTCUSDT-funding-2024-01.csv
│   ├── ETHUSDT/
│   │   ├── ETHUSDT-bars-1m-2024-01.csv
│   │   └── ETHUSDT-aggtrades-2024-01.csv
│   └── SOLUSDT/
│       └── SOLUSDT-bars-1h-2024-01.csv
└── binance_futures_coin/          # COIN-margined futures
    └── BTCUSD_PERP/
        └── BTCUSD_PERP-bars-1m-2024-01.csv
```

### Filename Format

Files follow this naming convention:
- **Bars:** `{SYMBOL}-bars-{interval}-{YYYY-MM}.csv`
- **AggTrades:** `{SYMBOL}-aggtrades-{YYYY-MM}.csv`
- **Funding:** `{SYMBOL}-funding-{YYYY-MM}.csv`

**Examples:**
- `BTCUSDT-bars-1m-2024-01.csv` - 1-minute bars for January 2024
- `ETHUSDT-aggtrades-2024-02.csv` - Aggregate trades for February 2024
- `SOLUSDT-funding-2024-03.csv` - Funding rates for March 2024

### Monthly File Splitting

Downloads spanning multiple months automatically create separate files:

```bash
# This command:
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1h \
  --start-time 2024-01-01 \
  --end-time 2024-03-31

# Creates these files:
# data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1h-2024-01.csv
# data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1h-2024-02.csv
# data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1h-2024-03.csv
```

**Benefits:**
- Easy to locate specific symbols and time periods
- Manageable file sizes (one month per file)
- Clean separation between venues and data types
- Scalable to thousands of files

### Custom Data Directory

Change the root data directory using `--data-dir`:

```bash
trading-data-downloader download bars \
  --data-dir /mnt/storage/crypto-data \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```

**Output:** `/mnt/storage/crypto-data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv`

## Symbol Discovery

### List All Available Symbols

```bash
trading-data-downloader sources list
```

Output format:
```
BINANCE:BTC/USDT:USDT | BTCUSDT | PERPETUAL | tick=0.10 | step=0.001
BINANCE:BTC/USDT:USDT | ETHUSDT | PERPETUAL | tick=0.01 | step=0.001
...
```

### Filter by Symbol Pattern

```bash
# Using grep to filter
trading-data-downloader sources list | grep BTC
trading-data-downloader sources list | grep ETH
```

### JSON Output

```bash
trading-data-downloader sources list --output-format json | jq
```

## Advanced Features

### Resume Interrupted Downloads

Enable automatic resume for long downloads:

```bash
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-12-31 \
  --resume on
```

**What happens:**
- Download progress is saved periodically
- If interrupted (Ctrl+C, network failure), restart with same command
- Tool detects progress and continues from last checkpoint
- No re-downloading of completed data

**Resume Modes:**
- `on` - Enable resume, continue from checkpoint if exists
- `off` - Disable resume (default)
- `reset` - Clear resume state and start fresh
- `verify` - Check resume state without downloading

### Retry Configuration

Customize retry behavior for unreliable networks:

```bash
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-01-31 \
  --max-retries 10
```

**Retry Notifications:**
The tool provides clear feedback during retries:
```
Retrying (attempt 2/10) after rate limit exceeded - waiting 4.0 seconds...
Retry attempt 2/10 succeeded - resuming download
```

### Force Re-download

Override existing data:

```bash
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-01-31 \
  --force
```

Without `--force`, the tool skips downloads if output file already covers the requested range.

### Concurrent Downloads

Download multiple symbols simultaneously:

```bash
# Download 5 symbols concurrently
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT,DOGEUSDT \
  --interval 1h \
  --start-time 2024-01-01 \
  --end-time 2024-12-31 \
  --concurrency 5
```

**Note:** Higher concurrency may trigger rate limits. Start with 3-5 concurrent downloads.

## Common Workflows

### 1. Backtest Dataset Preparation

Download complete historical data for multiple symbols and intervals:

```bash
#!/bin/bash
SYMBOLS=("BTCUSDT" "ETHUSDT" "SOLUSDT")
INTERVALS=("1m" "5m" "1h" "1d")

for symbol in "${SYMBOLS[@]}"; do
  for interval in "${INTERVALS[@]}"; do
    echo "Downloading $symbol $interval..."
    trading-data-downloader download bars \
      --identifier "BINANCE:BTC/USDT:USDT" \
      --symbol "$symbol" \
      --interval "$interval" \
      --start-time 2023-01-01 \
      --end-time 2024-12-31 \
      --resume on \
      --max-retries 5
  done
done
```

### 2. Daily Data Update

Update today's data only:

```bash
#!/bin/bash
TODAY=$(date +%Y-%m-%d)

trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time "$TODAY" \
  --end-time "$TODAY" \
  --force
```

### 3. Research Dataset

Download all data types for comprehensive analysis:

```bash
#!/bin/bash
SYMBOL="BTCUSDT"
START="2024-01-01"
END="2024-01-31"

# OHLCV bars
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol "$SYMBOL" \
  --interval 1m \
  --start-time "$START" \
  --end-time "$END"

# Aggregate trades
trading-data-downloader download agg-trades \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol "$SYMBOL" \
  --start-time "$START" \
  --end-time "$END"

# Funding rates
trading-data-downloader download funding \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol "$SYMBOL" \
  --start-time "$START" \
  --end-time "$END"
```

### 4. Monitoring Long Downloads

For multi-month downloads, use resume mode and monitor progress:

```bash
trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2020-01-01 \
  --end-time 2024-12-31 \
  --resume on \
  --max-retries 10
```

**Progress Updates:**
```
Downloaded 14,400 bars (10 days of 100 days) - 10% complete
Downloaded 28,800 bars (20 days of 100 days) - 20% complete
...
```

Press `Ctrl+C` to pause, then restart with same command to resume.

## Output Format

All data is saved as CSV files in the hierarchical directory structure `data/{venue}/{symbol}/`.

**Bars (OHLCV):**
- **Location:** `data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv`
- **Format:**
```csv
timestamp,open,high,low,close,volume,trades
1704067200000,42150.50,42200.00,42100.00,42180.30,150.250,1250
```

**Aggregate Trades:**
- **Location:** `data/binance_futures_usdt/BTCUSDT/BTCUSDT-aggtrades-2024-01.csv`
- **Format:**
```csv
agg_trade_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker
12345678,42150.50,0.500,98765,98770,1704067200000,false
```

**Funding Rates:**
- **Location:** `data/binance_futures_usdt/BTCUSDT/BTCUSDT-funding-2024-01.csv`
- **Format:**
```csv
timestamp,funding_rate,mark_price
1704067200000,0.0001,42150.50
```

## Security & Path Safety

### Path Trust Model

This tool is designed for **local use with trusted inputs**:

- **Symbol names** are used as-is from exchange APIs and directly incorporated into file paths
- **`--data-dir` paths** are user-controlled and joined to construct output paths
- The tool **does not sanitize or validate** symbol names or custom data directory paths
- **Untrusted inputs** should be validated by the caller before passing to this tool

### Safe Usage Recommendations

✅ **Recommended**:
- Use symbols directly from exchange APIs (Binance, etc.)
- Stick to default `./data` directory for most cases
- Validate custom `--data-dir` paths before use
- Run with minimal user permissions

❌ **Not Recommended**:
- Passing untrusted symbol names from user input without validation
- Using `--data-dir` pointing to system directories (`/etc`, `/usr`, `C:\Windows`)
- Running as root/administrator unless absolutely necessary
- Accepting arbitrary paths from external sources

### Example: Safe Automation

```bash
#!/bin/bash
# Safe: using known symbols
SYMBOLS=("BTCUSDT" "ETHUSDT" "SOLUSDT")
DATA_DIR="./trading-data"  # Safe: relative path in project

for symbol in "${SYMBOLS[@]}"; do
  trading-data-downloader download bars \
    --identifier "BINANCE:BTC/USDT:USDT" \
    --symbol "$symbol" \
    --interval 1h \
    --start-time 2024-01-01 \
    --end-time 2024-12-31 \
    --data-dir "$DATA_DIR"
done
```

## Troubleshooting

### Rate Limit Errors

If you see rate limit errors:
```
Rate limit exceeded (attempt 1/3) - waiting 2.0 seconds...
```

**Solutions:**
- Reduce `--concurrency` value
- Increase `--max-retries`
- Add delays between downloads in scripts

### Resume State Issues

If resume state is corrupted:
```bash
# Clear and start fresh
trading-data-downloader download bars \
  --resume reset \
  ...
```

### Network Timeouts

For unstable connections:
```bash
# Increase retries and enable resume
trading-data-downloader download bars \
  --max-retries 10 \
  --resume on \
  ...
```

## Environment Variables

```bash
# Set logging level
export RUST_LOG=info              # Basic logging
export RUST_LOG=debug             # Detailed logging
export RUST_LOG=trading_data_downloader=trace  # Full trace

# Custom resume directory
export RESUME_DIR=/path/to/resume
```

## Exit Codes

- `0` - Success
- `1` - General error (check error message)
- `2` - Invalid arguments
- `130` - Interrupted by user (Ctrl+C)
