# Trading Data Downloader

A high-performance CLI tool for downloading historical trading data from cryptocurrency exchanges.

## Features

- **Multiple Data Types**: Download OHLCV bars, aggregate trades, and funding rates
- **Hierarchical Organization**: Automatic folder structure by venue and symbol (`data/{venue}/{symbol}/`)
- **Monthly Files**: Data automatically split into monthly files for easy management
- **Reliable Downloads**: Automatic retry with exponential backoff and clear error messages
- **Resume Support**: Interrupted downloads resume from where they left off
- **Progress Tracking**: Real-time progress updates for long-running downloads
- **Production Ready**: Comprehensive error handling, rate limiting, and data validation

## Supported Exchanges

- **Binance Futures**
  - USDT-margined perpetuals (`BINANCE:BTC/USDT:USDT`)
  - COIN-margined perpetuals (`BINANCE:ETH/USD:ETH`)

## Quick Start

### Installation

```bash
cargo build --release
```

### Basic Usage

Download 1-minute OHLCV bars:
```bash
./target/release/trading-data-downloader download bars \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol BTCUSDT \
  --interval 1m \
  --start-time 2024-01-01 \
  --end-time 2024-01-31
```
**Output:** `data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv`

Download aggregate trades:
```bash
./target/release/trading-data-downloader download agg-trades \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol ETHUSDT \
  --start-time 2024-11-01 \
  --end-time 2024-11-15
```
**Output:** `data/binance_futures_usdt/ETHUSDT/ETHUSDT-aggtrades-2024-11.csv`

List available symbols:
```bash
./target/release/trading-data-downloader sources list
```

## File Organization

Downloaded files are automatically organized in a hierarchical structure:

```
data/
├── binance_futures_usdt/
│   ├── BTCUSDT/
│   │   ├── BTCUSDT-bars-1m-2024-01.csv
│   │   ├── BTCUSDT-bars-1m-2024-02.csv
│   │   └── BTCUSDT-aggtrades-2024-01.csv
│   └── ETHUSDT/
│       └── ETHUSDT-bars-1h-2024-01.csv
└── binance_futures_coin/
    └── BTCUSD_PERP/
        └── BTCUSD_PERP-bars-1m-2024-01.csv
```

- **Venue folders**: Exchange and market type (e.g., `binance_futures_usdt`)
- **Symbol folders**: Trading pair (e.g., `BTCUSDT`)
- **Monthly files**: One file per month with format `{SYMBOL}-{type}-{interval}-{YYYY-MM}.csv`

## Key Options

- `--data-dir DIR` - Custom data directory (default: `./data`)
- `--resume on` - Enable automatic resume for interrupted downloads
- `--max-retries N` - Set maximum retry attempts (default: 5)
- `--force` - Re-download even if data already exists
- `--concurrency N` - Download multiple symbols concurrently

## Security Considerations

**Path Trust Model**: This tool is designed for local use with trusted inputs.

- **Symbol names** are used as-is from exchange APIs and directly in file paths
- **`--data-dir`** paths are user-controlled and joined to construct output paths
- The tool **does not sanitize or validate** symbol names or custom data directories
- **Untrusted inputs** should be validated by the caller before passing to this tool

**Safe Usage**:
- Only use symbols from known, trusted exchanges
- Validate custom `--data-dir` paths before use
- Run with minimal permissions (avoid root/administrator)
- Use default `./data` directory for most cases

**Not Recommended**:
- Passing untrusted symbol names (e.g., from user input without validation)
- Using `--data-dir` pointing to system directories
- Running with elevated privileges unless necessary

## Documentation

- [Usage Guide](docs/usage.md) - Detailed command examples and common workflows

## Requirements

- Rust 1.75.0 or later
- Internet connection (downloads from public APIs)

## License

See LICENSE file for details.

## Contributing

Contributions welcome! Please ensure all tests pass:

```bash
cargo test
cargo clippy
```
