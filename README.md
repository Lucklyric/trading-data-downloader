# Trading Data Downloader

A high-performance CLI tool for downloading historical trading data from cryptocurrency exchanges.

## Features

- **Multiple Data Types**: Download OHLCV bars, aggregate trades, and funding rates
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

Download aggregate trades:
```bash
./target/release/trading-data-downloader download agg-trades \
  --identifier "BINANCE:BTC/USDT:USDT" \
  --symbol ETHUSDT \
  --start-time 2024-11-01 \
  --end-time 2024-11-15
```

List available symbols:
```bash
./target/release/trading-data-downloader sources list
```

## Key Options

- `--resume on` - Enable automatic resume for interrupted downloads
- `--max-retries N` - Set maximum retry attempts (default: 3)
- `--force` - Re-download even if data already exists
- `--concurrency N` - Download multiple symbols concurrently
- `--output FILE` - Specify output file path

## Documentation

- [Usage Guide](docs/usage.md) - Detailed command examples and common workflows
- [Metrics Guide](docs/metrics.md) - Observability and monitoring

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
