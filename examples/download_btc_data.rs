//! Example: Download BTC/USDT 1-hour bars (T185)
//!
//! This example demonstrates how to use the trading-data-downloader CLI
//! to download historical OHLCV data for Bitcoin.
//!
//! Run with: cargo run --example download_btc_data

fn main() {
    println!("Trading Data Downloader - Example Usage");
    println!("========================================\n");

    println!("This library is primarily designed to be used via the CLI.\n");

    println!("Example 1: Download BTC/USDT 1-hour bars");
    println!("-----------------------------------------");
    println!("cargo run -- download bars \\");
    println!("  --identifier BINANCE:BTC/USDT:USDT \\");
    println!("  --symbol BTCUSDT \\");
    println!("  --interval 1h \\");
    println!("  --start-time 2024-01-01 \\");
    println!("  --end-time 2024-01-02 \\");
    println!("  --output btcusdt_1h.csv\n");

    println!("Example 2: Download with resume capability");
    println!("------------------------------------------");
    println!("cargo run -- download bars \\");
    println!("  --identifier BINANCE:BTC/USDT:USDT \\");
    println!("  --symbol BTCUSDT \\");
    println!("  --interval 1h \\");
    println!("  --start-time 2024-01-01 \\");
    println!("  --end-time 2024-12-31 \\");
    println!("  --resume on\n");

    println!("Example 3: Download aggregate trades");
    println!("------------------------------------");
    println!("cargo run -- download aggtrades \\");
    println!("  --identifier BINANCE:BTC/USDT:USDT \\");
    println!("  --symbol BTCUSDT \\");
    println!("  --start-time 2024-01-01 \\");
    println!("  --end-time 2024-01-02\n");

    println!("Example 4: Download funding rates");
    println!("---------------------------------");
    println!("cargo run -- download funding \\");
    println!("  --identifier BINANCE:BTC/USDT:USDT \\");
    println!("  --symbol BTCUSDT \\");
    println!("  --start-time 2024-01-01 \\");
    println!("  --end-time 2024-01-31\n");

    println!("Example 5: Validate an identifier");
    println!("---------------------------------");
    println!("cargo run -- validate identifier BINANCE:BTC/USDT:USDT\n");

    println!("Example 6: List available symbols");
    println!("---------------------------------");
    println!("cargo run -- sources\n");

    println!("For more information:");
    println!("  cargo run -- --help");
    println!("  cargo run -- download --help");
    println!("  cargo run -- validate --help");
}
