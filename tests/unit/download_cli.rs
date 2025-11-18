//! Unit tests for CLI download command (T006)

use clap::Parser;
use trading_data_downloader::cli::download::Cli;

/// T006: Test that Cli::parse defaults to max_retries=5 when flag omitted
#[test]
fn test_cli_defaults_to_max_retries_5() {
    // Parse CLI with minimal args (no --max-retries flag)
    let args = vec![
        "trading-data-downloader",
        "download",
        "bars",
        "--identifier",
        "BINANCE:BTC/USDT:USDT",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1h",
        "--start-time",
        "2024-01-01",
        "--end-time",
        "2024-01-02",
    ];

    let cli = Cli::parse_from(args);

    // Verify default max_retries is 5 (changed from 3 per FR-002)
    assert_eq!(
        cli.max_retries, 5,
        "Default max_retries should be 5 per FR-002"
    );
}

/// T006: Test that Cli::parse respects custom --max-retries flag
#[test]
fn test_cli_respects_custom_max_retries() {
    // Parse CLI with custom --max-retries flag
    let args = vec![
        "trading-data-downloader",
        "--max-retries",
        "10",
        "download",
        "bars",
        "--identifier",
        "BINANCE:BTC/USDT:USDT",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1h",
        "--start-time",
        "2024-01-01",
        "--end-time",
        "2024-01-02",
    ];

    let cli = Cli::parse_from(args);

    // Verify custom max_retries value is used
    assert_eq!(
        cli.max_retries, 10,
        "CLI should respect custom --max-retries flag"
    );
}


