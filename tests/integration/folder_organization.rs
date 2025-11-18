//! Integration tests for hierarchical folder organization (Feature 005)
//!
//! Tests verify:
//! - Hierarchical directory structure: data/{venue}/{symbol}/
//! - Automatic directory creation
//! - Multi-venue and multi-symbol support
//! - Enhanced filename format
//! - Backward compatibility with --output flag
//! - Custom data directory via --data-dir flag

use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

/// T032: Test hierarchical structure is created by default
#[test]
fn test_hierarchical_structure_default() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify hierarchical structure: data/{venue}/{symbol}/
    let expected_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");

    assert!(
        expected_dir.exists(),
        "Expected directory structure not found: {}",
        expected_dir.display()
    );

    // Verify CSV file exists in the symbol directory
    let entries: Vec<_> = fs::read_dir(&expected_dir)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        !entries.is_empty(),
        "No files found in symbol directory: {}",
        expected_dir.display()
    );
}

/// T033: Test venue directory is created automatically
#[test]
fn test_venue_directory_created_automatically() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify venue directory exists
    let venue_dir = temp_dir.path().join("binance_futures_usdt");
    assert!(
        venue_dir.exists() && venue_dir.is_dir(),
        "Venue directory not created: {}",
        venue_dir.display()
    );
}

/// T034: Test symbol directory is created automatically
#[test]
fn test_symbol_directory_created_automatically() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify symbol directory exists
    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");
    assert!(
        symbol_dir.exists() && symbol_dir.is_dir(),
        "Symbol directory not created: {}",
        symbol_dir.display()
    );
}

/// T035: Test multiple symbols for the same venue create separate directories
#[test]
fn test_multiple_symbols_same_venue() {
    let temp_dir = TempDir::new().unwrap();

    // Download BTCUSDT
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Download ETHUSDT
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:ETH/USDT:USDT",
            "--symbol",
            "ETHUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify both symbol directories exist under the same venue
    let btc_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");
    let eth_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("ETHUSDT");

    assert!(btc_dir.exists(), "BTCUSDT directory not found");
    assert!(eth_dir.exists(), "ETHUSDT directory not found");
}

/// T036: Test multiple venues create separate directories
#[test]
fn test_multiple_venues() {
    let temp_dir = TempDir::new().unwrap();

    // Download from USDT-margined futures
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Download from COIN-margined futures
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/BTC:BTC",
            "--symbol",
            "BTCUSD_PERP",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify separate venue directories
    let usdt_venue_dir = temp_dir.path().join("binance_futures_usdt");
    let coin_venue_dir = temp_dir.path().join("binance_futures_coin");

    assert!(
        usdt_venue_dir.exists(),
        "USDT-margined venue directory not found"
    );
    assert!(
        coin_venue_dir.exists(),
        "COIN-margined venue directory not found"
    );
}

/// T057: Test filename for bars includes interval and month
#[test]
fn test_filename_bars_includes_interval_and_month() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-15",
            "--end-time",
            "2024-01-16",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify filename format: {SYMBOL}-bars-{interval}-{YYYY-MM}.csv
    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");

    let entries: Vec<_> = fs::read_dir(&symbol_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(entries.len(), 1, "Expected exactly one file");
    assert_eq!(entries[0], "BTCUSDT-bars-1m-2024-01.csv");
}

/// T058: Test filename for aggtrades includes month
#[test]
fn test_filename_aggtrades_includes_month() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "agg-trades",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--start-time",
            "2024-02-15",
            "--end-time",
            "2024-02-16",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify filename format: {SYMBOL}-aggtrades-{YYYY-MM}.csv
    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");

    let entries: Vec<_> = fs::read_dir(&symbol_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(entries.len(), 1, "Expected exactly one file");
    assert_eq!(entries[0], "BTCUSDT-aggtrades-2024-02.csv");
}

/// T059: Test filename for funding includes month
#[test]
fn test_filename_funding_includes_month() {
    let temp_dir = TempDir::new().unwrap();

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "funding",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--start-time",
            "2024-03-15",
            "--end-time",
            "2024-03-16",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify filename format: {SYMBOL}-funding-{YYYY-MM}.csv
    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");

    let entries: Vec<_> = fs::read_dir(&symbol_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(entries.len(), 1, "Expected exactly one file");
    assert_eq!(entries[0], "BTCUSDT-funding-2024-03.csv");
}

/// T060: Test multi-month download creates separate files
#[test]
fn test_multi_month_creates_separate_files() {
    let temp_dir = TempDir::new().unwrap();

    // Download 3 months of data: Jan, Feb, Mar 2024
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
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
            "2024-03-31",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify 3 separate files created
    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");

    let mut entries: Vec<_> = fs::read_dir(&symbol_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    entries.sort();

    assert_eq!(entries.len(), 3, "Expected 3 monthly files");
    assert_eq!(entries[0], "BTCUSDT-bars-1h-2024-01.csv");
    assert_eq!(entries[1], "BTCUSDT-bars-1h-2024-02.csv");
    assert_eq!(entries[2], "BTCUSDT-bars-1h-2024-03.csv");
}

/// T061: Test downloading same month twice merges data
#[test]
fn test_same_month_twice_appends_data() {
    let temp_dir = TempDir::new().unwrap();

    // First download: Jan 1-10
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1d",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-10",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    let symbol_dir = temp_dir
        .path()
        .join("binance_futures_usdt")
        .join("BTCUSDT");
    let file_path = symbol_dir.join("BTCUSDT-bars-1d-2024-01.csv");

    // Check file exists and get initial size
    assert!(file_path.exists());
    let initial_metadata = fs::metadata(&file_path).unwrap();
    let initial_size = initial_metadata.len();

    // Second download: Jan 11-20 (different data, same month)
    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1d",
            "--start-time",
            "2024-01-11",
            "--end-time",
            "2024-01-20",
            "--data-dir",
            temp_dir.path().to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify still only one file (not two)
    let entries: Vec<_> = fs::read_dir(&symbol_dir)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert_eq!(entries.len(), 1, "Should still have only one file");

    // Verify file size increased (data was appended)
    let final_metadata = fs::metadata(&file_path).unwrap();
    let final_size = final_metadata.len();
    assert!(
        final_size > initial_size,
        "File should have grown after second download"
    );
}

/// Test backward compatibility: --output flag should bypass hierarchical structure
#[test]
#[ignore = "requires implementation - will be enabled in US4"]
fn test_backward_compatibility_output_flag() {
    let temp_dir = TempDir::new().unwrap();
    let custom_path = temp_dir.path().join("my_custom_file.csv");

    Command::cargo_bin("trading-data-downloader")
        .unwrap()
        .args(&[
            "download",
            "bars",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1m",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--output",
            custom_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify file exists at custom path
    assert!(
        custom_path.exists(),
        "Custom output file not created at: {}",
        custom_path.display()
    );

    // Verify NO hierarchical structure was created
    let hierarchical_dir = temp_dir.path().join("binance_futures_usdt");
    assert!(
        !hierarchical_dir.exists(),
        "Hierarchical structure should NOT be created when --output is specified"
    );
}
