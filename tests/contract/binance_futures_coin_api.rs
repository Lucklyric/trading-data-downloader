//! Contract tests for Binance Futures COIN-margined API (T152-T156)
//!
//! These tests validate API integration and response schemas for DAPI endpoints.

use chrono::Timelike;
use trading_data_downloader::fetcher::binance_futures_coin::BinanceFuturesCoinFetcher;
use trading_data_downloader::fetcher::DataFetcher;
use trading_data_downloader::Interval;

/// Test DAPI klines endpoint returns valid data (T153)
#[tokio::test]
#[ignore] // Requires network access
async fn test_coin_klines_endpoint() {
    let fetcher = BinanceFuturesCoinFetcher::new(5);

    // Test with BTCUSD_PERP (most liquid COIN-margined contract)
    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - 3600_000; // 1 hour ago

    let result = fetcher
        .fetch_bars_stream("BTCUSD_PERP", Interval::OneMinute, start_time, end_time)
        .await;

    assert!(result.is_ok(), "DAPI klines request should succeed");

    let mut stream = result.unwrap();
    let mut count = 0;

    // Collect some bars to validate
    while let Some(bar_result) = futures_util::StreamExt::next(&mut stream).await {
        if count >= 5 {
            break;
        }

        let bar = bar_result.expect("Should successfully parse bar");

        // Validate bar structure
        assert!(bar.open_time >= start_time);
        assert!(bar.close_time <= end_time);
        assert!(bar.open > rust_decimal::Decimal::ZERO);
        assert!(bar.high >= bar.open);
        assert!(bar.low <= bar.close);
        assert!(bar.volume >= rust_decimal::Decimal::ZERO);

        count += 1;
    }

    assert!(count > 0, "Should receive at least one bar");
}

/// Test DAPI exchangeInfo endpoint returns COIN-margined symbols (T154)
#[tokio::test]
#[ignore] // Requires network access
async fn test_coin_exchange_info() {
    let fetcher = BinanceFuturesCoinFetcher::new(5);

    let symbols = fetcher
        .list_symbols()
        .await
        .expect("exchangeInfo should succeed");

    assert!(!symbols.is_empty(), "Should return at least one symbol");

    // Validate COIN-margined symbol characteristics
    let btc_perp = symbols.iter().find(|s| s.symbol == "BTCUSD_PERP");
    assert!(btc_perp.is_some(), "Should include BTCUSD_PERP");

    let btc_perp = btc_perp.unwrap();
    assert_eq!(btc_perp.base_asset, "BTC");
    assert_eq!(btc_perp.quote_asset, "USD");
    assert_eq!(btc_perp.margin_asset, "BTC"); // COIN-margined: margin in base asset
    assert_eq!(
        btc_perp.contract_type,
        trading_data_downloader::ContractType::Perpetual
    );
    assert_eq!(
        btc_perp.status,
        trading_data_downloader::TradingStatus::Trading
    );

    // Validate precision and filters
    assert!(btc_perp.tick_size > rust_decimal::Decimal::ZERO);
    assert!(btc_perp.step_size > rust_decimal::Decimal::ZERO);
}

/// Test DAPI aggTrades endpoint with 1-hour chunking (T155)
#[tokio::test]
#[ignore] // Requires network access
async fn test_coin_aggtrades_endpoint() {
    let fetcher = BinanceFuturesCoinFetcher::new(5);

    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - 1800_000; // 30 minutes ago

    let result = fetcher
        .fetch_aggtrades_stream("BTCUSD_PERP", start_time, end_time)
        .await;

    assert!(result.is_ok(), "DAPI aggTrades request should succeed");

    let mut stream = result.unwrap();
    let mut count = 0;

    // Collect some trades to validate
    while let Some(trade_result) = futures_util::StreamExt::next(&mut stream).await {
        if count >= 10 {
            break;
        }

        let trade = trade_result.expect("Should successfully parse trade");

        // Validate trade structure
        assert!(trade.timestamp >= start_time);
        assert!(trade.timestamp <= end_time);
        assert!(trade.price > rust_decimal::Decimal::ZERO);
        assert!(trade.quantity > rust_decimal::Decimal::ZERO);
        assert!(trade.last_trade_id >= trade.first_trade_id);

        count += 1;
    }

    assert!(count > 0, "Should receive at least one trade");
}

/// Test DAPI fundingRate endpoint (T156)
#[tokio::test]
#[ignore] // Requires network access
async fn test_coin_funding_rates() {
    let fetcher = BinanceFuturesCoinFetcher::new(5);

    // Get funding rates for last 7 days
    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - (7 * 24 * 3600_000); // 7 days ago

    let result = fetcher
        .fetch_funding_stream("BTCUSD_PERP", start_time, end_time)
        .await;

    assert!(result.is_ok(), "DAPI fundingRate request should succeed");

    let mut stream = result.unwrap();
    let mut count = 0;

    // Collect funding rates
    while let Some(rate_result) = futures_util::StreamExt::next(&mut stream).await {
        if count >= 5 {
            break;
        }

        let rate = rate_result.expect("Should successfully parse funding rate");

        // Validate funding rate structure
        assert!(rate.funding_time >= start_time);
        assert!(rate.funding_time <= end_time);
        assert_eq!(rate.symbol, "BTCUSD_PERP");

        // Validate funding time alignment (8-hour intervals)
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(rate.funding_time)
            .expect("Valid timestamp");
        let hour = dt.hour();
        assert!(
            hour % 8 == 0,
            "Funding time should be at 8-hour intervals, got hour: {}",
            hour
        );

        count += 1;
    }

    assert!(count > 0, "Should receive at least one funding rate");
}

/// Test DAPI base URL is correct (T152)
#[test]
fn test_coin_fetcher_base_url() {
    let fetcher = BinanceFuturesCoinFetcher::new(5);
    assert!(
        fetcher.base_url().contains("dapi.binance.com"),
        "COIN-margined should use DAPI endpoint"
    );
}

/// Integration test: Download COIN-margined bars to CSV (T157)
#[tokio::test]
#[ignore] // Requires network access
async fn test_coin_download_integration() {
    use trading_data_downloader::output::csv::CsvBarsWriter;
    use trading_data_downloader::output::{BarsWriter, OutputWriter};

    let fetcher = BinanceFuturesCoinFetcher::new(5);
    let temp_dir = tempfile::TempDir::new().unwrap();
    let output_path = temp_dir.path().join("btcusd_perp_1m.csv");

    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - 300_000; // 5 minutes ago

    let mut writer = CsvBarsWriter::new(&output_path).expect("Should create CSV writer");

    let mut stream = fetcher
        .fetch_bars_stream("BTCUSD_PERP", Interval::OneMinute, start_time, end_time)
        .await
        .expect("Should create stream");

    let mut bar_count = 0;

    while let Some(bar_result) = futures_util::StreamExt::next(&mut stream).await {
        let bar = bar_result.expect("Should parse bar");
        writer.write_bar(&bar).expect("Should write bar");
        bar_count += 1;
    }

    writer.close().expect("Should close writer");

    assert!(output_path.exists(), "CSV file should be created");
    assert!(bar_count > 0, "Should have downloaded at least one bar");

    // Validate CSV content
    let content = std::fs::read_to_string(&output_path).expect("Should read CSV");
    let lines: Vec<&str> = content.lines().collect();

    assert!(lines.len() > 1, "Should have header + data rows");
    assert!(lines[0].contains("open_time"), "Should have CSV header");
}
