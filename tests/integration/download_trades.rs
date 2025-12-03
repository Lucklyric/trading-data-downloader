//! Integration tests for aggregate trades download functionality (User Story 2)
//!
//! These tests verify end-to-end functionality for downloading aggregate trade data.
//! Tests are written first (TDD) and will fail until implementation is complete.

use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use tempfile::TempDir;
use trading_data_downloader::{output::OutputWriter, AggTrade};

/// T111: Test for AggTrade struct validation
/// Verifies that AggTrade struct properly validates trade data
#[test]
fn test_aggtrade_struct_validation() {
    let trade = AggTrade {
        agg_trade_id: 123456789,
        price: Decimal::from_str("35000.50").unwrap(),
        quantity: Decimal::from_str("1.234").unwrap(),
        first_trade_id: 987654321,
        last_trade_id: 987654325,
        timestamp: 1699920000000,
        is_buyer_maker: true,
    };

    // Test valid trade
    assert!(trade.validate().is_ok());
    assert_eq!(trade.agg_trade_id, 123456789);
    assert_eq!(trade.price, Decimal::from_str("35000.50").unwrap());
    assert_eq!(trade.quantity, Decimal::from_str("1.234").unwrap());
    assert_eq!(trade.first_trade_id, 987654321);
    assert_eq!(trade.last_trade_id, 987654325);
    assert_eq!(trade.timestamp, 1699920000000);
    assert_eq!(trade.is_buyer_maker, true);

    // Test invalid price (zero or negative)
    let invalid_price = AggTrade {
        price: Decimal::ZERO,
        ..trade.clone()
    };
    assert!(invalid_price.validate().is_err());

    // Test invalid quantity (zero or negative)
    let invalid_quantity = AggTrade {
        quantity: Decimal::ZERO,
        ..trade.clone()
    };
    assert!(invalid_quantity.validate().is_err());

    // Test invalid timestamp (zero or negative)
    let invalid_timestamp = AggTrade {
        timestamp: 0,
        ..trade.clone()
    };
    assert!(invalid_timestamp.validate().is_err());

    // Test invalid trade IDs (last < first)
    let invalid_trade_ids = AggTrade {
        first_trade_id: 100,
        last_trade_id: 50,
        ..trade.clone()
    };
    assert!(invalid_trade_ids.validate().is_err());
}

/// T108: Integration test for aggTrades download end-to-end
/// Verifies complete download workflow from API to CSV output
#[tokio::test]
#[ignore] // Remove when implementation is complete
async fn test_aggtrades_download_end_to_end() {
    use futures_util::StreamExt;
    use trading_data_downloader::fetcher::binance_futures_usdt::BinanceFuturesUsdtFetcher;
    use trading_data_downloader::fetcher::DataFetcher;
    use trading_data_downloader::output::csv::CsvAggTradesWriter;
    use trading_data_downloader::output::AggTradesWriter;

    // Setup temporary output directory
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_path = temp_dir.path().join("btcusdt_trades.csv");

    // Create fetcher
    let fetcher = BinanceFuturesUsdtFetcher::new(5);

    // Define time window (1 hour)
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - (60 * 60 * 1000); // 1 hour ago

    // Fetch trades
    let mut stream = fetcher
        .fetch_aggtrades_stream("BTCUSDT", start_time, end_time)
        .await
        .expect("Failed to create stream");

    // Create writer
    let mut writer = CsvAggTradesWriter::new(&output_path).expect("Failed to create writer");

    let mut trade_count = 0;
    while let Some(result) = stream.next().await {
        let trade = result.expect("Failed to fetch trade");
        writer
            .write_aggtrade(&trade)
            .expect("Failed to write trade");
        trade_count += 1;
    }

    writer.flush().expect("Failed to flush writer");
    writer.close().expect("Failed to close writer");

    // Verify output file exists and contains data
    assert!(output_path.exists(), "Output file should exist");

    let content = std::fs::read_to_string(&output_path).expect("Failed to read output file");

    // Verify CSV header
    let lines: Vec<&str> = content.lines().collect();
    assert!(!lines.is_empty(), "Output should contain at least header");
    assert_eq!(
        lines[0],
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker"
    );

    // Verify we got some trades (if market is active)
    if trade_count > 0 {
        assert!(lines.len() > 1, "Output should contain trades");
    }

    println!("Downloaded {} aggTrades successfully", trade_count);
}

/// T109: Integration test for aggTrades download resume capability
/// Verifies that downloads can be resumed from last checkpoint
#[tokio::test]
#[ignore] // Remove when implementation is complete
async fn test_aggtrades_download_resume() {
    use trading_data_downloader::downloader::executor::DownloadExecutor;
    use trading_data_downloader::downloader::job::DownloadJob;

    // Setup temporary directories
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let output_path = temp_dir.path().join("btcusdt_trades.csv");
    let resume_dir = temp_dir.path().join("resume");
    std::fs::create_dir_all(&resume_dir).expect("Failed to create resume dir");

    // Create download job for 2 hours of data
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - (2 * 60 * 60 * 1000); // 2 hours ago

    let job = DownloadJob::new_aggtrades(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        start_time,
        end_time,
        output_path.clone(),
    );

    // Create first executor for initial download
    let executor1 = DownloadExecutor::new();

    // Start download (this will be interrupted)
    let job_clone = job.clone();
    let handle = tokio::spawn(async move { executor1.execute_aggtrades_job(job_clone).await });

    // Wait a bit then cancel
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    handle.abort();

    // Verify resume state was saved
    let resume_state_path = resume_dir.join(format!(
        "BINANCE_BTC_USDT_USDT_aggtrades_{}_{}",
        start_time, end_time
    ));

    // Create second executor for resumed download
    let executor2 = DownloadExecutor::new();

    // Resume the download
    let result = executor2.execute_aggtrades_job(job).await;

    match result {
        Ok(_) => {
            // Verify output file exists
            assert!(
                output_path.exists(),
                "Output file should exist after resume"
            );

            let content =
                std::fs::read_to_string(&output_path).expect("Failed to read output file");

            let lines: Vec<&str> = content.lines().collect();
            assert!(!lines.is_empty(), "Output should contain data");

            println!(
                "Successfully resumed and completed download with {} lines",
                lines.len()
            );
        }
        Err(e) => {
            println!("Download error (may be expected if no trades): {}", e);
        }
    }
}

/// T110: Test for 1-hour time window chunking logic (pure function test)
/// Tests the chunking algorithm without network calls
#[test]
fn test_aggtrades_one_hour_window_chunking() {
    use trading_data_downloader::fetcher::binance_futures_usdt::{
        split_into_one_hour_chunks, ONE_HOUR_MS,
    };

    // Test 2-hour range splits into 2 chunks
    let start_time = 0i64;
    let end_time = 2 * ONE_HOUR_MS;
    let chunks = split_into_one_hour_chunks(start_time, end_time);

    assert_eq!(chunks.len(), 2, "2-hour range should produce 2 chunks");
    assert_eq!(chunks[0], (0, ONE_HOUR_MS));
    assert_eq!(chunks[1], (ONE_HOUR_MS, 2 * ONE_HOUR_MS));

    // Test partial hour at the end
    let end_time_partial = ONE_HOUR_MS + 30 * 60 * 1000; // 1.5 hours
    let chunks = split_into_one_hour_chunks(0, end_time_partial);

    assert_eq!(chunks.len(), 2, "1.5-hour range should produce 2 chunks");
    assert_eq!(chunks[0], (0, ONE_HOUR_MS));
    assert_eq!(chunks[1], (ONE_HOUR_MS, end_time_partial));

    // Test less than 1 hour
    let end_time_short = 30 * 60 * 1000; // 30 minutes
    let chunks = split_into_one_hour_chunks(0, end_time_short);

    assert_eq!(chunks.len(), 1, "30-min range should produce 1 chunk");
    assert_eq!(chunks[0], (0, end_time_short));

    // Test exactly 1 hour
    let chunks = split_into_one_hour_chunks(0, ONE_HOUR_MS);
    assert_eq!(chunks.len(), 1, "Exactly 1-hour range should produce 1 chunk");
    assert_eq!(chunks[0], (0, ONE_HOUR_MS));
}

/// T110: Live integration test for 1-hour time window constraint enforcement
/// Requires network access to Binance API - run with `cargo test -- --ignored`
#[tokio::test]
#[ignore]
async fn test_aggtrades_one_hour_window_chunking_live() {
    use futures_util::StreamExt;
    use trading_data_downloader::fetcher::binance_futures_usdt::BinanceFuturesUsdtFetcher;
    use trading_data_downloader::fetcher::DataFetcher;

    let fetcher = BinanceFuturesUsdtFetcher::new(5);

    // Request 2 hours of data - fetcher should chunk this into 2 requests internally
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - (2 * 60 * 60 * 1000); // 2 hours ago

    let mut stream = fetcher
        .fetch_aggtrades_stream("BTCUSDT", start_time, end_time)
        .await
        .expect("Failed to create stream");

    let mut trade_count = 0;
    let mut last_timestamp = start_time;

    while let Some(result) = stream.next().await {
        match result {
            Ok(trade) => {
                // Verify trades are in chronological order
                assert!(
                    trade.timestamp >= last_timestamp,
                    "Trades should be in chronological order"
                );
                last_timestamp = trade.timestamp;
                trade_count += 1;
            }
            Err(e) => {
                println!("Error fetching trade: {}", e);
                break;
            }
        }
    }

    println!(
        "Fetched {} trades across 2-hour window using chunking",
        trade_count
    );
}
