//! Integration tests for funding rate download functionality
//!
//! These tests verify the end-to-end functionality of downloading funding rate data.

use chrono::{DateTime, Timelike, Utc};
use std::path::PathBuf;
use tempfile::TempDir;

/// T132: Integration test for funding rate download end-to-end
/// This test verifies that we can successfully download funding rate data
#[tokio::test]
async fn test_funding_rate_download_end_to_end() {
    // This test will be implemented after the fetcher and writer are ready
    // For now, we just ensure it compiles

    // TODO: Implement once BinanceFuturesUsdtFetcher::fetch_funding_stream() exists
    // TODO: Implement once CsvFundingWriter exists
    // TODO: Implement once DownloadExecutor::execute_funding_job() exists
}

/// T133: Integration test for funding rate download resume capability
/// This test verifies that interrupted funding downloads can be resumed
#[tokio::test]
async fn test_funding_rate_download_resume() {
    // This test will be implemented after resume support is added

    // TODO: Implement test that:
    // 1. Starts a funding download
    // 2. Interrupts it midway
    // 3. Resumes the download
    // 4. Verifies no duplicate data and all records present
}

/// T134: Test for funding time alignment validation
/// This test verifies that funding times are correctly aligned to 8-hour intervals
#[test]
fn test_funding_time_alignment_validation() {
    // Funding rates on Binance occur at 00:00, 08:00, and 16:00 UTC
    let valid_hours = [0, 8, 16];

    for hour in valid_hours {
        // Create a timestamp at the valid hour
        let dt = Utc::now()
            .date_naive()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
            .and_utc();

        // Verify it's at 8-hour interval
        assert_eq!(dt.hour() % 8, 0, "Hour should be divisible by 8");
        assert_eq!(dt.minute(), 0, "Minutes should be 0");
        assert_eq!(dt.second(), 0, "Seconds should be 0");
    }

    // Invalid hours should fail the check
    let invalid_hours = [1, 4, 7, 9, 12, 15, 20, 23];
    for hour in invalid_hours {
        let dt = Utc::now()
            .date_naive()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
            .and_utc();

        assert_ne!(
            dt.hour() % 8,
            0,
            "Hour {} should NOT be divisible by 8",
            hour
        );
    }
}

/// T135: Test for FundingRate struct validation
/// This test verifies the FundingRate struct can be created and validated correctly
#[test]
fn test_funding_rate_struct_validation() {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use trading_data_downloader::FundingRate;

    // Test valid funding rate
    let rate = FundingRate {
        symbol: "BTCUSDT".to_string(),
        funding_rate: Decimal::from_str("0.0001").unwrap(),
        funding_time: 1704067200000, // 2024-01-01 00:00:00 UTC (divisible by 8 hours)
    };

    assert!(rate.validate().is_ok());
    assert_eq!(rate.symbol, "BTCUSDT");
    assert_eq!(rate.funding_rate, Decimal::from_str("0.0001").unwrap());
    assert_eq!(rate.funding_time, 1704067200000);

    // Test invalid symbol (empty)
    let invalid_symbol = FundingRate {
        symbol: "".to_string(),
        ..rate.clone()
    };
    assert!(invalid_symbol.validate().is_err());

    // Test invalid funding time (not at 8-hour boundary)
    let invalid_time = FundingRate {
        funding_time: 1704070800000, // 2024-01-01 01:00:00 UTC (not divisible by 8)
        ..rate.clone()
    };
    assert!(invalid_time.validate().is_err());
}

/// T137: Test for BinanceFuturesUsdtFetcher::fetch_funding_stream() pagination
/// This test verifies that the fetcher can paginate through funding rate data
#[tokio::test]
async fn test_funding_fetcher_pagination() {
    // This will be implemented once fetch_funding_stream() exists

    // TODO: Test that fetcher correctly handles:
    // - Multiple pages of funding rate data
    // - 1000 rate limit per request
    // - Correct time window handling
}

/// T139: Test for funding request builder
/// This test verifies the funding rate API request is built correctly
#[test]
fn test_funding_request_builder() {
    // This will be implemented once the request builder exists

    // TODO: Test that request includes:
    // - Correct symbol
    // - Correct time range parameters
    // - Correct limit parameter (max 1000)
}

/// T141: Test for funding response parser
/// This test verifies the funding rate response is parsed correctly
#[test]
fn test_funding_response_parser() {
    // This will be implemented once the parser exists

    // TODO: Test parsing of funding rate JSON response
    // TODO: Test conversion to FundingRate struct
    // TODO: Test error handling for malformed responses
}

/// T144: Test for CsvFundingWriter schema validation
/// This test verifies the CSV output has the correct schema
#[tokio::test]
async fn test_csv_funding_writer_schema() {
    // This will be implemented once CsvFundingWriter exists

    // TODO: Test CSV headers match expected schema:
    // - symbol
    // - funding_rate
    // - funding_time
}

/// T148: Test for DownloadExecutor funding job orchestration
/// This test verifies the executor can orchestrate a complete funding download
#[tokio::test]
#[ignore] // Requires live API access - run manually with `cargo test --ignored`
async fn test_download_executor_funding_job() {
    use tempfile::TempDir;
    use trading_data_downloader::downloader::executor::DownloadExecutor;
    use trading_data_downloader::downloader::{DownloadJob, JobType};

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("btcusdt_funding.csv");

    // Create a job for downloading funding rates for a short time window
    // 2024-01-01 00:00:00 to 2024-01-02 00:00:00 (24 hours, should get 3 funding rates)
    let job = DownloadJob {
        identifier: "BINANCE:BTC/USDT:USDT".to_string(),
        symbol: "BTCUSDT".to_string(),
        job_type: JobType::FundingRates,
        start_time: 1704067200000, // 2024-01-01 00:00:00 UTC
        end_time: 1704153600000,   // 2024-01-02 00:00:00 UTC
        output_path: output_path.clone(),
        resume_dir: None,
        status: trading_data_downloader::downloader::JobStatus::Pending,
        progress: trading_data_downloader::downloader::JobProgress::default(),
    };

    // Execute the job
    let executor = DownloadExecutor::new();
    let result = executor.execute_funding_job(job).await;

    // Verify successful execution
    assert!(
        result.is_ok(),
        "Funding job execution should succeed: {:?}",
        result.err()
    );
    let progress = result.unwrap();

    // Verify output file was created
    assert!(output_path.exists(), "Output file should exist");

    // Verify we downloaded the expected number of funding rates
    // For a 24-hour period, we expect 3 funding rates (00:00, 08:00, 16:00)
    assert!(
        progress.downloaded_bars >= 3,
        "Should download at least 3 funding rates for 24-hour period, got {}",
        progress.downloaded_bars
    );
}

/// T150: Test for CLI download command with funding data type
/// This test verifies the CLI can handle funding download commands
#[test]
#[ignore] // Requires full CLI integration - run manually with `cargo test --ignored`
fn test_cli_funding_command() {
    use std::path::PathBuf;
    use std::process::Command;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("btcusdt_funding.csv");

    // Build the CLI command
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--",
            "download",
            "funding",
            "--identifier",
            "BINANCE:BTC/USDT:USDT",
            "--symbol",
            "BTCUSDT",
            "--start-time",
            "2024-01-01",
            "--end-time",
            "2024-01-02",
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output();

    // Verify command executed successfully
    assert!(output.is_ok(), "CLI command should execute");
    let output = output.unwrap();

    // Check exit code
    assert!(
        output.status.success(),
        "CLI should exit successfully. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify output file was created
    assert!(output_path.exists(), "Output file should be created by CLI");

    // Verify output contains funding rate data
    let content = std::fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("symbol"), "Output should have CSV headers");
    assert!(
        content.contains("funding_rate"),
        "Output should have funding_rate column"
    );
    assert!(
        content.contains("funding_time"),
        "Output should have funding_time column"
    );
    assert!(
        content.contains("BTCUSDT"),
        "Output should contain symbol data"
    );
}
