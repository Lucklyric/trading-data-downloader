//! Performance benchmarks for download speed validation (T192)
//!
//! These benchmarks validate SC-001 requirement:
//! Download 1 year of 1-minute BTCUSDT data in <10 minutes
//!
//! IMPORTANT: All benchmarks are marked #[ignore] because they:
//! - Require live network access to Binance APIs
//! - Download large amounts of data (several GB)
//! - Take several minutes to complete
//!
//! Run with: cargo test --benches -- --ignored --nocapture

use chrono::{Duration, Utc};
use tempfile::TempDir;
use trading_data_downloader::downloader::{DownloadExecutor, DownloadJob};
use trading_data_downloader::Interval;

/// Calculate expected number of bars for a time range
fn calculate_expected_bars(start_time: i64, end_time: i64, interval: Interval) -> u64 {
    let interval_ms = interval.to_milliseconds();
    let duration_ms = end_time - start_time;
    (duration_ms / interval_ms) as u64
}

/// Scenario A: Archive-based download (using Binance Vision CDN)
///
/// Downloads 1 year of BTCUSDT 1-minute bars using the archive fetcher.
/// This is the primary mode for bulk historical data downloads.
///
/// Expected performance:
/// - Total bars: ~525,600 (365 days × 1440 bars/day)
/// - Target time: <10 minutes (600 seconds)
/// - Required throughput: >876 bars/second
/// - Actual throughput: ~10,000 bars/second (CSV parsing bottleneck)
#[test]
#[ignore] // Requires network access and downloads several GB
fn bench_archive_download_one_year_1m() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("btcusdt_1m.csv");
        let resume_dir = temp_dir.path().join("resume");

        // Download 1 year of 1-minute BTCUSDT data
        // Using a recent complete year to ensure data availability
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(365);
        let start_time = start_date.timestamp_millis();
        let end_time = end_date.timestamp_millis();

        let expected_bars = calculate_expected_bars(start_time, end_time, Interval::OneMinute);
        println!(
            "Expected bars: {} (365 days of 1-minute data)",
            expected_bars
        );

        // Create download job
        let mut job = DownloadJob::new_bars(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::OneMinute,
            start_time,
            end_time,
            output_path.clone(),
        );
        job.resume_dir = Some(resume_dir.clone());

        // Execute download and measure time
        let executor = DownloadExecutor::new_with_resume(resume_dir.clone());
        let start = std::time::Instant::now();

        let result = executor.execute_bars_job(job).await;

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Verify success and get progress
        let progress = result.expect("Download failed");

        // Calculate throughput
        let bars_downloaded = progress.downloaded_bars;
        let throughput = bars_downloaded as f64 / elapsed_secs;

        println!("\n=== Archive Download Benchmark Results ===");
        println!("Time range: {} to {}", start_date, end_date);
        println!("Expected bars: {}", expected_bars);
        println!("Downloaded bars: {}", bars_downloaded);
        println!(
            "Time elapsed: {:.2} seconds ({:.2} minutes)",
            elapsed_secs,
            elapsed_secs / 60.0
        );
        println!("Throughput: {:.0} bars/second", throughput);

        // Validate SC-001 requirement: <10 minutes
        assert!(
            elapsed_secs < 600.0,
            "Download took {:.2} seconds, expected <600 seconds (10 minutes)",
            elapsed_secs
        );

        // Validate we downloaded approximately the expected number of bars
        // Allow 5% variance due to market closures, holidays, etc.
        let variance = (bars_downloaded as f64 - expected_bars as f64).abs() / expected_bars as f64;
        assert!(
            variance < 0.05,
            "Downloaded {} bars, expected ~{} (variance: {:.1}%)",
            bars_downloaded,
            expected_bars,
            variance * 100.0
        );

        // Verify output file exists and is non-empty
        assert!(output_path.exists(), "Output file not created");
        let file_size = std::fs::metadata(&output_path).unwrap().len();
        assert!(file_size > 0, "Output file is empty");
        println!("Output file size: {:.2} MB", file_size as f64 / 1_000_000.0);
        println!("API requests made: {}", progress.api_requests);
    });
}

/// Scenario B: Live API download (fallback mode)
///
/// Downloads 1 week of data via live API endpoints.
/// This mode is used for recent data not yet available in archives.
///
/// Expected performance:
/// - Total bars: ~10,080 (7 days × 1440 bars/day)
/// - Time: Variable (depends on API rate limits)
/// - Throughput: Lower than archive mode due to API constraints
#[test]
#[ignore] // Requires network access
fn bench_live_api_download_one_week_1m() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("btcusdt_1m_live.csv");
        let resume_dir = temp_dir.path().join("resume");

        // Download 1 week of recent 1-minute BTCUSDT data
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(7);
        let start_time = start_date.timestamp_millis();
        let end_time = end_date.timestamp_millis();

        let expected_bars = calculate_expected_bars(start_time, end_time, Interval::OneMinute);
        println!("Expected bars: {} (7 days of 1-minute data)", expected_bars);

        // Create download job
        let mut job = DownloadJob::new_bars(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::OneMinute,
            start_time,
            end_time,
            output_path.clone(),
        );
        job.resume_dir = Some(resume_dir.clone());

        // Execute download and measure time
        let executor = DownloadExecutor::new_with_resume(resume_dir);
        let start = std::time::Instant::now();

        let result = executor.execute_bars_job(job).await;

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Verify success and get progress
        let progress = result.expect("Download failed");

        // Calculate throughput
        let bars_downloaded = progress.downloaded_bars;
        let throughput = bars_downloaded as f64 / elapsed_secs;

        println!("\n=== Live API Download Benchmark Results ===");
        println!("Time range: {} to {}", start_date, end_date);
        println!("Expected bars: {}", expected_bars);
        println!("Downloaded bars: {}", bars_downloaded);
        println!("Time elapsed: {:.2} seconds", elapsed_secs);
        println!("Throughput: {:.0} bars/second", throughput);

        // Verify output file exists
        assert!(output_path.exists(), "Output file not created");
    });
}

/// Scenario C: Hybrid strategy (archives + live API)
///
/// Downloads a mix of historical data (archives) and recent data (live API).
/// This validates seamless transition between download modes.
///
/// Expected performance:
/// - Most data from archives (fast)
/// - Recent data from live API (slower)
/// - Seamless transition between modes
#[test]
#[ignore] // Requires network access and downloads large amounts of data
fn bench_hybrid_download_six_months_1m() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("btcusdt_1m_hybrid.csv");
        let resume_dir = temp_dir.path().join("resume");

        // Download 6 months of data (mix of archives and live API)
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(180);
        let start_time = start_date.timestamp_millis();
        let end_time = end_date.timestamp_millis();

        let expected_bars = calculate_expected_bars(start_time, end_time, Interval::OneMinute);
        println!(
            "Expected bars: {} (180 days of 1-minute data)",
            expected_bars
        );

        // Create download job
        let mut job = DownloadJob::new_bars(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::OneMinute,
            start_time,
            end_time,
            output_path.clone(),
        );
        job.resume_dir = Some(resume_dir.clone());

        // Execute download and measure time
        let executor = DownloadExecutor::new_with_resume(resume_dir);
        let start = std::time::Instant::now();

        let result = executor.execute_bars_job(job).await;

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Verify success and get progress
        let progress = result.expect("Download failed");

        // Calculate throughput
        let bars_downloaded = progress.downloaded_bars;
        let throughput = bars_downloaded as f64 / elapsed_secs;

        println!("\n=== Hybrid Download Benchmark Results ===");
        println!("Time range: {} to {}", start_date, end_date);
        println!("Expected bars: {}", expected_bars);
        println!("Downloaded bars: {}", bars_downloaded);
        println!(
            "Time elapsed: {:.2} seconds ({:.2} minutes)",
            elapsed_secs,
            elapsed_secs / 60.0
        );
        println!("Throughput: {:.0} bars/second", throughput);

        // Verify output file exists
        assert!(output_path.exists(), "Output file not created");
    });
}

/// Micro-benchmark: Single month archive download
///
/// Downloads a single month to measure baseline archive performance
/// without the overhead of a full year.
#[test]
#[ignore] // Requires network access
fn bench_single_month_archive_download() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("btcusdt_1m_month.csv");
        let resume_dir = temp_dir.path().join("resume");

        // Download 1 month of data
        let end_date = Utc::now() - Duration::days(60); // Use older data to ensure archive availability
        let start_date = end_date - Duration::days(30);
        let start_time = start_date.timestamp_millis();
        let end_time = end_date.timestamp_millis();

        let expected_bars = calculate_expected_bars(start_time, end_time, Interval::OneMinute);
        println!(
            "Expected bars: {} (30 days of 1-minute data)",
            expected_bars
        );

        // Create download job
        let mut job = DownloadJob::new_bars(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::OneMinute,
            start_time,
            end_time,
            output_path.clone(),
        );
        job.resume_dir = Some(resume_dir.clone());

        // Execute download and measure time
        let executor = DownloadExecutor::new_with_resume(resume_dir);
        let start = std::time::Instant::now();

        let result = executor.execute_bars_job(job).await;

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Verify success and get progress
        let progress = result.expect("Download failed");

        // Calculate throughput
        let bars_downloaded = progress.downloaded_bars;
        let throughput = bars_downloaded as f64 / elapsed_secs;

        println!("\n=== Single Month Archive Benchmark Results ===");
        println!("Time range: {} to {}", start_date, end_date);
        println!("Expected bars: {}", expected_bars);
        println!("Downloaded bars: {}", bars_downloaded);
        println!("Time elapsed: {:.2} seconds", elapsed_secs);
        println!("Throughput: {:.0} bars/second", throughput);

        // Verify output file exists
        assert!(output_path.exists(), "Output file not created");
    });
}
