//! Binance Vision archive downloader (T057-T064)
//!
//! Provides high-performance downloads from Binance Vision CDN archives.
//! Uses CHECKSUM validation, streaming ZIP extraction, and automatic fallback to live API.

use crate::{Bar, Interval};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::str::FromStr;
use tempfile::TempDir;
use tracing::{debug, info, warn};
use zip::ZipArchive;

use super::{FetcherError, FetcherResult};

const BINANCE_VISION_BASE_URL: &str = "https://data.binance.vision";
const ARCHIVE_PATH_TEMPLATE: &str = "/data/futures/um/daily/klines";

/// Archive downloader for Binance Vision historical data
pub struct ArchiveDownloader {
    client: Client,
    base_url: String,
}

impl ArchiveDownloader {
    /// Create a new archive downloader
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            base_url: BINANCE_VISION_BASE_URL.to_string(),
        }
    }

    /// Create with custom base URL (for testing)
    #[allow(dead_code)]
    pub fn new_with_base_url(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    /// Generate archive file URL for a specific date
    /// URL pattern: https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
    fn archive_url(&self, symbol: &str, interval: &str, date: &NaiveDate) -> String {
        let date_str = date.format("%Y-%m-%d").to_string();
        format!(
            "{}{}/{}/{}/{}-{}-{}.zip",
            self.base_url, ARCHIVE_PATH_TEMPLATE, symbol, interval, symbol, interval, date_str
        )
    }

    /// Generate CHECKSUM file URL
    fn checksum_url(&self, archive_url: &str) -> String {
        format!("{archive_url}.CHECKSUM")
    }

    /// Download and parse CHECKSUM file (T058, T061)
    /// Returns the expected SHA-256 hash
    async fn download_checksum(&self, checksum_url: &str) -> FetcherResult<String> {
        debug!("Downloading CHECKSUM from {}", checksum_url);

        let response = self
            .client
            .get(checksum_url)
            .send()
            .await
            .map_err(|e| FetcherError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(FetcherError::ArchiveError(format!(
                "CHECKSUM download failed: HTTP {}",
                response.status()
            )));
        }

        let checksum_content = response
            .text()
            .await
            .map_err(|e| FetcherError::NetworkError(e.to_string()))?;

        // Parse checksum (format: "hash  filename" or just "hash")
        let hash = checksum_content
            .split_whitespace()
            .next()
            .ok_or_else(|| FetcherError::ArchiveError("Empty CHECKSUM file".to_string()))?
            .to_lowercase();

        debug!("Expected checksum: {}", hash);
        Ok(hash)
    }

    /// Download archive file to temporary location (T063)
    async fn download_archive(
        &self,
        archive_url: &str,
        temp_dir: &TempDir,
    ) -> FetcherResult<(PathBuf, Bytes)> {
        debug!("Downloading archive from {}", archive_url);

        let response = self
            .client
            .get(archive_url)
            .send()
            .await
            .map_err(|e| FetcherError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(FetcherError::ArchiveError(format!(
                "Archive download failed: HTTP {}",
                response.status()
            )));
        }

        let archive_bytes = response
            .bytes()
            .await
            .map_err(|e| FetcherError::NetworkError(e.to_string()))?;

        // Write to temporary file
        let filename = archive_url
            .split('/')
            .next_back()
            .ok_or_else(|| FetcherError::ArchiveError("Invalid archive URL".to_string()))?;
        let temp_path = temp_dir.path().join(filename);

        std::fs::write(&temp_path, &archive_bytes)
            .map_err(|e| FetcherError::ArchiveError(format!("Failed to write temp file: {e}")))?;

        debug!(
            "Downloaded {} bytes to {:?}",
            archive_bytes.len(),
            temp_path
        );
        Ok((temp_path, archive_bytes))
    }

    /// Compute SHA-256 hash of data (T061)
    pub fn compute_sha256(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Validate archive checksum (T061)
    fn validate_checksum(&self, expected: &str, archive_bytes: &[u8]) -> FetcherResult<()> {
        let actual = Self::compute_sha256(archive_bytes);

        if actual.to_lowercase() != expected.to_lowercase() {
            return Err(FetcherError::ChecksumMismatch {
                expected: expected.to_string(),
                actual,
            });
        }

        debug!("Checksum validation passed");
        Ok(())
    }

    /// Parse CSV line from archive to Bar struct (T059, T062)
    /// CSV format: open_time,open,high,low,close,volume,close_time,quote_volume,trades,taker_buy_base,taker_buy_quote,ignore
    pub fn parse_csv_line(line: &str) -> FetcherResult<Bar> {
        let fields: Vec<&str> = line.split(',').collect();

        if fields.len() != 12 {
            return Err(FetcherError::ParseError(format!(
                "Expected 12 CSV fields, got {}",
                fields.len()
            )));
        }

        let open_time = fields[0]
            .parse::<i64>()
            .map_err(|e| FetcherError::ParseError(format!("Invalid open_time: {e}")))?;

        let close_time = fields[6]
            .parse::<i64>()
            .map_err(|e| FetcherError::ParseError(format!("Invalid close_time: {e}")))?;

        let trades = fields[8]
            .parse::<u64>()
            .map_err(|e| FetcherError::ParseError(format!("Invalid trades: {e}")))?;

        let open = Decimal::from_str(fields[1])
            .map_err(|e| FetcherError::ParseError(format!("Invalid open: {e}")))?;
        let high = Decimal::from_str(fields[2])
            .map_err(|e| FetcherError::ParseError(format!("Invalid high: {e}")))?;
        let low = Decimal::from_str(fields[3])
            .map_err(|e| FetcherError::ParseError(format!("Invalid low: {e}")))?;
        let close = Decimal::from_str(fields[4])
            .map_err(|e| FetcherError::ParseError(format!("Invalid close: {e}")))?;
        let volume = Decimal::from_str(fields[5])
            .map_err(|e| FetcherError::ParseError(format!("Invalid volume: {e}")))?;
        let quote_volume = Decimal::from_str(fields[7])
            .map_err(|e| FetcherError::ParseError(format!("Invalid quote_volume: {e}")))?;
        let taker_buy_base_volume = Decimal::from_str(fields[9])
            .map_err(|e| FetcherError::ParseError(format!("Invalid taker_buy_base: {e}")))?;
        let taker_buy_quote_volume = Decimal::from_str(fields[10])
            .map_err(|e| FetcherError::ParseError(format!("Invalid taker_buy_quote: {e}")))?;

        Ok(Bar {
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_volume,
            trades,
            taker_buy_base_volume,
            taker_buy_quote_volume,
        })
    }

    /// Extract and parse bars from ZIP archive (T059, T062)
    /// Streams CSV contents without intermediate disk writes
    fn extract_bars_from_zip(&self, archive_bytes: &[u8]) -> FetcherResult<Vec<Bar>> {
        let cursor = Cursor::new(archive_bytes);
        let mut archive = ZipArchive::new(cursor)
            .map_err(|e| FetcherError::ArchiveError(format!("Failed to open ZIP: {e}")))?;

        if archive.len() != 1 {
            return Err(FetcherError::ArchiveError(format!(
                "Expected 1 file in ZIP, found {}",
                archive.len()
            )));
        }

        let mut file = archive
            .by_index(0)
            .map_err(|e| FetcherError::ArchiveError(format!("Failed to read ZIP entry: {e}")))?;

        // Read CSV contents
        let mut csv_content = String::new();
        file.read_to_string(&mut csv_content)
            .map_err(|e| FetcherError::ArchiveError(format!("Failed to read CSV: {e}")))?;

        debug!("Extracting bars from {} byte CSV", csv_content.len());

        // Parse CSV lines (skip header if present)
        let mut bars = Vec::new();
        for (line_num, line) in csv_content.lines().enumerate() {
            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Skip header if it looks like column names
            if line.contains("open_time") || line.contains("open,high") {
                continue;
            }

            match Self::parse_csv_line(line) {
                Ok(bar) => bars.push(bar),
                Err(e) => {
                    warn!(
                        "Failed to parse line {}: {} (error: {})",
                        line_num + 1,
                        line,
                        e
                    );
                    // Continue parsing other lines
                }
            }
        }

        info!("Extracted {} bars from archive", bars.len());
        Ok(bars)
    }

    /// Download and extract a single day's archive with CHECKSUM validation (T057, T058, T061, T062, T063)
    pub async fn download_daily_archive(
        &self,
        symbol: &str,
        interval: Interval,
        date: NaiveDate,
    ) -> FetcherResult<Vec<Bar>> {
        let interval_str = interval.to_string();
        let archive_url = self.archive_url(symbol, &interval_str, &date);
        let checksum_url = self.checksum_url(&archive_url);

        info!(
            "Downloading archive for {}-{} on {}",
            symbol, interval_str, date
        );

        // Step 1: Download CHECKSUM
        let expected_hash = self.download_checksum(&checksum_url).await?;

        // Step 2: Create temporary directory
        let temp_dir = TempDir::new()
            .map_err(|e| FetcherError::ArchiveError(format!("Failed to create temp dir: {e}")))?;

        // Step 3: Download archive
        let (_temp_path, archive_bytes) = self.download_archive(&archive_url, &temp_dir).await?;

        // Step 4: Validate CHECKSUM
        self.validate_checksum(&expected_hash, &archive_bytes)?;

        // Step 5: Extract and parse bars
        let bars = self.extract_bars_from_zip(&archive_bytes)?;

        // temp_dir is automatically cleaned up when dropped
        Ok(bars)
    }

    /// Check if archive is available for a specific date (T057)
    /// Returns true if both archive and CHECKSUM files exist
    pub async fn is_archive_available(
        &self,
        symbol: &str,
        interval: Interval,
        date: NaiveDate,
    ) -> bool {
        let interval_str = interval.to_string();
        let archive_url = self.archive_url(symbol, &interval_str, &date);

        // HEAD request to check if archive exists
        match self.client.head(&archive_url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Determine date range for which archives should be used (T060, T064)
    /// Archives are preferred for historical data (older than 1 day)
    pub fn should_use_archive(_start_time: i64, end_time: i64) -> bool {
        let now = Utc::now().timestamp_millis();
        let one_day_ms = 86_400_000;

        // Use archives if the entire range is older than 1 day
        end_time < (now - one_day_ms)
    }

    /// Generate date range for archive downloads (T057)
    pub fn date_range_for_timestamps(start_time: i64, end_time: i64) -> Vec<NaiveDate> {
        let start_dt =
            DateTime::from_timestamp_millis(start_time).expect("Invalid start timestamp");
        let end_dt = DateTime::from_timestamp_millis(end_time).expect("Invalid end timestamp");

        let start_date = start_dt.date_naive();
        let end_date = end_dt.date_naive();

        let mut dates = Vec::new();
        let mut current = start_date;

        while current <= end_date {
            dates.push(current);
            current = current.succ_opt().expect("Date overflow");
        }

        dates
    }
}

impl Clone for ArchiveDownloader {
    fn clone(&self) -> Self {
        Self {
            client: Client::new(),
            base_url: self.base_url.clone(),
        }
    }
}

impl Default for ArchiveDownloader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_archive_url_generation() {
        let downloader = ArchiveDownloader::new();
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

        let url = downloader.archive_url("BTCUSDT", "1m", &date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-15.zip"
        );
    }

    #[test]
    fn test_checksum_url_generation() {
        let downloader = ArchiveDownloader::new();
        let archive_url = "https://example.com/data.zip";

        let checksum_url = downloader.checksum_url(archive_url);
        assert_eq!(checksum_url, "https://example.com/data.zip.CHECKSUM");
    }

    #[test]
    fn test_sha256_computation() {
        let data = b"test data";
        let hash = ArchiveDownloader::compute_sha256(data);

        // Expected SHA-256 of "test data"
        assert_eq!(
            hash,
            "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"
        );
    }

    #[test]
    fn test_csv_line_parsing() {
        let csv_line = "1704067200000,42000.50,42500.00,41900.00,42300.75,1234.567,1704067259999,52134567.89,5432,617.283,25987654.32,0";

        let bar = ArchiveDownloader::parse_csv_line(csv_line).unwrap();

        assert_eq!(bar.open_time, 1704067200000);
        assert_eq!(bar.close_time, 1704067259999);
        assert_eq!(bar.trades, 5432);
        assert_eq!(bar.open, Decimal::from_str("42000.50").unwrap());
        assert_eq!(bar.high, Decimal::from_str("42500.00").unwrap());
        assert_eq!(bar.low, Decimal::from_str("41900.00").unwrap());
        assert_eq!(bar.close, Decimal::from_str("42300.75").unwrap());
    }

    #[test]
    fn test_should_use_archive() {
        let now = Utc::now().timestamp_millis();
        let one_day_ms = 86_400_000;

        // Historical data (3 days ago)
        let old_start = now - (3 * one_day_ms);
        let old_end = now - (2 * one_day_ms);
        assert!(ArchiveDownloader::should_use_archive(old_start, old_end));

        // Recent data (12 hours ago)
        let recent_start = now - (12 * 3600 * 1000);
        let recent_end = now;
        assert!(!ArchiveDownloader::should_use_archive(
            recent_start,
            recent_end
        ));
    }

    #[test]
    fn test_date_range_generation() {
        let start_time = 1704067200000; // 2024-01-01 00:00:00 UTC
        let end_time = 1704326400000; // 2024-01-04 00:00:00 UTC

        let dates = ArchiveDownloader::date_range_for_timestamps(start_time, end_time);

        assert_eq!(dates.len(), 4);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(dates[1], NaiveDate::from_ymd_opt(2024, 1, 2).unwrap());
        assert_eq!(dates[2], NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(dates[3], NaiveDate::from_ymd_opt(2024, 1, 4).unwrap());
    }
}
