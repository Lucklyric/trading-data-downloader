//! Hierarchical path generation for file organization
//!
//! This module provides utilities for generating hierarchical file paths
//! with a two-level directory structure: `data/{venue}/{symbol}/`
//!
//! # Architecture
//!
//! - [`OutputPathBuilder`] - Builder pattern for constructing output paths
//! - [`YearMonth`] - Year/month representation for filename generation
//! - [`DataType`] - Enumeration of supported data types
//! - [`split_into_month_ranges`] - Utility for splitting time ranges into monthly chunks
//!
//! # Usage Example
//!
//! ```rust
//! use trading_data_downloader::output::{OutputPathBuilder, DataType, YearMonth};
//! use trading_data_downloader::Interval;
//! use std::path::PathBuf;
//! use std::str::FromStr;
//!
//! let builder = OutputPathBuilder::new(
//!     PathBuf::from("data"),
//!     "BINANCE:BTC/USDT:USDT",
//!     "BTCUSDT",
//! )
//! .with_data_type(DataType::Bars)
//! .with_interval(Interval::from_str("1m").unwrap())
//! .with_month(YearMonth { year: 2024, month: 1 });
//!
//! let path = builder.build().unwrap();
//! // Result: data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv
//! ```

use super::OutputError;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use std::path::PathBuf;

/// Year and month representation for filenames
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct YearMonth {
    /// Year (e.g., 2024)
    pub year: i32,
    /// Month (1-12)
    pub month: u32,
}

impl YearMonth {
    /// Create YearMonth from millisecond timestamp
    ///
    /// # Fallback Behavior (T040/F029D)
    ///
    /// If the timestamp is invalid (e.g., out of range for DateTime), this function
    /// falls back to the current UTC time. This is a defensive measure since:
    /// - All production callers pass valid timestamps from API responses
    /// - Invalid timestamps would indicate a bug in upstream data
    ///
    /// For callers who need explicit error handling, use [`try_from_timestamp_ms`] instead.
    pub fn from_timestamp_ms(timestamp_ms: i64) -> Self {
        Self::try_from_timestamp_ms(timestamp_ms).unwrap_or_else(|_| Self::now())
    }

    /// Create YearMonth from millisecond timestamp, returning error on invalid timestamp
    ///
    /// Returns [`OutputError::InvalidTimestamp`] if the timestamp cannot be converted.
    pub fn try_from_timestamp_ms(timestamp_ms: i64) -> super::OutputResult<Self> {
        let dt = DateTime::from_timestamp_millis(timestamp_ms)
            .ok_or(super::OutputError::InvalidTimestamp(timestamp_ms))?;

        Ok(Self {
            year: dt.year(),
            month: dt.month(),
        })
    }

    /// Get current YearMonth
    pub fn now() -> Self {
        let now = Utc::now();
        Self {
            year: now.year(),
            month: now.month(),
        }
    }

    /// Get timestamp (milliseconds) of the first day of next month at 00:00:00 UTC
    pub fn next_month_start_timestamp_ms(&self) -> i64 {
        let next = if self.month == 12 {
            YearMonth {
                year: self.year + 1,
                month: 1,
            }
        } else {
            YearMonth {
                year: self.year,
                month: self.month + 1,
            }
        };

        // SAFETY: next.month is always 1-12 (validated above), day 1 is always valid.
        // expect() is safe here as input values are constrained by the struct logic.
        let date =
            NaiveDate::from_ymd_opt(next.year, next.month, 1).expect("valid month 1-12, day 1");
        // SAFETY: 00:00:00 is always a valid time.
        let datetime = date.and_hms_opt(0, 0, 0).expect("midnight is always valid");
        datetime.and_utc().timestamp_millis()
    }
}

impl std::fmt::Display for YearMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

/// Data type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// OHLCV candlestick bars
    Bars,
    /// Aggregate trade data
    AggTrades,
    /// Funding rate data
    Funding,
}

/// Path builder for hierarchical file organization
pub struct OutputPathBuilder {
    root_dir: PathBuf,
    venue: String,
    symbol: String,
    data_type: DataType,
    interval: Option<crate::Interval>,
    month: YearMonth,
}

impl OutputPathBuilder {
    /// Create a new path builder
    ///
    /// # Arguments
    ///
    /// * `root_dir` - Root data directory (e.g., "data" or "/var/data")
    /// * `identifier` - Exchange identifier (e.g., "BINANCE:BTC/USDT:USDT")
    /// * `symbol` - Trading symbol (e.g., "BTCUSDT")
    ///
    /// # Security (F039)
    ///
    /// Symbol is sanitized to prevent path traversal attacks.
    /// Characters `/`, `\`, `:`, `..` are replaced with `_`.
    pub fn new(root_dir: PathBuf, identifier: &str, symbol: &str) -> Self {
        let venue = Self::extract_venue(identifier);
        let sanitized_symbol = sanitize_symbol(symbol);

        Self {
            root_dir,
            venue,
            symbol: sanitized_symbol,
            data_type: DataType::Bars,
            interval: None,
            month: YearMonth::now(),
        }
    }

    /// Set data type
    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    /// Set interval (required for bars)
    pub fn with_interval(mut self, interval: crate::Interval) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set month directly
    pub fn with_month(mut self, month: YearMonth) -> Self {
        self.month = month;
        self
    }

    /// Set month from timestamp
    pub fn with_month_from_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.month = YearMonth::from_timestamp_ms(timestamp_ms);
        self
    }

    /// Build the complete file path
    pub fn build(&self) -> Result<PathBuf, OutputError> {
        let filename = self.generate_filename()?;
        Ok(self
            .root_dir
            .join(&self.venue)
            .join(&self.symbol)
            .join(filename))
    }

    /// Ensure venue and symbol directories exist
    pub fn ensure_directories(&self) -> Result<(), OutputError> {
        let dir_path = self.root_dir.join(&self.venue).join(&self.symbol);
        std::fs::create_dir_all(&dir_path).map_err(|e| {
            OutputError::IoError(format!(
                "Failed to create directory {}: {}",
                dir_path.display(),
                e
            ))
        })?;
        Ok(())
    }

    fn generate_filename(&self) -> Result<String, OutputError> {
        match self.data_type {
            DataType::Bars => {
                let interval = self.interval.ok_or_else(|| {
                    OutputError::ConfigurationError(
                        "Interval required for bars data type".to_string(),
                    )
                })?;
                Ok(format!(
                    "{}-bars-{}-{}.csv",
                    self.symbol, interval, self.month
                ))
            }
            DataType::AggTrades => Ok(format!("{}-aggtrades-{}.csv", self.symbol, self.month)),
            DataType::Funding => Ok(format!("{}-funding-{}.csv", self.symbol, self.month)),
        }
    }

    /// Extract venue from identifier format
    ///
    /// Parses format: "EXCHANGE:BASE/QUOTE:SETTLEMENT"
    /// - BINANCE:BTC/USDT:USDT → binance_futures_usdt
    /// - BINANCE:BTC/BTC:BTC → binance_futures_coin
    /// - BINANCE:BTC/USD:ETH → binance_futures_coin
    fn extract_venue(identifier: &str) -> String {
        let parts: Vec<&str> = identifier.split(':').collect();

        if parts.len() < 2 {
            return sanitize_venue(identifier);
        }

        let exchange = parts[0].to_lowercase();
        let settlement = parts.get(2).copied().unwrap_or("");

        let market_type = if settlement.is_empty() {
            "spot"
        } else if settlement == "USDT" || settlement == "USD" {
            "futures_usdt"
        } else {
            "futures_coin"
        };

        let venue = format!("{exchange}_{market_type}");
        sanitize_venue(&venue)
    }
}

/// Sanitize venue name for filesystem safety
///
/// Replaces: `/`, `\`, `:`, `-` → `_`
/// Converts to lowercase
fn sanitize_venue(name: &str) -> String {
    name.replace(['/', '\\', ':', '-'], "_").to_lowercase()
}

/// Sanitize symbol name for filesystem safety (F039)
///
/// Prevents path traversal attacks by replacing dangerous characters:
/// - `/`, `\`, `:` → `_` (directory separators)
/// - `..` → `__` (parent directory reference)
///
/// Preserves case (symbols are case-sensitive on exchanges).
fn sanitize_symbol(name: &str) -> String {
    name.replace("..", "__")
        .replace(['/', '\\', ':'], "_")
}

/// Month range with start/end timestamps
pub struct MonthRange {
    /// Year and month for this range
    pub month: YearMonth,
    /// Start timestamp in milliseconds
    pub start_ms: i64,
    /// End timestamp in milliseconds
    pub end_ms: i64,
}

/// Split a time range into monthly ranges
///
/// # Arguments
///
/// * `start_ms` - Start timestamp in milliseconds
/// * `end_ms` - End timestamp in milliseconds
///
/// # Returns
///
/// Vector of [`MonthRange`] structs, one for each month in the range
pub fn split_into_month_ranges(start_ms: i64, end_ms: i64) -> Vec<MonthRange> {
    let mut ranges = Vec::new();
    let mut current_month = YearMonth::from_timestamp_ms(start_ms);
    let mut current_start_ms = start_ms;

    loop {
        let month_end_ms = current_month.next_month_start_timestamp_ms();
        let range_end_ms = month_end_ms.min(end_ms);

        ranges.push(MonthRange {
            month: current_month,
            start_ms: current_start_ms,
            end_ms: range_end_ms,
        });

        if range_end_ms >= end_ms {
            break;
        }

        current_start_ms = month_end_ms;
        current_month = YearMonth::from_timestamp_ms(month_end_ms);
    }

    ranges
}
