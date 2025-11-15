//! Data output writers
//!
//! This module provides writer traits and implementations for persisting trading data
//! to various output formats. Currently supports CSV output with potential for future
//! formats (Parquet, Arrow, etc.).
//!
//! # Architecture
//!
//! The module defines three main writer traits, one for each data type:
//!
//! - [`BarsWriter`] - For OHLCV candlestick data
//! - [`AggTradesWriter`] - For aggregate trade data
//! - [`FundingWriter`] - For funding rate data
//!
//! All writers implement the base [`OutputWriter`] trait with common operations:
//! - [`OutputWriter::flush`] - Flush buffered data to disk
//! - [`OutputWriter::close`] - Close the writer and finalize output
//!
//! # CSV Implementation
//!
//! The [`csv`] module provides CSV-based implementations:
//! - [`csv::CsvBarsWriter`] - CSV writer for bars
//! - [`csv::CsvAggTradesWriter`] - CSV writer for aggregate trades
//! - [`csv::CsvFundingWriter`] - CSV writer for funding rates
//!
//! # Usage Example
//!
//! ```no_run
//! use trading_data_downloader::output::{BarsWriter, OutputWriter};
//! use trading_data_downloader::output::csv::CsvBarsWriter;
//! use trading_data_downloader::Bar;
//! use rust_decimal::Decimal;
//! use std::path::Path;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a CSV writer for bars
//! let mut writer = CsvBarsWriter::new(Path::new("bars.csv"))?;
//!
//! // Write bars
//! let bar = Bar {
//!     open_time: 1640995200000,
//!     open: Decimal::new(47000, 0),
//!     high: Decimal::new(48000, 0),
//!     low: Decimal::new(46500, 0),
//!     close: Decimal::new(47500, 0),
//!     volume: Decimal::new(100, 0),
//!     close_time: 1640998799999,
//!     quote_volume: Decimal::new(4750000, 0),
//!     trades: 1000,
//!     taker_buy_base_volume: Decimal::new(60, 0),
//!     taker_buy_quote_volume: Decimal::new(2850000, 0),
//! };
//!
//! writer.write_bar(&bar)?;
//! writer.flush()?;
//! writer.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! All operations return [`OutputResult<T>`] which wraps [`OutputError`].
//! Errors include IO failures, CSV formatting issues, and serialization problems.

use crate::{AggTrade, Bar, FundingRate};
pub mod csv;

/// Output writer errors (T034)
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// IO error
    #[error("IO error: {0}")]
    IoError(String),

    /// CSV write error
    #[error("CSV error: {0}")]
    CsvError(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Buffer flush error
    #[error("flush error: {0}")]
    FlushError(String),
}

/// Result type for output operations
pub type OutputResult<T> = Result<T, OutputError>;

/// Generic output writer trait (T068)
pub trait OutputWriter {
    /// Flush any buffered data to disk
    fn flush(&mut self) -> OutputResult<()>;

    /// Close the writer and finalize output
    fn close(self) -> OutputResult<()>;
}

/// Trait for writing OHLCV bars (T069)
pub trait BarsWriter: OutputWriter {
    /// Write a single bar to output
    fn write_bar(&mut self, bar: &Bar) -> OutputResult<()>;

    /// Write multiple bars at once
    fn write_bars(&mut self, bars: &[Bar]) -> OutputResult<()> {
        for bar in bars {
            self.write_bar(bar)?;
        }
        Ok(())
    }
}

/// Trait for writing aggregate trades (T121)
pub trait AggTradesWriter: OutputWriter {
    /// Write a single aggregate trade to output
    fn write_aggtrade(&mut self, trade: &AggTrade) -> OutputResult<()>;

    /// Write multiple aggregate trades at once
    fn write_aggtrades(&mut self, trades: &[AggTrade]) -> OutputResult<()> {
        for trade in trades {
            self.write_aggtrade(trade)?;
        }
        Ok(())
    }
}

/// Trait for writing funding rates (T145)
pub trait FundingWriter: OutputWriter {
    /// Write a single funding rate to output
    fn write_funding(&mut self, rate: &FundingRate) -> OutputResult<()>;

    /// Write multiple funding rates at once
    fn write_fundings(&mut self, rates: &[FundingRate]) -> OutputResult<()> {
        for rate in rates {
            self.write_funding(rate)?;
        }
        Ok(())
    }
}
