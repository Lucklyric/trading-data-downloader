//! Data output writers

use crate::{AggTrade, Bar, FundingRate};
use std::path::Path;

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
