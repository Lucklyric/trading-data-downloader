//! CSV output writer implementation (T070-T073, T122-T125, T146-T147)

use crate::{AggTrade, Bar, FundingRate};
use csv::Writer;
use serde::Serialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use tracing::{debug, info};

use super::{AggTradesWriter, BarsWriter, FundingWriter, OutputError, OutputResult, OutputWriter};

const DEFAULT_BUFFER_SIZE: usize = 8192; // 8KB buffer

/// CSV record for OHLCV bar
#[derive(Debug, Serialize)]
struct BarRecord {
    open_time: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    close_time: i64,
    quote_volume: String,
    trades: u64,
    taker_buy_base_volume: String,
    taker_buy_quote_volume: String,
}

impl From<&Bar> for BarRecord {
    fn from(bar: &Bar) -> Self {
        Self {
            open_time: bar.open_time,
            open: bar.open.to_string(),
            high: bar.high.to_string(),
            low: bar.low.to_string(),
            close: bar.close.to_string(),
            volume: bar.volume.to_string(),
            close_time: bar.close_time,
            quote_volume: bar.quote_volume.to_string(),
            trades: bar.trades,
            taker_buy_base_volume: bar.taker_buy_base_volume.to_string(),
            taker_buy_quote_volume: bar.taker_buy_quote_volume.to_string(),
        }
    }
}

/// CSV writer for OHLCV bars (T071)
pub struct CsvBarsWriter {
    writer: Writer<BufWriter<File>>,
    bars_written: u64,
    buffer_size: usize,
}

impl CsvBarsWriter {
    /// Create a new CSV bars writer (T070)
    ///
    /// # Arguments
    /// * `path` - Output file path
    ///
    /// # Returns
    /// New CsvBarsWriter with default buffer size
    pub fn new<P: AsRef<Path>>(path: P) -> OutputResult<Self> {
        Self::new_with_buffer_size(path, DEFAULT_BUFFER_SIZE)
    }

    /// Create a new CSV bars writer with custom buffer size
    ///
    /// # Arguments
    /// * `path` - Output file path
    /// * `buffer_size` - Size of write buffer in bytes
    ///
    /// # Returns
    /// New CsvBarsWriter with specified buffer size
    pub fn new_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> OutputResult<Self> {
        let path = path.as_ref();
        info!("Creating CSV writer: path={}", path.display());

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| OutputError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        let file = File::create(path)
            .map_err(|e| OutputError::IoError(format!("Failed to create file: {}", e)))?;

        let buf_writer = BufWriter::with_capacity(buffer_size, file);
        let csv_writer = Writer::from_writer(buf_writer);

        // Headers will be written automatically by csv::Writer when using serialize()
        debug!("CSV writer created (headers will be written on first serialize)");

        Ok(Self {
            writer: csv_writer,
            bars_written: 0,
            buffer_size,
        })
    }

    /// Get number of bars written so far
    pub fn bars_written(&self) -> u64 {
        self.bars_written
    }
}

impl BarsWriter for CsvBarsWriter {
    /// Write a single bar (T072)
    fn write_bar(&mut self, bar: &Bar) -> OutputResult<()> {
        let record = BarRecord::from(bar);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write bar: {}", e)))?;

        self.bars_written += 1;

        // Flush periodically (every 1000 bars)
        if self.bars_written % 1000 == 0 {
            self.flush()?;
            debug!("Progress: {} bars written", self.bars_written);
        }

        Ok(())
    }
}

impl OutputWriter for CsvBarsWriter {
    /// Flush buffered data to disk (T073)
    fn flush(&mut self) -> OutputResult<()> {
        self.writer
            .flush()
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {}", e)))
    }

    /// Close the writer and finalize output (T073)
    fn close(mut self) -> OutputResult<()> {
        debug!("Closing CSV writer: {} total bars written", self.bars_written);

        // Final flush
        self.flush()?;

        // Get inner writer and sync to disk
        let buf_writer = self.writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get inner writer: {}", e))
        })?;

        let file = buf_writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get file handle: {}", e))
        })?;

        file.sync_all()
            .map_err(|e| OutputError::IoError(format!("Failed to sync file: {}", e)))?;

        info!("CSV writer closed successfully: {} bars written", self.bars_written);
        Ok(())
    }
}

/// CSV record for aggregate trade
#[derive(Debug, Serialize)]
struct AggTradeRecord {
    agg_trade_id: i64,
    price: String,
    quantity: String,
    first_trade_id: i64,
    last_trade_id: i64,
    timestamp: i64,
    is_buyer_maker: bool,
}

impl From<&AggTrade> for AggTradeRecord {
    fn from(trade: &AggTrade) -> Self {
        Self {
            agg_trade_id: trade.agg_trade_id,
            price: trade.price.to_string(),
            quantity: trade.quantity.to_string(),
            first_trade_id: trade.first_trade_id,
            last_trade_id: trade.last_trade_id,
            timestamp: trade.timestamp,
            is_buyer_maker: trade.is_buyer_maker,
        }
    }
}

/// CSV writer for aggregate trades (T122)
pub struct CsvAggTradesWriter {
    writer: Writer<BufWriter<File>>,
    trades_written: u64,
    buffer_size: usize,
    /// HashSet for deduplication by agg_trade_id (T125)
    seen_ids: HashSet<i64>,
    duplicates_skipped: u64,
}

impl CsvAggTradesWriter {
    /// Create a new CSV aggTrades writer
    ///
    /// # Arguments
    /// * `path` - Output file path
    ///
    /// # Returns
    /// New CsvAggTradesWriter with default buffer size
    pub fn new<P: AsRef<Path>>(path: P) -> OutputResult<Self> {
        Self::new_with_buffer_size(path, DEFAULT_BUFFER_SIZE)
    }

    /// Create a new CSV aggTrades writer with custom buffer size
    ///
    /// # Arguments
    /// * `path` - Output file path
    /// * `buffer_size` - Size of write buffer in bytes
    ///
    /// # Returns
    /// New CsvAggTradesWriter with specified buffer size
    pub fn new_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> OutputResult<Self> {
        let path = path.as_ref();
        info!("Creating CSV aggTrades writer: path={}", path.display());

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| OutputError::IoError(format!("Failed to create directory: {}", e)))?;
        }

        let file = File::create(path)
            .map_err(|e| OutputError::IoError(format!("Failed to create file: {}", e)))?;

        let buf_writer = BufWriter::with_capacity(buffer_size, file);
        let csv_writer = Writer::from_writer(buf_writer);

        debug!("CSV aggTrades writer created (headers will be written on first serialize)");

        Ok(Self {
            writer: csv_writer,
            trades_written: 0,
            buffer_size,
            seen_ids: HashSet::new(),
            duplicates_skipped: 0,
        })
    }

    /// Get number of trades written so far
    pub fn trades_written(&self) -> u64 {
        self.trades_written
    }

    /// Get number of duplicate trades skipped
    pub fn duplicates_skipped(&self) -> u64 {
        self.duplicates_skipped
    }
}

impl AggTradesWriter for CsvAggTradesWriter {
    /// Write a single aggregate trade (T123)
    /// Implements deduplication by agg_trade_id (T125)
    fn write_aggtrade(&mut self, trade: &AggTrade) -> OutputResult<()> {
        // Check for duplicate agg_trade_id
        if self.seen_ids.contains(&trade.agg_trade_id) {
            self.duplicates_skipped += 1;
            debug!(
                "Skipping duplicate agg_trade_id: {} (total duplicates: {})",
                trade.agg_trade_id, self.duplicates_skipped
            );
            return Ok(());
        }

        // Mark this ID as seen
        self.seen_ids.insert(trade.agg_trade_id);

        let record = AggTradeRecord::from(trade);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write trade: {}", e)))?;

        self.trades_written += 1;

        // Flush periodically (every 1000 trades)
        if self.trades_written % 1000 == 0 {
            self.flush()?;
            debug!(
                "Progress: {} trades written, {} duplicates skipped",
                self.trades_written, self.duplicates_skipped
            );
        }

        Ok(())
    }
}

impl OutputWriter for CsvAggTradesWriter {
    /// Flush buffered data to disk
    fn flush(&mut self) -> OutputResult<()> {
        self.writer
            .flush()
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {}", e)))
    }

    /// Close the writer and finalize output
    fn close(mut self) -> OutputResult<()> {
        debug!(
            "Closing CSV aggTrades writer: {} total trades written, {} duplicates skipped",
            self.trades_written, self.duplicates_skipped
        );

        // Final flush
        self.flush()?;

        // Get inner writer and sync to disk
        let buf_writer = self.writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get inner writer: {}", e))
        })?;

        let file = buf_writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get file handle: {}", e))
        })?;

        file.sync_all()
            .map_err(|e| OutputError::IoError(format!("Failed to sync file: {}", e)))?;

        info!(
            "CSV aggTrades writer closed successfully: {} trades written, {} duplicates skipped",
            self.trades_written, self.duplicates_skipped
        );
        Ok(())
    }
}

/// CSV record for funding rate (T144)
#[derive(Debug, Serialize)]
struct FundingRateRecord {
    symbol: String,
    funding_rate: String,
    funding_time: i64,
}

impl From<&FundingRate> for FundingRateRecord {
    fn from(rate: &FundingRate) -> Self {
        Self {
            symbol: rate.symbol.clone(),
            funding_rate: rate.funding_rate.to_string(),
            funding_time: rate.funding_time,
        }
    }
}

/// CSV writer for funding rates (T146)
pub struct CsvFundingWriter {
    writer: Writer<BufWriter<File>>,
    rates_written: u64,
    buffer_size: usize,
}

impl CsvFundingWriter {
    /// Create a new CSV funding writer (T146)
    ///
    /// # Arguments
    /// * `path` - Output file path
    ///
    /// # Returns
    /// New CsvFundingWriter with default buffer size
    pub fn new<P: AsRef<Path>>(path: P) -> OutputResult<Self> {
        Self::new_with_buffer_size(path, DEFAULT_BUFFER_SIZE)
    }

    /// Create a new CSV funding writer with custom buffer size
    ///
    /// # Arguments
    /// * `path` - Output file path
    /// * `buffer_size` - Size of write buffer in bytes
    ///
    /// # Returns
    /// New CsvFundingWriter with specified buffer size
    pub fn new_with_buffer_size<P: AsRef<Path>>(
        path: P,
        buffer_size: usize,
    ) -> OutputResult<Self> {
        let file =
            File::create(path.as_ref()).map_err(|e| OutputError::IoError(e.to_string()))?;
        let buf_writer = BufWriter::with_capacity(buffer_size, file);
        let writer = Writer::from_writer(buf_writer);

        info!(
            "Created CSV funding writer: path={}, buffer={}",
            path.as_ref().display(),
            buffer_size
        );

        Ok(Self {
            writer,
            rates_written: 0,
            buffer_size,
        })
    }

    /// Get number of funding rates written
    pub fn rates_written(&self) -> u64 {
        self.rates_written
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl FundingWriter for CsvFundingWriter {
    /// Write a single funding rate to CSV (T147)
    fn write_funding(&mut self, rate: &FundingRate) -> OutputResult<()> {
        let record = FundingRateRecord::from(rate);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write funding rate: {}", e)))?;

        self.rates_written += 1;

        // Periodic flush every 1000 records
        if self.rates_written % 1000 == 0 {
            debug!("Progress: {} funding rates written", self.rates_written);
            self.flush()?;
        }

        Ok(())
    }
}

impl OutputWriter for CsvFundingWriter {
    /// Flush buffered data to disk
    fn flush(&mut self) -> OutputResult<()> {
        self.writer
            .flush()
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {}", e)))
    }

    /// Close the writer and finalize output
    fn close(mut self) -> OutputResult<()> {
        debug!(
            "Closing CSV funding writer: {} total rates written",
            self.rates_written
        );

        // Final flush
        self.flush()?;

        // Get inner writer and sync to disk
        let buf_writer = self.writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get inner writer: {}", e))
        })?;

        let file = buf_writer.into_inner().map_err(|e| {
            OutputError::IoError(format!("Failed to get file handle: {}", e))
        })?;

        file.sync_all()
            .map_err(|e| OutputError::IoError(format!("Failed to sync file: {}", e)))?;

        info!(
            "CSV funding writer closed successfully: {} rates written",
            self.rates_written
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use tempfile::TempDir;

    fn create_test_bar() -> Bar {
        Bar {
            open_time: 1699920000000,
            open: Decimal::from_str("35000.50").unwrap(),
            high: Decimal::from_str("35100.00").unwrap(),
            low: Decimal::from_str("34950.00").unwrap(),
            close: Decimal::from_str("35050.75").unwrap(),
            volume: Decimal::from_str("1234.567").unwrap(),
            close_time: 1699920059999,
            quote_volume: Decimal::from_str("43210987.65").unwrap(),
            trades: 5432,
            taker_buy_base_volume: Decimal::from_str("617.283").unwrap(),
            taker_buy_quote_volume: Decimal::from_str("21605493.82").unwrap(),
        }
    }

    #[test]
    fn test_csv_bars_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let mut writer = CsvBarsWriter::new(&output_path).unwrap();

        // Write one bar to trigger header writing
        let bar = create_test_bar();
        writer.write_bar(&bar).unwrap();
        writer.flush().unwrap();

        // Verify file was created
        assert!(output_path.exists());

        // Verify headers exist in file
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert!(contents.starts_with("open_time,open,high,low,close,volume"), "Expected headers at start of file, got: {}", contents);
    }

    #[test]
    fn test_csv_bars_writer_write_bar() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let mut writer = CsvBarsWriter::new(&output_path).unwrap();
        let bar = create_test_bar();

        writer.write_bar(&bar).unwrap();
        writer.close().unwrap();

        // Debug: check file contents
        let contents = std::fs::read_to_string(&output_path).unwrap();
        let line_count = contents.lines().count();

        // Should have header + 1 data line = 2 lines total
        assert_eq!(line_count, 2, "Expected 2 lines (header + data), got: {}\nContents:\n{}", line_count, contents);

        // Read and verify with CSV reader
        let mut reader = csv::Reader::from_path(&output_path).unwrap();
        let records: Vec<_> = reader.records().filter_map(Result::ok).collect();
        assert_eq!(records.len(), 1, "Expected 1 data record");

        let record = &records[0];
        assert_eq!(record.get(0), Some("1699920000000"));
        assert_eq!(record.get(1), Some("35000.50"));
        assert_eq!(record.get(8), Some("5432"));
    }

    #[test]
    fn test_csv_bars_writer_multiple_bars() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let mut writer = CsvBarsWriter::new(&output_path).unwrap();

        // Write 5 bars
        for i in 0..5 {
            let mut bar = create_test_bar();
            bar.open_time += i * 60000; // 1 minute apart
            bar.close_time += i * 60000;
            writer.write_bar(&bar).unwrap();
        }

        writer.close().unwrap();

        // Verify
        let mut reader = csv::Reader::from_path(&output_path).unwrap();
        let record_count = reader.records().filter_map(Result::ok).count();
        assert_eq!(record_count, 5, "Expected 5 data records");
    }

    #[test]
    fn test_bars_written_counter() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let mut writer = CsvBarsWriter::new(&output_path).unwrap();
        assert_eq!(writer.bars_written(), 0);

        writer.write_bar(&create_test_bar()).unwrap();
        assert_eq!(writer.bars_written(), 1);

        writer.write_bar(&create_test_bar()).unwrap();
        assert_eq!(writer.bars_written(), 2);
    }
}
