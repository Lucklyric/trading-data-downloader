//! CSV output writer implementation (T070-T073, T122-T125, T146-T147)

use crate::{AggTrade, Bar, FundingRate};
use csv::{ReaderBuilder, StringRecord, Writer};
use lru::LruCache;
use serde::Serialize;
use std::io::BufWriter;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use tempfile::{Builder as TempBuilder, NamedTempFile};
use tracing::{debug, info, warn};

use super::{AggTradesWriter, BarsWriter, FundingWriter, OutputError, OutputResult, OutputWriter};

/// Default write buffer size in bytes.
/// 8KB balances memory usage vs syscall overhead for typical CSV row sizes (~100-200 bytes).
/// Larger buffers reduce write syscalls but increase memory per open file.
const DEFAULT_BUFFER_SIZE: usize = 8192; // 8KB buffer

/// Maximum number of entries in deduplication cache to bound memory usage.
/// 100K entries at ~8 bytes per key = ~800KB memory per writer.
/// Sufficient for ~2.5 months of 1-minute bars per symbol (43,200 bars/month).
const DEFAULT_DEDUP_CAPACITY: usize = 100_000;

/// CSV record for OHLCV bar (NautilusTrader-compatible column names)
#[derive(Debug, Serialize)]
struct BarRecord {
    /// NautilusTrader expects "timestamp" as the datetime index column
    #[serde(rename = "timestamp")]
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
    // Writer to temp file in same dir
    writer: Writer<BufWriter<NamedTempFile>>,
    // Final path we will atomically persist to
    final_path: PathBuf,
    bars_written: u64,
    // P0-2: bounded in-memory dedup by open_time (LRU cache to limit memory)
    seen_timestamps: LruCache<i64, ()>,
    duplicates_skipped: u64,
}

impl CsvBarsWriter {
    /// Create a new CSV bars writer (T070, T169)
    ///
    /// # Arguments
    /// * `path` - Output file path
    ///
    /// # Returns
    /// New CsvBarsWriter with default buffer size
    pub fn new<P: AsRef<Path>>(path: P) -> OutputResult<Self> {
        Self::new_with_buffer_size(path, DEFAULT_BUFFER_SIZE)
    }

    /// Create a new CSV bars writer with custom buffer size (T169)
    pub fn new_with_buffer_size<P: AsRef<Path>>(path: P, buffer_size: usize) -> OutputResult<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), buffer_size, "Creating CSV bars writer");

        let (writer, existing_rows) = init_csv_writer(path, buffer_size)?;
        debug!(existing_rows, "CSV bars writer created (atomic temp file)");

        let seen_timestamps = init_dedup_cache(path, &["timestamp", "open_time"]);

        Ok(Self {
            writer,
            final_path: path.to_path_buf(),
            bars_written: 0,
            seen_timestamps,
            duplicates_skipped: 0,
        })
    }

    /// Get number of bars written so far
    pub fn bars_written(&self) -> u64 {
        self.bars_written
    }
}

impl BarsWriter for CsvBarsWriter {
    /// Write a single bar (T072, T169)
    /// Returns `Ok(true)` if bar was written, `Ok(false)` if duplicate was skipped
    fn write_bar(&mut self, bar: &Bar) -> OutputResult<bool> {
        // P0-2: dedup by open_time using LRU cache
        if self.seen_timestamps.get(&bar.open_time).is_some() {
            self.duplicates_skipped += 1;
            debug!(
                timestamp = bar.open_time,
                duplicates = self.duplicates_skipped,
                "Skipping duplicate bar"
            );
            return Ok(false); // Duplicate skipped
        }
        self.seen_timestamps.put(bar.open_time, ());

        let record = BarRecord::from(bar);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write bar: {e}")))?;

        self.bars_written += 1;

        // Flush periodically (every 1000 bars)
        if self.bars_written % 1000 == 0 {
            self.flush()?;
            debug!(bars_written = self.bars_written, "Periodic flush completed");
        }

        Ok(true) // Bar was written
    }
}

impl OutputWriter for CsvBarsWriter {
    /// Flush buffered data to disk (T073)
    fn flush(&mut self) -> OutputResult<()> {
        self.writer
            .flush()
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {e}")))
    }

    /// Close the writer and finalize output (T073, T169)
    fn close(mut self) -> OutputResult<()> {
        debug!(bars_written = self.bars_written, "Closing CSV bars writer");
        self.flush()?;
        atomic_persist(self.writer, &self.final_path)?;

        info!(
            bars_written = self.bars_written,
            duplicates_skipped = self.duplicates_skipped,
            "CSV bars writer closed successfully"
        );
        Ok(())
    }
}

/// CSV record for aggregate trade (NautilusTrader-compatible column names)
#[derive(Debug, Serialize)]
struct AggTradeRecord {
    /// NautilusTrader expects "timestamp" as the first column for datetime index
    timestamp: i64,
    /// NautilusTrader expects "trade_id"
    #[serde(rename = "trade_id")]
    agg_trade_id: i64,
    price: String,
    quantity: String,
    first_trade_id: i64,
    last_trade_id: i64,
    /// NautilusTrader expects "buyer_maker"
    #[serde(rename = "buyer_maker")]
    is_buyer_maker: bool,
}

impl From<&AggTrade> for AggTradeRecord {
    fn from(trade: &AggTrade) -> Self {
        Self {
            timestamp: trade.timestamp,
            agg_trade_id: trade.agg_trade_id,
            price: trade.price.to_string(),
            quantity: trade.quantity.to_string(),
            first_trade_id: trade.first_trade_id,
            last_trade_id: trade.last_trade_id,
            is_buyer_maker: trade.is_buyer_maker,
        }
    }
}

/// CSV writer for aggregate trades (T122)
pub struct CsvAggTradesWriter {
    writer: Writer<BufWriter<NamedTempFile>>,
    final_path: PathBuf,
    trades_written: u64,
    /// LRU cache for bounded deduplication by agg_trade_id (T125)
    seen_ids: LruCache<i64, ()>,
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
    pub fn new_with_buffer_size<P: AsRef<Path>>(path: P, buffer_size: usize) -> OutputResult<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), "Creating CSV aggTrades writer");

        let (writer, existing_rows) = init_csv_writer(path, buffer_size)?;
        debug!(existing_rows, "CSV aggTrades writer created (atomic temp file)");

        let seen_ids = init_dedup_cache(path, &["trade_id", "agg_trade_id"]);

        Ok(Self {
            writer,
            final_path: path.to_path_buf(),
            trades_written: 0,
            seen_ids,
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
        if self.seen_ids.get(&trade.agg_trade_id).is_some() {
            self.duplicates_skipped += 1;
            debug!(
                "Skipping duplicate agg_trade_id: {} (total duplicates: {})",
                trade.agg_trade_id, self.duplicates_skipped
            );
            return Ok(());
        }

        // Mark this ID as seen
        self.seen_ids.put(trade.agg_trade_id, ());

        let record = AggTradeRecord::from(trade);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write trade: {e}")))?;

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
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {e}")))
    }

    /// Close the writer and finalize output
    fn close(mut self) -> OutputResult<()> {
        debug!(
            "Closing CSV aggTrades writer: {} total trades written, {} duplicates skipped",
            self.trades_written, self.duplicates_skipped
        );
        self.flush()?;
        atomic_persist(self.writer, &self.final_path)?;

        info!(
            "CSV aggTrades writer closed successfully: {} trades written, {} duplicates skipped",
            self.trades_written, self.duplicates_skipped
        );
        Ok(())
    }
}

/// CSV record for funding rate (T144, NautilusTrader-compatible column names)
#[derive(Debug, Serialize)]
struct FundingRateRecord {
    /// NautilusTrader expects "timestamp" as the datetime index column
    #[serde(rename = "timestamp")]
    funding_time: i64,
    symbol: String,
    /// NautilusTrader expects "rate"
    #[serde(rename = "rate")]
    funding_rate: String,
}

impl From<&FundingRate> for FundingRateRecord {
    fn from(rate: &FundingRate) -> Self {
        Self {
            funding_time: rate.funding_time,
            symbol: rate.symbol.clone(),
            funding_rate: rate.funding_rate.to_string(),
        }
    }
}

/// CSV writer for funding rates (T146)
pub struct CsvFundingWriter {
    writer: Writer<BufWriter<NamedTempFile>>,
    final_path: PathBuf,
    rates_written: u64,
    buffer_size: usize,
    seen_timestamps: LruCache<i64, ()>,
    duplicates_skipped: u64,
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
    pub fn new_with_buffer_size<P: AsRef<Path>>(path: P, buffer_size: usize) -> OutputResult<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), buffer_size, "Creating CSV funding writer");

        let (writer, existing_rows) = init_csv_writer(path, buffer_size)?;
        debug!(existing_rows, "CSV funding writer created (atomic temp file)");

        let seen_timestamps = init_dedup_cache(path, &["timestamp", "funding_time"]);

        Ok(Self {
            writer,
            final_path: path.to_path_buf(),
            rates_written: 0,
            buffer_size,
            seen_timestamps,
            duplicates_skipped: 0,
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
        if self.seen_timestamps.get(&rate.funding_time).is_some() {
            self.duplicates_skipped += 1;
            debug!(
                timestamp = rate.funding_time,
                duplicates = self.duplicates_skipped,
                "Skipping duplicate funding"
            );
            return Ok(());
        }
        self.seen_timestamps.put(rate.funding_time, ());

        let record = FundingRateRecord::from(rate);

        self.writer
            .serialize(&record)
            .map_err(|e| OutputError::CsvError(format!("Failed to write funding rate: {e}")))?;

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
            .map_err(|e| OutputError::FlushError(format!("Failed to flush: {e}")))
    }

    /// Close the writer and finalize output
    fn close(mut self) -> OutputResult<()> {
        debug!(
            "Closing CSV funding writer: {} total rates written",
            self.rates_written
        );
        self.flush()?;
        atomic_persist(self.writer, &self.final_path)?;

        info!(
            "CSV funding writer closed successfully: {} rates written",
            self.rates_written
        );
        Ok(())
    }
}

// ===== Helpers: init, atomic persist, cleanup, coverage scan, and existing-key preload =====

/// Shared initialization for all CSV writers: creates parent dirs, cleans stale temps,
/// creates a temp file, and optionally copies existing data for append/merge mode (P0-4).
///
/// Returns `(csv_writer, existing_rows)`.
fn init_csv_writer(
    path: &Path,
    buffer_size: usize,
) -> OutputResult<(Writer<BufWriter<NamedTempFile>>, usize)> {
    let parent_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&parent_dir)
        .map_err(|e| OutputError::IoError(format!("Failed to create directory: {e}")))?;
    if let Err(e) = cleanup_stale_temp_files_for_target(path) {
        warn!(error = %e, "Failed to cleanup stale temp files");
    }

    let prefix = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let tempfile = TempBuilder::new()
        .prefix(prefix)
        .suffix(".tmp")
        .tempfile_in(&parent_dir)
        .map_err(|e| OutputError::IoError(format!("Failed to create temp file: {e}")))?;

    let buf_writer = BufWriter::with_capacity(buffer_size, tempfile);
    if path.exists() && path.metadata().map(|m| m.len()).unwrap_or(0) > 0 {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(buf_writer);
        match copy_existing_csv_to_writer(path, &mut writer) {
            Ok(count) => {
                info!(rows_copied = count, "Existing data copied for append/merge");
                Ok((writer, count))
            }
            Err(e) => {
                warn!(error = %e, "Failed to copy existing data, continuing without old data");
                Ok((writer, 0))
            }
        }
    } else {
        Ok((Writer::from_writer(buf_writer), 0))
    }
}

/// Create a new LRU dedup cache, optionally preloaded from an existing CSV file.
fn init_dedup_cache(path: &Path, column_names: &[&str]) -> LruCache<i64, ()> {
    let cap = NonZeroUsize::new(DEFAULT_DEDUP_CAPACITY)
        .expect("DEFAULT_DEDUP_CAPACITY must be non-zero");
    let mut cache = LruCache::new(cap);
    if path.exists() {
        if let Err(e) = load_existing_keys_lru(path, column_names, &mut cache) {
            warn!(error = %e, "Failed to preload dedup keys");
        }
    }
    cache
}

/// Atomically persist a CSV writer to its final path (P0-4)
///
/// Unwraps the CSV Writer -> BufWriter -> NamedTempFile chain, syncs data to disk,
/// atomically renames the temp file to the final path, and fsyncs the parent directory
/// to ensure rename durability.
fn atomic_persist(
    writer: Writer<BufWriter<NamedTempFile>>,
    final_path: &Path,
) -> OutputResult<()> {
    let buf_writer = writer
        .into_inner()
        .map_err(|e| OutputError::IoError(format!("Failed to get inner writer: {e}")))?;
    let tmp = buf_writer
        .into_inner()
        .map_err(|e| OutputError::IoError(format!("Failed to get temp file handle: {e}")))?;
    tmp.as_file()
        .sync_all()
        .map_err(|e| OutputError::IoError(format!("Failed to sync temp file: {e}")))?;
    tmp.persist(final_path).map_err(|e| {
        OutputError::IoError(format!(
            "Atomic persist to {} failed: {}",
            final_path.display(),
            e
        ))
    })?;

    // Fsync parent directory to ensure the rename is durable
    if let Some(parent) = final_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

/// Clean up stale .tmp files for a given target output file
pub fn cleanup_stale_temp_files_for_target<P: AsRef<Path>>(target: P) -> OutputResult<()> {
    let target = target.as_ref();
    if let Some(dir) = target.parent() {
        let stem = target.file_name().and_then(|s| s.to_str()).unwrap_or("");
        for entry in std::fs::read_dir(dir)
            .map_err(|e| OutputError::IoError(format!("Failed to read dir: {e}")))?
        {
            let path = entry
                .map_err(|e| OutputError::IoError(e.to_string()))?
                .path();
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name.starts_with(stem) && name.ends_with(".tmp") {
                    if let Err(e) = std::fs::remove_file(&path) {
                        debug!(error = %e, path = %path.display(), "Failed to remove stale temp file");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Read time range (min, max timestamps) from an existing CSV file
pub fn read_time_range(path: &Path) -> OutputResult<Option<(i64, i64)>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| OutputError::IoError(format!("Failed to open CSV: {e}")))?;
    let headers = rdr
        .headers()
        .map_err(|e| OutputError::CsvError(format!("Failed to read headers: {e}")))?
        .clone();
    let ts_index = find_timestamp_index(&headers)
        .ok_or_else(|| OutputError::CsvError("Could not find timestamp column".to_string()))?;
    let (mut min_ts, mut max_ts): (Option<i64>, Option<i64>) = (None, None);
    for rec in rdr.records() {
        let rec = rec.map_err(|e| OutputError::CsvError(format!("Bad record: {e}")))?;
        if let Some(ts) = parse_i64(rec.get(ts_index)) {
            min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
            max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
        }
    }
    Ok(match (min_ts, max_ts) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    })
}

/// Find the index of a timestamp column in CSV headers
/// Supports both new NautilusTrader-compatible names and legacy column names for backward compatibility
fn find_timestamp_index(headers: &StringRecord) -> Option<usize> {
    for name in ["timestamp", "open_time", "funding_time"] {
        if let Some(idx) = headers.iter().position(|h| h == name) {
            return Some(idx);
        }
    }
    None
}

/// Parse an i64 from a string
fn parse_i64(s: Option<&str>) -> Option<i64> {
    s.and_then(|v| v.parse::<i64>().ok())
}

/// P0-4: Copy existing CSV data to writer for append/merge mode
/// Streams existing file contents (header + all records) to prevent data loss
fn copy_existing_csv_to_writer<W: std::io::Write>(
    path: &Path,
    writer: &mut Writer<W>,
) -> OutputResult<usize> {
    if !path.exists() || path.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return Ok(0);
    }

    debug!(path = %path.display(), "Copying existing CSV data for append/merge");

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| OutputError::IoError(format!("Failed to open existing CSV: {e}")))?;

    // Copy header
    let headers = rdr
        .headers()
        .map_err(|e| OutputError::CsvError(format!("Failed to read headers: {e}")))?;
    writer
        .write_record(headers)
        .map_err(|e| OutputError::CsvError(format!("Failed to write headers: {e}")))?;

    // Copy all valid records (stops at first error to avoid replicating truncated data)
    let mut copied = 0;
    for result in rdr.records() {
        match result {
            Ok(record) => {
                writer
                    .write_record(&record)
                    .map_err(|e| OutputError::CsvError(format!("Failed to write record: {e}")))?;
                copied += 1;
            }
            Err(e) => {
                warn!(error = %e, copied = copied, "Stopped copying at first bad record (likely truncated)");
                break;
            }
        }
    }

    debug!(
        rows_copied = copied,
        "Existing CSV data copied successfully"
    );
    Ok(copied)
}

/// Load existing dedup keys from a CSV column into an LruCache (P0-2: bounded memory)
///
/// Searches for the first matching column name from `column_names` and loads all
/// values from that column into the cache. Supports both current NautilusTrader-compatible
/// and legacy column names for backward compatibility.
fn load_existing_keys_lru(
    path: &Path,
    column_names: &[&str],
    out: &mut LruCache<i64, ()>,
) -> OutputResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| OutputError::IoError(format!("Failed to open CSV: {e}")))?;
    let headers = rdr
        .headers()
        .map_err(|e| OutputError::CsvError(format!("Failed headers: {e}")))?
        .clone();
    let key_index = column_names
        .iter()
        .find_map(|name| headers.iter().position(|h| h == *name))
        .ok_or_else(|| {
            OutputError::CsvError(format!(
                "None of {column_names:?} found in CSV headers"
            ))
        })?;
    for rec in rdr.records() {
        let rec = rec.map_err(|e| OutputError::CsvError(format!("Bad record: {e}")))?;
        if let Some(val) = parse_i64(rec.get(key_index)) {
            out.put(val, ());
        }
    }
    Ok(())
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

        // Close writer to persist temp file atomically (P0-4 fix)
        writer.close().unwrap();

        // Verify file was created
        assert!(output_path.exists());

        // Verify headers exist in file
        let contents = std::fs::read_to_string(&output_path).unwrap();
        assert!(
            contents.starts_with("timestamp,open,high,low,close,volume"),
            "Expected NautilusTrader-compatible headers at start of file, got: {contents}"
        );
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
        assert_eq!(
            line_count, 2,
            "Expected 2 lines (header + data), got: {line_count}\nContents:\n{contents}"
        );

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

        // Write first bar
        writer.write_bar(&create_test_bar()).unwrap();
        assert_eq!(writer.bars_written(), 1);

        // Write second bar with different timestamp (deduplication requires unique timestamps)
        let mut bar2 = create_test_bar();
        bar2.open_time += 60000; // +1 minute
        bar2.close_time += 60000;
        writer.write_bar(&bar2).unwrap();
        assert_eq!(writer.bars_written(), 2);
    }

    #[test]
    fn test_lru_get_promotes_entry() {
        // Verify that using get() promotes the entry in the LRU cache,
        // keeping recently-checked duplicates from being evicted.
        let cap = NonZeroUsize::new(3).unwrap();
        let mut cache: LruCache<i64, ()> = LruCache::new(cap);

        // Insert 3 entries
        cache.put(1, ());
        cache.put(2, ());
        cache.put(3, ());

        // Access entry 1 via get() - this should promote it
        assert!(cache.get(&1).is_some());

        // Insert a 4th entry, which should evict entry 2 (least recently used)
        cache.put(4, ());

        // Entry 1 should still be present (was promoted by get())
        assert!(cache.get(&1).is_some(), "Entry 1 should be promoted by get()");
        // Entry 2 should have been evicted
        assert!(cache.get(&2).is_none(), "Entry 2 should be evicted");
        // Entries 3 and 4 should still be present
        assert!(cache.get(&3).is_some());
        assert!(cache.get(&4).is_some());
    }
}
