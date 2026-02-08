//! Download command implementation (T082-T087)

use crate::downloader::{DownloadExecutor, DownloadJob, JobProgress, JobType};
use crate::shutdown::SharedShutdown;
use crate::Interval;
use chrono::{DateTime, NaiveDate};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{error, info};

use super::CliError;

/// Maximum allowed concurrency to prevent self-inflicted rate limiting (L3)
const MAX_CONCURRENCY: usize = 32;

/// Try to parse datetime from RFC3339 format (M6)
///
/// Handles both inputs with and without timezone designators:
/// - "2024-01-01T00:00:00Z" - explicit UTC
/// - "2024-01-01T00:00:00+01:00" - explicit offset
/// - "2024-01-01T00:00:00" - no timezone, assumed UTC
///
/// Returns timestamp in milliseconds, or None if parsing fails.
fn try_parse_datetime_rfc3339(input: &str) -> Option<i64> {
    let input = input.trim();

    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Some(dt.timestamp_millis());
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{input}Z")) {
        return Some(dt.timestamp_millis());
    }

    None
}

/// Parse a start time from YYYY-MM-DD or RFC3339 datetime format.
///
/// For date-only format, uses start-of-day (00:00:00 UTC).
/// For RFC3339 format, uses the exact time specified.
fn parse_start_time_flexible(input: &str) -> Result<i64, CliError> {
    if let Some(ts) = try_parse_datetime_rfc3339(input) {
        return Ok(ts);
    }

    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|e| CliError::InvalidArgument(format!("Invalid start time: {e}")))?;
    let datetime = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| CliError::InvalidArgument("Invalid start time".to_string()))?;
    Ok(datetime.and_utc().timestamp_millis())
}

/// Parse an end time from YYYY-MM-DD or RFC3339 datetime format.
///
/// For date-only format, uses end-of-day (23:59:59.999 UTC) so the specified date
/// is fully included. For RFC3339 format, uses the exact time specified.
fn parse_end_time_flexible(input: &str) -> Result<i64, CliError> {
    if let Some(ts) = try_parse_datetime_rfc3339(input) {
        return Ok(ts);
    }

    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|e| CliError::InvalidArgument(format!("Invalid end time: {e}")))?;
    let datetime = date
        .and_hms_milli_opt(23, 59, 59, 999)
        .ok_or_else(|| CliError::InvalidArgument("Invalid end time".to_string()))?;
    Ok(datetime.and_utc().timestamp_millis())
}

/// Parse and validate concurrency value (L3)
fn parse_concurrency(s: &str) -> Result<usize, String> {
    let value: usize = s
        .parse()
        .map_err(|_| format!("'{s}' is not a valid number"))?;

    if value == 0 {
        return Err("concurrency must be at least 1".to_string());
    }
    if value > MAX_CONCURRENCY {
        return Err(format!(
            "concurrency {value} exceeds maximum of {MAX_CONCURRENCY}"
        ));
    }
    Ok(value)
}

/// Resume modes (T175)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeMode {
    /// Resume mode disabled
    Off,
    /// Resume from checkpoints if available
    On,
    /// Reset resume state and start fresh
    Reset,
    /// Verify resume state integrity before starting
    Verify,
}

impl FromStr for ResumeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "off" => Ok(ResumeMode::Off),
            "on" => Ok(ResumeMode::On),
            "reset" => Ok(ResumeMode::Reset),
            "verify" => Ok(ResumeMode::Verify),
            _ => Err(format!(
                "Invalid resume mode: {s}. Valid options: on, off, reset, verify"
            )),
        }
    }
}

/// Handle Reset mode: delete existing resume directory
fn handle_resume_reset(resume_dir: &Path) -> Result<(), CliError> {
    if resume_dir.exists() {
        info!("Reset mode: deleting existing resume directory: {:?}", resume_dir);
        std::fs::remove_dir_all(resume_dir).map_err(|e| {
            CliError::InvalidArgument(format!(
                "Failed to delete resume directory {resume_dir:?}: {e}"
            ))
        })?;
        info!("Resume directory deleted successfully");
    }
    Ok(())
}

/// Handle Verify mode: check resume state integrity
fn handle_resume_verify(resume_dir: &Path) -> Result<(), CliError> {
    use crate::resume::ResumeState;

    if !resume_dir.exists() {
        info!("Verify mode: resume directory does not exist, nothing to verify");
        return Ok(());
    }

    let entries = std::fs::read_dir(resume_dir).map_err(|e| {
        CliError::InvalidArgument(format!(
            "Failed to read resume directory {resume_dir:?}: {e}"
        ))
    })?;

    let mut valid_count = 0;
    let mut error_count = 0;

    for entry_result in entries {
        let entry = entry_result.map_err(|e| {
            CliError::InvalidArgument(format!("Failed to read directory entry: {e}"))
        })?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            match ResumeState::load(&path) {
                Ok(_state) => {
                    info!("Valid resume state: {:?}", path);
                    valid_count += 1;
                }
                Err(e) => {
                    error!("Invalid resume state: {:?}: {}", path, e);
                    error_count += 1;
                }
            }
        }
    }

    if error_count > 0 {
        return Err(CliError::InvalidArgument(format!(
            "Verify failed: {error_count} invalid resume state file(s) found. Use --resume reset to clear."
        )));
    }

    info!("Verify passed: {} valid resume state file(s)", valid_count);
    Ok(())
}

/// Configuration needed for concurrent job execution (F040)
#[derive(Clone)]
struct JobConfig {
    resume: ResumeMode,
    resume_dir: Option<PathBuf>,
    max_retries: u32,
    force: bool,
    output_format: OutputFormat,
}

impl JobConfig {
    fn from_cli(cli: &Cli) -> Self {
        Self {
            resume: cli.resume,
            resume_dir: cli.resume_dir.clone(),
            max_retries: cli.max_retries,
            force: cli.force,
            output_format: cli.output_format,
        }
    }

    fn create_executor(&self, shutdown: SharedShutdown) -> DownloadExecutor {
        match self.resume {
            ResumeMode::Off => DownloadExecutor::new(),
            ResumeMode::On | ResumeMode::Verify | ResumeMode::Reset => {
                let resume_dir = self
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                DownloadExecutor::new_with_resume(resume_dir)
            }
        }
        .with_max_retries(self.max_retries)
        .with_force(self.force)
        .with_shutdown(shutdown)
    }
}

/// Trading Data Downloader CLI (T082)
#[derive(Parser, Debug)]
#[command(name = "trading-data-downloader")]
#[command(about = "Download crypto trading data from supported exchanges", long_about = None)]
#[command(version)]
pub struct Cli {
    /// Command to execute
    #[command(subcommand)]
    pub command: Commands,

    /// Output format (json or human)
    #[arg(long, global = true, default_value = "human")]
    pub output_format: OutputFormat,

    /// Resume mode: on, off, reset, or verify
    #[arg(long, global = true, default_value = "off")]
    pub resume: ResumeMode,

    /// Resume state directory
    #[arg(long, global = true)]
    pub resume_dir: Option<PathBuf>,

    /// Number of concurrent downloads (default: 4, max: 32)
    ///
    /// Higher values increase throughput for large date ranges by downloading
    /// multiple months in parallel. The rate limiter coordinates all concurrent
    /// requests to stay within Binance API limits.
    ///
    /// Recommended: 4-8 for most use cases, up to 16 for very large backfills.
    #[arg(long, global = true, default_value = "4", value_parser = parse_concurrency)]
    pub concurrency: usize,

    /// Maximum number of retries for failed requests (default: 5, range: 1-20)
    #[arg(long, global = true, default_value = "5", value_parser = clap::value_parser!(u32).range(1..=20))]
    pub max_retries: u32,

    /// Force re-download even if output file already covers the requested range
    #[arg(long, global = true, default_value_t = false)]
    pub force: bool,

    /// Data root directory for hierarchical file organization (default: "data")
    #[arg(long, global = true)]
    pub data_dir: Option<PathBuf>,
}

/// CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Download trading data
    Download(DownloadArgs),

    /// List available data sources and symbols
    Sources(super::SourcesCommand),

    /// Validate identifiers or resume state
    Validate(super::ValidateCommand),
}

/// Download command arguments (T083-T084)
#[derive(Parser, Debug)]
pub struct DownloadArgs {
    /// Data type to download
    #[command(subcommand)]
    pub data_type: DataType,
}

/// Data types available for download
#[derive(Subcommand, Debug)]
pub enum DataType {
    /// Download OHLCV bars
    Bars(BarsArgs),
    /// Download aggregate trades
    AggTrades(AggTradesArgs),
    /// Download funding rates
    Funding(FundingArgs),
}

/// Arguments for downloading bars (T084)
#[derive(Parser, Debug)]
pub struct BarsArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Time interval (e.g., 1m, 1h, 1d)
    #[arg(long)]
    pub interval: String,

    /// Start time (YYYY-MM-DD format)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format)
    #[arg(long)]
    pub end_time: String,
}

/// Arguments for downloading aggregate trades (T129)
#[derive(Parser, Debug)]
pub struct AggTradesArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Start time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub end_time: String,
}

/// Arguments for downloading funding rates (T151)
#[derive(Parser, Debug)]
pub struct FundingArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Start time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub end_time: String,
}

/// Output format options (T086)
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// JSON output
    Json,
    /// Human-readable output
    Human,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "human" => Ok(OutputFormat::Human),
            _ => Err(format!("Invalid output format: {s}")),
        }
    }
}

// ─── Unified data type descriptor ────────────────────────────────────────────

/// Describes a data type for unified output formatting
struct DataTypeDescriptor {
    /// Display name for completion message (e.g., "Download", "AggTrades download")
    display_name: &'static str,
    /// Label for downloaded count (e.g., "Bars", "Trades", "Rates")
    count_label: &'static str,
    /// Data type name for JSON output (e.g., "bars", "aggtrades", "funding")
    data_type_name: &'static str,
    /// Prefix for JSON count fields (e.g., "bars", "trades", "rates")
    json_count_prefix: &'static str,
    /// Typed data type for dispatch logic
    data_type: crate::output::DataType,
}

const BARS_DESCRIPTOR: DataTypeDescriptor = DataTypeDescriptor {
    display_name: "Download",
    count_label: "Bars",
    data_type_name: "bars",
    json_count_prefix: "bars",
    data_type: crate::output::DataType::Bars,
};

const AGGTRADES_DESCRIPTOR: DataTypeDescriptor = DataTypeDescriptor {
    display_name: "AggTrades download",
    count_label: "Trades",
    data_type_name: "aggtrades",
    json_count_prefix: "trades",
    data_type: crate::output::DataType::AggTrades,
};

const FUNDING_DESCRIPTOR: DataTypeDescriptor = DataTypeDescriptor {
    display_name: "Funding rates download",
    count_label: "Rates",
    data_type_name: "funding",
    json_count_prefix: "rates",
    data_type: crate::output::DataType::Funding,
};

// ─── Unified output functions ────────────────────────────────────────────────

/// Output result as JSON (unified for all data types)
fn output_json(
    desc: &DataTypeDescriptor,
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    let (success, downloaded, written, api_requests, retries, err) = match result {
        Ok(progress) => (
            true,
            progress.downloaded_bars,
            progress.written_bars,
            progress.api_requests,
            progress.retries,
            None,
        ),
        Err(e) => (false, 0, 0, 0, 0, Some(e.to_string())),
    };

    // Build JSON matching original per-type format:
    // - Bars: has "interval", no "data_type"
    // - AggTrades/Funding: has "data_type", no "interval"
    let mut output = serde_json::json!({
        "success": success,
        "identifier": job.identifier,
        "symbol": job.symbol,
        "start_time": job.start_time,
        "end_time": job.end_time,
        "output_path": job.output_path.display().to_string(),
        format!("{}_downloaded", desc.json_count_prefix): downloaded,
        format!("{}_written", desc.json_count_prefix): written,
        "api_requests": api_requests,
        "retries": retries,
        "error": err,
    });

    if let JobType::Bars { interval } = job.job_type {
        output["interval"] = serde_json::Value::String(interval.to_string());
    } else {
        output["data_type"] = serde_json::Value::String(desc.data_type_name.to_string());
    }

    println!("{}", serde_json::to_string(&output).unwrap());
}

/// Output result in human-readable format (unified for all data types)
fn output_human(
    desc: &DataTypeDescriptor,
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    match result {
        Ok(progress) => {
            println!("\n{} completed successfully!", desc.display_name);
            if let JobType::Bars { interval } = job.job_type {
                println!("Symbol: {} {}", job.symbol, interval);
            } else {
                println!("Symbol: {}", job.symbol);
            }
            println!("Output: {}", job.output_path.display());
            println!("{} downloaded: {}", desc.count_label, progress.downloaded_bars);
            println!("{} written: {}", desc.count_label, progress.written_bars);
            if progress.retries > 0 {
                println!("Retries: {}", progress.retries);
            }
        }
        Err(e) => {
            eprintln!("\n{} failed!", desc.display_name);
            eprintln!("Error: {e}");
            error!("Download failed: {}", e);
        }
    }
}

// ─── Unified job execution ───────────────────────────────────────────────────

struct DownloadJobParams<'a> {
    desc: &'a DataTypeDescriptor,
    identifier: &'a str,
    symbol: &'a str,
    config: &'a JobConfig,
    shutdown: SharedShutdown,
    interval: Option<Interval>,
    start_time: i64,
    end_time: i64,
    output_path: PathBuf,
}

/// Unified download job execution helper (F040)
async fn execute_download_job(params: DownloadJobParams<'_>) -> Result<(), CliError> {
    let DownloadJobParams {
        desc,
        identifier,
        symbol,
        config,
        shutdown,
        interval,
        start_time,
        end_time,
        output_path,
    } = params;
    let job = match interval {
        Some(iv) => DownloadJob::new(
            identifier.to_string(),
            symbol.to_string(),
            iv,
            start_time,
            end_time,
            output_path.clone(),
        ),
        None => {
            if desc.data_type == crate::output::DataType::AggTrades {
                DownloadJob::new_aggtrades(
                    identifier.to_string(),
                    symbol.to_string(),
                    start_time,
                    end_time,
                    output_path.clone(),
                )
            } else {
                DownloadJob::new_funding(
                    identifier.to_string(),
                    symbol.to_string(),
                    start_time,
                    end_time,
                    output_path.clone(),
                )
            }
        }
    };

    let executor = config.create_executor(shutdown);
    let progress = create_progress_bar(&job);

    info!(
        "Starting {} download: {} from {} to {}",
        desc.data_type_name, symbol, start_time, end_time
    );

    let result = match job.job_type {
        JobType::Bars { .. } => {
            executor
                .execute_bars_job(job.clone(), Some(progress.clone()))
                .await
        }
        JobType::AggTrades => executor.execute_aggtrades_job(job.clone()).await,
        JobType::FundingRates => executor.execute_funding_job(job.clone()).await,
    };

    progress.finish_and_clear();

    match config.output_format {
        OutputFormat::Json => output_json(desc, &job, &result),
        OutputFormat::Human => output_human(desc, &job, &result),
    }

    result.map(|_| ()).map_err(CliError::DownloadError)
}

// ─── Shared execute logic for Args structs ───────────────────────────────────

struct DownloadParams<'a> {
    identifier: &'a str,
    symbol: &'a str,
    start_time_str: &'a str,
    end_time_str: &'a str,
    data_type: crate::output::DataType,
    interval: Option<Interval>,
    desc: &'static DataTypeDescriptor,
    cli: &'a Cli,
    shutdown: SharedShutdown,
}

/// Common download execution: resume handling, directory setup, month splitting, concurrency
async fn execute_download(params: DownloadParams<'_>) -> Result<(), CliError> {
    let DownloadParams {
        identifier,
        symbol,
        start_time_str,
        end_time_str,
        data_type,
        interval,
        desc,
        cli,
        shutdown,
    } = params;
    let start_time = parse_start_time_flexible(start_time_str)?;
    let end_time = parse_end_time_flexible(end_time_str)?;

    // Handle Reset/Verify modes before starting downloads
    let resume_dir = cli
        .resume_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(".resume"));
    match cli.resume {
        ResumeMode::Reset => handle_resume_reset(&resume_dir)?,
        ResumeMode::Verify => handle_resume_verify(&resume_dir)?,
        _ => {}
    }

    use crate::output::{split_into_month_ranges, OutputPathBuilder};

    let root = cli
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("data"));

    // Ensure base directories exist
    let builder = OutputPathBuilder::new(root.clone(), identifier, symbol)
        .with_data_type(data_type);
    builder
        .ensure_directories()
        .map_err(|e| CliError::InvalidArgument(format!("Failed to create directories: {e}")))?;

    let month_ranges = split_into_month_ranges(start_time, end_time);

    info!(
        "Downloading {} months of {} data: {} from {} to {}",
        month_ranges.len(),
        desc.data_type_name,
        symbol,
        start_time_str,
        end_time_str
    );

    // Pre-build job configs for concurrent execution (F040)
    let job_configs: Vec<_> = month_ranges
        .into_iter()
        .map(|month_range| {
            let mut path_builder = OutputPathBuilder::new(root.clone(), identifier, symbol)
                .with_data_type(data_type)
                .with_month(month_range.month);

            if let Some(iv) = interval {
                path_builder = path_builder.with_interval(iv);
            }

            let output_path = path_builder.build().map_err(|e| {
                CliError::InvalidArgument(format!("Failed to build output path: {e}"))
            })?;
            Ok((month_range, output_path))
        })
        .collect::<Result<Vec<_>, CliError>>()?;

    let concurrency = cli.concurrency.max(1);
    let config = JobConfig::from_cli(cli);
    info!(
        "Processing {} months with concurrency {}",
        job_configs.len(),
        concurrency
    );

    let results: Vec<Result<(), CliError>> = stream::iter(job_configs)
        .map(|(month_range, output_path)| {
            let identifier = identifier.to_string();
            let symbol = symbol.to_string();
            let config_clone = config.clone();
            let shutdown_clone = shutdown.clone();

            async move {
                info!(
                    "Processing month {}: {} to {}",
                    month_range.month, month_range.start_ms, month_range.end_ms
                );

                execute_download_job(DownloadJobParams {
                    desc,
                    identifier: &identifier,
                    symbol: &symbol,
                    config: &config_clone,
                    shutdown: shutdown_clone,
                    interval,
                    start_time: month_range.start_ms,
                    end_time: month_range.end_ms,
                    output_path,
                })
                .await
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    for result in results {
        result?;
    }

    Ok(())
}

// ─── Args execute implementations ────────────────────────────────────────────

impl BarsArgs {
    /// Execute bars download (T086-T087)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let interval = Interval::from_str(&self.interval)
            .map_err(|e| CliError::InvalidArgument(format!("Invalid interval: {e}")))?;

        execute_download(DownloadParams {
            identifier: &self.identifier,
            symbol: &self.symbol,
            start_time_str: &self.start_time,
            end_time_str: &self.end_time,
            data_type: crate::output::DataType::Bars,
            interval: Some(interval),
            desc: &BARS_DESCRIPTOR,
            cli,
            shutdown,
        })
        .await
    }
}

impl AggTradesArgs {
    /// Execute aggTrades download (T129)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        execute_download(DownloadParams {
            identifier: &self.identifier,
            symbol: &self.symbol,
            start_time_str: &self.start_time,
            end_time_str: &self.end_time,
            data_type: crate::output::DataType::AggTrades,
            interval: None,
            desc: &AGGTRADES_DESCRIPTOR,
            cli,
            shutdown,
        })
        .await
    }
}

impl FundingArgs {
    /// Execute funding rates download (T151)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        execute_download(DownloadParams {
            identifier: &self.identifier,
            symbol: &self.symbol,
            start_time_str: &self.start_time,
            end_time_str: &self.end_time,
            data_type: crate::output::DataType::Funding,
            interval: None,
            desc: &FUNDING_DESCRIPTOR,
            cli,
            shutdown,
        })
        .await
    }
}

// ─── Progress bar ────────────────────────────────────────────────────────────

/// Create progress bar with style (T087)
fn create_progress_bar(job: &DownloadJob) -> ProgressBar {
    let (total_bars, message) = match job.job_type {
        JobType::Bars { interval } => {
            let interval_ms = interval.to_milliseconds();
            let total_duration = job.end_time - job.start_time;
            let total_bars = ((total_duration + interval_ms - 1) / interval_ms) as u64;
            (
                total_bars,
                format!("Downloading {} {}", job.symbol, interval),
            )
        }
        _ => (0, format!("Downloading {}", job.symbol)),
    };

    let pb = ProgressBar::new(total_bars);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .expect("hardcoded template is valid")
            .progress_chars("#>-"),
    );
    pb.set_message(message);
    pb
}
