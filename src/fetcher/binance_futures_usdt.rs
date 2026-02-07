//! Binance Futures USDT-margined data fetcher (T050-T065, T096-T099, T114-T119, T138-T143)

use crate::{AggTrade, Bar, ContractType, FundingRate, Interval, Symbol, TradingStatus};
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use tracing::{debug, info, warn};

use super::archive::ArchiveDownloader;
use super::binance_config::USDT_FUTURES_CONFIG;
use super::binance_http::BinanceHttpClient;
use super::binance_parser::BinanceParser;
use super::shared_resources::{global_binance_rate_limiter, global_http_client};
use super::{AggTradeStream, BarStream, DataFetcher, FetcherError, FetcherResult, FundingStream};

/// One hour in milliseconds (aggTrades API constraint)
pub const ONE_HOUR_MS: i64 = 60 * 60 * 1000;
const AGGTRADES_LIMIT: usize = 1000; // Max aggTrades per request

/// Split a time range into one-hour chunks (pure function for testing)
///
/// The Binance aggTrades API has a 1-hour maximum window constraint.
/// This function splits any time range into chunks of at most 1 hour.
///
/// # Arguments
/// * `start_time` - Start timestamp in milliseconds
/// * `end_time` - End timestamp in milliseconds
///
/// # Returns
/// Vector of (chunk_start, chunk_end) tuples
pub fn split_into_one_hour_chunks(start_time: i64, end_time: i64) -> Vec<(i64, i64)> {
    let mut chunks = Vec::new();
    let mut current_start = start_time;

    while current_start < end_time {
        let chunk_end = std::cmp::min(current_start + ONE_HOUR_MS, end_time);
        chunks.push((current_start, chunk_end));
        current_start = chunk_end;
    }

    chunks
}
const MAX_LIMIT: usize = 1500; // Binance API limit per klines request
const FUNDING_RATE_LIMIT: usize = 1000; // Max funding rates per request

/// Binance Futures USDT-margined data fetcher (T050)
pub struct BinanceFuturesUsdtFetcher {
    http_client: BinanceHttpClient,
    archive_downloader: ArchiveDownloader,
}

impl BinanceFuturesUsdtFetcher {
    /// Create a new Binance Futures USDT fetcher
    ///
    /// Uses global shared HTTP client and rate limiter to ensure:
    /// - Connection pooling across all download operations
    /// - Proper rate limit enforcement across concurrent downloads
    ///
    /// # Arguments
    /// * `max_retries` - Maximum number of retry attempts for failed requests
    pub fn new(max_retries: u32) -> Self {
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            USDT_FUTURES_CONFIG.base_url,
            global_binance_rate_limiter(),
            max_retries,
        );

        Self {
            http_client,
            archive_downloader: ArchiveDownloader::new(),
        }
    }

    /// Create with custom base URL (for testing)
    ///
    /// NOTE: This still uses the global rate limiter to ensure tests don't bypass quotas
    #[allow(dead_code)]
    pub fn new_with_base_url(base_url: String, max_retries: u32) -> Self {
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            base_url,
            global_binance_rate_limiter(),
            max_retries,
        );

        Self {
            http_client,
            archive_downloader: ArchiveDownloader::new(),
        }
    }

    /// Parse a symbol from exchangeInfo response (T098)
    fn parse_symbol(symbol_data: &Value) -> FetcherResult<Symbol> {
        let symbol = symbol_data
            .get("symbol")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid symbol".to_string()))?
            .to_string();

        let pair = symbol_data
            .get("pair")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid pair".to_string()))?
            .to_string();

        let contract_type_str = symbol_data
            .get("contractType")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid contractType".to_string())
            })?;
        let contract_type =
            ContractType::from_str(contract_type_str).map_err(FetcherError::ParseError)?;

        let status_str = symbol_data
            .get("status")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid status".to_string()))?;
        let status = TradingStatus::from_str(status_str).map_err(FetcherError::ParseError)?;

        let base_asset = symbol_data
            .get("baseAsset")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid baseAsset".to_string()))?
            .to_string();

        let quote_asset = symbol_data
            .get("quoteAsset")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid quoteAsset".to_string()))?
            .to_string();

        let margin_asset = symbol_data
            .get("marginAsset")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid marginAsset".to_string()))?
            .to_string();

        let price_precision_raw = symbol_data
            .get("pricePrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid pricePrecision".to_string())
            })?;
        let price_precision = u8::try_from(price_precision_raw).map_err(|_| {
            FetcherError::ParseError(format!(
                "pricePrecision {} exceeds u8 range",
                price_precision_raw
            ))
        })?;

        let quantity_precision_raw = symbol_data
            .get("quantityPrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid quantityPrecision".to_string())
            })?;
        let quantity_precision = u8::try_from(quantity_precision_raw).map_err(|_| {
            FetcherError::ParseError(format!(
                "quantityPrecision {} exceeds u8 range",
                quantity_precision_raw
            ))
        })?;

        // Extract tick_size and step_size from filters
        let filters = symbol_data
            .get("filters")
            .and_then(|v| v.as_array())
            .ok_or_else(|| FetcherError::ParseError("Missing or invalid filters".to_string()))?;

        let mut tick_size = None;
        let mut step_size = None;

        for filter in filters {
            let filter_type = filter
                .get("filterType")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            match filter_type {
                "PRICE_FILTER" => {
                    if let Some(ts) = filter.get("tickSize").and_then(|v| v.as_str()) {
                        tick_size = Some(Decimal::from_str(ts).map_err(|e| {
                            FetcherError::ParseError(format!("Invalid tickSize: {e}"))
                        })?);
                    }
                }
                "LOT_SIZE" => {
                    if let Some(ss) = filter.get("stepSize").and_then(|v| v.as_str()) {
                        step_size = Some(Decimal::from_str(ss).map_err(|e| {
                            FetcherError::ParseError(format!("Invalid stepSize: {e}"))
                        })?);
                    }
                }
                _ => {}
            }
        }

        let tick_size = tick_size.ok_or_else(|| {
            FetcherError::ParseError("Missing tickSize in PRICE_FILTER".to_string())
        })?;
        let step_size = step_size
            .ok_or_else(|| FetcherError::ParseError("Missing stepSize in LOT_SIZE".to_string()))?;

        Ok(Symbol {
            symbol,
            pair,
            contract_type,
            status,
            base_asset,
            quote_asset,
            margin_asset,
            price_precision,
            quantity_precision,
            tick_size,
            step_size,
        })
    }

    /// Fetch and parse exchangeInfo (T097, T098, T099, T169)
    async fn fetch_exchange_info(&self) -> FetcherResult<Vec<Symbol>> {
        info!("Fetching exchange info from API");

        let params: Vec<(&str, String)> = vec![];
        let body: Value = self
            .http_client
            .get(
                USDT_FUTURES_CONFIG.exchange_info_endpoint,
                &params,
                USDT_FUTURES_CONFIG.exchange_info_weight,
            )
            .await?;

        let symbols_array = body
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| FetcherError::InvalidResponse("Missing symbols array".to_string()))?;

        debug!("Received {} symbols from API", symbols_array.len());

        let mut symbols = Vec::new();
        let mut parse_errors = 0;
        for symbol_data in symbols_array {
            match Self::parse_symbol(symbol_data) {
                Ok(symbol) => {
                    // T099: Filter for TRADING status and PERPETUAL contracts
                    if symbol.status == TradingStatus::Trading
                        && symbol.contract_type == ContractType::Perpetual
                    {
                        symbols.push(symbol);
                    }
                }
                Err(e) => {
                    parse_errors += 1;
                    debug!("Failed to parse symbol: {}", e);
                    // Continue parsing other symbols
                }
            }
        }

        if parse_errors > 0 {
            warn!(parse_errors = parse_errors, "Some symbols failed to parse");
        }

        info!(
            tradable_symbols = symbols.len(),
            "Discovered tradable perpetual symbols"
        );
        Ok(symbols)
    }

    /// Fetch klines for a time range (T054)
    /// Returns bars that fall within [start_time, end_time)
    async fn fetch_klines_batch(
        &self,
        symbol: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
        limit: usize,
    ) -> FetcherResult<Vec<Bar>> {
        debug!(
            "Fetching klines: symbol={}, interval={}, start={}, end={}, limit={}",
            symbol, interval, start_time, end_time, limit
        );

        let params = [
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
            ("limit", limit.to_string()),
        ];

        let klines: Vec<Value> = self
            .http_client
            .get(
                USDT_FUTURES_CONFIG.klines_endpoint,
                &params,
                USDT_FUTURES_CONFIG.klines_weight,
            )
            .await?;

        let bars = BinanceParser::parse_klines(klines)?;

        debug!("Fetched {} bars", bars.len());
        Ok(bars)
    }

    /// Fetch aggregate trades for a time range (T116)
    /// Returns trades that fall within [start_time, end_time)
    /// Uses fromId for pagination within a single time window
    async fn fetch_aggtrades_batch(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
    ) -> FetcherResult<Vec<AggTrade>> {
        debug!(
            "Fetching aggTrades: symbol={}, start={}, end={}, from_id={:?}",
            symbol, start_time, end_time, from_id
        );

        let mut params = vec![
            ("symbol", symbol.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
            ("limit", AGGTRADES_LIMIT.to_string()),
        ];

        // If from_id is provided, use it for pagination (takes precedence over time-based)
        if let Some(id) = from_id {
            params.push(("fromId", id.to_string()));
        }

        let trades_json: Vec<Value> = self
            .http_client
            .get(
                USDT_FUTURES_CONFIG.aggtrades_endpoint,
                &params,
                USDT_FUTURES_CONFIG.aggtrades_weight,
            )
            .await?;

        let trades = BinanceParser::parse_aggtrades(trades_json)?;

        debug!("Fetched {} aggTrades", trades.len());
        Ok(trades)
    }

    /// Calculate number of bars for a time range (T065)
    fn calculate_total_bars(start_time: i64, end_time: i64, interval_ms: i64) -> u64 {
        let duration = end_time - start_time;
        ((duration + interval_ms - 1) / interval_ms) as u64 // Round up
    }

    /// Fetch funding rates for a time range (T140)
    /// Returns funding rates that fall within [start_time, end_time)
    async fn fetch_funding_batch(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        limit: usize,
    ) -> FetcherResult<Vec<FundingRate>> {
        debug!(
            "Fetching funding rates: symbol={}, start={}, end={}, limit={}",
            symbol, start_time, end_time, limit
        );

        let params = [
            ("symbol", symbol.to_string()),
            ("startTime", start_time.to_string()),
            ("endTime", end_time.to_string()),
            ("limit", limit.to_string()),
        ];

        let rates_json: Vec<Value> = self
            .http_client
            .get(
                USDT_FUTURES_CONFIG.funding_endpoint,
                &params,
                USDT_FUTURES_CONFIG.funding_weight,
            )
            .await?;

        let funding_rates = BinanceParser::parse_funding_rates(rates_json)?;

        debug!("Fetched {} funding rates", funding_rates.len());
        Ok(funding_rates)
    }

    /// Create a stream of funding rates with automatic pagination (T138, T143)
    fn create_funding_stream(
        &self,
        symbol: String,
        start_time: i64,
        end_time: i64,
    ) -> FundingStream {
        let fetcher = self.clone();

        let stream = stream::unfold((start_time, false), move |(current_time, done)| {
            let fetcher = fetcher.clone();
            let symbol = symbol.clone();

            async move {
                if done || current_time >= end_time {
                    return None;
                }

                // Fetch next batch
                match fetcher
                    .fetch_funding_batch(&symbol, current_time, end_time, FUNDING_RATE_LIMIT)
                    .await
                {
                    Ok(rates) => {
                        if rates.is_empty() {
                            // No more data available
                            return None;
                        }

                        // Get the last rate's funding time to determine next starting point
                        let last_funding_time =
                            rates.last().map(|r| r.funding_time).unwrap_or(current_time);
                        let next_time = last_funding_time + 1; // Start from next millisecond

                        // Convert rates to stream items
                        let items: Vec<FetcherResult<FundingRate>> =
                            rates.into_iter().map(Ok).collect();

                        Some((stream::iter(items), (next_time, false)))
                    }
                    Err(e) => {
                        // Return error and mark as done
                        Some((stream::iter(vec![Err(e)]), (current_time, true)))
                    }
                }
            }
        })
        .flatten();

        Box::pin(stream)
    }

    /// Create a hybrid bar stream using archives when possible, falling back to live API (T060, T064)
    fn create_hybrid_bar_stream(
        &self,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> BarStream {
        // Check if we should use archives
        let use_archives = ArchiveDownloader::should_use_archive(start_time, end_time);

        if use_archives {
            info!("Using archive strategy for historical data");
            let archive_downloader = self.archive_downloader.clone();
            let fetcher = self.clone(); // Capture self for live API fallback

            // Generate date range for archives (F013, F014)
            let dates = match ArchiveDownloader::date_range_for_timestamps(start_time, end_time) {
                Ok(dates) => dates,
                Err(e) => {
                    // Return a stream that immediately yields the error
                    return Box::pin(stream::once(async move { Err(e) }));
                }
            };

            let stream = stream::unfold(
                (dates, 0usize, false),
                move |(dates, index, done)| {
                    let archive_downloader = archive_downloader.clone();
                    let fetcher = fetcher.clone();
                    let symbol = symbol.clone();
                    let interval_str = interval.to_string();

                    async move {
                        if done || index >= dates.len() {
                            return None;
                        }

                        let date = dates[index];

                        // Try to download archive for this date
                        match archive_downloader.download_daily_archive(&symbol, interval, date).await {
                            Ok(bars) => {
                                // Filter bars to only include those in the requested time range
                                let filtered_bars: Vec<Bar> = bars
                                    .into_iter()
                                    .filter(|b| b.open_time >= start_time && b.open_time < end_time)
                                    .collect();

                                let items: Vec<FetcherResult<Bar>> =
                                    filtered_bars.into_iter().map(Ok).collect();

                                Some((stream::iter(items), (dates, index + 1, false)))
                            }
                            Err(e) => {
                                let date_str = date.format("%Y-%m-%d").to_string();
                                warn!("Archive download failed for {}: {}, falling back to live API for this date", date_str, e);

                                // Calculate the time range for this specific date (start and end of day)
                                // NOTE: Binance data archives use UTC timezone. The NaiveDate from
                                // date_range_for_timestamps represents a date in UTC, so we explicitly
                                // convert to UTC timestamp here.
                                // Archive date "2024-01-01" = "2024-01-01 00:00:00 UTC" to "2024-01-01 23:59:59 UTC"
                                let day_start = date.and_hms_opt(0, 0, 0)
                                    .expect("valid time").and_utc().timestamp() * 1000;
                                let day_end = day_start + 86400000; // +24 hours in ms (full UTC day)

                                // Clamp to the requested range
                                let range_start = day_start.max(start_time);
                                let range_end = day_end.min(end_time);

                                // Fetch from live API for this date range
                                match fetcher.fetch_klines_batch(
                                    &symbol,
                                    &interval_str,
                                    range_start,
                                    range_end,
                                    MAX_LIMIT,
                                ).await {
                                    Ok(bars) => {
                                        info!("Successfully fetched {} bars from live API for {}", bars.len(), date_str);
                                        let items: Vec<FetcherResult<Bar>> =
                                            bars.into_iter().map(Ok).collect();
                                        Some((stream::iter(items), (dates, index + 1, false)))
                                    }
                                    Err(api_err) => {
                                        warn!("Live API fallback also failed for {}: {}", date_str, api_err);
                                        // Return the API error and continue to next date
                                        Some((stream::iter(vec![Err(api_err)]), (dates, index + 1, false)))
                                    }
                                }
                            }
                        }
                    }
                },
            )
            .flatten();

            Box::pin(stream)
        } else {
            info!("Using live API strategy for recent data");
            self.create_bar_stream(symbol, interval, start_time, end_time)
        }
    }

    /// Create a stream of bars with automatic pagination (T052, T065)
    fn create_bar_stream(
        &self,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> BarStream {
        let fetcher = self.clone();
        let interval_str = interval.to_string();
        let stream = stream::unfold((start_time, false), move |(current_time, done)| {
            let fetcher = fetcher.clone();
            let symbol = symbol.clone();
            let interval_str = interval_str.clone();

            async move {
                if done || current_time >= end_time {
                    return None;
                }

                // Fetch next batch
                match fetcher
                    .fetch_klines_batch(&symbol, &interval_str, current_time, end_time, MAX_LIMIT)
                    .await
                {
                    Ok(bars) => {
                        if bars.is_empty() {
                            // No more data available
                            return None;
                        }

                        // Get the last bar's close time to determine next starting point
                        let last_close_time =
                            bars.last().map(|b| b.close_time).unwrap_or(current_time);
                        let next_time = last_close_time + 1; // Start from next millisecond

                        // Convert bars to stream items
                        let items: Vec<FetcherResult<Bar>> = bars.into_iter().map(Ok).collect();

                        Some((stream::iter(items), (next_time, false)))
                    }
                    Err(e) => {
                        // Return error and mark as done
                        Some((stream::iter(vec![Err(e)]), (current_time, true)))
                    }
                }
            }
        })
        .flatten();

        Box::pin(stream)
    }

    /// Create a stream of aggTrades with automatic 1-hour window chunking and pagination (T114, T116, T119)
    /// Handles the 1-hour API constraint by splitting the time range into 1-hour chunks
    /// Within each chunk, uses fromId pagination to fetch all trades
    ///
    /// # Arguments
    /// * `from_id` - Optional starting trade ID for resume; used as initial `last_trade_id`
    fn create_aggtrades_stream(
        &self,
        symbol: String,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
    ) -> AggTradeStream {
        let fetcher = self.clone();

        // State: (current_chunk_start, current_chunk_end, last_trade_id, done)
        let stream = stream::unfold(
            (start_time, None, from_id, false),
            move |(chunk_start, chunk_end_opt, last_trade_id, done)| {
                let fetcher = fetcher.clone();
                let symbol = symbol.clone();

                async move {
                    if done {
                        return None;
                    }

                    // Determine the chunk boundaries
                    let (chunk_start, chunk_end) = if let Some(end) = chunk_end_opt {
                        // Continue with current chunk
                        (chunk_start, end)
                    } else {
                        // Start new chunk (max 1 hour)
                        let chunk_end = std::cmp::min(chunk_start + ONE_HOUR_MS, end_time);
                        (chunk_start, chunk_end)
                    };

                    if chunk_start >= end_time {
                        return None;
                    }

                    // Fetch next batch within current chunk
                    match fetcher
                        .fetch_aggtrades_batch(&symbol, chunk_start, chunk_end, last_trade_id)
                        .await
                    {
                        Ok(trades) => {
                            // Filter trades to respect chunk_end boundary
                            let trades: Vec<AggTrade> = trades
                                .into_iter()
                                .filter(|t| t.timestamp < chunk_end)
                                .collect();

                            if trades.is_empty() {
                                // No more trades in current chunk, move to next chunk
                                if chunk_end >= end_time {
                                    // We've covered the entire time range
                                    return None;
                                }
                                // Move to next 1-hour chunk
                                let next_chunk_start = chunk_end;
                                Some((stream::iter(vec![]), (next_chunk_start, None, None, false)))
                            } else {
                                // Got trades, check if we need more from this chunk
                                let last_trade = trades.last().unwrap();
                                let next_trade_id = last_trade.agg_trade_id + 1;

                                // Convert trades to stream items
                                let items: Vec<FetcherResult<AggTrade>> =
                                    trades.into_iter().map(Ok).collect();

                                // Check if we got a full batch (might be more trades in this chunk)
                                if items.len() >= AGGTRADES_LIMIT {
                                    // Continue fetching from same chunk with next trade ID
                                    Some((
                                        stream::iter(items),
                                        (chunk_start, Some(chunk_end), Some(next_trade_id), false),
                                    ))
                                } else {
                                    // Partial batch, move to next chunk
                                    if chunk_end >= end_time {
                                        // This was the last chunk
                                        Some((
                                            stream::iter(items),
                                            (chunk_start, Some(chunk_end), None, true),
                                        ))
                                    } else {
                                        let next_chunk_start = chunk_end;
                                        Some((
                                            stream::iter(items),
                                            (next_chunk_start, None, None, false),
                                        ))
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // Return error and mark as done
                            Some((
                                stream::iter(vec![Err(e)]),
                                (chunk_start, Some(chunk_end), None, true),
                            ))
                        }
                    }
                }
            },
        )
        .flatten();

        Box::pin(stream)
    }
}

impl Clone for BinanceFuturesUsdtFetcher {
    fn clone(&self) -> Self {
        // CRITICAL: Use shared global resources to ensure rate limits are enforced
        // across all clones. Creating new rate limiters would bypass quotas!
        // Clone uses the same max_retries as the original
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            USDT_FUTURES_CONFIG.base_url,
            global_binance_rate_limiter(),
            self.http_client.max_retries(),
        );

        Self {
            http_client,
            archive_downloader: ArchiveDownloader::new(),
        }
    }
}

impl Default for BinanceFuturesUsdtFetcher {
    fn default() -> Self {
        // Default uses max_retries=5 per FR-002
        Self::new(5)
    }
}

#[async_trait]
impl DataFetcher for BinanceFuturesUsdtFetcher {
    async fn fetch_bars_stream(
        &self,
        symbol: &str,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<BarStream> {
        info!(
            "Creating bar stream: symbol={}, interval={}, range=[{}, {})",
            symbol, interval, start_time, end_time
        );

        let interval_ms = interval.to_milliseconds();
        let total_bars = Self::calculate_total_bars(start_time, end_time, interval_ms);
        debug!("Expected approximately {} bars", total_bars);

        // Use hybrid strategy (archives for historical data, live API for recent data)
        Ok(self.create_hybrid_bar_stream(symbol.to_string(), interval, start_time, end_time))
    }

    /// T096: List all tradable symbols from Binance Futures USDT
    async fn list_symbols(&self) -> FetcherResult<Vec<Symbol>> {
        self.fetch_exchange_info().await
    }

    /// T114: Fetch aggregate trades as a stream
    /// Automatically handles 1-hour window chunking and fromId pagination
    async fn fetch_aggtrades_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
    ) -> FetcherResult<super::AggTradeStream> {
        info!(
            "Creating aggTrades stream: symbol={}, range=[{}, {}), from_id={:?}",
            symbol, start_time, end_time, from_id
        );

        // Calculate number of 1-hour chunks
        let duration = end_time - start_time;
        let chunks = (duration + ONE_HOUR_MS - 1) / ONE_HOUR_MS;
        debug!("Will fetch aggTrades across {} one-hour chunks", chunks);

        Ok(self.create_aggtrades_stream(symbol.to_string(), start_time, end_time, from_id))
    }

    /// T138: Fetch funding rates as a stream
    async fn fetch_funding_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<super::FundingStream> {
        info!(
            "Creating funding rate stream: symbol={}, range=[{}, {})",
            symbol, start_time, end_time
        );

        Ok(self.create_funding_stream(symbol.to_string(), start_time, end_time))
    }

    fn base_url(&self) -> &str {
        USDT_FUTURES_CONFIG.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_kline() {
        let kline_json = json!([
            1699920000000i64,
            "35000.50",
            "35100.00",
            "34950.00",
            "35050.75",
            "1234.567",
            1699920059999i64,
            "43210987.65",
            5432,
            "617.283",
            "21605493.82",
            "0"
        ]);

        let bars = BinanceParser::parse_klines(vec![kline_json]).unwrap();
        let bar = &bars[0];

        assert_eq!(bar.open_time, 1699920000000);
        assert_eq!(bar.close_time, 1699920059999);
        assert_eq!(bar.trades, 5432);
        assert_eq!(bar.open, Decimal::from_str("35000.50").unwrap());
        assert_eq!(bar.high, Decimal::from_str("35100.00").unwrap());
        assert_eq!(bar.low, Decimal::from_str("34950.00").unwrap());
        assert_eq!(bar.close, Decimal::from_str("35050.75").unwrap());
    }

    #[test]
    fn test_calculate_total_bars() {
        // 1 hour of 1-minute bars
        let start = 1704067200000;
        let end = 1704070800000;
        let interval_ms = 60_000;

        let total = BinanceFuturesUsdtFetcher::calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 60);

        // 24 hours of 1-minute bars
        let end = 1704153600000;
        let total = BinanceFuturesUsdtFetcher::calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 1440);
    }

    #[test]
    fn test_fetcher_initialization() {
        let fetcher = BinanceFuturesUsdtFetcher::new(5);
        assert!(fetcher.base_url().contains("fapi.binance.com"));
    }

    #[test]
    fn test_precision_truncation_rejects_overflow() {
        // Build a symbol JSON with pricePrecision > u8::MAX
        let symbol_data = json!({
            "symbol": "BTCUSDT",
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "pricePrecision": 300u64,  // exceeds u8 max (255)
            "quantityPrecision": 3,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"}
            ]
        });

        let result = BinanceFuturesUsdtFetcher::parse_symbol(&symbol_data);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("pricePrecision") && err_msg.contains("exceeds u8 range"),
            "Error should mention pricePrecision overflow, got: {err_msg}"
        );
    }

    #[test]
    fn test_precision_truncation_accepts_valid() {
        let symbol_data = json!({
            "symbol": "BTCUSDT",
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "pricePrecision": 8,
            "quantityPrecision": 3,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"}
            ]
        });

        let result = BinanceFuturesUsdtFetcher::parse_symbol(&symbol_data);
        assert!(result.is_ok());
        let symbol = result.unwrap();
        assert_eq!(symbol.price_precision, 8);
        assert_eq!(symbol.quantity_precision, 3);
    }
}
