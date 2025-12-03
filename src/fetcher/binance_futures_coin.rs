//! Binance Futures COIN-margined data fetcher (T157-T162)

use crate::{AggTrade, Bar, ContractType, FundingRate, Interval, Symbol, TradingStatus};
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use tracing::{debug, info, warn};

use super::binance_config::COIN_FUTURES_CONFIG;
use super::binance_http::BinanceHttpClient;
use super::binance_parser::BinanceParser;
use super::shared_resources::{global_binance_rate_limiter, global_http_client};
use super::{AggTradeStream, BarStream, DataFetcher, FetcherError, FetcherResult, FundingStream};

const MAX_LIMIT: usize = 1500; // Binance API limit per klines request
const AGGTRADES_LIMIT: usize = 1000; // Max aggTrades per request
const ONE_HOUR_MS: i64 = 60 * 60 * 1000; // 1 hour in milliseconds (aggTrades API constraint)
const FUNDING_RATE_LIMIT: usize = 1000; // Max funding rates per request

/// Binance Futures COIN-margined data fetcher (T157)
pub struct BinanceFuturesCoinFetcher {
    http_client: BinanceHttpClient,
}

impl BinanceFuturesCoinFetcher {
    /// Create a new Binance Futures COIN-margined fetcher
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
            COIN_FUTURES_CONFIG.base_url,
            global_binance_rate_limiter(),
            max_retries,
        );

        Self { http_client }
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

        Self { http_client }
    }

    /// Parse a symbol from exchangeInfo response (T159)
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
            .get("contractStatus")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid contractStatus".to_string())
            })?;
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

        let price_precision = symbol_data
            .get("pricePrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid pricePrecision".to_string())
            })? as u8;

        let quantity_precision = symbol_data
            .get("quantityPrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid quantityPrecision".to_string())
            })? as u8;

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

    /// Fetch and parse exchangeInfo (T158, T159)
    async fn fetch_exchange_info(&self) -> FetcherResult<Vec<Symbol>> {
        debug!("Fetching exchange info");

        let params: Vec<(&str, String)> = vec![];
        let body: Value = self
            .http_client
            .get(
                COIN_FUTURES_CONFIG.exchange_info_endpoint,
                &params,
                COIN_FUTURES_CONFIG.exchange_info_weight,
            )
            .await?;

        let symbols_array = body
            .get("symbols")
            .and_then(|v| v.as_array())
            .ok_or_else(|| FetcherError::InvalidResponse("Missing symbols array".to_string()))?;

        let mut symbols = Vec::new();
        for symbol_data in symbols_array {
            match Self::parse_symbol(symbol_data) {
                Ok(symbol) => {
                    // Filter for TRADING status and PERPETUAL contracts
                    if symbol.status == TradingStatus::Trading
                        && symbol.contract_type == ContractType::Perpetual
                    {
                        symbols.push(symbol);
                    }
                }
                Err(e) => {
                    warn!("Failed to parse symbol: {}", e);
                    // Continue parsing other symbols
                }
            }
        }

        info!("Discovered {} tradable perpetual symbols", symbols.len());
        Ok(symbols)
    }

    /// Fetch klines for a time range (T160)
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
                COIN_FUTURES_CONFIG.klines_endpoint,
                &params,
                COIN_FUTURES_CONFIG.klines_weight,
            )
            .await?;

        let bars = BinanceParser::parse_klines(klines)?;

        debug!("Fetched {} bars", bars.len());
        Ok(bars)
    }

    /// Fetch aggregate trades for a time range (T161)
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
                COIN_FUTURES_CONFIG.aggtrades_endpoint,
                &params,
                COIN_FUTURES_CONFIG.aggtrades_weight,
            )
            .await?;

        let trades = BinanceParser::parse_aggtrades(trades_json)?;

        debug!("Fetched {} aggTrades", trades.len());
        Ok(trades)
    }

    /// Calculate number of bars for a time range
    fn calculate_total_bars(start_time: i64, end_time: i64, interval_ms: i64) -> u64 {
        let duration = end_time - start_time;
        ((duration + interval_ms - 1) / interval_ms) as u64 // Round up
    }

    /// Fetch funding rates for a time range (T162)
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
                COIN_FUTURES_CONFIG.funding_endpoint,
                &params,
                COIN_FUTURES_CONFIG.funding_weight,
            )
            .await?;

        let funding_rates = BinanceParser::parse_funding_rates(rates_json)?;

        debug!("Fetched {} funding rates", funding_rates.len());
        Ok(funding_rates)
    }

    /// Create a stream of funding rates with automatic pagination
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

    /// Create a stream of bars with automatic pagination
    fn create_bar_stream(
        &self,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> BarStream {
        let fetcher = self.clone();
        let interval_str = interval.to_string();
        let _interval_ms = interval.to_milliseconds();

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

    /// Create a stream of aggTrades with automatic 1-hour window chunking and pagination
    /// Handles the 1-hour API constraint by splitting the time range into 1-hour chunks
    /// Within each chunk, uses fromId pagination to fetch all trades
    fn create_aggtrades_stream(
        &self,
        symbol: String,
        start_time: i64,
        end_time: i64,
    ) -> AggTradeStream {
        let fetcher = self.clone();

        // State: (current_chunk_start, current_chunk_end, last_trade_id, done)
        let stream = stream::unfold(
            (start_time, None, None, false),
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

impl Clone for BinanceFuturesCoinFetcher {
    fn clone(&self) -> Self {
        // CRITICAL: Use shared global resources to ensure rate limits are enforced
        // across all clones. Creating new rate limiters would bypass quotas!
        // Clone uses the same max_retries as the original
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            COIN_FUTURES_CONFIG.base_url,
            global_binance_rate_limiter(),
            self.http_client.max_retries(),
        );

        Self { http_client }
    }
}

impl Default for BinanceFuturesCoinFetcher {
    fn default() -> Self {
        // Default uses max_retries=5 per FR-002
        Self::new(5)
    }
}

#[async_trait]
impl DataFetcher for BinanceFuturesCoinFetcher {
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

        Ok(self.create_bar_stream(symbol.to_string(), interval, start_time, end_time))
    }

    /// List all tradable symbols from Binance Futures COIN-margined
    async fn list_symbols(&self) -> FetcherResult<Vec<Symbol>> {
        self.fetch_exchange_info().await
    }

    /// Fetch aggregate trades as a stream
    /// Automatically handles 1-hour window chunking and fromId pagination
    async fn fetch_aggtrades_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<super::AggTradeStream> {
        info!(
            "Creating aggTrades stream: symbol={}, range=[{}, {})",
            symbol, start_time, end_time
        );

        // Calculate number of 1-hour chunks
        let duration = end_time - start_time;
        let chunks = (duration + ONE_HOUR_MS - 1) / ONE_HOUR_MS;
        debug!("Will fetch aggTrades across {} one-hour chunks", chunks);

        Ok(self.create_aggtrades_stream(symbol.to_string(), start_time, end_time))
    }

    /// Fetch funding rates as a stream
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
        COIN_FUTURES_CONFIG.base_url
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

        let total = BinanceFuturesCoinFetcher::calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 60);

        // 24 hours of 1-minute bars
        let end = 1704153600000;
        let total = BinanceFuturesCoinFetcher::calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 1440);
    }

    #[test]
    fn test_fetcher_initialization() {
        let fetcher = BinanceFuturesCoinFetcher::new(5);
        assert!(fetcher.base_url().contains("dapi.binance.com"));
    }
}
