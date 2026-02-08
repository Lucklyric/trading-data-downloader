//! Shared base implementation for Binance Futures fetchers
//!
//! Contains all logic common to both USDT-margined and COIN-margined fetchers.
//! Each concrete fetcher wraps a `BinanceFuturesBase` and delegates shared operations.

use crate::{AggTrade, Bar, ContractType, FundingRate, Interval, Symbol, TradingStatus};
use futures_util::{stream, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use tracing::{debug, info, warn};

use super::binance_config::BinanceMarketConfig;
use super::binance_http::BinanceHttpClient;
use super::binance_parser::BinanceParser;
use super::shared_resources::{global_binance_rate_limiter, global_http_client};
use super::{AggTradeStream, BarStream, FetcherError, FetcherResult, FundingStream};

/// Binance API limit per klines request
pub const MAX_LIMIT: usize = 1500;
/// Max aggTrades per request
pub const AGGTRADES_LIMIT: usize = 1000;
/// One hour in milliseconds (aggTrades API constraint)
pub const ONE_HOUR_MS: i64 = 60 * 60 * 1000;
/// Max funding rates per request
pub const FUNDING_RATE_LIMIT: usize = 1000;

/// Calculate number of bars for a time range
pub fn calculate_total_bars(start_time: i64, end_time: i64, interval_ms: i64) -> u64 {
    let duration = end_time - start_time;
    ((duration + interval_ms - 1) / interval_ms) as u64
}

/// Split a time range into one-hour chunks (pure function for testing)
///
/// The Binance aggTrades API has a 1-hour maximum window constraint.
/// This function splits any time range into chunks of at most 1 hour.
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

/// Shared base for Binance Futures fetchers (USDT and COIN)
pub struct BinanceFuturesBase {
    /// HTTP client for API requests
    pub http_client: BinanceHttpClient,
    /// Market-specific configuration
    pub config: &'static BinanceMarketConfig,
}

impl BinanceFuturesBase {
    /// Create a new base fetcher with global shared resources
    pub fn new(config: &'static BinanceMarketConfig, max_retries: u32) -> Self {
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            config.base_url,
            global_binance_rate_limiter(),
            max_retries,
        );
        Self {
            http_client,
            config,
        }
    }

    /// Create with custom base URL (for testing)
    #[allow(dead_code)]
    pub fn new_with_base_url(
        config: &'static BinanceMarketConfig,
        base_url: String,
        max_retries: u32,
    ) -> Self {
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            base_url,
            global_binance_rate_limiter(),
            max_retries,
        );
        Self {
            http_client,
            config,
        }
    }

    /// Clone using global shared resources (preserves rate limiting)
    pub fn clone_with_config(&self) -> Self {
        let http_client = BinanceHttpClient::new(
            global_http_client(),
            self.config.base_url,
            global_binance_rate_limiter(),
            self.http_client.max_retries(),
        );
        Self {
            http_client,
            config: self.config,
        }
    }

    /// Parse a symbol from exchangeInfo response, using config for field names
    pub fn parse_symbol(
        config: &BinanceMarketConfig,
        symbol_data: &Value,
    ) -> FetcherResult<Symbol> {
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
            .get(config.status_field_name)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                FetcherError::ParseError(format!(
                    "Missing or invalid {}",
                    config.status_field_name
                ))
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

        let price_precision_raw = symbol_data
            .get("pricePrecision")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                FetcherError::ParseError("Missing or invalid pricePrecision".to_string())
            })?;
        let price_precision = u8::try_from(price_precision_raw).map_err(|_| {
            FetcherError::ParseError(format!(
                "pricePrecision {price_precision_raw} exceeds u8 range"
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
                "quantityPrecision {quantity_precision_raw} exceeds u8 range"
            ))
        })?;

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

    /// Fetch and parse exchangeInfo
    pub async fn fetch_exchange_info(&self) -> FetcherResult<Vec<Symbol>> {
        info!("Fetching exchange info from API");

        let params: Vec<(&str, String)> = vec![];
        let body: Value = self
            .http_client
            .get(
                self.config.exchange_info_endpoint,
                &params,
                self.config.exchange_info_weight,
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
            match Self::parse_symbol(self.config, symbol_data) {
                Ok(symbol) => {
                    if symbol.status == TradingStatus::Trading
                        && symbol.contract_type == ContractType::Perpetual
                    {
                        symbols.push(symbol);
                    }
                }
                Err(e) => {
                    parse_errors += 1;
                    debug!("Failed to parse symbol: {}", e);
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

    /// Fetch klines for a time range
    pub async fn fetch_klines_batch(
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
                self.config.klines_endpoint,
                &params,
                self.config.klines_weight,
            )
            .await?;

        let bars = BinanceParser::parse_klines(klines)?;
        debug!("Fetched {} bars", bars.len());
        Ok(bars)
    }

    /// Fetch aggregate trades for a time range
    pub async fn fetch_aggtrades_batch(
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

        if let Some(id) = from_id {
            params.push(("fromId", id.to_string()));
        }

        let trades_json: Vec<Value> = self
            .http_client
            .get(
                self.config.aggtrades_endpoint,
                &params,
                self.config.aggtrades_weight,
            )
            .await?;

        let trades = BinanceParser::parse_aggtrades(trades_json)?;
        debug!("Fetched {} aggTrades", trades.len());
        Ok(trades)
    }

    /// Fetch funding rates for a time range
    pub async fn fetch_funding_batch(
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
                self.config.funding_endpoint,
                &params,
                self.config.funding_weight,
            )
            .await?;

        let funding_rates = BinanceParser::parse_funding_rates(rates_json)?;
        debug!("Fetched {} funding rates", funding_rates.len());
        Ok(funding_rates)
    }

    /// Create a stream of bars with automatic pagination
    pub fn create_bar_stream(
        &self,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> BarStream {
        let base = self.clone_with_config();
        let interval_str = interval.to_string();
        let stream = stream::unfold((start_time, false), move |(current_time, done)| {
            let base = base.clone_with_config();
            let symbol = symbol.clone();
            let interval_str = interval_str.clone();

            async move {
                if done || current_time >= end_time {
                    return None;
                }

                match base
                    .fetch_klines_batch(&symbol, &interval_str, current_time, end_time, MAX_LIMIT)
                    .await
                {
                    Ok(bars) => {
                        if bars.is_empty() {
                            return None;
                        }

                        let last_close_time =
                            bars.last().map(|b| b.close_time).unwrap_or(current_time);
                        let next_time = last_close_time + 1;

                        let items: Vec<FetcherResult<Bar>> = bars.into_iter().map(Ok).collect();
                        Some((stream::iter(items), (next_time, false)))
                    }
                    Err(e) => Some((stream::iter(vec![Err(e)]), (current_time, true))),
                }
            }
        })
        .flatten();

        Box::pin(stream)
    }

    /// Create a stream of aggTrades with automatic 1-hour window chunking and pagination
    pub fn create_aggtrades_stream(
        &self,
        symbol: String,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
    ) -> AggTradeStream {
        let base = self.clone_with_config();

        let stream = stream::unfold(
            (start_time, None, from_id, false),
            move |(chunk_start, chunk_end_opt, last_trade_id, done)| {
                let base = base.clone_with_config();
                let symbol = symbol.clone();

                async move {
                    if done {
                        return None;
                    }

                    let (chunk_start, chunk_end) = if let Some(end) = chunk_end_opt {
                        (chunk_start, end)
                    } else {
                        let chunk_end = std::cmp::min(chunk_start + ONE_HOUR_MS, end_time);
                        (chunk_start, chunk_end)
                    };

                    if chunk_start >= end_time {
                        return None;
                    }

                    match base
                        .fetch_aggtrades_batch(&symbol, chunk_start, chunk_end, last_trade_id)
                        .await
                    {
                        Ok(trades) => {
                            let trades: Vec<AggTrade> = trades
                                .into_iter()
                                .filter(|t| t.timestamp < chunk_end)
                                .collect();

                            if trades.is_empty() {
                                if chunk_end >= end_time {
                                    return None;
                                }
                                let next_chunk_start = chunk_end;
                                Some((stream::iter(vec![]), (next_chunk_start, None, None, false)))
                            } else {
                                let last_trade = trades.last().unwrap();
                                let next_trade_id = last_trade.agg_trade_id + 1;

                                let items: Vec<FetcherResult<AggTrade>> =
                                    trades.into_iter().map(Ok).collect();

                                if items.len() >= AGGTRADES_LIMIT {
                                    Some((
                                        stream::iter(items),
                                        (
                                            chunk_start,
                                            Some(chunk_end),
                                            Some(next_trade_id),
                                            false,
                                        ),
                                    ))
                                } else if chunk_end >= end_time {
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
                        Err(e) => Some((
                            stream::iter(vec![Err(e)]),
                            (chunk_start, Some(chunk_end), None, true),
                        )),
                    }
                }
            },
        )
        .flatten();

        Box::pin(stream)
    }

    /// Create a stream of funding rates with automatic pagination
    pub fn create_funding_stream(
        &self,
        symbol: String,
        start_time: i64,
        end_time: i64,
    ) -> FundingStream {
        let base = self.clone_with_config();

        let stream = stream::unfold((start_time, false), move |(current_time, done)| {
            let base = base.clone_with_config();
            let symbol = symbol.clone();

            async move {
                if done || current_time >= end_time {
                    return None;
                }

                match base
                    .fetch_funding_batch(&symbol, current_time, end_time, FUNDING_RATE_LIMIT)
                    .await
                {
                    Ok(rates) => {
                        if rates.is_empty() {
                            return None;
                        }

                        let last_funding_time =
                            rates.last().map(|r| r.funding_time).unwrap_or(current_time);
                        let next_time = last_funding_time + 1;

                        let items: Vec<FetcherResult<FundingRate>> =
                            rates.into_iter().map(Ok).collect();
                        Some((stream::iter(items), (next_time, false)))
                    }
                    Err(e) => Some((stream::iter(vec![Err(e)]), (current_time, true))),
                }
            }
        })
        .flatten();

        Box::pin(stream)
    }
}
