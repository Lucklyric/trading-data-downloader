//! Binance Futures USDT-margined data fetcher (T050-T065, T096-T099, T114-T119, T138-T143)
//!
//! Thin wrapper around [`BinanceFuturesBase`] with USDT-margined configuration.
//! Adds archive-based hybrid bar stream (USDT-only feature).

use crate::{Bar, Interval, Symbol};
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use tracing::{debug, info, warn};

use super::archive::ArchiveDownloader;
use super::binance_config::USDT_FUTURES_CONFIG;
use super::binance_futures_base::{calculate_total_bars, BinanceFuturesBase, MAX_LIMIT};
use super::{AggTradeStream, BarStream, DataFetcher, FetcherResult, FundingStream};

// Re-export for backward compatibility (used in integration tests)
pub use super::binance_futures_base::{split_into_one_hour_chunks, ONE_HOUR_MS};

/// Binance Futures USDT-margined data fetcher (T050)
pub struct BinanceFuturesUsdtFetcher {
    base: BinanceFuturesBase,
    archive_downloader: ArchiveDownloader,
}

impl BinanceFuturesUsdtFetcher {
    /// Create a new Binance Futures USDT fetcher
    pub fn new(max_retries: u32) -> Self {
        Self {
            base: BinanceFuturesBase::new(&USDT_FUTURES_CONFIG, max_retries),
            archive_downloader: ArchiveDownloader::new(),
        }
    }

    /// Create with custom base URL (for testing)
    #[allow(dead_code)]
    pub fn new_with_base_url(base_url: String, max_retries: u32) -> Self {
        Self {
            base: BinanceFuturesBase::new_with_base_url(
                &USDT_FUTURES_CONFIG,
                base_url,
                max_retries,
            ),
            archive_downloader: ArchiveDownloader::new(),
        }
    }

    /// Create a hybrid bar stream using archives when possible, falling back to live API (T060, T064)
    fn create_hybrid_bar_stream(
        &self,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> BarStream {
        let use_archives = ArchiveDownloader::should_use_archive(start_time, end_time);

        if use_archives {
            info!("Using archive strategy for historical data");
            let archive_downloader = self.archive_downloader.clone();
            let base = self.base.clone_with_config();

            let dates = match ArchiveDownloader::date_range_for_timestamps(start_time, end_time) {
                Ok(dates) => dates,
                Err(e) => {
                    return Box::pin(stream::once(async move { Err(e) }));
                }
            };

            let stream = stream::unfold(
                (dates, 0usize, false),
                move |(dates, index, done)| {
                    let archive_downloader = archive_downloader.clone();
                    let base = base.clone_with_config();
                    let symbol = symbol.clone();
                    let interval_str = interval.to_string();

                    async move {
                        if done || index >= dates.len() {
                            return None;
                        }

                        let date = dates[index];

                        match archive_downloader
                            .download_daily_archive(&symbol, interval, date)
                            .await
                        {
                            Ok(bars) => {
                                let filtered_bars: Vec<Bar> = bars
                                    .into_iter()
                                    .filter(|b| {
                                        b.open_time >= start_time && b.open_time < end_time
                                    })
                                    .collect();

                                let items: Vec<FetcherResult<Bar>> =
                                    filtered_bars.into_iter().map(Ok).collect();

                                Some((stream::iter(items), (dates, index + 1, false)))
                            }
                            Err(e) => {
                                let date_str = date.format("%Y-%m-%d").to_string();
                                warn!(
                                    "Archive download failed for {}: {}, falling back to live API",
                                    date_str, e
                                );

                                let day_start = date
                                    .and_hms_opt(0, 0, 0)
                                    .expect("valid time")
                                    .and_utc()
                                    .timestamp()
                                    * 1000;
                                let day_end = day_start + 86400000;

                                let range_start = day_start.max(start_time);
                                let range_end = day_end.min(end_time);

                                match base
                                    .fetch_klines_batch(
                                        &symbol,
                                        &interval_str,
                                        range_start,
                                        range_end,
                                        MAX_LIMIT,
                                    )
                                    .await
                                {
                                    Ok(bars) => {
                                        info!(
                                            "Successfully fetched {} bars from live API for {}",
                                            bars.len(),
                                            date_str
                                        );
                                        let items: Vec<FetcherResult<Bar>> =
                                            bars.into_iter().map(Ok).collect();
                                        Some((stream::iter(items), (dates, index + 1, false)))
                                    }
                                    Err(api_err) => {
                                        warn!(
                                            "Live API fallback also failed for {}: {}",
                                            date_str, api_err
                                        );
                                        Some((
                                            stream::iter(vec![Err(api_err)]),
                                            (dates, index + 1, false),
                                        ))
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
            self.base
                .create_bar_stream(symbol, interval, start_time, end_time)
        }
    }

    /// Parse a symbol from exchangeInfo response (backward compatibility for tests)
    pub fn parse_symbol(
        symbol_data: &serde_json::Value,
    ) -> FetcherResult<crate::Symbol> {
        BinanceFuturesBase::parse_symbol(&USDT_FUTURES_CONFIG, symbol_data)
    }
}

impl Clone for BinanceFuturesUsdtFetcher {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone_with_config(),
            archive_downloader: ArchiveDownloader::new(),
        }
    }
}

impl Default for BinanceFuturesUsdtFetcher {
    fn default() -> Self {
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
        let total_bars = calculate_total_bars(start_time, end_time, interval_ms);
        debug!("Expected approximately {} bars", total_bars);

        Ok(self.create_hybrid_bar_stream(symbol.to_string(), interval, start_time, end_time))
    }

    async fn list_symbols(&self) -> FetcherResult<Vec<Symbol>> {
        self.base.fetch_exchange_info().await
    }

    async fn fetch_aggtrades_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
    ) -> FetcherResult<AggTradeStream> {
        info!(
            "Creating aggTrades stream: symbol={}, range=[{}, {}), from_id={:?}",
            symbol, start_time, end_time, from_id
        );

        let duration = end_time - start_time;
        let chunks = (duration + ONE_HOUR_MS - 1) / ONE_HOUR_MS;
        debug!("Will fetch aggTrades across {} one-hour chunks", chunks);

        Ok(self
            .base
            .create_aggtrades_stream(symbol.to_string(), start_time, end_time, from_id))
    }

    async fn fetch_funding_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<FundingStream> {
        info!(
            "Creating funding rate stream: symbol={}, range=[{}, {})",
            symbol, start_time, end_time
        );

        Ok(self
            .base
            .create_funding_stream(symbol.to_string(), start_time, end_time))
    }

    fn base_url(&self) -> &str {
        self.base.config.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetcher::binance_parser::BinanceParser;
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::str::FromStr;

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
        let start = 1704067200000;
        let end = 1704070800000;
        let interval_ms = 60_000;

        let total = calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 60);

        let end = 1704153600000;
        let total = calculate_total_bars(start, end, interval_ms);
        assert_eq!(total, 1440);
    }

    #[test]
    fn test_fetcher_initialization() {
        let fetcher = BinanceFuturesUsdtFetcher::new(5);
        assert!(fetcher.base_url().contains("fapi.binance.com"));
    }

    #[test]
    fn test_precision_truncation_rejects_overflow() {
        let symbol_data = json!({
            "symbol": "BTCUSDT",
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "pricePrecision": 300u64,
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
