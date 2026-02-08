//! Binance Futures COIN-margined data fetcher (T157-T162)
//!
//! Thin wrapper around [`BinanceFuturesBase`] with COIN-margined configuration.

use crate::{Interval, Symbol};
use async_trait::async_trait;
use tracing::{debug, info};

use super::binance_config::COIN_FUTURES_CONFIG;
use super::binance_futures_base::{calculate_total_bars, BinanceFuturesBase, ONE_HOUR_MS};
use super::{AggTradeStream, BarStream, DataFetcher, FetcherResult, FundingStream};

/// Binance Futures COIN-margined data fetcher (T157)
pub struct BinanceFuturesCoinFetcher {
    base: BinanceFuturesBase,
}

impl BinanceFuturesCoinFetcher {
    /// Create a new Binance Futures COIN-margined fetcher
    pub fn new(max_retries: u32) -> Self {
        Self {
            base: BinanceFuturesBase::new(&COIN_FUTURES_CONFIG, max_retries),
        }
    }

    /// Create with custom base URL (for testing)
    #[allow(dead_code)]
    pub fn new_with_base_url(base_url: String, max_retries: u32) -> Self {
        Self {
            base: BinanceFuturesBase::new_with_base_url(
                &COIN_FUTURES_CONFIG,
                base_url,
                max_retries,
            ),
        }
    }
}

impl Clone for BinanceFuturesCoinFetcher {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone_with_config(),
        }
    }
}

impl Default for BinanceFuturesCoinFetcher {
    fn default() -> Self {
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
        let total_bars = calculate_total_bars(start_time, end_time, interval_ms);
        debug!("Expected approximately {} bars", total_bars);

        Ok(self
            .base
            .create_bar_stream(symbol.to_string(), interval, start_time, end_time))
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

    #[test]
    fn test_fetcher_initialization() {
        let fetcher = BinanceFuturesCoinFetcher::new(5);
        assert!(fetcher.base_url().contains("dapi.binance.com"));
    }
}
