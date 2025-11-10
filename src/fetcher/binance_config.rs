//! Binance market configuration (T225-T228)
//!
//! This module provides configuration structs for Binance market types,
//! making USDT vs COIN differences purely configuration rather than code duplication.
//!
//! # Market Types
//!
//! - **USDT-margined futures (FAPI)**: Uses <https://fapi.binance.com> with /fapi/v1/* endpoints
//! - **COIN-margined futures (DAPI)**: Uses <https://dapi.binance.com> with /dapi/v1/* endpoints

/// T228: Symbol format for market type
///
/// Different Binance market types use different symbol naming conventions:
/// - Perpetual: USDT-margined perpetuals (e.g., BTCUSDT)
/// - CoinPerpetual: COIN-margined perpetuals with _PERP suffix (e.g., BTCUSD_PERP)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolFormat {
    /// USDT-margined perpetuals (e.g., BTCUSDT)
    Perpetual,
    /// COIN-margined perpetuals with _PERP suffix (e.g., BTCUSD_PERP)
    CoinPerpetual,
}

/// T225: Configuration for a Binance market type
///
/// This struct encapsulates all market-specific constants that differ between
/// USDT-margined (FAPI) and COIN-margined (DAPI) futures markets.
///
/// # Examples
///
/// ```
/// use trading_data_downloader::fetcher::binance_config::USDT_FUTURES_CONFIG;
///
/// let klines_url = USDT_FUTURES_CONFIG.klines_url();
/// assert_eq!(klines_url, "https://fapi.binance.com/fapi/v1/klines");
/// ```
#[derive(Debug, Clone)]
pub struct BinanceMarketConfig {
    /// Base URL for API (e.g., <https://fapi.binance.com>)
    pub base_url: &'static str,

    /// Klines endpoint path (e.g., /fapi/v1/klines)
    pub klines_endpoint: &'static str,

    /// Aggregate trades endpoint path (e.g., /fapi/v1/aggTrades)
    pub aggtrades_endpoint: &'static str,

    /// Funding rate endpoint path (e.g., /fapi/v1/fundingRate)
    pub funding_endpoint: &'static str,

    /// Exchange info endpoint path (e.g., /fapi/v1/exchangeInfo)
    pub exchange_info_endpoint: &'static str,

    /// Symbol naming format for this market
    pub symbol_format: SymbolFormat,
}

/// T226: USDT-margined futures configuration (FAPI)
///
/// Configuration for Binance USDT-margined perpetual futures:
/// - Base URL: <https://fapi.binance.com>
/// - Endpoints: /fapi/v1/*
/// - Symbol format: Perpetual (e.g., BTCUSDT)
pub const USDT_FUTURES_CONFIG: BinanceMarketConfig = BinanceMarketConfig {
    base_url: "https://fapi.binance.com",
    klines_endpoint: "/fapi/v1/klines",
    aggtrades_endpoint: "/fapi/v1/aggTrades",
    funding_endpoint: "/fapi/v1/fundingRate",
    exchange_info_endpoint: "/fapi/v1/exchangeInfo",
    symbol_format: SymbolFormat::Perpetual,
};

/// T227: COIN-margined futures configuration (DAPI)
///
/// Configuration for Binance COIN-margined perpetual futures:
/// - Base URL: <https://dapi.binance.com>
/// - Endpoints: /dapi/v1/*
/// - Symbol format: CoinPerpetual (e.g., BTCUSD_PERP)
pub const COIN_FUTURES_CONFIG: BinanceMarketConfig = BinanceMarketConfig {
    base_url: "https://dapi.binance.com",
    klines_endpoint: "/dapi/v1/klines",
    aggtrades_endpoint: "/dapi/v1/aggTrades",
    funding_endpoint: "/dapi/v1/fundingRate",
    exchange_info_endpoint: "/dapi/v1/exchangeInfo",
    symbol_format: SymbolFormat::CoinPerpetual,
};

impl BinanceMarketConfig {
    /// Get full URL for klines endpoint
    ///
    /// # Returns
    /// Complete URL by combining base_url + klines_endpoint
    ///
    /// # Examples
    ///
    /// ```
    /// use trading_data_downloader::fetcher::binance_config::USDT_FUTURES_CONFIG;
    ///
    /// let url = USDT_FUTURES_CONFIG.klines_url();
    /// assert_eq!(url, "https://fapi.binance.com/fapi/v1/klines");
    /// ```
    pub fn klines_url(&self) -> String {
        format!("{}{}", self.base_url, self.klines_endpoint)
    }

    /// Get full URL for aggTrades endpoint
    ///
    /// # Returns
    /// Complete URL by combining base_url + aggtrades_endpoint
    pub fn aggtrades_url(&self) -> String {
        format!("{}{}", self.base_url, self.aggtrades_endpoint)
    }

    /// Get full URL for funding rate endpoint
    ///
    /// # Returns
    /// Complete URL by combining base_url + funding_endpoint
    pub fn funding_url(&self) -> String {
        format!("{}{}", self.base_url, self.funding_endpoint)
    }

    /// Get full URL for exchange info endpoint
    ///
    /// # Returns
    /// Complete URL by combining base_url + exchange_info_endpoint
    pub fn exchange_info_url(&self) -> String {
        format!("{}{}", self.base_url, self.exchange_info_endpoint)
    }
}
