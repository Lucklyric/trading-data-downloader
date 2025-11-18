//! # Trading Data Downloader Library
//!
//! A high-performance library for downloading historical cryptocurrency trading data
//! from major exchanges. Designed for quantitative research, backtesting, and data analysis.
//!
//! ## Features
//!
//! - **Multi-Exchange Support**: Currently supports Binance Futures (USDT-margined and COIN-margined)
//! - **Multiple Data Types**: OHLCV bars, aggregate trades, and funding rates
//! - **Resume Capability**: Automatic checkpointing and resume for interrupted downloads
//! - **Rate Limiting**: Built-in rate limiting to respect exchange API limits
//! - **Archive Support**: Download from both live API and historical archives
//! - **Type-Safe**: Strong typing with validation for all data structures
//!
//! ## Quick Start
//!
//! ```no_run
//! use trading_data_downloader::{ExchangeIdentifier, downloader::DownloadJob, Interval};
//! use chrono::Utc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Parse exchange identifier
//! let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT")?;
//!
//! // Create a download job for 1-hour bars
//! let job = DownloadJob::bars(
//!     id,
//!     "BTCUSDT".to_string(),
//!     Interval::OneHour,
//!     1640995200000, // 2022-01-01 00:00:00 UTC
//!     1672531200000, // 2023-01-01 00:00:00 UTC
//!     "./output.csv".into()
//! );
//!
//! // Execute download
//! let executor = trading_data_downloader::downloader::DownloadExecutor::new();
//! executor.execute(job).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! The library is organized into several core modules:
//!
//! - [`identifier`] - Exchange identifier parsing and validation (EXCHANGE:BASE/QUOTE:SETTLE)
//! - [`registry`] - Registry of supported exchanges and their capabilities
//! - [`fetcher`] - Data fetchers for different exchanges and markets
//! - [`downloader`] - Download orchestration with retry and rate limiting
//! - [`output`] - Data output writers (CSV, etc.)
//! - [`resume`] - Resume capability with checkpointing
//!
//! ## Data Types
//!
//! The library defines strongly-typed structures for all trading data:
//!
//! - [`Bar`] - OHLCV candlestick data with volume and trade counts
//! - [`AggTrade`] - Aggregated trade data
//! - [`FundingRate`] - Perpetual futures funding rates
//! - [`Symbol`] - Exchange symbol metadata
//! - [`Interval`] - Time intervals for OHLCV data
//!
//! ## Supported Exchanges
//!
//! v0.1 supports:
//! - Binance Futures USDT-margined (FAPI)
//! - Binance Futures COIN-margined (DAPI)

#![warn(missing_docs)]
#![warn(clippy::all)]

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// CLI command implementations
pub mod cli;

/// Download orchestration
pub mod downloader;

/// Data fetchers
pub mod fetcher;

/// Exchange identifier parsing and validation
pub mod identifier;

/// Data output writers
pub mod output;

/// Identifier registry with exchange metadata
pub mod registry;

/// Resume capability for download jobs
pub mod resume;

/// Graceful shutdown coordination shared across modules
pub mod shutdown;

// Re-export commonly used types
pub use identifier::ExchangeIdentifier;

/// OHLCV bar data structure (T042)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bar {
    /// Open time (Unix timestamp in milliseconds)
    pub open_time: i64,
    /// Open price
    pub open: Decimal,
    /// High price
    pub high: Decimal,
    /// Low price
    pub low: Decimal,
    /// Close price
    pub close: Decimal,
    /// Volume (base asset)
    pub volume: Decimal,
    /// Close time (Unix timestamp in milliseconds)
    pub close_time: i64,
    /// Quote asset volume
    pub quote_volume: Decimal,
    /// Number of trades
    pub trades: u64,
    /// Taker buy base asset volume
    pub taker_buy_base_volume: Decimal,
    /// Taker buy quote asset volume
    pub taker_buy_quote_volume: Decimal,
}

impl Bar {
    /// Validate bar data integrity
    pub fn validate(&self) -> Result<(), String> {
        if self.close_time <= self.open_time {
            return Err(format!(
                "Close time ({}) must be after open time ({})",
                self.close_time, self.open_time
            ));
        }

        if self.high < self.open || self.high < self.close {
            return Err(format!(
                "High ({}) must be >= open ({}) and close ({})",
                self.high, self.open, self.close
            ));
        }

        if self.low > self.open || self.low > self.close {
            return Err(format!(
                "Low ({}) must be <= open ({}) and close ({})",
                self.low, self.open, self.close
            ));
        }

        if self.volume < Decimal::ZERO {
            return Err(format!("Volume must be non-negative, got {}", self.volume));
        }

        Ok(())
    }
}

/// Time interval for OHLCV bars (T044)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    /// 1 minute
    #[serde(rename = "1m")]
    OneMinute,
    /// 3 minutes
    #[serde(rename = "3m")]
    ThreeMinutes,
    /// 5 minutes
    #[serde(rename = "5m")]
    FiveMinutes,
    /// 15 minutes
    #[serde(rename = "15m")]
    FifteenMinutes,
    /// 30 minutes
    #[serde(rename = "30m")]
    ThirtyMinutes,
    /// 1 hour
    #[serde(rename = "1h")]
    OneHour,
    /// 2 hours
    #[serde(rename = "2h")]
    TwoHours,
    /// 4 hours
    #[serde(rename = "4h")]
    FourHours,
    /// 6 hours
    #[serde(rename = "6h")]
    SixHours,
    /// 8 hours
    #[serde(rename = "8h")]
    EightHours,
    /// 12 hours
    #[serde(rename = "12h")]
    TwelveHours,
    /// 1 day
    #[serde(rename = "1d")]
    OneDay,
    /// 3 days
    #[serde(rename = "3d")]
    ThreeDays,
    /// 1 week
    #[serde(rename = "1w")]
    OneWeek,
    /// 1 month
    #[serde(rename = "1M")]
    OneMonth,
}

impl Interval {
    /// Convert interval to milliseconds
    pub fn to_milliseconds(&self) -> i64 {
        match self {
            Interval::OneMinute => 60_000,
            Interval::ThreeMinutes => 180_000,
            Interval::FiveMinutes => 300_000,
            Interval::FifteenMinutes => 900_000,
            Interval::ThirtyMinutes => 1_800_000,
            Interval::OneHour => 3_600_000,
            Interval::TwoHours => 7_200_000,
            Interval::FourHours => 14_400_000,
            Interval::SixHours => 21_600_000,
            Interval::EightHours => 28_800_000,
            Interval::TwelveHours => 43_200_000,
            Interval::OneDay => 86_400_000,
            Interval::ThreeDays => 259_200_000,
            Interval::OneWeek => 604_800_000,
            Interval::OneMonth => 2_592_000_000, // Approximate: 30 days
        }
    }
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Interval::OneMinute => "1m",
            Interval::ThreeMinutes => "3m",
            Interval::FiveMinutes => "5m",
            Interval::FifteenMinutes => "15m",
            Interval::ThirtyMinutes => "30m",
            Interval::OneHour => "1h",
            Interval::TwoHours => "2h",
            Interval::FourHours => "4h",
            Interval::SixHours => "6h",
            Interval::EightHours => "8h",
            Interval::TwelveHours => "12h",
            Interval::OneDay => "1d",
            Interval::ThreeDays => "3d",
            Interval::OneWeek => "1w",
            Interval::OneMonth => "1M",
        };
        write!(f, "{s}")
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1m" => Ok(Interval::OneMinute),
            "3m" => Ok(Interval::ThreeMinutes),
            "5m" => Ok(Interval::FiveMinutes),
            "15m" => Ok(Interval::FifteenMinutes),
            "30m" => Ok(Interval::ThirtyMinutes),
            "1h" => Ok(Interval::OneHour),
            "2h" => Ok(Interval::TwoHours),
            "4h" => Ok(Interval::FourHours),
            "6h" => Ok(Interval::SixHours),
            "8h" => Ok(Interval::EightHours),
            "12h" => Ok(Interval::TwelveHours),
            "1d" => Ok(Interval::OneDay),
            "3d" => Ok(Interval::ThreeDays),
            "1w" => Ok(Interval::OneWeek),
            "1M" => Ok(Interval::OneMonth),
            _ => Err(format!("Invalid interval: {s}")),
        }
    }
}

/// Contract type for futures contracts (T094)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContractType {
    /// Perpetual contract (no expiry)
    #[serde(rename = "PERPETUAL")]
    Perpetual,
    /// Current quarter delivery contract
    #[serde(rename = "CURRENT_QUARTER")]
    CurrentQuarter,
    /// Next quarter delivery contract
    #[serde(rename = "NEXT_QUARTER")]
    NextQuarter,
    /// Current quarter delivering
    #[serde(rename = "CURRENT_QUARTER_DELIVERING")]
    CurrentQuarterDelivering,
    /// Next quarter delivering
    #[serde(rename = "NEXT_QUARTER_DELIVERING")]
    NextQuarterDelivering,
}

impl std::fmt::Display for ContractType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ContractType::Perpetual => "PERPETUAL",
            ContractType::CurrentQuarter => "CURRENT_QUARTER",
            ContractType::NextQuarter => "NEXT_QUARTER",
            ContractType::CurrentQuarterDelivering => "CURRENT_QUARTER_DELIVERING",
            ContractType::NextQuarterDelivering => "NEXT_QUARTER_DELIVERING",
        };
        write!(f, "{s}")
    }
}

impl FromStr for ContractType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PERPETUAL" => Ok(ContractType::Perpetual),
            "CURRENT_QUARTER" => Ok(ContractType::CurrentQuarter),
            "NEXT_QUARTER" => Ok(ContractType::NextQuarter),
            "CURRENT_QUARTER_DELIVERING" => Ok(ContractType::CurrentQuarterDelivering),
            "NEXT_QUARTER_DELIVERING" => Ok(ContractType::NextQuarterDelivering),
            _ => Err(format!("Invalid contract type: {s}")),
        }
    }
}

/// Trading status for a symbol (T094)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingStatus {
    /// Symbol is actively trading
    #[serde(rename = "TRADING")]
    Trading,
    /// Pre-trading phase
    #[serde(rename = "PRE_TRADING")]
    PreTrading,
    /// Contract is delivering
    #[serde(rename = "DELIVERING")]
    Delivering,
    /// Contract has been delivered
    #[serde(rename = "DELIVERED")]
    Delivered,
    /// Trading is paused/halted
    #[serde(rename = "BREAK")]
    Break,
}

impl std::fmt::Display for TradingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TradingStatus::Trading => "TRADING",
            TradingStatus::PreTrading => "PRE_TRADING",
            TradingStatus::Delivering => "DELIVERING",
            TradingStatus::Delivered => "DELIVERED",
            TradingStatus::Break => "BREAK",
        };
        write!(f, "{s}")
    }
}

impl FromStr for TradingStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TRADING" => Ok(TradingStatus::Trading),
            "PRE_TRADING" => Ok(TradingStatus::PreTrading),
            "DELIVERING" => Ok(TradingStatus::Delivering),
            "DELIVERED" => Ok(TradingStatus::Delivered),
            "BREAK" => Ok(TradingStatus::Break),
            _ => Err(format!("Invalid trading status: {s}")),
        }
    }
}

/// Symbol metadata from exchange (T093)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Symbol {
    /// Trading symbol name (e.g., "BTCUSDT", "ETHUSD_PERP")
    pub symbol: String,
    /// Underlying pair (e.g., "BTCUSDT", "ETHUSD")
    pub pair: String,
    /// Contract type
    pub contract_type: ContractType,
    /// Trading status
    pub status: TradingStatus,
    /// Base asset (e.g., "BTC", "ETH")
    pub base_asset: String,
    /// Quote asset (e.g., "USDT", "USD")
    pub quote_asset: String,
    /// Margin asset (USDT for USDT-margined, base asset for coin-margined)
    pub margin_asset: String,
    /// Price precision (decimal places)
    pub price_precision: u8,
    /// Quantity precision (decimal places)
    pub quantity_precision: u8,
    /// Price tick size (minimum price increment)
    pub tick_size: Decimal,
    /// Quantity step size (minimum quantity increment)
    pub step_size: Decimal,
}

impl Symbol {
    /// Validate symbol data integrity
    pub fn validate(&self) -> Result<(), String> {
        if self.symbol.is_empty() {
            return Err("Symbol name cannot be empty".to_string());
        }

        if self.pair.is_empty() {
            return Err("Pair cannot be empty".to_string());
        }

        if self.base_asset.is_empty() {
            return Err("Base asset cannot be empty".to_string());
        }

        if self.quote_asset.is_empty() {
            return Err("Quote asset cannot be empty".to_string());
        }

        if self.margin_asset.is_empty() {
            return Err("Margin asset cannot be empty".to_string());
        }

        if self.tick_size <= Decimal::ZERO {
            return Err(format!(
                "Tick size must be positive, got {}",
                self.tick_size
            ));
        }

        if self.step_size <= Decimal::ZERO {
            return Err(format!(
                "Step size must be positive, got {}",
                self.step_size
            ));
        }

        Ok(())
    }
}

/// Aggregate trade data structure (T112)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggTrade {
    /// Aggregate trade ID
    pub agg_trade_id: i64,
    /// Price
    pub price: Decimal,
    /// Quantity
    pub quantity: Decimal,
    /// First trade ID
    pub first_trade_id: i64,
    /// Last trade ID
    pub last_trade_id: i64,
    /// Timestamp (Unix timestamp in milliseconds)
    pub timestamp: i64,
    /// Was the buyer the maker? (true = seller initiated, false = buyer initiated)
    pub is_buyer_maker: bool,
}

impl AggTrade {
    /// Validate aggregate trade data integrity
    pub fn validate(&self) -> Result<(), String> {
        if self.price <= Decimal::ZERO {
            return Err(format!("Price must be positive, got {}", self.price));
        }

        if self.quantity <= Decimal::ZERO {
            return Err(format!("Quantity must be positive, got {}", self.quantity));
        }

        if self.timestamp <= 0 {
            return Err(format!(
                "Timestamp must be positive, got {}",
                self.timestamp
            ));
        }

        if self.last_trade_id < self.first_trade_id {
            return Err(format!(
                "Last trade ID ({}) must be >= first trade ID ({})",
                self.last_trade_id, self.first_trade_id
            ));
        }

        Ok(())
    }
}

/// Funding rate data structure (T136)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FundingRate {
    /// Trading symbol name (e.g., "BTCUSDT")
    pub symbol: String,
    /// Funding rate value (as decimal)
    pub funding_rate: Decimal,
    /// Funding time (Unix timestamp in milliseconds)
    pub funding_time: i64,
}

impl FundingRate {
    /// Validate funding rate data integrity
    pub fn validate(&self) -> Result<(), String> {
        if self.symbol.is_empty() {
            return Err("Symbol name cannot be empty".to_string());
        }

        if self.funding_time <= 0 {
            return Err(format!(
                "Funding time must be positive, got {}",
                self.funding_time
            ));
        }

        // Validate funding time alignment (should be at 00:00, 08:00, or 16:00 UTC)
        use chrono::Timelike;
        if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(self.funding_time) {
            let hour = dt.hour();
            if hour % 8 != 0 {
                return Err(format!(
                    "Funding time should be at 8-hour intervals (00:00, 08:00, 16:00 UTC), got hour: {hour}"
                ));
            }
            if dt.minute() != 0 || dt.second() != 0 {
                return Err(format!(
                    "Funding time should be at exact hour boundary, got {:02}:{:02}:{:02}",
                    hour,
                    dt.minute(),
                    dt.second()
                ));
            }
        } else {
            return Err(format!(
                "Invalid funding time timestamp: {}",
                self.funding_time
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_from_str() {
        assert_eq!(Interval::from_str("1m").unwrap(), Interval::OneMinute);
        assert_eq!(Interval::from_str("3m").unwrap(), Interval::ThreeMinutes);
        assert_eq!(Interval::from_str("5m").unwrap(), Interval::FiveMinutes);
        assert_eq!(Interval::from_str("15m").unwrap(), Interval::FifteenMinutes);
        assert_eq!(Interval::from_str("30m").unwrap(), Interval::ThirtyMinutes);
        assert_eq!(Interval::from_str("1h").unwrap(), Interval::OneHour);
        assert_eq!(Interval::from_str("2h").unwrap(), Interval::TwoHours);
        assert_eq!(Interval::from_str("4h").unwrap(), Interval::FourHours);
        assert_eq!(Interval::from_str("6h").unwrap(), Interval::SixHours);
        assert_eq!(Interval::from_str("8h").unwrap(), Interval::EightHours);
        assert_eq!(Interval::from_str("12h").unwrap(), Interval::TwelveHours);
        assert_eq!(Interval::from_str("1d").unwrap(), Interval::OneDay);
        assert_eq!(Interval::from_str("3d").unwrap(), Interval::ThreeDays);
        assert_eq!(Interval::from_str("1w").unwrap(), Interval::OneWeek);
        assert_eq!(Interval::from_str("1M").unwrap(), Interval::OneMonth);
    }

    #[test]
    fn test_interval_from_str_invalid() {
        assert!(Interval::from_str("2m").is_err());
        assert!(Interval::from_str("10h").is_err());
        assert!(Interval::from_str("invalid").is_err());
        assert!(Interval::from_str("").is_err());
    }

    #[test]
    fn test_interval_to_string() {
        assert_eq!(Interval::OneMinute.to_string(), "1m");
        assert_eq!(Interval::ThreeMinutes.to_string(), "3m");
        assert_eq!(Interval::FiveMinutes.to_string(), "5m");
        assert_eq!(Interval::FifteenMinutes.to_string(), "15m");
        assert_eq!(Interval::ThirtyMinutes.to_string(), "30m");
        assert_eq!(Interval::OneHour.to_string(), "1h");
        assert_eq!(Interval::TwoHours.to_string(), "2h");
        assert_eq!(Interval::FourHours.to_string(), "4h");
        assert_eq!(Interval::SixHours.to_string(), "6h");
        assert_eq!(Interval::EightHours.to_string(), "8h");
        assert_eq!(Interval::TwelveHours.to_string(), "12h");
        assert_eq!(Interval::OneDay.to_string(), "1d");
        assert_eq!(Interval::ThreeDays.to_string(), "3d");
        assert_eq!(Interval::OneWeek.to_string(), "1w");
        assert_eq!(Interval::OneMonth.to_string(), "1M");
    }

    #[test]
    fn test_interval_to_milliseconds() {
        assert_eq!(Interval::OneMinute.to_milliseconds(), 60_000);
        assert_eq!(Interval::ThreeMinutes.to_milliseconds(), 180_000);
        assert_eq!(Interval::FiveMinutes.to_milliseconds(), 300_000);
        assert_eq!(Interval::FifteenMinutes.to_milliseconds(), 900_000);
        assert_eq!(Interval::ThirtyMinutes.to_milliseconds(), 1_800_000);
        assert_eq!(Interval::OneHour.to_milliseconds(), 3_600_000);
        assert_eq!(Interval::TwoHours.to_milliseconds(), 7_200_000);
        assert_eq!(Interval::FourHours.to_milliseconds(), 14_400_000);
        assert_eq!(Interval::SixHours.to_milliseconds(), 21_600_000);
        assert_eq!(Interval::EightHours.to_milliseconds(), 28_800_000);
        assert_eq!(Interval::TwelveHours.to_milliseconds(), 43_200_000);
        assert_eq!(Interval::OneDay.to_milliseconds(), 86_400_000);
        assert_eq!(Interval::ThreeDays.to_milliseconds(), 259_200_000);
        assert_eq!(Interval::OneWeek.to_milliseconds(), 604_800_000);
        assert_eq!(Interval::OneMonth.to_milliseconds(), 2_592_000_000);
    }

    #[test]
    fn test_interval_round_trip() {
        let intervals = vec![
            Interval::OneMinute,
            Interval::ThreeMinutes,
            Interval::FiveMinutes,
            Interval::FifteenMinutes,
            Interval::ThirtyMinutes,
            Interval::OneHour,
            Interval::TwoHours,
            Interval::FourHours,
            Interval::SixHours,
            Interval::EightHours,
            Interval::TwelveHours,
            Interval::OneDay,
            Interval::ThreeDays,
            Interval::OneWeek,
            Interval::OneMonth,
        ];

        for interval in intervals {
            let string = interval.to_string();
            let parsed = Interval::from_str(&string).unwrap();
            assert_eq!(parsed, interval);
        }
    }

    #[test]
    fn test_bar_validate() {
        let mut bar = Bar {
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
        };

        assert!(bar.validate().is_ok());

        // Test invalid close_time
        let original_close_time = bar.close_time;
        bar.close_time = bar.open_time - 1;
        assert!(bar.validate().is_err());
        bar.close_time = original_close_time;

        // Test invalid high
        bar.high = Decimal::from_str("34900.00").unwrap();
        assert!(bar.validate().is_err());
        bar.high = Decimal::from_str("35100.00").unwrap();

        // Test invalid low
        bar.low = Decimal::from_str("35200.00").unwrap();
        assert!(bar.validate().is_err());
        bar.low = Decimal::from_str("34950.00").unwrap();

        // Test negative volume
        bar.volume = Decimal::from_str("-100.0").unwrap();
        assert!(bar.validate().is_err());
    }

    #[test]
    fn test_aggtrade_validate() {
        let mut trade = AggTrade {
            agg_trade_id: 12345,
            price: Decimal::from_str("35000.50").unwrap(),
            quantity: Decimal::from_str("1.5").unwrap(),
            first_trade_id: 100,
            last_trade_id: 105,
            timestamp: 1699920000000,
            is_buyer_maker: false,
        };

        assert!(trade.validate().is_ok());

        // Test invalid first_trade_id > last_trade_id
        trade.first_trade_id = 110;
        assert!(trade.validate().is_err());
        trade.first_trade_id = 100;

        // Test negative price
        trade.price = Decimal::from_str("-100.0").unwrap();
        assert!(trade.validate().is_err());
        trade.price = Decimal::from_str("35000.50").unwrap();

        // Test zero quantity
        trade.quantity = Decimal::ZERO;
        assert!(trade.validate().is_err());
        trade.quantity = Decimal::from_str("1.5").unwrap();

        // Test negative timestamp
        trade.timestamp = -1;
        assert!(trade.validate().is_err());
    }

    #[test]
    fn test_funding_rate_validate() {
        let mut rate = FundingRate {
            symbol: "BTCUSDT".to_string(),
            funding_rate: Decimal::from_str("0.0001").unwrap(),
            funding_time: 1577836800000, // 2020-01-01 00:00:00 UTC
        };

        assert!(rate.validate().is_ok());

        // Test empty symbol
        rate.symbol = String::new();
        assert!(rate.validate().is_err());
        rate.symbol = "BTCUSDT".to_string();

        // Test negative funding_time
        rate.funding_time = -1;
        assert!(rate.validate().is_err());
        rate.funding_time = 1577836800000;

        // Test funding_time not at 8-hour boundary
        rate.funding_time = 1577840400000; // 01:00:00 UTC (not at 8-hour boundary)
        assert!(rate.validate().is_err());
    }
}
