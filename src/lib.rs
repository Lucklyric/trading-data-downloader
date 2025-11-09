//! # Trading Data Downloader Library
//!
//! Core library for downloading crypto trading data from supported exchanges.
//!
//! v0.1 supports BINANCE Futures markets (USDT-margined and COIN-margined).

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
        write!(f, "{}", s)
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
            _ => Err(format!("Invalid interval: {}", s)),
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
        write!(f, "{}", s)
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
            _ => Err(format!("Invalid contract type: {}", s)),
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
        write!(f, "{}", s)
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
            _ => Err(format!("Invalid trading status: {}", s)),
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
            return Err(format!("Timestamp must be positive, got {}", self.timestamp));
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
                    "Funding time should be at 8-hour intervals (00:00, 08:00, 16:00 UTC), got hour: {}",
                    hour
                ));
            }
            if dt.minute() != 0 || dt.second() != 0 {
                return Err(format!(
                    "Funding time should be at exact hour boundary, got {:02}:{:02}:{:02}",
                    hour, dt.minute(), dt.second()
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
