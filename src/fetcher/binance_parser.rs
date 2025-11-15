//! Binance response parser (T203-T211)
//!
//! This module provides stateless parsing functions for converting Binance API
//! JSON responses into typed data structures. Eliminates duplication between
//! USDT and COIN fetchers by centralizing all parsing logic.

use crate::fetcher::binance_config::SymbolFormat;
use crate::fetcher::{FetcherError, FetcherResult};
use crate::{AggTrade, Bar, FundingRate};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

/// Stateless parser for Binance API responses
pub struct BinanceParser;

impl BinanceParser {
    /// Parse Binance klines JSON array to Bar structs (T204)
    ///
    /// # Arguments
    /// * `klines` - Vector of JSON values representing klines from Binance API
    ///
    /// # Returns
    /// Vector of parsed Bar structs
    ///
    /// # Errors
    /// Returns FetcherError::ParseError if JSON structure is invalid or fields cannot be parsed
    ///
    /// # Format
    /// Binance klines format: `[open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]`
    pub fn parse_klines(klines: Vec<Value>) -> FetcherResult<Vec<Bar>> {
        let mut bars = Vec::with_capacity(klines.len());

        for kline in klines {
            let arr = kline
                .as_array()
                .ok_or_else(|| FetcherError::ParseError("Kline is not an array".to_string()))?;

            if arr.len() != 12 {
                return Err(FetcherError::ParseError(format!(
                    "Expected 12 elements in kline, got {}",
                    arr.len()
                )));
            }

            let open_time = arr[0]
                .as_i64()
                .ok_or_else(|| FetcherError::ParseError("Invalid open_time".to_string()))?;

            let close_time = arr[6]
                .as_i64()
                .ok_or_else(|| FetcherError::ParseError("Invalid close_time".to_string()))?;

            let trades = arr[8]
                .as_u64()
                .ok_or_else(|| FetcherError::ParseError("Invalid trades count".to_string()))?;

            // Parse decimal values from strings
            let open = Self::parse_decimal(&arr[1], "open")?;
            let high = Self::parse_decimal(&arr[2], "high")?;
            let low = Self::parse_decimal(&arr[3], "low")?;
            let close = Self::parse_decimal(&arr[4], "close")?;
            let volume = Self::parse_decimal(&arr[5], "volume")?;
            let quote_volume = Self::parse_decimal(&arr[7], "quote_volume")?;
            let taker_buy_base_volume = Self::parse_decimal(&arr[9], "taker_buy_base_volume")?;
            let taker_buy_quote_volume = Self::parse_decimal(&arr[10], "taker_buy_quote_volume")?;

            bars.push(Bar {
                open_time,
                open,
                high,
                low,
                close,
                volume,
                close_time,
                quote_volume,
                trades,
                taker_buy_base_volume,
                taker_buy_quote_volume,
            });
        }

        Ok(bars)
    }

    /// Parse Binance aggTrades JSON array to AggTrade structs (T206)
    ///
    /// # Arguments
    /// * `trades` - Vector of JSON values representing aggregate trades from Binance API
    ///
    /// # Returns
    /// Vector of parsed AggTrade structs
    ///
    /// # Errors
    /// Returns FetcherError::ParseError if JSON structure is invalid or fields cannot be parsed
    pub fn parse_aggtrades(trades: Vec<Value>) -> FetcherResult<Vec<AggTrade>> {
        let mut aggtrades = Vec::with_capacity(trades.len());

        for trade in trades {
            // Field 'a': Aggregate trade ID
            let agg_trade_id = trade.get("a").and_then(|v| v.as_i64()).ok_or_else(|| {
                FetcherError::ParseError("Invalid or missing agg_trade_id (a)".to_string())
            })?;

            // Field 'p': Price (string)
            let price = Self::parse_decimal(
                trade
                    .get("p")
                    .ok_or_else(|| FetcherError::ParseError("Missing price (p)".to_string()))?,
                "price",
            )?;

            // Field 'q': Quantity (string)
            let quantity = Self::parse_decimal(
                trade
                    .get("q")
                    .ok_or_else(|| FetcherError::ParseError("Missing quantity (q)".to_string()))?,
                "quantity",
            )?;

            // Field 'f': First trade ID
            let first_trade_id = trade.get("f").and_then(|v| v.as_i64()).ok_or_else(|| {
                FetcherError::ParseError("Invalid or missing first_trade_id (f)".to_string())
            })?;

            // Field 'l': Last trade ID
            let last_trade_id = trade.get("l").and_then(|v| v.as_i64()).ok_or_else(|| {
                FetcherError::ParseError("Invalid or missing last_trade_id (l)".to_string())
            })?;

            // Field 'T': Timestamp (integer, milliseconds)
            let timestamp = trade.get("T").and_then(|v| v.as_i64()).ok_or_else(|| {
                FetcherError::ParseError("Invalid or missing timestamp (T)".to_string())
            })?;

            // Field 'm': Is buyer maker (boolean)
            let is_buyer_maker = trade.get("m").and_then(|v| v.as_bool()).ok_or_else(|| {
                FetcherError::ParseError("Invalid or missing is_buyer_maker (m)".to_string())
            })?;

            aggtrades.push(AggTrade {
                agg_trade_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                timestamp,
                is_buyer_maker,
            });
        }

        Ok(aggtrades)
    }

    /// Parse Binance fundingRate JSON array to FundingRate structs (T208)
    ///
    /// # Arguments
    /// * `rates` - Vector of JSON values representing funding rates from Binance API
    ///
    /// # Returns
    /// Vector of parsed FundingRate structs
    ///
    /// # Errors
    /// Returns FetcherError::ParseError if JSON structure is invalid or fields cannot be parsed
    pub fn parse_funding_rates(rates: Vec<Value>) -> FetcherResult<Vec<FundingRate>> {
        let mut funding_rates = Vec::with_capacity(rates.len());

        for rate_data in rates {
            let symbol = rate_data
                .get("symbol")
                .and_then(|v| v.as_str())
                .ok_or_else(|| FetcherError::ParseError("Missing or invalid symbol".to_string()))?
                .to_string();

            let funding_rate = Self::parse_decimal(
                rate_data
                    .get("fundingRate")
                    .ok_or_else(|| FetcherError::ParseError("Missing fundingRate".to_string()))?,
                "fundingRate",
            )?;

            let funding_time = rate_data
                .get("fundingTime")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| {
                    FetcherError::ParseError("Missing or invalid fundingTime".to_string())
                })?;

            funding_rates.push(FundingRate {
                symbol,
                funding_rate,
                funding_time,
            });
        }

        Ok(funding_rates)
    }

    /// Normalize symbol format based on market type (T211)
    ///
    /// # Arguments
    /// * `symbol` - Symbol with "/" delimiter (e.g., "BTC/USDT", "ETH/USD")
    /// * `format` - Symbol format for target market
    ///
    /// # Returns
    /// Normalized symbol string
    ///
    /// # Examples
    /// ```
    /// use trading_data_downloader::fetcher::binance_parser::BinanceParser;
    /// use trading_data_downloader::fetcher::binance_config::SymbolFormat;
    ///
    /// let normalized = BinanceParser::normalize_symbol("BTC/USDT", SymbolFormat::Perpetual);
    /// assert_eq!(normalized, "BTCUSDT");
    ///
    /// let normalized = BinanceParser::normalize_symbol("BTC/USD", SymbolFormat::CoinPerpetual);
    /// assert_eq!(normalized, "BTCUSD_PERP");
    /// ```
    pub fn normalize_symbol(symbol: &str, format: SymbolFormat) -> String {
        // Remove "/" delimiter
        let base_symbol = symbol.replace("/", "");

        // Apply format-specific suffix
        match format {
            SymbolFormat::Perpetual => base_symbol,
            SymbolFormat::CoinPerpetual => format!("{base_symbol}_PERP"),
        }
    }

    /// Helper to parse decimal from JSON value
    fn parse_decimal(value: &Value, field_name: &str) -> FetcherResult<Decimal> {
        let s = value
            .as_str()
            .ok_or_else(|| FetcherError::ParseError(format!("{field_name} is not a string")))?;

        Decimal::from_str(s)
            .map_err(|e| FetcherError::ParseError(format!("Failed to parse {field_name}: {e}")))
    }
}
