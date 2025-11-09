//! Exchange identifier parsing and validation
//!
//! Implements the standardized identifier format: EXCHANGE:BASE/QUOTE:SETTLE

use std::fmt;

/// Exchange identifier using format EXCHANGE:BASE/QUOTE:SETTLE
///
/// All components are normalized to uppercase for consistency.
///
/// # Examples
///
/// ```
/// use trading_data_downloader::identifier::ExchangeIdentifier;
///
/// let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
/// assert_eq!(id.exchange(), "BINANCE");
/// assert_eq!(id.base(), "BTC");
/// assert_eq!(id.quote(), "USDT");
/// assert_eq!(id.settle(), "USDT");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExchangeIdentifier {
    exchange: String,
    base: String,
    quote: String,
    settle: String,
}

impl ExchangeIdentifier {
    /// Parse an identifier string into an ExchangeIdentifier
    ///
    /// Input is case-insensitive and will be normalized to uppercase.
    ///
    /// # Format
    ///
    /// `EXCHANGE:BASE/QUOTE:SETTLE`
    ///
    /// # Errors
    ///
    /// Returns an error if the format is invalid or any component is empty.
    pub fn parse(s: &str) -> Result<Self, IdentifierError> {
        // Split on first colon to get exchange
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return Err(IdentifierError::InvalidFormat(
                "invalid identifier format: expected EXCHANGE:BASE/QUOTE:SETTLE".to_string(),
            ));
        }

        let exchange = parts[0].trim().to_uppercase();
        if exchange.is_empty() {
            return Err(IdentifierError::InvalidFormat(
                "exchange component cannot be empty".to_string(),
            ));
        }

        // Split middle part on slash to get base/quote
        let symbol_parts: Vec<&str> = parts[1].split('/').collect();
        if symbol_parts.len() != 2 {
            return Err(IdentifierError::InvalidFormat(
                "invalid symbol format: expected BASE/QUOTE".to_string(),
            ));
        }

        let base = symbol_parts[0].trim().to_uppercase();
        let quote = symbol_parts[1].trim().to_uppercase();

        if base.is_empty() {
            return Err(IdentifierError::InvalidFormat(
                "base component cannot be empty".to_string(),
            ));
        }
        if quote.is_empty() {
            return Err(IdentifierError::InvalidFormat(
                "quote component cannot be empty".to_string(),
            ));
        }

        let settle = parts[2].trim().to_uppercase();
        if settle.is_empty() {
            return Err(IdentifierError::InvalidFormat(
                "settle component cannot be empty".to_string(),
            ));
        }

        Ok(Self { exchange, base, quote, settle })
    }

    /// Get the exchange component (uppercase)
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Get the base asset (uppercase)
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Get the quote asset (uppercase)
    pub fn quote(&self) -> &str {
        &self.quote
    }

    /// Get the settlement asset (uppercase)
    pub fn settle(&self) -> &str {
        &self.settle
    }

    /// Convert identifier to filesystem-safe format
    ///
    /// Returns lowercase with underscores instead of special characters.
    ///
    /// # Examples
    ///
    /// ```
    /// use trading_data_downloader::identifier::ExchangeIdentifier;
    ///
    /// let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    /// assert_eq!(id.to_filesystem_safe(), "binance_btc_usdt_usdt");
    /// ```
    pub fn to_filesystem_safe(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.exchange.to_lowercase(),
            self.base.to_lowercase(),
            self.quote.to_lowercase(),
            self.settle.to_lowercase()
        )
    }
}

impl fmt::Display for ExchangeIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}/{}:{}", self.exchange, self.base, self.quote, self.settle)
    }
}

/// Errors that can occur during identifier parsing
#[derive(Debug, thiserror::Error)]
pub enum IdentifierError {
    /// Invalid identifier format
    #[error("identifier error: {0}")]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_and_accessors() {
        let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
        assert_eq!(id.exchange(), "BINANCE");
        assert_eq!(id.base(), "BTC");
        assert_eq!(id.quote(), "USDT");
        assert_eq!(id.settle(), "USDT");
    }

    #[test]
    fn test_normalization() {
        let id = ExchangeIdentifier::parse("binance:btc/usdt:usdt").unwrap();
        assert_eq!(id.to_string(), "BINANCE:BTC/USDT:USDT");
    }

    #[test]
    fn test_filesystem_safe() {
        let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
        assert_eq!(id.to_filesystem_safe(), "binance_btc_usdt_usdt");
    }
}
