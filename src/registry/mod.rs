//! Identifier registry for supported exchanges and trading pairs
//!
//! The registry contains metadata about supported identifiers including
//! capabilities (data types, intervals) and validation rules.

use crate::identifier::ExchangeIdentifier;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Embedded registry data
const REGISTRY_JSON: &str = include_str!("identifiers.json");

/// Global registry instance (loaded once)
static REGISTRY: Lazy<Result<IdentifierRegistry, RegistryError>> =
    Lazy::new(|| IdentifierRegistry::from_json(REGISTRY_JSON));

/// Registry of supported exchange identifiers
#[derive(Debug, Clone)]
pub struct IdentifierRegistry {
    #[allow(dead_code)]
    schema_version: String,
    #[allow(dead_code)]
    last_updated: String,
    entries_map: HashMap<String, RegistryEntry>,
}

impl IdentifierRegistry {
    /// Load the embedded registry
    ///
    /// This is a singleton operation - the registry is loaded once and cached.
    pub fn load() -> Result<&'static Self, &'static RegistryError> {
        REGISTRY.as_ref()
    }

    /// Load embedded registry, returning an owned copy
    pub fn load_embedded() -> Result<Self, RegistryError> {
        Self::from_json(REGISTRY_JSON)
    }

    /// Parse registry from JSON string
    fn from_json(json: &str) -> Result<Self, RegistryError> {
        let raw: RawRegistry = serde_json::from_str(json)
            .map_err(|e| RegistryError::ParseError(format!("Failed to parse registry: {e}")))?;

        let mut entries_map = HashMap::new();
        for entry in raw.identifiers {
            entries_map.insert(entry.canonical.clone(), entry);
        }

        Ok(Self {
            schema_version: raw.schema_version,
            last_updated: raw.last_updated,
            entries_map,
        })
    }

    /// Get all registry entries
    pub fn entries(&self) -> Vec<&RegistryEntry> {
        self.entries_map.values().collect()
    }

    /// Get a specific entry by identifier
    pub fn get_entry(&self, id: &ExchangeIdentifier) -> Option<&RegistryEntry> {
        let canonical = id.to_string();
        self.entries_map.get(&canonical)
    }

    /// Validate an identifier against the registry
    ///
    /// Returns an error if the identifier is not in the registry.
    pub fn validate_identifier(&self, id: &ExchangeIdentifier) -> Result<(), RegistryError> {
        if self.get_entry(id).is_none() {
            return Err(RegistryError::NotFound(format!(
                "Identifier {id} not found in registry"
            )));
        }
        Ok(())
    }

    /// List all registered identifiers (T105)
    pub fn list_all(&self) -> Vec<ExchangeIdentifier> {
        self.entries_map
            .keys()
            .filter_map(|k| ExchangeIdentifier::parse(k).ok())
            .collect()
    }

    /// Resolve wildcard pattern to matching identifiers (T105)
    ///
    /// # Pattern Syntax
    /// - `*` matches any sequence of characters
    /// - `BINANCE:*/USDT:USDT` matches all USDT-margined pairs
    /// - `BINANCE:BTC/*:*` matches all BTC pairs
    ///
    /// # Examples
    /// ```ignore
    /// let registry = IdentifierRegistry::load_embedded()?;
    /// let matches = registry.resolve_pattern("BINANCE:*/USDT:USDT")?;
    /// ```
    pub fn resolve_pattern(&self, pattern: &str) -> Result<Vec<ExchangeIdentifier>, RegistryError> {
        // Parse pattern - if no wildcards, try exact match
        if !pattern.contains('*') {
            let id = ExchangeIdentifier::parse(pattern)
                .map_err(|e| RegistryError::ParseError(e.to_string()))?;
            if self.get_entry(&id).is_some() {
                return Ok(vec![id]);
            } else {
                return Err(RegistryError::NotFound(format!(
                    "Pattern {} does not match any identifier",
                    pattern
                )));
            }
        }

        // Wildcard matching
        let pattern_parts: Vec<&str> = pattern.split(':').collect();
        if pattern_parts.len() != 3 {
            return Err(RegistryError::ParseError(format!(
                "Invalid pattern format: {}. Expected EXCHANGE:BASE/QUOTE:SETTLE",
                pattern
            )));
        }

        let exchange_pattern = pattern_parts[0];
        let pair_pattern = pattern_parts[1];
        let settle_pattern = pattern_parts[2];

        let pair_parts: Vec<&str> = pair_pattern.split('/').collect();
        if pair_parts.len() != 2 {
            return Err(RegistryError::ParseError(format!(
                "Invalid pair pattern: {}. Expected BASE/QUOTE",
                pair_pattern
            )));
        }

        let base_pattern = pair_parts[0];
        let quote_pattern = pair_parts[1];

        let mut matches = Vec::new();

        for entry in self.entries() {
            let exchange_match = matches_pattern(exchange_pattern, &entry.exchange);
            let base_match = matches_pattern(base_pattern, &entry.base);
            let quote_match = matches_pattern(quote_pattern, &entry.quote);
            let settle_match = matches_pattern(settle_pattern, &entry.settle);

            if exchange_match && base_match && quote_match && settle_match {
                if let Ok(id) = ExchangeIdentifier::parse(&entry.canonical) {
                    matches.push(id);
                }
            }
        }

        if matches.is_empty() {
            return Err(RegistryError::NotFound(format!(
                "Pattern {} does not match any identifier",
                pattern
            )));
        }

        Ok(matches)
    }
}

/// Helper function to match a pattern with wildcards
fn matches_pattern(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return pattern == value;
    }

    // Simple wildcard matching
    let parts: Vec<&str> = pattern.split('*').collect();

    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First part must match start
            if !value.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Last part must match end
            if !value.ends_with(part) {
                return false;
            }
        } else {
            // Middle parts must be found
            if let Some(found_pos) = value[pos..].find(part) {
                pos += found_pos + part.len();
            } else {
                return false;
            }
        }
    }

    true
}

/// A single entry in the identifier registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    canonical: String,
    exchange: String,
    base: String,
    quote: String,
    settle: String,
    description: String,
    capabilities: FetcherCapabilities,
    validation_rules: ValidationRules,
}

impl RegistryEntry {
    /// Get the canonical identifier string
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    /// Get the exchange name
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Get the base asset
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Get the quote asset
    pub fn quote(&self) -> &str {
        &self.quote
    }

    /// Get the settlement asset
    pub fn settle(&self) -> &str {
        &self.settle
    }

    /// Get the description
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Get the fetcher capabilities
    pub fn capabilities(&self) -> &FetcherCapabilities {
        &self.capabilities
    }

    /// Get the validation rules
    pub fn validation_rules(&self) -> &ValidationRules {
        &self.validation_rules
    }
}

/// Capabilities of a data fetcher for a specific identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetcherCapabilities {
    data_types: Vec<String>,
    intervals: Vec<String>,
    archive_support: bool,
    live_api_support: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    historical_limit_days: Option<u32>,
}

impl FetcherCapabilities {
    /// Get supported data types
    pub fn data_types(&self) -> &[String] {
        &self.data_types
    }

    /// Get supported intervals
    pub fn intervals(&self) -> &[String] {
        &self.intervals
    }

    /// Check if archive downloads are supported
    pub fn archive_support(&self) -> bool {
        self.archive_support
    }

    /// Check if live API is supported
    pub fn live_api_support(&self) -> bool {
        self.live_api_support
    }

    /// Get historical data limit in days (if applicable)
    pub fn historical_limit_days(&self) -> Option<u32> {
        self.historical_limit_days
    }
}

/// Validation rules for an identifier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRules {
    symbol_pattern: String,
    interval_pattern: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_date: Option<String>,
}

impl ValidationRules {
    /// Get the symbol validation pattern
    pub fn symbol_pattern(&self) -> &str {
        &self.symbol_pattern
    }

    /// Get the interval validation pattern
    pub fn interval_pattern(&self) -> &str {
        &self.interval_pattern
    }

    /// Get the minimum date (if applicable)
    pub fn min_date(&self) -> Option<&str> {
        self.min_date.as_deref()
    }
}

/// Raw registry structure for deserialization
#[derive(Debug, Deserialize)]
struct RawRegistry {
    schema_version: String,
    last_updated: String,
    identifiers: Vec<RegistryEntry>,
}

/// Errors that can occur when working with the registry
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Failed to parse registry JSON
    #[error("registry parse error: {0}")]
    ParseError(String),

    /// Identifier not found in registry
    #[error("identifier not found: {0}")]
    NotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_loads() {
        let registry = IdentifierRegistry::load().unwrap();
        assert!(!registry.entries().is_empty());
    }

    #[test]
    fn test_registry_has_binance_entries() {
        let registry = IdentifierRegistry::load().unwrap();
        let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
        assert!(registry.get_entry(&id).is_some());
    }
}
