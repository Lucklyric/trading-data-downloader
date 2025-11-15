//! Integration tests for ExchangeIdentifier parsing and validation

use trading_data_downloader::identifier::ExchangeIdentifier;
use trading_data_downloader::registry::IdentifierRegistry;

#[test]
fn test_identifier_parsing_valid_usdt_margined() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    assert_eq!(id.exchange(), "BINANCE");
    assert_eq!(id.base(), "BTC");
    assert_eq!(id.quote(), "USDT");
    assert_eq!(id.settle(), "USDT");
}

#[test]
fn test_identifier_parsing_valid_coin_margined() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USD:BTC").unwrap();
    assert_eq!(id.exchange(), "BINANCE");
    assert_eq!(id.base(), "BTC");
    assert_eq!(id.quote(), "USD");
    assert_eq!(id.settle(), "BTC");
}

#[test]
fn test_identifier_parsing_invalid_format_missing_colon() {
    let result = ExchangeIdentifier::parse("BINANCEBTC/USDT:USDT");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid identifier format"));
}

#[test]
fn test_identifier_parsing_invalid_format_missing_slash() {
    let result = ExchangeIdentifier::parse("BINANCE:BTCUSDT:USDT");
    assert!(result.is_err());
}

#[test]
fn test_identifier_parsing_invalid_empty_component() {
    let result = ExchangeIdentifier::parse("BINANCE:/USDT:USDT");
    assert!(result.is_err());
}

#[test]
fn test_identifier_normalization_lowercase_input() {
    let id = ExchangeIdentifier::parse("binance:btc/usdt:usdt").unwrap();
    assert_eq!(id.exchange(), "BINANCE");
    assert_eq!(id.base(), "BTC");
    assert_eq!(id.quote(), "USDT");
    assert_eq!(id.settle(), "USDT");
}

#[test]
fn test_identifier_normalization_mixed_case_input() {
    let id = ExchangeIdentifier::parse("Binance:Btc/Usdt:Usdt").unwrap();
    assert_eq!(id.exchange(), "BINANCE");
    assert_eq!(id.base(), "BTC");
    assert_eq!(id.quote(), "USDT");
    assert_eq!(id.settle(), "USDT");
}

#[test]
fn test_identifier_to_string_canonical() {
    let id = ExchangeIdentifier::parse("binance:btc/usdt:usdt").unwrap();
    assert_eq!(id.to_string(), "BINANCE:BTC/USDT:USDT");
}

#[test]
fn test_identifier_to_filesystem_safe() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    let safe = id.to_filesystem_safe();
    assert_eq!(safe, "binance_btc_usdt_usdt");
    assert!(!safe.contains(':'));
    assert!(!safe.contains('/'));
}

#[test]
fn test_identifier_to_filesystem_safe_coin_margined() {
    let id = ExchangeIdentifier::parse("BINANCE:ETH/USD:ETH").unwrap();
    let safe = id.to_filesystem_safe();
    assert_eq!(safe, "binance_eth_usd_eth");
}

// IdentifierRegistry Tests (T010-T011)

#[test]
fn test_registry_loading_from_json() {
    let registry = IdentifierRegistry::load().unwrap();
    assert!(
        !registry.entries().is_empty(),
        "Registry should contain entries"
    );
}

#[test]
fn test_registry_get_entry_binance_usdt() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    let entry = registry.get_entry(&id);
    assert!(entry.is_some(), "Should find BINANCE:BTC/USDT:USDT entry");

    let entry = entry.unwrap();
    assert_eq!(entry.exchange(), "BINANCE");
    assert!(entry
        .capabilities()
        .data_types()
        .contains(&"bars".to_string()));
    assert!(entry
        .capabilities()
        .data_types()
        .contains(&"aggtrades".to_string()));
    assert!(entry
        .capabilities()
        .data_types()
        .contains(&"funding".to_string()));
}

#[test]
fn test_registry_get_entry_binance_coin() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USD:BTC").unwrap();
    let entry = registry.get_entry(&id);
    assert!(entry.is_some(), "Should find BINANCE:BTC/USD:BTC entry");
}

#[test]
fn test_registry_get_entry_not_found() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("UNKNOWN:BTC/USDT:USDT").unwrap();
    let entry = registry.get_entry(&id);
    assert!(entry.is_none(), "Should not find unknown exchange");
}

#[test]
fn test_registry_validate_identifier_valid() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    let result = registry.validate_identifier(&id);
    assert!(result.is_ok(), "Valid identifier should pass validation");
}

#[test]
fn test_registry_validate_identifier_unsupported_exchange() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("BYBIT:BTC/USDT:USDT").unwrap();
    let result = registry.validate_identifier(&id);
    assert!(
        result.is_err(),
        "Unsupported exchange should fail validation"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not found in registry"));
}

#[test]
fn test_registry_validate_capabilities() {
    let registry = IdentifierRegistry::load().unwrap();
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    let entry = registry.get_entry(&id).unwrap();

    assert!(entry.capabilities().archive_support());
    assert!(entry.capabilities().live_api_support());
    assert!(!entry.capabilities().intervals().is_empty());
}
