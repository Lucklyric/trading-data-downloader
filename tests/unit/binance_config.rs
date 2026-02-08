//! Tests for Binance market configuration (T223-T224)

use trading_data_downloader::fetcher::binance_config::{
    SymbolFormat, COIN_FUTURES_CONFIG, USDT_FUTURES_CONFIG,
};

/// T223: Test USDT_FUTURES_CONFIG validation
#[test]
fn test_usdt_futures_config() {
    let config = &USDT_FUTURES_CONFIG;

    // Verify base_url is correct (FAPI)
    assert_eq!(config.base_url, "https://fapi.binance.com");

    // Verify all endpoint paths are correct
    assert_eq!(config.klines_endpoint, "/fapi/v1/klines");
    assert_eq!(config.aggtrades_endpoint, "/fapi/v1/aggTrades");
    assert_eq!(config.funding_endpoint, "/fapi/v1/fundingRate");
    assert_eq!(config.exchange_info_endpoint, "/fapi/v1/exchangeInfo");

    // Verify symbol format is Perpetual
    assert_eq!(config.symbol_format, SymbolFormat::Perpetual);
}

/// T224: Test COIN_FUTURES_CONFIG validation
#[test]
fn test_coin_futures_config() {
    let config = &COIN_FUTURES_CONFIG;

    // Verify base_url is correct (DAPI)
    assert_eq!(config.base_url, "https://dapi.binance.com");

    // Verify all endpoint paths are correct
    assert_eq!(config.klines_endpoint, "/dapi/v1/klines");
    assert_eq!(config.aggtrades_endpoint, "/dapi/v1/aggTrades");
    assert_eq!(config.funding_endpoint, "/dapi/v1/fundingRate");
    assert_eq!(config.exchange_info_endpoint, "/dapi/v1/exchangeInfo");

    // Verify symbol format is CoinPerpetual
    assert_eq!(config.symbol_format, SymbolFormat::CoinPerpetual);
}

/// Test that configs can be cloned
#[test]
fn test_config_clone() {
    let usdt = USDT_FUTURES_CONFIG.clone();
    let coin = COIN_FUTURES_CONFIG.clone();

    assert_eq!(usdt.base_url, "https://fapi.binance.com");
    assert_eq!(coin.base_url, "https://dapi.binance.com");
}

/// Test symbol format enum properties
#[test]
fn test_symbol_format_enum() {
    let perpetual = SymbolFormat::Perpetual;
    let coin_perpetual = SymbolFormat::CoinPerpetual;

    // Test Copy trait
    let perpetual_copy = perpetual;
    assert_eq!(perpetual, perpetual_copy);

    // Test inequality
    assert_ne!(perpetual, coin_perpetual);

    // Test Clone trait
    #[allow(clippy::clone_on_copy)]
    let coin_clone = coin_perpetual.clone();
    assert_eq!(coin_perpetual, coin_clone);
}
