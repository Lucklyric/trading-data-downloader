use trading_data_downloader::fetcher::create_fetcher;
use trading_data_downloader::identifier::ExchangeIdentifier;

#[test]
fn test_fetcher_factory_usdt() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT").unwrap();
    let fetcher = create_fetcher(&id, 5).unwrap();
    assert!(fetcher.base_url().contains("fapi.binance.com"));
}

#[test]
fn test_fetcher_factory_coin() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/USD:BTC").unwrap();
    let fetcher = create_fetcher(&id, 5).unwrap();
    assert!(fetcher.base_url().contains("dapi.binance.com"));
}

#[test]
fn test_fetcher_factory_eth_coin() {
    let id = ExchangeIdentifier::parse("BINANCE:ETH/USD:ETH").unwrap();
    let fetcher = create_fetcher(&id, 5).unwrap();
    assert!(fetcher.base_url().contains("dapi.binance.com"));
}

#[test]
fn test_fetcher_factory_unsupported_asset() {
    let id = ExchangeIdentifier::parse("BINANCE:BTC/EUR:EUR").unwrap();
    let result = create_fetcher(&id, 5);
    assert!(result.is_err());
}

#[test]
fn test_fetcher_factory_unsupported_exchange() {
    let id = ExchangeIdentifier::parse("KRAKEN:BTC/USDT:USDT").unwrap();
    let result = create_fetcher(&id, 5);
    assert!(result.is_err());
}
