//! Unit tests for BinanceParser (T203-T211)

use rust_decimal::Decimal;
use serde_json::json;
use std::str::FromStr;
use trading_data_downloader::fetcher::binance_config::SymbolFormat;
use trading_data_downloader::fetcher::binance_parser::BinanceParser;

/// T203: Test parsing Binance klines JSON array to Bar structs
#[test]
fn test_binance_parser_klines_to_bars() {
    // Sample klines response from Binance API (12 elements per kline)
    let klines_json = vec![
        json!([
            1699920000000i64, // 0: Open time
            "35000.50",       // 1: Open price
            "35100.00",       // 2: High price
            "34950.00",       // 3: Low price
            "35050.75",       // 4: Close price
            "1234.567",       // 5: Volume
            1699920059999i64, // 6: Close time
            "43210987.65",    // 7: Quote volume
            5432,             // 8: Number of trades
            "617.283",        // 9: Taker buy base volume
            "21605493.82",    // 10: Taker buy quote volume
            "0"               // 11: Ignore
        ]),
        json!([
            1699920060000i64,
            "35050.75",
            "35200.00",
            "35000.00",
            "35150.25",
            "2345.678",
            1699920119999i64,
            "82345678.90",
            6543,
            "1234.567",
            "43456789.01",
            "0"
        ]),
    ];

    let bars = BinanceParser::parse_klines(klines_json).unwrap();

    assert_eq!(bars.len(), 2);

    // Verify first bar
    let bar1 = &bars[0];
    assert_eq!(bar1.open_time, 1699920000000);
    assert_eq!(bar1.close_time, 1699920059999);
    assert_eq!(bar1.trades, 5432);
    assert_eq!(bar1.open, Decimal::from_str("35000.50").unwrap());
    assert_eq!(bar1.high, Decimal::from_str("35100.00").unwrap());
    assert_eq!(bar1.low, Decimal::from_str("34950.00").unwrap());
    assert_eq!(bar1.close, Decimal::from_str("35050.75").unwrap());
    assert_eq!(bar1.volume, Decimal::from_str("1234.567").unwrap());
    assert_eq!(bar1.quote_volume, Decimal::from_str("43210987.65").unwrap());
    assert_eq!(
        bar1.taker_buy_base_volume,
        Decimal::from_str("617.283").unwrap()
    );
    assert_eq!(
        bar1.taker_buy_quote_volume,
        Decimal::from_str("21605493.82").unwrap()
    );

    // Verify second bar
    let bar2 = &bars[1];
    assert_eq!(bar2.open_time, 1699920060000);
    assert_eq!(bar2.close, Decimal::from_str("35150.25").unwrap());
}

/// T205: Test parsing Binance aggTrades JSON to AggTrade structs
#[test]
fn test_binance_parser_aggtrades_to_aggtrades() {
    // Sample aggTrades response from Binance API
    let trades_json = vec![
        json!({
            "a": 12345678,          // Aggregate trade ID
            "p": "35000.50",        // Price
            "q": "1.234",           // Quantity
            "f": 100001,            // First trade ID
            "l": 100005,            // Last trade ID
            "T": 1699920000000i64,  // Timestamp
            "m": true               // Is buyer maker
        }),
        json!({
            "a": 12345679,
            "p": "35050.75",
            "q": "2.345",
            "f": 100006,
            "l": 100010,
            "T": 1699920001000i64,
            "m": false
        }),
    ];

    let trades = BinanceParser::parse_aggtrades(trades_json).unwrap();

    assert_eq!(trades.len(), 2);

    // Verify first trade
    let trade1 = &trades[0];
    assert_eq!(trade1.agg_trade_id, 12345678);
    assert_eq!(trade1.price, Decimal::from_str("35000.50").unwrap());
    assert_eq!(trade1.quantity, Decimal::from_str("1.234").unwrap());
    assert_eq!(trade1.first_trade_id, 100001);
    assert_eq!(trade1.last_trade_id, 100005);
    assert_eq!(trade1.timestamp, 1699920000000);
    assert_eq!(trade1.is_buyer_maker, true);

    // Verify second trade
    let trade2 = &trades[1];
    assert_eq!(trade2.agg_trade_id, 12345679);
    assert_eq!(trade2.is_buyer_maker, false);
}

/// T207: Test parsing Binance fundingRate JSON to FundingRate structs
#[test]
fn test_binance_parser_funding_to_funding_rates() {
    // Sample fundingRate response from Binance API
    let rates_json = vec![
        json!({
            "symbol": "BTCUSDT",
            "fundingRate": "0.00010000",
            "fundingTime": 1699920000000i64
        }),
        json!({
            "symbol": "ETHUSDT",
            "fundingRate": "-0.00005000",
            "fundingTime": 1699948800000i64
        }),
    ];

    let rates = BinanceParser::parse_funding_rates(rates_json).unwrap();

    assert_eq!(rates.len(), 2);

    // Verify first rate
    let rate1 = &rates[0];
    assert_eq!(rate1.symbol, "BTCUSDT");
    assert_eq!(rate1.funding_rate, Decimal::from_str("0.00010000").unwrap());
    assert_eq!(rate1.funding_time, 1699920000000);

    // Verify second rate
    let rate2 = &rates[1];
    assert_eq!(rate2.symbol, "ETHUSDT");
    assert_eq!(
        rate2.funding_rate,
        Decimal::from_str("-0.00005000").unwrap()
    );
    assert_eq!(rate2.funding_time, 1699948800000);
}

/// T209: Test symbol normalization for USDT-margined (Perpetual)
#[test]
fn test_binance_parser_normalize_symbol_usdt() {
    // USDT-margined: just remove "/"
    let normalized = BinanceParser::normalize_symbol("BTC/USDT", SymbolFormat::Perpetual);
    assert_eq!(normalized, "BTCUSDT");

    let normalized = BinanceParser::normalize_symbol("ETH/USDT", SymbolFormat::Perpetual);
    assert_eq!(normalized, "ETHUSDT");

    let normalized = BinanceParser::normalize_symbol("XRP/USDT", SymbolFormat::Perpetual);
    assert_eq!(normalized, "XRPUSDT");
}

/// T210: Test symbol normalization for COIN-margined (CoinPerpetual)
#[test]
fn test_binance_parser_normalize_symbol_coin() {
    // COIN-margined: remove "/" and append "_PERP"
    let normalized = BinanceParser::normalize_symbol("BTC/USD", SymbolFormat::CoinPerpetual);
    assert_eq!(normalized, "BTCUSD_PERP");

    let normalized = BinanceParser::normalize_symbol("ETH/USD", SymbolFormat::CoinPerpetual);
    assert_eq!(normalized, "ETHUSD_PERP");

    let normalized = BinanceParser::normalize_symbol("XRP/USD", SymbolFormat::CoinPerpetual);
    assert_eq!(normalized, "XRPUSD_PERP");
}
