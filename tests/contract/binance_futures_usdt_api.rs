//! Contract tests for Binance Futures USDT API endpoints
//!
//! These tests verify that the actual Binance API responses match our expectations.
//! They are designed to fail until we implement the corresponding functionality.

use reqwest;
use serde_json::Value;

/// T037: Contract test for Binance klines API (/fapi/v1/klines)
/// This test verifies the API endpoint is accessible and returns expected structure
#[tokio::test]
#[ignore] // Remove this once implementation is ready
async fn test_binance_klines_api_contract() {
    let client = reqwest::Client::new();

    // Use BTCUSDT as a stable symbol for testing
    let url = "https://fapi.binance.com/fapi/v1/klines";
    let params = [
        ("symbol", "BTCUSDT"),
        ("interval", "1m"),
        ("limit", "2"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    // Verify response is an array
    assert!(body.is_array(), "Response should be an array");

    let klines = body.as_array().expect("Expected array");
    assert!(!klines.is_empty(), "Response should contain at least one kline");

    // Verify first kline has expected structure
    let first_kline = &klines[0];
    assert!(first_kline.is_array(), "Each kline should be an array");

    let kline_array = first_kline.as_array().expect("Expected array");
    assert_eq!(
        kline_array.len(),
        12,
        "Each kline should have exactly 12 elements"
    );
}

/// T038: Contract test for klines response schema validation
/// This test validates the structure and types of each field in the kline response
#[tokio::test]
#[ignore] // Remove this once implementation is ready
async fn test_binance_klines_response_schema() {
    let client = reqwest::Client::new();

    let url = "https://fapi.binance.com/fapi/v1/klines";
    let params = [
        ("symbol", "BTCUSDT"),
        ("interval", "1m"),
        ("limit", "1"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    let klines = body.as_array().expect("Expected array");
    let kline = &klines[0].as_array().expect("Expected kline array");

    // Validate schema according to binance-klines-schema.json

    // Index 0: Open time (integer timestamp in milliseconds)
    assert!(
        kline[0].is_number(),
        "Open time should be a number, got: {:?}",
        kline[0]
    );
    let open_time = kline[0].as_i64().expect("Open time should be i64");
    assert!(open_time > 0, "Open time should be positive");

    // Index 1: Open price (string)
    assert!(
        kline[1].is_string(),
        "Open price should be string, got: {:?}",
        kline[1]
    );
    let open_price = kline[1].as_str().expect("Open price should be string");
    assert!(
        open_price.parse::<f64>().is_ok(),
        "Open price should be parseable as number"
    );

    // Index 2: High price (string)
    assert!(
        kline[2].is_string(),
        "High price should be string, got: {:?}",
        kline[2]
    );

    // Index 3: Low price (string)
    assert!(
        kline[3].is_string(),
        "Low price should be string, got: {:?}",
        kline[3]
    );

    // Index 4: Close price (string)
    assert!(
        kline[4].is_string(),
        "Close price should be string, got: {:?}",
        kline[4]
    );

    // Index 5: Volume (string)
    assert!(
        kline[5].is_string(),
        "Volume should be string, got: {:?}",
        kline[5]
    );

    // Index 6: Close time (integer timestamp)
    assert!(
        kline[6].is_number(),
        "Close time should be a number, got: {:?}",
        kline[6]
    );
    let close_time = kline[6].as_i64().expect("Close time should be i64");
    assert!(close_time > open_time, "Close time should be after open time");

    // Index 7: Quote asset volume (string)
    assert!(
        kline[7].is_string(),
        "Quote volume should be string, got: {:?}",
        kline[7]
    );

    // Index 8: Number of trades (integer)
    assert!(
        kline[8].is_number(),
        "Number of trades should be number, got: {:?}",
        kline[8]
    );

    // Index 9: Taker buy base volume (string)
    assert!(
        kline[9].is_string(),
        "Taker buy base volume should be string, got: {:?}",
        kline[9]
    );

    // Index 10: Taker buy quote volume (string)
    assert!(
        kline[10].is_string(),
        "Taker buy quote volume should be string, got: {:?}",
        kline[10]
    );

    // Index 11: Unused field (always "0")
    assert!(
        kline[11].is_string(),
        "Unused field should be string, got: {:?}",
        kline[11]
    );
}

/// T089: Contract test for Binance exchangeInfo API (/fapi/v1/exchangeInfo)
/// This test verifies the API endpoint is accessible and returns expected structure
#[tokio::test]
async fn test_binance_exchangeinfo_api_contract() {
    let client = reqwest::Client::new();

    let url = "https://fapi.binance.com/fapi/v1/exchangeInfo";

    let response = client
        .get(url)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    // Verify required top-level fields
    assert!(body.is_object(), "Response should be an object");
    assert!(
        body.get("timezone").is_some(),
        "Response should contain 'timezone' field"
    );
    assert!(
        body.get("serverTime").is_some(),
        "Response should contain 'serverTime' field"
    );
    assert!(
        body.get("symbols").is_some(),
        "Response should contain 'symbols' field"
    );

    // Verify symbols is an array
    let symbols = body.get("symbols").expect("symbols field should exist");
    assert!(symbols.is_array(), "symbols should be an array");

    let symbols_array = symbols.as_array().expect("Expected array");
    assert!(
        !symbols_array.is_empty(),
        "symbols array should not be empty"
    );
}

/// T090: Contract test for exchangeInfo response schema validation
/// This test validates the structure and types of each field in the exchangeInfo response
#[tokio::test]
async fn test_binance_exchangeinfo_response_schema() {
    let client = reqwest::Client::new();

    let url = "https://fapi.binance.com/fapi/v1/exchangeInfo";

    let response = client
        .get(url)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    // Validate schema according to binance-exchangeinfo-schema.json

    // Validate timezone
    let timezone = body.get("timezone").expect("timezone should exist");
    assert!(timezone.is_string(), "timezone should be a string");
    assert_eq!(
        timezone.as_str().expect("timezone should be string"),
        "UTC"
    );

    // Validate serverTime
    let server_time = body.get("serverTime").expect("serverTime should exist");
    assert!(server_time.is_number(), "serverTime should be a number");
    let server_time_value = server_time.as_i64().expect("serverTime should be i64");
    assert!(server_time_value > 0, "serverTime should be positive");

    // Validate symbols array
    let symbols = body.get("symbols").expect("symbols should exist");
    assert!(symbols.is_array(), "symbols should be an array");

    let symbols_array = symbols.as_array().expect("Expected array");
    assert!(!symbols_array.is_empty(), "symbols should not be empty");

    // Validate first symbol structure
    let first_symbol = &symbols_array[0];
    assert!(first_symbol.is_object(), "symbol should be an object");

    // Required fields
    assert!(
        first_symbol.get("symbol").is_some(),
        "symbol should have 'symbol' field"
    );
    assert!(
        first_symbol.get("pair").is_some(),
        "symbol should have 'pair' field"
    );
    assert!(
        first_symbol.get("contractType").is_some(),
        "symbol should have 'contractType' field"
    );
    assert!(
        first_symbol.get("status").is_some(),
        "symbol should have 'status' field"
    );
    assert!(
        first_symbol.get("baseAsset").is_some(),
        "symbol should have 'baseAsset' field"
    );
    assert!(
        first_symbol.get("quoteAsset").is_some(),
        "symbol should have 'quoteAsset' field"
    );
    assert!(
        first_symbol.get("pricePrecision").is_some(),
        "symbol should have 'pricePrecision' field"
    );
    assert!(
        first_symbol.get("quantityPrecision").is_some(),
        "symbol should have 'quantityPrecision' field"
    );

    // Validate symbol field is string
    let symbol_name = first_symbol.get("symbol").expect("symbol should exist");
    assert!(symbol_name.is_string(), "symbol should be a string");

    // Validate contractType is one of expected values
    let contract_type = first_symbol
        .get("contractType")
        .expect("contractType should exist");
    assert!(
        contract_type.is_string(),
        "contractType should be a string"
    );
    let contract_type_str = contract_type.as_str().expect("contractType should be string");
    assert!(
        ["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER", "CURRENT_QUARTER_DELIVERING", "NEXT_QUARTER_DELIVERING"]
            .contains(&contract_type_str),
        "contractType should be one of valid values, got: {}",
        contract_type_str
    );

    // Validate status is one of expected values
    let status = first_symbol.get("status").expect("status should exist");
    assert!(status.is_string(), "status should be a string");
    let status_str = status.as_str().expect("status should be string");
    assert!(
        ["TRADING", "PRE_TRADING", "DELIVERING", "DELIVERED", "BREAK"].contains(&status_str),
        "status should be one of valid values, got: {}",
        status_str
    );

    // Validate precision fields are integers
    let price_precision = first_symbol
        .get("pricePrecision")
        .expect("pricePrecision should exist");
    assert!(
        price_precision.is_number(),
        "pricePrecision should be a number"
    );

    let quantity_precision = first_symbol
        .get("quantityPrecision")
        .expect("quantityPrecision should exist");
    assert!(
        quantity_precision.is_number(),
        "quantityPrecision should be a number"
    );
}

/// T106: Contract test for Binance aggTrades API (/fapi/v1/aggTrades)
/// This test verifies the API endpoint is accessible and returns expected structure
#[tokio::test]
async fn test_binance_aggtrades_api_contract() {
    let client = reqwest::Client::new();

    // Use recent time window (last 10 minutes)
    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - (10 * 60 * 1000); // 10 minutes ago

    let url = "https://fapi.binance.com/fapi/v1/aggTrades";
    let params = [
        ("symbol", "BTCUSDT"),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", "10"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    // Verify response is an array
    assert!(body.is_array(), "Response should be an array");

    let trades = body.as_array().expect("Expected array");
    // Note: might be empty if no trades in last 10 minutes, but that's okay
    if !trades.is_empty() {
        // Verify first trade has expected structure
        let first_trade = &trades[0];
        assert!(first_trade.is_object(), "Each trade should be an object");
    }
}

/// T107: Contract test for aggTrades response schema validation
/// This test validates the structure and types of each field in the aggTrades response
#[tokio::test]
async fn test_binance_aggtrades_response_schema() {
    let client = reqwest::Client::new();

    // Use a wider time window to ensure we get data
    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - (60 * 60 * 1000); // 1 hour ago

    let url = "https://fapi.binance.com/fapi/v1/aggTrades";
    let params = [
        ("symbol", "BTCUSDT"),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", "1"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    let trades = body.as_array().expect("Expected array");

    // Skip if no trades available
    if trades.is_empty() {
        println!("No trades in test window, skipping schema validation");
        return;
    }

    let trade = &trades[0];

    // Validate schema according to binance-aggtrades-schema.json

    // a: Aggregate trade ID (integer)
    assert!(
        trade.get("a").is_some(),
        "Trade should have 'a' field (agg trade ID)"
    );
    let agg_id = trade.get("a").expect("a should exist");
    assert!(agg_id.is_number(), "a should be a number");

    // p: Price (string)
    assert!(
        trade.get("p").is_some(),
        "Trade should have 'p' field (price)"
    );
    let price = trade.get("p").expect("p should exist");
    assert!(price.is_string(), "p should be a string");
    let price_str = price.as_str().expect("p should be string");
    assert!(
        price_str.parse::<f64>().is_ok(),
        "p should be parseable as number"
    );

    // q: Quantity (string)
    assert!(
        trade.get("q").is_some(),
        "Trade should have 'q' field (quantity)"
    );
    let qty = trade.get("q").expect("q should exist");
    assert!(qty.is_string(), "q should be a string");

    // f: First trade ID (integer)
    assert!(
        trade.get("f").is_some(),
        "Trade should have 'f' field (first trade ID)"
    );
    let first_id = trade.get("f").expect("f should exist");
    assert!(first_id.is_number(), "f should be a number");

    // l: Last trade ID (integer)
    assert!(
        trade.get("l").is_some(),
        "Trade should have 'l' field (last trade ID)"
    );
    let last_id = trade.get("l").expect("l should exist");
    assert!(last_id.is_number(), "l should be a number");

    // T: Timestamp (integer)
    assert!(
        trade.get("T").is_some(),
        "Trade should have 'T' field (timestamp)"
    );
    let timestamp = trade.get("T").expect("T should exist");
    assert!(timestamp.is_number(), "T should be a number");
    let timestamp_value = timestamp.as_i64().expect("T should be i64");
    assert!(timestamp_value > 0, "T should be positive");

    // m: Is buyer maker (boolean)
    assert!(
        trade.get("m").is_some(),
        "Trade should have 'm' field (is buyer maker)"
    );
    let is_buyer_maker = trade.get("m").expect("m should exist");
    assert!(is_buyer_maker.is_boolean(), "m should be a boolean");
}

/// T110: Test for 1-hour time window constraint enforcement
#[tokio::test]
async fn test_aggtrades_one_hour_window_constraint() {
    let client = reqwest::Client::new();

    // Try to fetch more than 1 hour of data - this should work but may timeout or fail
    let end_time = chrono::Utc::now().timestamp_millis();
    let start_time = end_time - (2 * 60 * 60 * 1000); // 2 hours ago (violates constraint)

    let url = "https://fapi.binance.com/fapi/v1/aggTrades";
    let params = [
        ("symbol", "BTCUSDT"),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await;

    // According to docs, requests > 1 hour window may timeout or return error
    // We're just documenting this constraint exists, not necessarily that it fails
    match response {
        Ok(resp) => {
            // If it succeeds, that's fine - Binance might be lenient
            println!("Response status for 2-hour window: {}", resp.status());
        }
        Err(e) => {
            // Expected for windows > 1 hour
            println!("Request for 2-hour window failed as expected: {}", e);
        }
    }

    // Now test with proper 1-hour window
    let end_time_ok = chrono::Utc::now().timestamp_millis();
    let start_time_ok = end_time_ok - (59 * 60 * 1000); // 59 minutes (within constraint)

    let params_ok = [
        ("symbol", "BTCUSDT"),
        ("startTime", &start_time_ok.to_string()),
        ("endTime", &end_time_ok.to_string()),
    ];

    let response_ok = client
        .get(url)
        .query(&params_ok)
        .send()
        .await
        .expect("1-hour window should succeed");

    assert!(
        response_ok.status().is_success(),
        "1-hour window request should succeed"
    );
}

/// T130: Contract test for Binance funding rate API (/fapi/v1/fundingRate)
/// This test verifies the API endpoint is accessible and returns expected structure
#[tokio::test]
async fn test_binance_funding_rate_api_contract() {
    let client = reqwest::Client::new();

    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let params = [
        ("symbol", "BTCUSDT"),
        ("limit", "2"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    // Verify response is an array
    assert!(body.is_array(), "Response should be an array");

    let funding_rates = body.as_array().expect("Expected array");
    assert!(!funding_rates.is_empty(), "Response should contain at least one funding rate");

    // Verify first funding rate has expected structure
    let first_rate = &funding_rates[0];
    assert!(first_rate.is_object(), "Each funding rate should be an object");
}

/// T131: Contract test for funding rate response schema validation
/// This test validates the structure and types of each field in the funding rate response
#[tokio::test]
async fn test_binance_funding_rate_response_schema() {
    let client = reqwest::Client::new();

    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let params = [
        ("symbol", "BTCUSDT"),
        ("limit", "1"),
    ];

    let response = client
        .get(url)
        .query(&params)
        .send()
        .await
        .expect("Failed to send request to Binance API");

    let body: Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    let funding_rates = body.as_array().expect("Expected array");
    let rate = &funding_rates[0];

    // Validate schema according to binance-fundingrate-schema.json

    // symbol field (string)
    assert!(
        rate.get("symbol").is_some(),
        "Response should contain 'symbol' field"
    );
    let symbol = rate.get("symbol").expect("symbol should exist");
    assert!(symbol.is_string(), "symbol should be a string");

    // fundingRate field (string - decimal representation)
    assert!(
        rate.get("fundingRate").is_some(),
        "Response should contain 'fundingRate' field"
    );
    let funding_rate = rate.get("fundingRate").expect("fundingRate should exist");
    assert!(funding_rate.is_string(), "fundingRate should be a string");
    let funding_rate_str = funding_rate.as_str().expect("fundingRate should be string");
    assert!(
        funding_rate_str.parse::<f64>().is_ok(),
        "fundingRate should be parseable as number"
    );

    // fundingTime field (integer timestamp in milliseconds)
    assert!(
        rate.get("fundingTime").is_some(),
        "Response should contain 'fundingTime' field"
    );
    let funding_time = rate.get("fundingTime").expect("fundingTime should exist");
    assert!(funding_time.is_number(), "fundingTime should be a number");
    let funding_time_value = funding_time.as_i64().expect("fundingTime should be i64");
    assert!(funding_time_value > 0, "fundingTime should be positive");

    // Validate funding time alignment (should be at 00:00, 08:00, or 16:00 UTC)
    use chrono::{DateTime, Utc, Timelike};
    let dt = DateTime::<Utc>::from_timestamp_millis(funding_time_value)
        .expect("Valid timestamp");
    let hour = dt.hour();
    assert!(
        hour % 8 == 0,
        "Funding time should be at 8-hour intervals (00:00, 08:00, 16:00 UTC), got hour: {}",
        hour
    );
    assert_eq!(
        dt.minute(), 0,
        "Funding time should be at exact hour boundary"
    );
    assert_eq!(
        dt.second(), 0,
        "Funding time should have zero seconds"
    );
}
