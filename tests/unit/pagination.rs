//! Unit tests for PaginationHelper module (T212-T222)

use reqwest::Client;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use trading_data_downloader::downloader::rate_limit::RateLimiter;
use trading_data_downloader::fetcher::binance_http::BinanceHttpClient;
use trading_data_downloader::fetcher::pagination::PaginationHelper;
use trading_data_downloader::fetcher::FetcherResult;
use trading_data_downloader::{AggTrade, Bar, FundingRate};

/// Helper struct to track fetch calls
#[derive(Clone)]
struct FetchTracker {
    call_count: Arc<Mutex<usize>>,
}

impl FetchTracker {
    fn new() -> Self {
        Self {
            call_count: Arc::new(Mutex::new(0)),
        }
    }

    fn get_count(&self) -> usize {
        *self.call_count.lock().unwrap()
    }

    fn increment(&self) {
        let mut count = self.call_count.lock().unwrap();
        *count += 1;
    }
}

/// Create mock bars for testing
fn create_mock_bars(start_time: i64, count: usize) -> Vec<Bar> {
    let mut bars = Vec::new();
    for i in 0..count {
        let open_time = start_time + (i as i64 * 60_000); // 1 minute intervals
        bars.push(Bar {
            open_time,
            open: Decimal::from_str("35000.50").unwrap(),
            high: Decimal::from_str("35100.00").unwrap(),
            low: Decimal::from_str("34950.00").unwrap(),
            close: Decimal::from_str("35050.75").unwrap(),
            volume: Decimal::from_str("1234.567").unwrap(),
            close_time: open_time + 59_999, // close_time is open_time + 59.999s
            quote_volume: Decimal::from_str("43210987.65").unwrap(),
            trades: 5432,
            taker_buy_base_volume: Decimal::from_str("617.283").unwrap(),
            taker_buy_quote_volume: Decimal::from_str("21605493.82").unwrap(),
        });
    }
    bars
}

/// Create mock aggregate trades for testing
fn create_mock_aggtrades(start_id: i64, start_time: i64, count: usize) -> Vec<AggTrade> {
    let mut trades = Vec::new();
    for i in 0..count {
        let agg_trade_id = start_id + i as i64;
        trades.push(AggTrade {
            agg_trade_id,
            price: Decimal::from_str("35000.50").unwrap(),
            quantity: Decimal::from_str("1.5").unwrap(),
            first_trade_id: agg_trade_id,
            last_trade_id: agg_trade_id,
            timestamp: start_time + (i as i64 * 1000), // 1 second intervals
            is_buyer_maker: false,
        });
    }
    trades
}

/// Create mock funding rates for testing
fn create_mock_funding_rates(symbol: &str, start_time: i64, count: usize) -> Vec<FundingRate> {
    let mut rates = Vec::new();
    for i in 0..count {
        rates.push(FundingRate {
            symbol: symbol.to_string(),
            funding_rate: Decimal::from_str("0.0001").unwrap(),
            funding_time: start_time + (i as i64 * 8 * 3600 * 1000), // 8-hour intervals
        });
    }
    rates
}

/// T212: Test fetching first page of klines (startTime-based pagination)
/// This test verifies the basic pagination mechanism for klines with a single page
#[tokio::test]
async fn test_pagination_klines_first_page() {
    // Create HTTP client
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    // Define parameters
    let endpoint = "/fapi/v1/klines";
    let symbol = "BTCUSDT";
    let interval = "1m";
    let start_time = 1699920000000;
    let end_time = 1699920180000; // 3 minutes later

    // Create fetch tracker
    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    // Mock fetch function that returns one page of 3 bars
    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();

        // Extract params we need to verify
        let has_symbol = params.iter().any(|(k, _)| *k == "symbol");
        let has_interval = params.iter().any(|(k, _)| *k == "interval");
        let has_start_time = params.iter().any(|(k, _)| *k == "startTime");
        let has_end_time = params.iter().any(|(k, _)| *k == "endTime");
        let has_limit = params.iter().any(|(k, _)| *k == "limit");

        async move {
            tracker.increment();

            // Verify parameters are present
            assert!(has_symbol, "symbol parameter should be present");
            assert!(has_interval, "interval parameter should be present");
            assert!(has_start_time, "startTime parameter should be present");
            assert!(has_end_time, "endTime parameter should be present");
            assert!(has_limit, "limit parameter should be present");

            // Return 3 bars
            Ok(create_mock_bars(1699920000000, 3))
        }
    };

    // Execute pagination
    let result = PaginationHelper::paginate_klines(
        &http_client,
        endpoint,
        symbol,
        interval,
        start_time,
        end_time,
        fetch_fn,
    )
    .await;

    // Verify result
    assert!(result.is_ok(), "Pagination should succeed");
    let bars = result.unwrap();
    assert_eq!(bars.len(), 3, "Should return 3 bars");
    assert_eq!(bars[0].open_time, 1699920000000);

    // Verify fetch was called once
    assert_eq!(tracker.get_count(), 1, "Fetch should be called once");
}

/// T213: Test pagination with subsequent pages
#[tokio::test]
async fn test_pagination_klines_subsequent_pages() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/klines";
    let symbol = "BTCUSDT";
    let interval = "1m";
    let start_time = 1699920000000;
    let end_time = 1699920300000; // 5 minutes later

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    // Mock fetch function that returns 2 pages: first with 3 bars, second with 2 bars
    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();

        // Extract startTime from params before entering async block
        let start_time_param = params
            .iter()
            .find(|(k, _)| *k == "startTime")
            .map(|(_, v)| v.parse::<i64>().unwrap())
            .unwrap();

        async move {
            let count = tracker.get_count();
            tracker.increment();

            // First call: return 3 bars starting from initial time
            if count == 0 {
                assert_eq!(start_time_param, 1699920000000);
                Ok(create_mock_bars(1699920000000, 3))
            } else {
                // Second call: should start after last bar's close_time + 1
                // Last bar from first page has close_time = 1699920000000 + 2*60000 + 59999 = 1699920179999
                // So next startTime should be 1699920179999 + 1 = 1699920180000
                assert_eq!(start_time_param, 1699920180000);
                Ok(create_mock_bars(1699920180000, 2))
            }
        }
    };

    let result = PaginationHelper::paginate_klines(
        &http_client,
        endpoint,
        symbol,
        interval,
        start_time,
        end_time,
        fetch_fn,
    )
    .await;

    assert!(result.is_ok());
    let bars = result.unwrap();
    assert_eq!(bars.len(), 5, "Should return 5 bars across 2 pages");
    assert_eq!(tracker.get_count(), 2, "Fetch should be called twice");

    // Verify no overlapping timestamps
    for i in 1..bars.len() {
        assert!(
            bars[i].open_time > bars[i - 1].close_time,
            "Bars should not overlap"
        );
    }
}

/// T215: Test fetching first page of aggTrades (fromId-based pagination)
#[tokio::test]
async fn test_pagination_aggtrades_first_page() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/aggTrades";
    let symbol = "BTCUSDT";
    let start_time = 1699920000000;
    let end_time = 1699920060000; // 1 minute later

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    // Mock fetch function
    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();
        let has_from_id = params.iter().any(|(k, _)| *k == "fromId");

        async move {
            let count = tracker.get_count();
            tracker.increment();

            if count == 0 {
                // First page should not have fromId
                assert!(!has_from_id, "First page should not have fromId parameter");
                Ok(create_mock_aggtrades(100000, 1699920000000, 5))
            } else {
                // Return empty to signal end of data
                Ok(Vec::new())
            }
        }
    };

    let result = PaginationHelper::paginate_aggtrades(
        &http_client,
        endpoint,
        symbol,
        start_time,
        end_time,
        None, // No fromId for first page
        fetch_fn,
    )
    .await;

    assert!(result.is_ok());
    let trades = result.unwrap();
    assert_eq!(trades.len(), 5);
    assert_eq!(trades[0].agg_trade_id, 100000);
    assert_eq!(tracker.get_count(), 2, "Should call fetch twice (once for data, once for empty)");
}

/// T216: Test pagination with subsequent pages using fromId
#[tokio::test]
async fn test_pagination_aggtrades_subsequent_pages() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/aggTrades";
    let symbol = "BTCUSDT";
    let start_time = 1699920000000;
    let end_time = 1699920120000;

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();
        let from_id = params
            .iter()
            .find(|(k, _)| *k == "fromId")
            .map(|(_, v)| v.parse::<i64>().unwrap());

        async move {
            let count = tracker.get_count();
            tracker.increment();

            if count == 0 {
                // First page: no fromId
                assert!(from_id.is_none());
                Ok(create_mock_aggtrades(100000, 1699920000000, 3))
            } else if count == 1 {
                // Second page: should have fromId = 100003 (last_id + 1)
                assert_eq!(from_id, Some(100003));
                Ok(create_mock_aggtrades(100003, 1699920060000, 2))
            } else {
                // No more data
                Ok(Vec::new())
            }
        }
    };

    let result = PaginationHelper::paginate_aggtrades(
        &http_client,
        endpoint,
        symbol,
        start_time,
        end_time,
        None,
        fetch_fn,
    )
    .await;

    assert!(result.is_ok());
    let trades = result.unwrap();
    assert_eq!(trades.len(), 5);
    assert_eq!(tracker.get_count(), 3, "Should call fetch 3 times (2 pages + 1 empty)");

    // Verify IDs are sequential
    for i in 1..trades.len() {
        assert!(
            trades[i].agg_trade_id > trades[i - 1].agg_trade_id,
            "Trade IDs should be increasing"
        );
    }
}

/// T218: Test fetching funding rates (typically single page)
#[tokio::test]
async fn test_pagination_funding_rates_single_page() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/fundingRate";
    let symbol = "BTCUSDT";
    let start_time = 1699920000000; // Nov 14, 2023 00:00:00
    let end_time = 1699977600000;   // Nov 14, 2023 16:00:00 (2 funding periods)

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, _params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();
        async move {
            let count = tracker.get_count();
            tracker.increment();

            if count == 0 {
                // Return 2 funding rates (at 00:00 and 08:00)
                Ok(create_mock_funding_rates("BTCUSDT", 1699920000000, 2))
            } else {
                // No more data
                Ok(Vec::new())
            }
        }
    };

    let result = PaginationHelper::paginate_funding_rates(
        &http_client,
        endpoint,
        symbol,
        start_time,
        end_time,
        fetch_fn,
    )
    .await;

    if result.is_err() {
        eprintln!("Funding rates error: {:?}", result.as_ref().unwrap_err());
    }
    assert!(result.is_ok(), "Result should be ok, got: {:?}", result.as_ref().unwrap_err());
    let rates = result.unwrap();
    assert_eq!(rates.len(), 2);
    assert_eq!(rates[0].symbol, "BTCUSDT");
    assert_eq!(tracker.get_count(), 2, "Should call fetch twice (once for data, once for empty)");
}

/// T220: Test pagination stops on empty response
#[tokio::test]
async fn test_pagination_empty_response_handling() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/klines";
    let symbol = "BTCUSDT";
    let interval = "1m";
    let start_time = 1699920000000;
    let end_time = 1699920300000; // 5 minutes

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    // First page returns data, second page returns empty
    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, _params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();
        async move {
            let count = tracker.get_count();
            tracker.increment();

            if count == 0 {
                Ok(create_mock_bars(1699920000000, 2))
            } else {
                // Return empty on second call
                Ok(Vec::new())
            }
        }
    };

    let result = PaginationHelper::paginate_klines(
        &http_client,
        endpoint,
        symbol,
        interval,
        start_time,
        end_time,
        fetch_fn,
    )
    .await;

    assert!(result.is_ok());
    let bars = result.unwrap();
    assert_eq!(bars.len(), 2);
    assert_eq!(tracker.get_count(), 2, "Should stop after empty response");
}

/// T221: Test pagination has max iterations safeguard
#[tokio::test]
async fn test_pagination_max_iterations_safeguard() {
    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

    let endpoint = "/fapi/v1/klines";
    let symbol = "BTCUSDT";
    let interval = "1m";
    let start_time = 1699920000000;
    let end_time = 1699920000000 + (20000 * 60 * 1000); // 20,000 minutes (triggers max iterations)

    let tracker = FetchTracker::new();
    let tracker_clone = tracker.clone();

    // Always return data to simulate infinite loop
    let fetch_fn = move |_client: &BinanceHttpClient, _endpoint: &str, _params: &[(&str, String)]| {
        let tracker = tracker_clone.clone();
        let count = tracker.get_count();

        async move {
            tracker.increment();
            // Always return one bar to keep pagination going
            Ok(create_mock_bars(1699920000000 + (count as i64 * 60_000), 1))
        }
    };

    let result = PaginationHelper::paginate_klines(
        &http_client,
        endpoint,
        symbol,
        interval,
        start_time,
        end_time,
        fetch_fn,
    )
    .await;

    // Should return error due to max iterations exceeded
    assert!(result.is_err(), "Should fail with max iterations error");
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("Max iterations"),
        "Error should mention max iterations. Got: {}",
        error
    );
    assert_eq!(tracker.get_count(), 10_000, "Should stop at max iterations");
}
