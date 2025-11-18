use std::path::PathBuf;
use std::str::FromStr;
use trading_data_downloader::output::{DataType, OutputPathBuilder, YearMonth};
use trading_data_downloader::Interval;

#[test]
fn test_sanitize_venue_replaces_special_chars() {
    // Test: hyphens, slashes, colons, backslashes → underscores
    // Identifier format: "EXCHANGE-WITH-HYPHENS:BASE/QUOTE:SETTLEMENT"
    let builder = OutputPathBuilder::new(
        PathBuf::from("data"),
        "BINANCE-FUTURES:BTC/USDT:USDT",
        "BTCUSDT",
    )
    .with_data_type(DataType::AggTrades)
    .with_month(YearMonth {
        year: 2024,
        month: 1,
    });

    let path = builder.build().unwrap();
    let path_str = path.to_string_lossy();
    // "BINANCE-FUTURES" → "binance_futures" (hyphen → underscore)
    assert!(
        path_str.contains("binance_futures"),
        "Expected path to contain 'binance_futures', got: {}",
        path_str
    );
}

#[test]
fn test_extract_venue_usdt_futures() {
    let builder = OutputPathBuilder::new(PathBuf::from("data"), "BINANCE:BTC/USDT:USDT", "BTCUSDT")
        .with_data_type(DataType::AggTrades)
        .with_month(YearMonth {
            year: 2024,
            month: 1,
        });

    let path = builder.build().unwrap();
    assert!(path.starts_with("data/binance_futures_usdt"));
}

#[test]
fn test_extract_venue_coin_futures() {
    let builder = OutputPathBuilder::new(PathBuf::from("data"), "BINANCE:BTC/BTC:BTC", "BTCBTC")
        .with_data_type(DataType::AggTrades)
        .with_month(YearMonth {
            year: 2024,
            month: 1,
        });

    let path = builder.build().unwrap();
    assert!(path.starts_with("data/binance_futures_coin"));
}

#[test]
fn test_year_month_format() {
    let ym = YearMonth {
        year: 2024,
        month: 1,
    };
    assert_eq!(ym.to_string(), "2024-01");

    let ym2 = YearMonth {
        year: 2024,
        month: 12,
    };
    assert_eq!(ym2.to_string(), "2024-12");
}

#[test]
fn test_build_path_bars() {
    let builder = OutputPathBuilder::new(PathBuf::from("data"), "BINANCE:BTC/USDT:USDT", "BTCUSDT")
        .with_data_type(DataType::Bars)
        .with_interval(Interval::from_str("1m").unwrap())
        .with_month(YearMonth {
            year: 2024,
            month: 1,
        });

    let expected = PathBuf::from("data/binance_futures_usdt/BTCUSDT/BTCUSDT-bars-1m-2024-01.csv");
    assert_eq!(builder.build().unwrap(), expected);
}

#[test]
fn test_build_path_aggtrades() {
    let builder = OutputPathBuilder::new(PathBuf::from("data"), "BINANCE:BTC/USDT:USDT", "BTCUSDT")
        .with_data_type(DataType::AggTrades)
        .with_month(YearMonth {
            year: 2024,
            month: 2,
        });

    let expected = PathBuf::from("data/binance_futures_usdt/BTCUSDT/BTCUSDT-aggtrades-2024-02.csv");
    assert_eq!(builder.build().unwrap(), expected);
}

#[test]
fn test_build_path_funding() {
    let builder = OutputPathBuilder::new(PathBuf::from("data"), "BINANCE:BTC/USDT:USDT", "BTCUSDT")
        .with_data_type(DataType::Funding)
        .with_month(YearMonth {
            year: 2024,
            month: 3,
        });

    let expected = PathBuf::from("data/binance_futures_usdt/BTCUSDT/BTCUSDT-funding-2024-03.csv");
    assert_eq!(builder.build().unwrap(), expected);
}

#[test]
fn test_year_month_from_timestamp() {
    // 2024-01-15 12:34:56 UTC = 1705323296000 ms
    let ym = YearMonth::from_timestamp_ms(1705323296000);
    assert_eq!(ym.year, 2024);
    assert_eq!(ym.month, 1);
}

#[test]
fn test_next_month_start_timestamp() {
    let ym = YearMonth {
        year: 2024,
        month: 1,
    };
    let next_ms = ym.next_month_start_timestamp_ms();

    // 2024-02-01 00:00:00 UTC = 1706745600000 ms
    assert_eq!(next_ms, 1706745600000);
}

#[test]
fn test_next_month_start_december_rollover() {
    let ym = YearMonth {
        year: 2024,
        month: 12,
    };
    let next_ms = ym.next_month_start_timestamp_ms();

    // 2025-01-01 00:00:00 UTC = 1735689600000 ms
    assert_eq!(next_ms, 1735689600000);
}

// Edge case tests for split_into_month_ranges

#[test]
fn test_split_into_month_ranges_single_month() {
    use trading_data_downloader::output::split_into_month_ranges;

    // 2024-01-15 00:00:00 to 2024-01-20 00:00:00 (same month)
    let start_ms = 1705276800000;
    let end_ms = 1705708800000;

    let ranges = split_into_month_ranges(start_ms, end_ms);

    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].month.year, 2024);
    assert_eq!(ranges[0].month.month, 1);
    assert_eq!(ranges[0].start_ms, start_ms);
    assert_eq!(ranges[0].end_ms, end_ms);
}

#[test]
fn test_split_into_month_ranges_cross_year() {
    use trading_data_downloader::output::split_into_month_ranges;

    // 2024-12-15 00:00:00 to 2025-02-10 00:00:00 (across year boundary)
    let start_ms = 1734220800000; // 2024-12-15
    let end_ms = 1739145600000; // 2025-02-10

    let ranges = split_into_month_ranges(start_ms, end_ms);

    // Should have 3 months: Dec 2024, Jan 2025, Feb 2025
    assert_eq!(ranges.len(), 3);

    // December 2024
    assert_eq!(ranges[0].month.year, 2024);
    assert_eq!(ranges[0].month.month, 12);
    assert_eq!(ranges[0].start_ms, start_ms);
    assert!(ranges[0].end_ms < end_ms);

    // January 2025
    assert_eq!(ranges[1].month.year, 2025);
    assert_eq!(ranges[1].month.month, 1);

    // February 2025
    assert_eq!(ranges[2].month.year, 2025);
    assert_eq!(ranges[2].month.month, 2);
    assert_eq!(ranges[2].end_ms, end_ms);
}

#[test]
fn test_split_into_month_ranges_end_at_month_boundary() {
    use trading_data_downloader::output::split_into_month_ranges;

    // 2024-01-01 00:00:00 to 2024-02-01 00:00:00 (exactly at boundary)
    let start_ms = 1704067200000; // 2024-01-01 00:00:00 UTC
    let end_ms = 1706745600000; // 2024-02-01 00:00:00 UTC

    let ranges = split_into_month_ranges(start_ms, end_ms);

    // Should have 1 month: January (end exactly at Feb 1st boundary)
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].month.year, 2024);
    assert_eq!(ranges[0].month.month, 1);
    assert_eq!(ranges[0].start_ms, start_ms);
    assert_eq!(ranges[0].end_ms, end_ms);
}

#[test]
fn test_split_into_month_ranges_partial_month() {
    use trading_data_downloader::output::split_into_month_ranges;

    // 2024-03-10 12:00:00 to 2024-03-15 18:00:00 (partial month, mid-day times)
    let start_ms = 1710072000000;
    let end_ms = 1710526800000;

    let ranges = split_into_month_ranges(start_ms, end_ms);

    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].month.year, 2024);
    assert_eq!(ranges[0].month.month, 3);
    assert_eq!(ranges[0].start_ms, start_ms);
    assert_eq!(ranges[0].end_ms, end_ms);
}

#[test]
fn test_split_into_month_ranges_three_months() {
    use trading_data_downloader::output::split_into_month_ranges;

    // 2024-01-01 to 2024-03-31 (three full months)
    let start_ms = 1704067200000; // 2024-01-01 00:00:00
    let end_ms = 1711843200000; // 2024-03-31 00:00:00

    let ranges = split_into_month_ranges(start_ms, end_ms);

    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0].month.month, 1);
    assert_eq!(ranges[1].month.month, 2);
    assert_eq!(ranges[2].month.month, 3);
}
