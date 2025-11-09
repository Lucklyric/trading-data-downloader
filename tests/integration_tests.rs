//! Integration tests module loader

mod contract {
    pub mod binance_futures_coin_api;
    pub mod binance_futures_usdt_api;
}

mod integration {
    pub mod download_bars;
    pub mod download_bars_archive;
    pub mod download_trades;
    pub mod identifier_validation;
    pub mod rate_limiting;
    pub mod resume_capability;
    pub mod symbol_discovery;
}

mod unit {
    pub mod fetcher_factory;
}
