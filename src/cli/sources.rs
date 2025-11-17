//! CLI command for listing available data sources (T101-T103)

use crate::fetcher::binance_futures_usdt::BinanceFuturesUsdtFetcher;
use crate::fetcher::DataFetcher;
use crate::registry::IdentifierRegistry;
use anyhow::{Context, Result};
use clap::Args;
use serde_json::json;

/// Sources subcommand
#[derive(Debug, Args)]
pub struct SourcesCommand {
    #[command(subcommand)]
    action: SourcesAction,
}

/// Sources actions
#[derive(Debug, clap::Subcommand)]
enum SourcesAction {
    /// List all registered identifiers and their symbols
    List {
        /// Optional identifier pattern (supports wildcards)
        pattern: Option<String>,

        /// Output format
        #[arg(long, default_value = "human")]
        format: OutputFormat,
    },
}

/// Output format for sources command
#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    /// Human-readable output
    Human,
    /// JSON output
    Json,
}

impl SourcesCommand {
    /// Execute the sources command (T102)
    pub async fn execute(&self) -> Result<()> {
        match &self.action {
            SourcesAction::List { pattern, format } => {
                self.execute_list(pattern.as_deref(), format).await
            }
        }
    }

    /// Execute the list subcommand (T102, T103)
    async fn execute_list(&self, pattern: Option<&str>, format: &OutputFormat) -> Result<()> {
        // Load registry
        let registry = IdentifierRegistry::load_embedded()?;

        // If pattern is provided, resolve it
        let identifiers = if let Some(p) = pattern {
            registry.resolve_pattern(p)?
        } else {
            // List all registered identifiers
            registry.list_all()
        };

        // For each identifier, fetch symbols if it's BINANCE
        let mut all_results = Vec::new();

        for identifier in identifiers {
            let identifier_str = identifier.to_string();

            // Only BINANCE is currently supported
            if identifier_str.starts_with("BINANCE:") {
                // Determine if USDT-margined or COIN-margined
                if identifier_str.contains(":USDT") {
                    let fetcher = BinanceFuturesUsdtFetcher::new(5); // Use default max_retries
                    match fetcher.list_symbols().await {
                        Ok(symbols) => {
                            for symbol in symbols {
                                all_results.push(json!({
                                    "identifier": identifier_str,
                                    "symbol": symbol.symbol,
                                    "pair": symbol.pair,
                                    "contract_type": symbol.contract_type.to_string(),
                                    "status": symbol.status.to_string(),
                                    "base_asset": symbol.base_asset,
                                    "quote_asset": symbol.quote_asset,
                                    "margin_asset": symbol.margin_asset,
                                    "tick_size": symbol.tick_size.to_string(),
                                    "step_size": symbol.step_size.to_string(),
                                }));
                            }
                        }
                        Err(e) => {
                            eprintln!("Error fetching symbols for {identifier_str}: {e}");
                        }
                    }
                }
                // TODO: Add COIN-margined support in Phase 7
            }
        }

        // T103: Output formatting
        match format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&all_results)
                        .context("Failed to serialize results to JSON")?
                );
            }
            OutputFormat::Human => {
                println!("Found {} symbols:\n", all_results.len());
                for result in all_results {
                    println!(
                        "{} | {} | {} | tick={} | step={}",
                        result["identifier"].as_str().unwrap(),
                        result["symbol"].as_str().unwrap(),
                        result["contract_type"].as_str().unwrap(),
                        result["tick_size"].as_str().unwrap(),
                        result["step_size"].as_str().unwrap()
                    );
                }
            }
        }

        Ok(())
    }
}
