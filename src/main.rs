//! Main entry point for trading-data-downloader CLI (T088)

use clap::Parser;
use trading_data_downloader::cli::{Cli, Commands};
use tracing::error;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("trading_data_downloader=info")),
        )
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Execute command
    let result = match cli.command {
        Commands::Download(ref args) => {
            match &args.data_type {
                trading_data_downloader::cli::download::DataType::Bars(bars_args) => {
                    bars_args.execute(&cli).await.map_err(|e| anyhow::anyhow!(e))
                }
                trading_data_downloader::cli::download::DataType::AggTrades(aggtrades_args) => {
                    aggtrades_args.execute(&cli).await.map_err(|e| anyhow::anyhow!(e))
                }
                trading_data_downloader::cli::download::DataType::Funding(funding_args) => {
                    funding_args.execute(&cli).await.map_err(|e| anyhow::anyhow!(e))
                }
            }
        }
        Commands::Sources(ref sources_cmd) => {
            sources_cmd.execute().await.map_err(|e| anyhow::anyhow!(e))
        }
    };

    // Handle result
    if let Err(e) = result {
        error!("Command failed: {}", e);
        std::process::exit(1);
    }
}
