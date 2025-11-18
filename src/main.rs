//! Main entry point for trading-data-downloader CLI (T088, T167)

use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;
use trading_data_downloader::cli::{Cli, Commands};
use trading_data_downloader::shutdown::{self, ShutdownCoordinator};

/// Initialize tracing subscriber with optional JSON formatting (T167)
fn init_tracing() {
    // Check if JSON output is requested via environment variable
    let json_format = std::env::var("LOG_FORMAT")
        .map(|v| v.to_lowercase() == "json")
        .unwrap_or(false);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trading_data_downloader=info"));

    if json_format {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing with enhanced configuration
    init_tracing();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Install global shutdown coordinator and Ctrl+C handler
    let shutdown = ShutdownCoordinator::shared();
    shutdown::set_global_shutdown(shutdown.clone());
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::warn!("Ctrl+C received - saving progress...");
                shutdown.request_shutdown();
            }
        }
    });

    // Execute command
    let result = match cli.command {
        Commands::Download(ref args) => match &args.data_type {
            trading_data_downloader::cli::download::DataType::Bars(bars_args) => bars_args
                .execute(&cli, shutdown.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e)),
            trading_data_downloader::cli::download::DataType::AggTrades(aggtrades_args) => {
                aggtrades_args
                    .execute(&cli, shutdown.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
            }
            trading_data_downloader::cli::download::DataType::Funding(funding_args) => funding_args
                .execute(&cli, shutdown.clone())
                .await
                .map_err(|e| anyhow::anyhow!(e)),
        },
        Commands::Sources(ref sources_cmd) => {
            sources_cmd.execute(cli.max_retries).await.map_err(|e| anyhow::anyhow!(e))
        }
        Commands::Validate(ref validate_cmd) => {
            validate_cmd.execute().await.map_err(|e| anyhow::anyhow!(e))
        }
    };

    // Handle result
    if let Err(e) = result {
        error!("Command failed: {}", e);
        std::process::exit(1);
    }
}
