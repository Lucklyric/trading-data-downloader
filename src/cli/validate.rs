//! Validation subcommand (T181, T182)

use clap::Parser;
use crate::identifier::ExchangeIdentifier;
use super::CliError;
use std::path::PathBuf;

/// Validate command for checking identifiers and resume state
#[derive(Parser, Debug)]
pub struct ValidateCommand {
    /// What to validate
    #[command(subcommand)]
    pub target: ValidateTarget,
}

/// Target type for validation
#[derive(clap::Subcommand, Debug)]
pub enum ValidateTarget {
    /// Validate an exchange identifier (T181)
    Identifier {
        /// Identifier to validate (e.g., BINANCE:BTC/USDT:USDT)
        identifier: String,
    },
    /// Validate resume state integrity (T182)
    ResumeState {
        /// Resume state directory
        #[arg(long, default_value = ".resume")]
        resume_dir: PathBuf,
    },
}

impl ValidateCommand {
    /// Execute the validation command
    pub async fn execute(&self) -> Result<(), CliError> {
        match &self.target {
            ValidateTarget::Identifier { identifier } => {
                self.validate_identifier(identifier)
            }
            ValidateTarget::ResumeState { resume_dir } => {
                self.validate_resume_state(resume_dir)
            }
        }
    }

    /// Validate an exchange identifier (T181)
    fn validate_identifier(&self, identifier: &str) -> Result<(), CliError> {
        match ExchangeIdentifier::parse(identifier) {
            Ok(id) => {
                println!("Valid identifier: {}", id);
                println!("  Exchange: {}", id.exchange());
                println!("  Base: {}", id.base());
                println!("  Quote: {}", id.quote());
                println!("  Settlement: {}", id.settle());
                println!("  Filesystem safe: {}", id.to_filesystem_safe());
                Ok(())
            }
            Err(e) => {
                eprintln!("Invalid identifier: {}", e);
                Err(CliError::InvalidArgument(e.to_string()))
            }
        }
    }

    /// Validate resume state integrity (T182)
    fn validate_resume_state(&self, resume_dir: &PathBuf) -> Result<(), CliError> {
        if !resume_dir.exists() {
            println!("No resume state found at {}", resume_dir.display());
            return Ok(());
        }

        if !resume_dir.is_dir() {
            return Err(CliError::InvalidArgument(
                format!("{} is not a directory", resume_dir.display())
            ));
        }

        let state_files: Vec<_> = std::fs::read_dir(resume_dir)
            .map_err(|e| CliError::InvalidArgument(format!("Failed to read resume dir: {}", e)))?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
            .collect();

        if state_files.is_empty() {
            println!("Resume directory exists but contains no state files");
        } else {
            println!("Found {} resume state file(s)", state_files.len());

            let mut valid_count = 0;
            let mut invalid_count = 0;

            for file in state_files {
                let path = file.path();
                let filename = path.file_name().unwrap().to_string_lossy();

                // Try to read and parse the JSON file
                match std::fs::read_to_string(&path) {
                    Ok(content) => {
                        match serde_json::from_str::<serde_json::Value>(&content) {
                            Ok(_) => {
                                println!("  - {} (valid JSON)", filename);
                                valid_count += 1;
                            }
                            Err(e) => {
                                println!("  - {} (invalid JSON: {})", filename, e);
                                invalid_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        println!("  - {} (cannot read: {})", filename, e);
                        invalid_count += 1;
                    }
                }
            }

            println!("\nSummary:");
            println!("  Valid files: {}", valid_count);
            if invalid_count > 0 {
                println!("  Invalid files: {}", invalid_count);
                return Err(CliError::InvalidArgument(
                    format!("Found {} invalid resume state file(s)", invalid_count)
                ));
            }
        }

        Ok(())
    }
}
