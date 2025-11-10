//! Integration tests for validation command (T180)

use std::path::PathBuf;
use trading_data_downloader::cli::{ValidateCommand, validate::ValidateTarget};

/// Test identifier validation with valid identifier (T180)
#[tokio::test]
async fn test_validate_valid_identifier() {
    let cmd = ValidateCommand {
        target: ValidateTarget::Identifier {
            identifier: "BINANCE:BTC/USDT:USDT".to_string(),
        },
    };

    let result = cmd.execute().await;
    assert!(result.is_ok());
}

/// Test identifier validation with invalid identifier (T180)
#[tokio::test]
async fn test_validate_invalid_identifier() {
    let cmd = ValidateCommand {
        target: ValidateTarget::Identifier {
            identifier: "INVALID".to_string(),
        },
    };

    let result = cmd.execute().await;
    assert!(result.is_err());
}

/// Test identifier validation with missing components (T180)
#[tokio::test]
async fn test_validate_identifier_missing_components() {
    let cmd = ValidateCommand {
        target: ValidateTarget::Identifier {
            identifier: "BINANCE:BTC".to_string(),
        },
    };

    let result = cmd.execute().await;
    assert!(result.is_err());
}

/// Test resume state validation with non-existent directory (T180, T182)
#[tokio::test]
async fn test_validate_resume_state_nonexistent() {
    let cmd = ValidateCommand {
        target: ValidateTarget::ResumeState {
            resume_dir: PathBuf::from("/tmp/nonexistent_resume_dir"),
        },
    };

    let result = cmd.execute().await;
    // Should succeed - just reports no state found
    assert!(result.is_ok());
}

/// Test resume state validation with valid state directory (T180, T182)
#[tokio::test]
async fn test_validate_resume_state_valid() {
    use std::fs;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let resume_dir = temp_dir.path().to_path_buf();

    // Create a valid JSON state file
    let state_file = resume_dir.join("test_state.json");
    let state_content = r#"{"symbol": "BTCUSDT", "progress": 100}"#;
    fs::write(&state_file, state_content).unwrap();

    let cmd = ValidateCommand {
        target: ValidateTarget::ResumeState { resume_dir },
    };

    let result = cmd.execute().await;
    assert!(result.is_ok());
}

/// Test resume state validation with invalid JSON (T180, T182)
#[tokio::test]
async fn test_validate_resume_state_invalid_json() {
    use std::fs;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let resume_dir = temp_dir.path().to_path_buf();

    // Create an invalid JSON state file
    let state_file = resume_dir.join("invalid_state.json");
    let state_content = r#"{"symbol": "BTCUSDT", invalid"#;
    fs::write(&state_file, state_content).unwrap();

    let cmd = ValidateCommand {
        target: ValidateTarget::ResumeState { resume_dir },
    };

    let result = cmd.execute().await;
    assert!(result.is_err());
}

/// Test resume state validation with empty directory (T180, T182)
#[tokio::test]
async fn test_validate_resume_state_empty() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let resume_dir = temp_dir.path().to_path_buf();

    let cmd = ValidateCommand {
        target: ValidateTarget::ResumeState { resume_dir },
    };

    let result = cmd.execute().await;
    assert!(result.is_ok());
}
