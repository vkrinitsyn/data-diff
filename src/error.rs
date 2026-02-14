use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataDiffError {
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Connection pool error: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),

    #[error("No primary key found for table {0}")]
    NoPrimaryKey(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column mismatch between {ref_table} and {target_table}")]
    ColumnMismatch {
        ref_table: String,
        target_table: String,
    },

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Operation cancelled")]
    Cancelled,

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Checksum error: {0}")]
    Checksum(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

pub type Result<T> = std::result::Result<T, DataDiffError>;
