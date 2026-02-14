use std::fmt;

use postgres_types::Type;
use serde::{Deserialize, Serialize};

/// Chunk strategy determined by table size and available timestamp column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkStrategy {
    /// Small table (<100K rows): chunk by ID.
    IdChunkSmall,
    /// Medium table (100K-1M rows): chunk by ID.
    IdChunkMedium,
    /// Large table (>1M rows) with timestamp column: MD5 hash comparison on time-based chunks.
    Md5TimestampChunk {
        timestamp_column: String,
        min_ts: chrono::NaiveDateTime,
        max_ts: chrono::NaiveDateTime,
        chunk_days: f64,
    },
    /// Large table (>1M rows) without timestamp, but excluded from strict comparison.
    IdChunkLargeExcluded,
}

impl fmt::Display for ChunkStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChunkStrategy::IdChunkSmall => write!(f, "IdChunkSmall"),
            ChunkStrategy::IdChunkMedium => write!(f, "IdChunkMedium"),
            ChunkStrategy::Md5TimestampChunk { timestamp_column, chunk_days, .. } => {
                write!(f, "Md5TimestampChunk(col={}, chunk_days={:.1})", timestamp_column, chunk_days)
            }
            ChunkStrategy::IdChunkLargeExcluded => write!(f, "IdChunkLargeExcluded"),
        }
    }
}

/// Analysis result for a single table, including the chosen chunk strategy and ETC estimate.
#[derive(Debug, Clone)]
pub struct TableAnalysis {
    pub ref_table: String,
    pub target_table: String,
    pub row_count: i64,
    pub timestamp_column: Option<String>,
    pub strategy: ChunkStrategy,
    pub estimated_seconds: f64,
}

/// Result of comparing schemas between reference and target for a single table.
#[derive(Debug, Clone)]
pub enum SchemaComparisonResult {
    /// Schemas match exactly (only comparing shared columns).
    Match,
    /// Target has all reference columns plus extra columns.
    TargetSuperset { extra_columns: Vec<String> },
    /// Table is in the exclusion list.
    Excluded,
    /// Schemas differ in incompatible ways.
    Mismatch { message: String },
}

/// Dynamic value extracted from a PostgreSQL row.
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Uuid(uuid::Uuid),
    Json(serde_json::Value),
    /// NUMERIC/DECIMAL stored as string to avoid precision loss.
    Numeric(String),
}

/// Metadata for a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: Type,
    pub is_pk: bool,
    pub is_identity: bool,
    /// Ordinal position (0-based) in the table's column list.
    pub ordinal: usize,
}

/// Result of attempting to set up a table pair for comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDiffStatus {
    Ok,
    ColumnMismatch,
    ReferenceNotFound,
    TargetNotFound,
    NoPrimaryKey,
}

/// A row is a vector of column values in ordinal order.
pub type Row = Vec<RowValue>;

/// Statistics for the INSERT/UPDATE phase.
#[derive(Debug, Clone, Default)]
pub struct DiffStats {
    pub inserts: u64,
    pub updates: u64,
}

/// Statistics for a single table's diff.
#[derive(Debug, Clone, Default)]
pub struct TableDiffReport {
    pub table_name: String,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub insert_file: Option<String>,
    pub update_file: Option<String>,
    pub delete_file: Option<String>,
}

/// Overall diff report.
#[derive(Debug, Clone, Default)]
pub struct DiffReport {
    pub tables: Vec<TableDiffReport>,
    pub warnings: Vec<String>,
    pub master_script: Option<String>,
}

/// Extract a `RowValue` from a `tokio_postgres::Row` at a given column index
/// based on the column's PostgreSQL type.
pub fn extract_row_value(row: &tokio_postgres::Row, idx: usize, pg_type: &Type) -> RowValue {
    // Check for NULL first via try_get with Option
    match *pg_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(idx) {
            Ok(Some(v)) => RowValue::Bool(v),
            _ => RowValue::Null,
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(idx) {
            Ok(Some(v)) => RowValue::I16(v),
            _ => RowValue::Null,
        },
        Type::INT4 | Type::OID => match row.try_get::<_, Option<i32>>(idx) {
            Ok(Some(v)) => RowValue::I32(v),
            _ => RowValue::Null,
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(idx) {
            Ok(Some(v)) => RowValue::I64(v),
            _ => RowValue::Null,
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(idx) {
            Ok(Some(v)) => RowValue::F32(v),
            _ => RowValue::Null,
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(idx) {
            Ok(Some(v)) => RowValue::F64(v),
            _ => RowValue::Null,
        },
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => RowValue::Text(v),
                _ => RowValue::Null,
            }
        }
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(idx) {
            Ok(Some(v)) => RowValue::Bytes(v),
            _ => RowValue::Null,
        },
        Type::TIMESTAMP => match row.try_get::<_, Option<chrono::NaiveDateTime>>(idx) {
            Ok(Some(v)) => RowValue::Timestamp(v),
            _ => RowValue::Null,
        },
        Type::TIMESTAMPTZ => {
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
                Ok(Some(v)) => RowValue::TimestampTz(v),
                _ => RowValue::Null,
            }
        }
        Type::DATE => match row.try_get::<_, Option<chrono::NaiveDate>>(idx) {
            Ok(Some(v)) => RowValue::Date(v),
            _ => RowValue::Null,
        },
        Type::TIME => match row.try_get::<_, Option<chrono::NaiveTime>>(idx) {
            Ok(Some(v)) => RowValue::Time(v),
            _ => RowValue::Null,
        },
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(idx) {
            Ok(Some(v)) => RowValue::Uuid(v),
            _ => RowValue::Null,
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<serde_json::Value>>(idx) {
            Ok(Some(v)) => RowValue::Json(v),
            _ => RowValue::Null,
        },
        Type::NUMERIC => {
            // Numeric types are read as string via the text representation
            // to avoid precision loss. We query the raw text.
            match row.try_get::<_, Option<String>>(idx) {
                Ok(Some(v)) => RowValue::Numeric(v),
                _ => RowValue::Null,
            }
        }
        // Fallback: try to read as string
        _ => match row.try_get::<_, Option<String>>(idx) {
            Ok(Some(v)) => RowValue::Text(v),
            _ => RowValue::Null,
        },
    }
}

/// Extract an entire row into a `Vec<RowValue>` given column info.
pub fn extract_row(row: &tokio_postgres::Row, columns: &[ColumnInfo]) -> Row {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| extract_row_value(row, idx, &col.pg_type))
        .collect()
}
