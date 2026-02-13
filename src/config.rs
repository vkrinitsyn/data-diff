use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;

/// A pair of tables to compare.
#[derive(Debug, Clone)]
pub struct TablePair {
    /// Reference table name (schema.table or just table).
    pub reference: String,
    /// Target table name (schema.table or just table).
    pub target: String,
}

/// Progress event emitted during diff processing.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Starting comparison for a table pair.
    TableStart {
        table: String,
        table_index: usize,
        total_tables: usize,
    },
    /// Row processed during INSERT/UPDATE phase.
    RowProcessed { table: String, row_number: u64 },
    /// INSERT/UPDATE phase completed for a table.
    TableDiffComplete {
        table: String,
        inserts: u64,
        updates: u64,
    },
    /// DELETE phase completed for a table.
    TableDeleteComplete { table: String, deletes: u64 },
    /// Sorting tables by foreign key dependencies.
    SortingDependencies,
    /// Starting schema comparison phase.
    SchemaCompareStart,
    /// Schema comparison result for a table.
    SchemaCompareResult { table: String, result: String },
    /// Starting ETC estimation phase.
    EstimationStart { total_tables: usize },
    /// ETC estimation complete for a single table.
    EstimationTableDone {
        table: String,
        estimated_seconds: f64,
        strategy: String,
    },
    /// ETC estimation complete for all tables.
    EstimationComplete { total_estimated_seconds: f64 },
    /// Progress within a table's MD5 timestamp chunk comparison.
    ChunkProgress {
        table: String,
        chunk_index: usize,
        total_chunks: usize,
    },
}

/// Configuration for a data diff operation.
#[derive(Clone)]
pub struct DiffConfig {
    /// Connection pool for the reference (source) database.
    pub reference_pool: PgPool,
    /// Connection pool for the target database.
    pub target_pool: PgPool,
    /// Explicit table pairs to compare. If empty, tables are discovered from schemas.
    pub tables: Vec<TablePair>,
    /// Directory where output files are written.
    pub output_dir: PathBuf,
    /// Path for the master SQL script.
    pub output_file: PathBuf,
    /// Whether to generate DELETE statements for rows missing in reference.
    /// Default: false (matches original doc).
    pub include_delete: bool,
    /// Number of rows to fetch per chunk for comparison. Default: 25.
    pub chunk_size: usize,
    /// Column names to ignore during comparison (case-insensitive).
    pub columns_to_ignore: HashSet<String>,
    /// Alternate PK columns per table: table_name -> list of column names.
    pub alternate_keys: HashMap<String, Vec<String>>,
    /// Exclude the real PK columns from INSERT when alternate keys are used.
    pub exclude_real_pk: bool,
    /// Exclude ignored columns from generated SQL output.
    pub exclude_ignored_columns: bool,
    /// Additional WHERE condition applied to all reference table queries.
    /// Do not include the WHERE keyword itself.
    pub table_where: Option<String>,
    /// Write all SQL into a single file instead of separate per-table files.
    pub single_file: bool,
    /// Reference schema for auto-discovering tables. When set and `tables` is empty,
    /// all tables from this schema are compared.
    pub reference_schema: Option<String>,
    /// Target schema to compare against. Used with `reference_schema`.
    pub target_schema: Option<String>,
    /// Table names to exclude from comparison (case-insensitive).
    pub exclude_tables: HashSet<String>,
    /// Sort generated scripts by foreign key dependencies.
    /// INSERT/UPDATE scripts sorted for insert order, DELETE scripts for delete order.
    /// Default: true.
    pub check_dependencies: bool,
    /// Controls whether identity/auto-increment columns are included in generated SQL.
    /// `None` = use default behavior (exclude identity from INSERT),
    /// `Some(true)` = always include, `Some(false)` = always exclude.
    pub include_identity_columns: Option<bool>,
    /// Override auto-detect timestamp column name for chunking large tables.
    pub timestamp_column: Option<String>,
    /// Force run even if ETC exceeds `max_etc_seconds`.
    pub force: bool,
    /// Emit progress events (default: true).
    pub show_progress: bool,
    /// Reference database name for schema_guard_tokio schema loading.
    pub reference_db_name: Option<String>,
    /// Target database name for schema_guard_tokio schema loading.
    pub target_db_name: Option<String>,
    /// Maximum estimated time in seconds before aborting (default: 10800.0 = 3h).
    pub max_etc_seconds: f64,
    /// Optional output stream. When set, all SQL is written to this stream
    /// instead of per-table files. The stream receives the same content that
    /// would otherwise go into the master script and individual table files.
    pub output_stream: Option<Arc<Mutex<dyn io::Write + Send>>>,
    /// Optional progress callback.
    pub on_progress: Option<Arc<dyn Fn(ProgressEvent) + Send + Sync>>,
}

impl std::fmt::Debug for DiffConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffConfig")
            .field("tables", &self.tables)
            .field("output_dir", &self.output_dir)
            .field("output_file", &self.output_file)
            .field("include_delete", &self.include_delete)
            .field("chunk_size", &self.chunk_size)
            .field("columns_to_ignore", &self.columns_to_ignore)
            .field("alternate_keys", &self.alternate_keys)
            .field("exclude_real_pk", &self.exclude_real_pk)
            .field("exclude_ignored_columns", &self.exclude_ignored_columns)
            .field("table_where", &self.table_where)
            .field("single_file", &self.single_file)
            .field("reference_schema", &self.reference_schema)
            .field("target_schema", &self.target_schema)
            .field("exclude_tables", &self.exclude_tables)
            .field("check_dependencies", &self.check_dependencies)
            .field("include_identity_columns", &self.include_identity_columns)
            .field("timestamp_column", &self.timestamp_column)
            .field("force", &self.force)
            .field("show_progress", &self.show_progress)
            .field("reference_db_name", &self.reference_db_name)
            .field("target_db_name", &self.target_db_name)
            .field("max_etc_seconds", &self.max_etc_seconds)
            .field("output_stream", &self.output_stream.as_ref().map(|_| "..."))
            .field("on_progress", &self.on_progress.as_ref().map(|_| "..."))
            .finish()
    }
}

impl DiffConfig {
    pub fn new(reference_pool: PgPool, target_pool: PgPool, output_file: PathBuf) -> Self {
        let output_dir = output_file
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));

        Self {
            reference_pool,
            target_pool,
            tables: Vec::new(),
            output_dir,
            output_file,
            include_delete: false,
            chunk_size: 25,
            columns_to_ignore: HashSet::new(),
            alternate_keys: HashMap::new(),
            exclude_real_pk: false,
            exclude_ignored_columns: false,
            table_where: None,
            single_file: false,
            reference_schema: None,
            target_schema: None,
            exclude_tables: HashSet::new(),
            check_dependencies: true,
            include_identity_columns: None,
            timestamp_column: None,
            force: false,
            show_progress: true,
            reference_db_name: None,
            target_db_name: None,
            max_etc_seconds: 10800.0,
            output_stream: None,
            on_progress: None,
        }
    }

    /// Emit a progress event if a callback is configured.
    pub(crate) fn emit_progress(&self, event: ProgressEvent) {
        if let Some(ref cb) = self.on_progress {
            cb(event);
        }
    }
}

/// Parse a possibly schema-qualified table name into (schema, table).
/// Returns (None, name) if no schema is specified.
pub fn parse_table_name(name: &str) -> (Option<&str>, &str) {
    if let Some(dot_pos) = name.find('.') {
        let schema = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        (Some(schema), table)
    } else {
        (None, name)
    }
}
