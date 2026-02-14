use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};

use tracing::{debug, info, warn};

use crate::checksum::{self, TableDiffSummary, compare_checksums, load_checksums, save_checksums};
use crate::config::{DiffConfig, ProgressEvent, parse_table_name};
use crate::delete_sync::TableDeleteSync;
use crate::error::{DataDiffError, Result};
use crate::estimation::analyze_and_estimate;
use crate::metadata::{get_columns, resolve_pk_columns};
use crate::schema_compare::compare_all_schemas;
use crate::table_diff::TableDataDiff;
use crate::types::{
    ColumnInfo, DiffReport, SchemaComparisonResult, TableAnalysis, TableDiffReport,
};

/// Top-level orchestrator for comparing data between two PostgreSQL databases.
///
/// Runs in 4 phases:
/// 1. **Schema Comparison** - Compare table schemas, filter out incompatible tables
/// 2. **Analysis & ETC** - Determine chunk strategies and estimate time to completion
/// 3. **Per-Table Diff** - Generate INSERT/UPDATE/DELETE scripts
/// 4. **Master Script** - Write the master SQL script
pub struct DataDiff {
    config: DiffConfig,
    cancelled: bool,
}

impl DataDiff {
    pub fn new(config: DiffConfig) -> Self {
        Self {
            config,
            cancelled: false,
        }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Run the full data diff operation.
    ///
    /// Routes to the appropriate method based on config:
    /// - `checksum_only` -> `run_compute_checksums()`: compute + save checksums, no SQL
    /// - both checksum files set -> `run_from_checksums()`: load checksums, compare, generate SQL for diffs only
    /// - otherwise -> default row-by-row comparison
    pub async fn run(&mut self) -> Result<DiffReport> {
        if self.config.checksum_only {
            return self.run_compute_checksums().await;
        }
        if self.config.reference_checksum_file.is_some()
            && self.config.target_checksum_file.is_some()
        {
            return self.run_from_checksums().await;
        }
        self.run_default().await
    }

    /// Compute MD5 checksums for both reference and target databases and save to YAML.
    ///
    /// This is the "checksum calculation only" mode. It:
    /// 1. Connects to both databases
    /// 2. Runs schema comparison and analysis to determine chunk strategies
    /// 3. Computes MD5 checksums for every row in every table
    /// 4. Saves checksum manifests to the configured paths
    /// 5. Returns a summary report (no SQL files generated)
    ///
    /// Requires `save_reference_checksums` and/or `save_target_checksums` to be set.
    pub async fn run_compute_checksums(&mut self) -> Result<DiffReport> {
        if self.config.save_reference_checksums.is_none()
            && self.config.save_target_checksums.is_none()
        {
            return Err(DataDiffError::Config(
                "checksum_only mode requires at least one of save_reference_checksums \
                 or save_target_checksums to be set"
                    .to_string(),
            ));
        }

        let ref_conn = self.config.reference_pool.get().await?;
        let target_conn = self.config.target_pool.get().await?;

        let mut report = DiffReport::default();

        // ---------------------------------------------------------------
        // Phase 1: Schema Comparison
        // ---------------------------------------------------------------
        let (active_tables, schema_warnings) = self.run_schema_comparison().await?;
        report.warnings.extend(schema_warnings);

        // ---------------------------------------------------------------
        // Phase 2: Analysis & ETC Estimation
        // ---------------------------------------------------------------
        let analyses = analyze_and_estimate(
            &ref_conn,
            &target_conn,
            &active_tables,
            &self.config,
        )
        .await?;

        // ---------------------------------------------------------------
        // Phase 3: Compute Checksums
        // ---------------------------------------------------------------

        // Build table info for ref
        if self.config.save_reference_checksums.is_some() {
            let mut ref_table_info: Vec<(String, Vec<ColumnInfo>, Vec<ColumnInfo>)> = Vec::new();
            for pair in &active_tables {
                let ref_columns = get_columns(&ref_conn, &pair.reference).await?;
                let (pk_cols, _) = resolve_pk_columns(
                    &ref_columns,
                    self.config
                        .alternate_keys
                        .get(&pair.reference)
                        .map(|v| v.as_slice()),
                );
                ref_table_info.push((pair.reference.clone(), ref_columns, pk_cols));
            }

            let ref_manifest = checksum::compute_db_checksums(
                &ref_conn,
                &ref_table_info,
                &analyses,
                self.config.reference_db_name.as_deref(),
                self.config.reference_schema.as_deref(),
            )
            .await?;

            let path = self.config.save_reference_checksums.as_ref().unwrap();
            save_checksums(&ref_manifest, path)?;
            info!("Saved reference checksums to {}", path.display());

            for tc in &ref_manifest.tables {
                let total_hashes: usize = tc.chunks.iter().map(|c| c.row_hashes.len()).sum();
                report.tables.push(TableDiffReport {
                    table_name: tc.table_name.clone(),
                    inserts: total_hashes as u64,
                    ..Default::default()
                });
            }
        }

        // Build table info for target
        if self.config.save_target_checksums.is_some() {
            let mut target_table_info: Vec<(String, Vec<ColumnInfo>, Vec<ColumnInfo>)> = Vec::new();
            for pair in &active_tables {
                let target_columns = get_columns(&target_conn, &pair.target).await?;
                let (pk_cols, _) = resolve_pk_columns(
                    &target_columns,
                    self.config
                        .alternate_keys
                        .get(&pair.target)
                        .map(|v| v.as_slice()),
                );
                target_table_info.push((pair.target.clone(), target_columns, pk_cols));
            }

            let target_analyses: Vec<TableAnalysis> = analyses
                .iter()
                .map(|a| TableAnalysis {
                    ref_table: a.target_table.clone(),
                    target_table: a.target_table.clone(),
                    row_count: a.row_count,
                    timestamp_column: a.timestamp_column.clone(),
                    strategy: a.strategy.clone(),
                    estimated_seconds: a.estimated_seconds,
                })
                .collect();

            let target_manifest = checksum::compute_db_checksums(
                &target_conn,
                &target_table_info,
                &target_analyses,
                self.config.target_db_name.as_deref(),
                self.config.target_schema.as_deref(),
            )
            .await?;

            let path = self.config.save_target_checksums.as_ref().unwrap();
            save_checksums(&target_manifest, path)?;
            info!("Saved target checksums to {}", path.display());
        }

        Ok(report)
    }

    /// Generate SQL from pre-computed checksum files.
    ///
    /// This is the "SQL from checksums" mode. It:
    /// 1. Loads reference and target checksum manifests from YAML files
    /// 2. Compares them to identify tables/chunks with differences
    /// 3. Connects to both databases
    /// 4. For differing tables only, runs the standard row-by-row comparison to generate SQL
    /// 5. Skips tables where checksums match (significant speedup for large DBs)
    ///
    /// Requires both `reference_checksum_file` and `target_checksum_file` to be set.
    pub async fn run_from_checksums(&mut self) -> Result<DiffReport> {
        let ref_checksum_path = self.config.reference_checksum_file.as_ref().ok_or_else(|| {
            DataDiffError::Config("reference_checksum_file is required for run_from_checksums".to_string())
        })?;
        let target_checksum_path = self.config.target_checksum_file.as_ref().ok_or_else(|| {
            DataDiffError::Config("target_checksum_file is required for run_from_checksums".to_string())
        })?;

        // Load pre-computed checksums
        let ref_manifest = load_checksums(ref_checksum_path)?;
        let target_manifest = load_checksums(target_checksum_path)?;

        info!(
            "Loaded checksums: ref={} tables, target={} tables",
            ref_manifest.tables.len(),
            target_manifest.tables.len()
        );

        // Compare checksums to find differing tables
        let diff_summaries = compare_checksums(&ref_manifest, &target_manifest);

        info!(
            "Checksum comparison: {} tables with differences",
            diff_summaries.len()
        );

        if diff_summaries.is_empty() {
            info!("All checksums match - no SQL to generate");
            return Ok(DiffReport::default());
        }

        // Build set of table names that have differences
        let diff_table_names: HashMap<&str, &TableDiffSummary> = diff_summaries
            .iter()
            .map(|s| (s.table_name.as_str(), s))
            .collect();

        // Log what we found
        for summary in &diff_summaries {
            info!(
                "Table '{}': {} differing chunks, {} missing in target, {} missing in ref, {} modified",
                summary.table_name,
                summary.differing_chunks.len(),
                summary.missing_in_target.len(),
                summary.missing_in_reference.len(),
                summary.modified.len(),
            );
        }

        // Now connect to DBs and generate SQL only for differing tables
        if self.config.output_stream.is_none() {
            fs::create_dir_all(&self.config.output_dir)?;
        }

        let ref_conn = self.config.reference_pool.get().await?;
        let target_conn = self.config.target_pool.get().await?;

        let mut report = DiffReport::default();

        // Schema comparison (to filter out incompatible tables)
        let (active_tables, schema_warnings) = self.run_schema_comparison().await?;
        report.warnings.extend(schema_warnings);

        // Analysis for differing tables only
        let analyses = analyze_and_estimate(
            &ref_conn,
            &target_conn,
            &active_tables,
            &self.config,
        )
        .await?;

        let analysis_map: HashMap<String, TableAnalysis> = analyses
            .into_iter()
            .map(|a| (a.ref_table.clone(), a))
            .collect();

        let use_stream = self.config.output_stream.is_some();

        for (table_idx, table_pair) in active_tables.iter().enumerate() {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let (_, ref_tname) = parse_table_name(&table_pair.reference);
            let (_, target_tname) = parse_table_name(&table_pair.target);

            // Skip tables with no checksum differences
            if !diff_table_names.contains_key(target_tname)
                && !diff_table_names.contains_key(ref_tname)
            {
                debug!("Skipping table {} - checksums match", target_tname);
                continue;
            }

            self.config.emit_progress(ProgressEvent::TableStart {
                table: target_tname.to_string(),
                table_index: table_idx,
                total_tables: active_tables.len(),
            });

            info!(
                "Generating SQL for differing table: {} -> {}",
                ref_tname, target_tname
            );

            let mut table_report = TableDiffReport {
                table_name: target_tname.to_string(),
                ..Default::default()
            };

            let alternate_keys = self
                .config
                .alternate_keys
                .get(target_tname)
                .or_else(|| self.config.alternate_keys.get(ref_tname))
                .cloned();

            let mut diff = TableDataDiff::new(
                &ref_conn,
                &target_conn,
                table_pair.reference.clone(),
                table_pair.target.clone(),
            );
            diff.set_chunk_size(self.config.chunk_size);
            diff.set_columns_to_ignore(self.config.columns_to_ignore.clone());
            diff.set_alternate_keys(alternate_keys.clone());
            diff.set_exclude_real_pk(self.config.exclude_real_pk);
            diff.set_on_progress(self.config.on_progress.clone());

            let table_analysis = analysis_map.get(&table_pair.reference).cloned();
            diff.set_analysis(table_analysis);

            if self.config.exclude_ignored_columns {
                diff.set_exclude_columns(self.config.columns_to_ignore.clone());
            }

            if use_stream {
                let stream = self.config.output_stream.as_ref().unwrap();
                let mut insert_buf: Vec<u8> = Vec::new();
                let mut update_buf: Vec<u8> = Vec::new();

                match diff.execute(&mut insert_buf, &mut update_buf).await {
                    Ok(stats) => {
                        table_report.inserts = stats.inserts;
                        table_report.updates = stats.updates;

                        let mut locked = stream.lock().map_err(|e| {
                            DataDiffError::Config(format!("Failed to lock output stream: {}", e))
                        })?;
                        if !insert_buf.is_empty() {
                            locked.write_all(&insert_buf)?;
                        }
                        if !update_buf.is_empty() {
                            locked.write_all(&update_buf)?;
                        }

                        self.config
                            .emit_progress(ProgressEvent::TableDiffComplete {
                                table: target_tname.to_string(),
                                inserts: stats.inserts,
                                updates: stats.updates,
                            });
                    }
                    Err(e) => {
                        if let Some(warning) = diff_error_to_warning(&e) {
                            warn!("{}", warning);
                            report.warnings.push(warning);
                            report.tables.push(table_report);
                            continue;
                        }
                        return Err(e);
                    }
                }

                // DELETE phase to stream
                if self.config.include_delete {
                    let mut delete_buf: Vec<u8> = Vec::new();

                    let mut del_sync = TableDeleteSync::new(
                        &target_conn,
                        &ref_conn,
                        table_pair.target.clone(),
                        table_pair.reference.clone(),
                    );
                    del_sync.set_chunk_size(self.config.chunk_size);
                    del_sync.set_alternate_keys(alternate_keys);

                    match del_sync.execute(&mut delete_buf).await {
                        Ok(deletes) => {
                            table_report.deletes = deletes;
                            if !delete_buf.is_empty() {
                                let mut locked = stream.lock().map_err(|e| {
                                    DataDiffError::Config(format!(
                                        "Failed to lock output stream: {}",
                                        e
                                    ))
                                })?;
                                locked.write_all(&delete_buf)?;
                            }
                            self.config
                                .emit_progress(ProgressEvent::TableDeleteComplete {
                                    table: target_tname.to_string(),
                                    deletes,
                                });
                        }
                        Err(DataDiffError::NoPrimaryKey(_)) => {
                            debug!(
                                "Skipping delete sync for {} - no PK",
                                table_pair.target
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                // File mode
                let insert_filename = format!("{}_$insert.sql", target_tname);
                let update_filename = format!("{}_$update.sql", target_tname);
                let insert_path = self.config.output_dir.join(&insert_filename);
                let update_path = self.config.output_dir.join(&update_filename);

                let mut insert_file = BufWriter::new(File::create(&insert_path)?);
                let mut update_file = BufWriter::new(File::create(&update_path)?);

                match diff.execute(&mut insert_file, &mut update_file).await {
                    Ok(stats) => {
                        table_report.inserts = stats.inserts;
                        table_report.updates = stats.updates;

                        self.config
                            .emit_progress(ProgressEvent::TableDiffComplete {
                                table: target_tname.to_string(),
                                inserts: stats.inserts,
                                updates: stats.updates,
                            });
                    }
                    Err(e) => {
                        if let Some(warning) = diff_error_to_warning(&e) {
                            warn!("{}", warning);
                            report.warnings.push(warning);
                            cleanup_empty(&insert_path);
                            cleanup_empty(&update_path);
                            report.tables.push(table_report);
                            continue;
                        }
                        return Err(e);
                    }
                }

                drop(insert_file);
                drop(update_file);

                if file_has_content(&insert_path) {
                    table_report.insert_file = Some(insert_filename.clone());
                } else {
                    cleanup_empty(&insert_path);
                }
                if file_has_content(&update_path) {
                    table_report.update_file = Some(update_filename.clone());
                } else {
                    cleanup_empty(&update_path);
                }

                // DELETE phase
                if self.config.include_delete {
                    let delete_filename = format!("{}_$delete.sql", target_tname);
                    let delete_path = self.config.output_dir.join(&delete_filename);
                    let mut delete_file = BufWriter::new(File::create(&delete_path)?);

                    let mut del_sync = TableDeleteSync::new(
                        &target_conn,
                        &ref_conn,
                        table_pair.target.clone(),
                        table_pair.reference.clone(),
                    );
                    del_sync.set_chunk_size(self.config.chunk_size);
                    del_sync.set_alternate_keys(alternate_keys);

                    match del_sync.execute(&mut delete_file).await {
                        Ok(deletes) => {
                            table_report.deletes = deletes;
                            self.config
                                .emit_progress(ProgressEvent::TableDeleteComplete {
                                    table: target_tname.to_string(),
                                    deletes,
                                });
                        }
                        Err(DataDiffError::NoPrimaryKey(_)) => {
                            debug!(
                                "Skipping delete sync for {} - no PK",
                                table_pair.target
                            );
                        }
                        Err(e) => return Err(e),
                    }

                    drop(delete_file);

                    if file_has_content(&delete_path) {
                        table_report.delete_file = Some(delete_filename);
                    } else {
                        cleanup_empty(&delete_path);
                    }
                }
            }

            report.tables.push(table_report);
        }

        // Master Script (file mode only)
        if !self.cancelled && !use_stream {
            self.write_master_script(&report)?;
            report.master_script = Some(
                self.config.output_file.to_string_lossy().into_owned(),
            );
        }

        Ok(report)
    }

    /// Run schema comparison and return active table pairs + warnings.
    async fn run_schema_comparison(
        &self,
    ) -> Result<(Vec<crate::config::TablePair>, Vec<String>)> {
        let mut schema_results: HashMap<String, SchemaComparisonResult> = HashMap::new();
        let mut warnings = Vec::new();

        if self.config.reference_db_name.is_some()
            && self.config.target_db_name.is_some()
            && self.config.reference_schema.is_some()
            && self.config.target_schema.is_some()
        {
            self.config.emit_progress(ProgressEvent::SchemaCompareStart);

            let results = compare_all_schemas(
                &self.config.reference_pool,
                &self.config.target_pool,
                self.config.reference_db_name.as_deref().unwrap(),
                self.config.target_db_name.as_deref().unwrap(),
                self.config.reference_schema.as_deref().unwrap(),
                self.config.target_schema.as_deref().unwrap(),
                &self.config.exclude_tables,
            )
            .await?;

            for (table_name, result) in &results {
                let result_str = match result {
                    SchemaComparisonResult::Match => "Match".to_string(),
                    SchemaComparisonResult::TargetSuperset { extra_columns } => {
                        format!("Target superset (extra: {})", extra_columns.join(", "))
                    }
                    SchemaComparisonResult::Excluded => "Excluded".to_string(),
                    SchemaComparisonResult::Mismatch { message } => {
                        format!("Mismatch: {}", message)
                    }
                };

                self.config
                    .emit_progress(ProgressEvent::SchemaCompareResult {
                        table: table_name.clone(),
                        result: result_str,
                    });

                if let SchemaComparisonResult::Mismatch { message } = result {
                    warn!(
                        "Schema mismatch for table '{}': {}. Skipping.",
                        table_name, message
                    );
                    warnings.push(format!(
                        "Schema mismatch for '{}': {}. Add to exclusion list or define a timestamp column.",
                        table_name, message
                    ));
                }
            }

            schema_results = results;
        }

        let active_tables: Vec<_> = self
            .config
            .tables
            .iter()
            .filter(|pair| {
                let (_, ref_tname) = parse_table_name(&pair.reference);
                !matches!(
                    schema_results.get(ref_tname),
                    Some(SchemaComparisonResult::Mismatch { .. })
                )
            })
            .cloned()
            .collect();

        Ok((active_tables, warnings))
    }

    /// Run the default (original) data diff operation without checksum flow.
    async fn run_default(&mut self) -> Result<DiffReport> {
        // Ensure output directory exists (only needed in file mode)
        if self.config.output_stream.is_none() {
            fs::create_dir_all(&self.config.output_dir)?;
        }

        let ref_conn = self.config.reference_pool.get().await?;
        let target_conn = self.config.target_pool.get().await?;

        let mut report = DiffReport::default();

        // ---------------------------------------------------------------
        // Phase 1: Schema Comparison
        // ---------------------------------------------------------------
        let mut schema_results: HashMap<String, SchemaComparisonResult> = HashMap::new();

        if self.config.reference_db_name.is_some()
            && self.config.target_db_name.is_some()
            && self.config.reference_schema.is_some()
            && self.config.target_schema.is_some()
        {
            self.config.emit_progress(ProgressEvent::SchemaCompareStart);

            let results = compare_all_schemas(
                &self.config.reference_pool,
                &self.config.target_pool,
                self.config.reference_db_name.as_deref().unwrap(),
                self.config.target_db_name.as_deref().unwrap(),
                self.config.reference_schema.as_deref().unwrap(),
                self.config.target_schema.as_deref().unwrap(),
                &self.config.exclude_tables,
            )
            .await?;

            for (table_name, result) in &results {
                let result_str = match result {
                    SchemaComparisonResult::Match => "Match".to_string(),
                    SchemaComparisonResult::TargetSuperset { extra_columns } => {
                        format!("Target superset (extra: {})", extra_columns.join(", "))
                    }
                    SchemaComparisonResult::Excluded => "Excluded".to_string(),
                    SchemaComparisonResult::Mismatch { message } => {
                        format!("Mismatch: {}", message)
                    }
                };

                self.config
                    .emit_progress(ProgressEvent::SchemaCompareResult {
                        table: table_name.clone(),
                        result: result_str,
                    });

                if let SchemaComparisonResult::Mismatch { message } = result {
                    warn!(
                        "Schema mismatch for table '{}': {}. Skipping.",
                        table_name, message
                    );
                    report.warnings.push(format!(
                        "Schema mismatch for '{}': {}. Add to exclusion list or define a timestamp column.",
                        table_name, message
                    ));
                }
            }

            schema_results = results;
        }

        // Filter table pairs: skip tables with schema Mismatch
        let active_tables: Vec<_> = self
            .config
            .tables
            .iter()
            .filter(|pair| {
                let (_, ref_tname) = parse_table_name(&pair.reference);
                match schema_results.get(ref_tname) {
                    Some(SchemaComparisonResult::Mismatch { .. }) => false,
                    _ => true,
                }
            })
            .cloned()
            .collect();

        // ---------------------------------------------------------------
        // Phase 2: Analysis & ETC Estimation
        // ---------------------------------------------------------------
        let analyses = analyze_and_estimate(
            &ref_conn,
            &target_conn,
            &active_tables,
            &self.config,
        )
        .await?;

        // Build analysis lookup by reference table name
        let analysis_map: HashMap<String, TableAnalysis> = analyses
            .into_iter()
            .map(|a| (a.ref_table.clone(), a))
            .collect();

        // ---------------------------------------------------------------
        // Phase 3: Per-Table Diff
        // ---------------------------------------------------------------
        let use_stream = self.config.output_stream.is_some();

        for (table_idx, table_pair) in active_tables.iter().enumerate() {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let (_, ref_tname) = parse_table_name(&table_pair.reference);
            let (_, target_tname) = parse_table_name(&table_pair.target);

            self.config.emit_progress(ProgressEvent::TableStart {
                table: target_tname.to_string(),
                table_index: table_idx,
                total_tables: active_tables.len(),
            });

            info!("Comparing table: {} -> {}", ref_tname, target_tname);

            let mut table_report = TableDiffReport {
                table_name: target_tname.to_string(),
                ..Default::default()
            };

            let alternate_keys = self
                .config
                .alternate_keys
                .get(target_tname)
                .or_else(|| self.config.alternate_keys.get(ref_tname))
                .cloned();

            let mut diff = TableDataDiff::new(
                &ref_conn,
                &target_conn,
                table_pair.reference.clone(),
                table_pair.target.clone(),
            );
            diff.set_chunk_size(self.config.chunk_size);
            diff.set_columns_to_ignore(self.config.columns_to_ignore.clone());
            diff.set_alternate_keys(alternate_keys.clone());
            diff.set_exclude_real_pk(self.config.exclude_real_pk);
            diff.set_on_progress(self.config.on_progress.clone());

            // Pass analysis to the table diff so it can choose the right strategy
            let table_analysis = analysis_map.get(&table_pair.reference).cloned();
            diff.set_analysis(table_analysis);

            if self.config.exclude_ignored_columns {
                diff.set_exclude_columns(self.config.columns_to_ignore.clone());
            }

            if use_stream {
                // Stream mode: write SQL directly to the output stream
                let stream = self.config.output_stream.as_ref().unwrap();
                let mut insert_buf: Vec<u8> = Vec::new();
                let mut update_buf: Vec<u8> = Vec::new();

                match diff.execute(&mut insert_buf, &mut update_buf).await {
                    Ok(stats) => {
                        table_report.inserts = stats.inserts;
                        table_report.updates = stats.updates;

                        // Write buffered SQL to the stream
                        let mut locked = stream.lock().map_err(|e| {
                            DataDiffError::Config(format!("Failed to lock output stream: {}", e))
                        })?;
                        if !insert_buf.is_empty() {
                            locked.write_all(&insert_buf)?;
                        }
                        if !update_buf.is_empty() {
                            locked.write_all(&update_buf)?;
                        }

                        self.config
                            .emit_progress(ProgressEvent::TableDiffComplete {
                                table: target_tname.to_string(),
                                inserts: stats.inserts,
                                updates: stats.updates,
                            });
                    }
                    Err(e) => {
                        if let Some(warning) = diff_error_to_warning(&e) {
                            warn!("{}", warning);
                            report.warnings.push(warning);
                            report.tables.push(table_report);
                            continue;
                        }
                        return Err(e);
                    }
                }

                // DELETE phase to stream
                if self.config.include_delete {
                    let mut delete_buf: Vec<u8> = Vec::new();

                    let mut del_sync = TableDeleteSync::new(
                        &target_conn,
                        &ref_conn,
                        table_pair.target.clone(),
                        table_pair.reference.clone(),
                    );
                    del_sync.set_chunk_size(self.config.chunk_size);
                    del_sync.set_alternate_keys(alternate_keys);

                    match del_sync.execute(&mut delete_buf).await {
                        Ok(deletes) => {
                            table_report.deletes = deletes;
                            if !delete_buf.is_empty() {
                                let mut locked = stream.lock().map_err(|e| {
                                    DataDiffError::Config(format!(
                                        "Failed to lock output stream: {}",
                                        e
                                    ))
                                })?;
                                locked.write_all(&delete_buf)?;
                            }
                            self.config
                                .emit_progress(ProgressEvent::TableDeleteComplete {
                                    table: target_tname.to_string(),
                                    deletes,
                                });
                        }
                        Err(DataDiffError::NoPrimaryKey(_)) => {
                            debug!(
                                "Skipping delete sync for {} - no PK",
                                table_pair.target
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                // File mode: write SQL to per-table files (default)
                let insert_filename = format!("{}_$insert.sql", target_tname);
                let update_filename = format!("{}_$update.sql", target_tname);
                let insert_path = self.config.output_dir.join(&insert_filename);
                let update_path = self.config.output_dir.join(&update_filename);

                let mut insert_file = BufWriter::new(File::create(&insert_path)?);
                let mut update_file = BufWriter::new(File::create(&update_path)?);

                match diff.execute(&mut insert_file, &mut update_file).await {
                    Ok(stats) => {
                        table_report.inserts = stats.inserts;
                        table_report.updates = stats.updates;

                        self.config
                            .emit_progress(ProgressEvent::TableDiffComplete {
                                table: target_tname.to_string(),
                                inserts: stats.inserts,
                                updates: stats.updates,
                            });
                    }
                    Err(e) => {
                        if let Some(warning) = diff_error_to_warning(&e) {
                            warn!("{}", warning);
                            report.warnings.push(warning);
                            cleanup_empty(&insert_path);
                            cleanup_empty(&update_path);
                            report.tables.push(table_report);
                            continue;
                        }
                        return Err(e);
                    }
                }

                drop(insert_file);
                drop(update_file);

                // Track which files have content
                if file_has_content(&insert_path) {
                    table_report.insert_file = Some(insert_filename.clone());
                } else {
                    cleanup_empty(&insert_path);
                }
                if file_has_content(&update_path) {
                    table_report.update_file = Some(update_filename.clone());
                } else {
                    cleanup_empty(&update_path);
                }

                // DELETE phase
                if self.config.include_delete {
                    let delete_filename = format!("{}_$delete.sql", target_tname);
                    let delete_path = self.config.output_dir.join(&delete_filename);
                    let mut delete_file = BufWriter::new(File::create(&delete_path)?);

                    let mut del_sync = TableDeleteSync::new(
                        &target_conn,
                        &ref_conn,
                        table_pair.target.clone(),
                        table_pair.reference.clone(),
                    );
                    del_sync.set_chunk_size(self.config.chunk_size);
                    del_sync.set_alternate_keys(alternate_keys);

                    match del_sync.execute(&mut delete_file).await {
                        Ok(deletes) => {
                            table_report.deletes = deletes;
                            self.config
                                .emit_progress(ProgressEvent::TableDeleteComplete {
                                    table: target_tname.to_string(),
                                    deletes,
                                });
                        }
                        Err(DataDiffError::NoPrimaryKey(_)) => {
                            debug!(
                                "Skipping delete sync for {} - no PK",
                                table_pair.target
                            );
                        }
                        Err(e) => return Err(e),
                    }

                    drop(delete_file);

                    if file_has_content(&delete_path) {
                        table_report.delete_file = Some(delete_filename);
                    } else {
                        cleanup_empty(&delete_path);
                    }
                }
            }

            report.tables.push(table_report);
        }

        // ---------------------------------------------------------------
        // Phase 4: Master Script (file mode only)
        // ---------------------------------------------------------------
        if !self.cancelled && !use_stream {
            self.write_master_script(&report)?;
            report.master_script = Some(
                self.config
                    .output_file
                    .to_string_lossy()
                    .into_owned(),
            );
        }

        Ok(report)
    }

    /// Write the master SQL script that includes all per-table scripts.
    fn write_master_script(&self, report: &DiffReport) -> Result<()> {
        let mut out = BufWriter::new(File::create(&self.config.output_file)?);

        writeln!(out, "-- ----------------------------------------------------------------")?;
        writeln!(out, "-- Data migration script")?;
        writeln!(out, "-- ----------------------------------------------------------------")?;
        writeln!(out)?;
        writeln!(out, "-- Tables included:")?;
        for table in &report.tables {
            writeln!(out, "-- {}", table.table_name)?;
        }
        writeln!(out)?;

        // Warnings
        if !report.warnings.is_empty() {
            writeln!(out, "-- Warnings:")?;
            for w in &report.warnings {
                writeln!(out, "-- {}", w)?;
            }
            writeln!(out)?;
        }

        // INSERT/UPDATE scripts
        writeln!(out, "-- ----------------------")?;
        writeln!(out, "-- UPDATE/INSERT scripts")?;
        writeln!(out, "-- ----------------------")?;

        let mut has_updates = false;
        for table in &report.tables {
            if let Some(ref f) = table.insert_file {
                writeln!(out, "\\i '{}'", f)?;
                has_updates = true;
            } else {
                writeln!(
                    out,
                    "-- No INSERTs for {} necessary",
                    table.table_name
                )?;
            }
            if let Some(ref f) = table.update_file {
                writeln!(out, "\\i '{}'", f)?;
                has_updates = true;
            } else {
                writeln!(
                    out,
                    "-- No UPDATEs for {} necessary",
                    table.table_name
                )?;
            }
        }

        if has_updates {
            writeln!(out)?;
            writeln!(out, "COMMIT;")?;
        }

        // DELETE scripts
        let has_deletes = report.tables.iter().any(|t| t.delete_file.is_some());
        if self.config.include_delete {
            writeln!(out)?;
            writeln!(out, "-- ---------------")?;
            writeln!(out, "-- DELETE scripts")?;
            writeln!(out, "-- ---------------")?;

            for table in &report.tables {
                if let Some(ref f) = table.delete_file {
                    writeln!(out, "\\i '{}'", f)?;
                } else {
                    writeln!(
                        out,
                        "-- No DELETEs for {} necessary",
                        table.table_name
                    )?;
                }
            }

            if has_deletes {
                writeln!(out)?;
                writeln!(out, "COMMIT;")?;
            }
        }

        out.flush()?;
        Ok(())
    }
}

/// Convert recoverable diff errors into warning strings, returning None for fatal errors.
fn diff_error_to_warning(e: &DataDiffError) -> Option<String> {
    match e {
        DataDiffError::TableNotFound(t) => Some(format!("Table not found: {}", t)),
        DataDiffError::NoPrimaryKey(t) => Some(format!("No primary key for table: {}", t)),
        DataDiffError::ColumnMismatch {
            ref_table,
            target_table,
        } => Some(format!(
            "Column mismatch between {} and {}",
            ref_table, target_table
        )),
        _ => None,
    }
}

fn file_has_content(path: &std::path::Path) -> bool {
    fs::metadata(path)
        .map(|m| m.len() > 0)
        .unwrap_or(false)
}

fn cleanup_empty(path: &std::path::Path) {
    if !file_has_content(path) {
        let _ = fs::remove_file(path);
    }
}
