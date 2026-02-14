use std::collections::HashMap;
use std::path::Path;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use tracing::debug;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::sql_gen::{format_table_name, quote_identifier};
use crate::types::{ChunkStrategy, ColumnInfo, TableAnalysis};

/// Checksum data for a single table chunk.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChunkChecksum {
    pub chunk_index: usize,
    pub chunk_start: String,
    pub chunk_end: String,
    pub row_hashes: HashMap<String, String>,
}

/// Checksum data for a single table.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableChecksum {
    pub table_name: String,
    pub strategy: String,
    pub row_count: i64,
    pub timestamp_column: Option<String>,
    pub chunks: Vec<ChunkChecksum>,
}

/// Top-level checksum manifest for a database.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChecksumManifest {
    pub db_name: Option<String>,
    pub schema: Option<String>,
    pub created_at: String,
    pub tables: Vec<TableChecksum>,
}

/// Summary of differences between two tables' checksums.
#[derive(Debug, Clone)]
pub struct TableDiffSummary {
    pub table_name: String,
    pub differing_chunks: Vec<usize>,
    pub missing_in_target: Vec<String>,
    pub missing_in_reference: Vec<String>,
    pub modified: Vec<String>,
}

/// Compute MD5 checksums for all chunks of a single table.
pub async fn compute_table_checksums(
    client: &Client,
    table_name: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    analysis: &TableAnalysis,
) -> Result<TableChecksum> {
    let table_sql = format_table_name(table_name);
    let col_list: String = columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_col_list: String = pk_cols
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let chunks = match &analysis.strategy {
        ChunkStrategy::Md5TimestampChunk {
            timestamp_column,
            min_ts,
            max_ts,
            chunk_days,
        } => {
            let ts_col_sql = quote_identifier(timestamp_column);
            let total_duration = max_ts.signed_duration_since(*min_ts);
            let total_days = total_duration.num_seconds() as f64 / 86400.0;
            let num_chunks = if total_days > 0.0 {
                (total_days / chunk_days).ceil() as usize
            } else {
                1
            };

            let mut chunks = Vec::with_capacity(num_chunks);
            for chunk_idx in 0..num_chunks {
                let chunk_start =
                    *min_ts + Duration::seconds((chunk_idx as f64 * chunk_days * 86400.0) as i64);
                let chunk_end = if chunk_idx == num_chunks - 1 {
                    *max_ts + Duration::seconds(1)
                } else {
                    *min_ts
                        + Duration::seconds(((chunk_idx + 1) as f64 * chunk_days * 86400.0) as i64)
                };

                let sql = format!(
                    "SELECT md5(ROW({col_list})::text), {pk_cols} FROM {table} \
                     WHERE {ts} >= $1 AND {ts} < $2 ORDER BY {pk_cols}",
                    col_list = col_list,
                    pk_cols = pk_col_list,
                    table = table_sql,
                    ts = ts_col_sql,
                );

                let rows = client.query(&sql, &[&chunk_start, &chunk_end]).await?;

                let mut row_hashes = HashMap::new();
                for row in &rows {
                    let md5: String = row.get(0);
                    let pk_key = extract_pk_key_from_row(row, pk_cols);
                    row_hashes.insert(pk_key, md5);
                }

                chunks.push(ChunkChecksum {
                    chunk_index: chunk_idx,
                    chunk_start: chunk_start.to_string(),
                    chunk_end: chunk_end.to_string(),
                    row_hashes,
                });
            }
            chunks
        }
        _ => {
            // For IdChunkSmall/Medium/LargeExcluded: single chunk with all rows
            let sql = format!(
                "SELECT md5(ROW({col_list})::text), {pk_cols} FROM {table} ORDER BY {pk_cols}",
                col_list = col_list,
                pk_cols = pk_col_list,
                table = table_sql,
            );

            let rows = client.query(&sql, &[]).await?;

            let mut row_hashes = HashMap::new();
            for row in &rows {
                let md5: String = row.get(0);
                let pk_key = extract_pk_key_from_row(row, pk_cols);
                row_hashes.insert(pk_key, md5);
            }

            vec![ChunkChecksum {
                chunk_index: 0,
                chunk_start: String::new(),
                chunk_end: String::new(),
                row_hashes,
            }]
        }
    };

    let (_, tname) = parse_table_name(table_name);

    Ok(TableChecksum {
        table_name: tname.to_string(),
        strategy: analysis.strategy.to_string(),
        row_count: analysis.row_count,
        timestamp_column: analysis.timestamp_column.clone(),
        chunks,
    })
}

/// Compute checksums for all tables in a database.
pub async fn compute_db_checksums(
    client: &Client,
    tables: &[(String, Vec<ColumnInfo>, Vec<ColumnInfo>)],
    analyses: &[TableAnalysis],
    db_name: Option<&str>,
    schema: Option<&str>,
) -> Result<ChecksumManifest> {
    let analysis_map: HashMap<&str, &TableAnalysis> =
        analyses.iter().map(|a| (a.ref_table.as_str(), a)).collect();

    let mut table_checksums = Vec::new();
    for (table_name, columns, pk_cols) in tables {
        if let Some(analysis) = analysis_map.get(table_name.as_str()) {
            let tc = compute_table_checksums(client, table_name, columns, pk_cols, analysis).await?;
            table_checksums.push(tc);
        }
    }

    Ok(ChecksumManifest {
        db_name: db_name.map(|s| s.to_string()),
        schema: schema.map(|s| s.to_string()),
        created_at: Utc::now().to_rfc3339(),
        tables: table_checksums,
    })
}

/// Serialize a checksum manifest to a YAML file.
pub fn save_checksums(manifest: &ChecksumManifest, path: &Path) -> Result<()> {
    let file = std::fs::File::create(path)?;
    serde_yaml::to_writer(file, manifest)?;
    debug!("Saved checksums to {}", path.display());
    Ok(())
}

/// Deserialize a checksum manifest from a YAML file.
pub fn load_checksums(path: &Path) -> Result<ChecksumManifest> {
    let file = std::fs::File::open(path).map_err(|e| {
        DataDiffError::Checksum(format!("Failed to open checksum file {}: {}", path.display(), e))
    })?;
    let manifest: ChecksumManifest = serde_yaml::from_reader(file)?;
    debug!("Loaded checksums from {}", path.display());
    Ok(manifest)
}

/// Compare two checksum manifests and return per-table diff summaries.
pub fn compare_checksums(
    ref_manifest: &ChecksumManifest,
    target_manifest: &ChecksumManifest,
) -> Vec<TableDiffSummary> {
    let target_map: HashMap<&str, &TableChecksum> = target_manifest
        .tables
        .iter()
        .map(|t| (t.table_name.as_str(), t))
        .collect();

    let mut summaries = Vec::new();

    for ref_table in &ref_manifest.tables {
        let target_table = match target_map.get(ref_table.table_name.as_str()) {
            Some(t) => t,
            None => {
                // Entire table missing in target - all ref PKs are missing
                let all_pks: Vec<String> = ref_table
                    .chunks
                    .iter()
                    .flat_map(|c| c.row_hashes.keys().cloned())
                    .collect();
                summaries.push(TableDiffSummary {
                    table_name: ref_table.table_name.clone(),
                    differing_chunks: (0..ref_table.chunks.len()).collect(),
                    missing_in_target: all_pks,
                    missing_in_reference: Vec::new(),
                    modified: Vec::new(),
                });
                continue;
            }
        };

        let mut differing_chunks = Vec::new();
        let mut missing_in_target = Vec::new();
        let mut missing_in_reference = Vec::new();
        let mut modified = Vec::new();

        // Build a map of target chunks by index for lookup
        let target_chunk_map: HashMap<usize, &ChunkChecksum> = target_table
            .chunks
            .iter()
            .map(|c| (c.chunk_index, c))
            .collect();

        for ref_chunk in &ref_table.chunks {
            let target_chunk = target_chunk_map.get(&ref_chunk.chunk_index);

            let target_hashes: &HashMap<String, String> = match target_chunk {
                Some(tc) => &tc.row_hashes,
                None => {
                    // Entire chunk missing in target
                    differing_chunks.push(ref_chunk.chunk_index);
                    missing_in_target.extend(ref_chunk.row_hashes.keys().cloned());
                    continue;
                }
            };

            let mut chunk_differs = false;

            // Check ref rows against target
            for (pk, ref_hash) in &ref_chunk.row_hashes {
                match target_hashes.get(pk) {
                    None => {
                        missing_in_target.push(pk.clone());
                        chunk_differs = true;
                    }
                    Some(target_hash) if target_hash != ref_hash => {
                        modified.push(pk.clone());
                        chunk_differs = true;
                    }
                    _ => {}
                }
            }

            // Check for rows in target but not in ref
            for pk in target_hashes.keys() {
                if !ref_chunk.row_hashes.contains_key(pk) {
                    missing_in_reference.push(pk.clone());
                    chunk_differs = true;
                }
            }

            if chunk_differs {
                differing_chunks.push(ref_chunk.chunk_index);
            }
        }

        // Check for chunks in target that aren't in ref
        for target_chunk in &target_table.chunks {
            let in_ref = ref_table
                .chunks
                .iter()
                .any(|c| c.chunk_index == target_chunk.chunk_index);
            if !in_ref {
                differing_chunks.push(target_chunk.chunk_index);
                missing_in_reference.extend(target_chunk.row_hashes.keys().cloned());
            }
        }

        if !differing_chunks.is_empty()
            || !missing_in_target.is_empty()
            || !missing_in_reference.is_empty()
            || !modified.is_empty()
        {
            summaries.push(TableDiffSummary {
                table_name: ref_table.table_name.clone(),
                differing_chunks,
                missing_in_target,
                missing_in_reference,
                modified,
            });
        }
    }

    summaries
}

/// Extract a PK key string from a tokio_postgres::Row where PK columns start at index 1.
fn extract_pk_key_from_row(row: &tokio_postgres::Row, pk_cols: &[ColumnInfo]) -> String {
    let mut pk_key = String::new();
    for (i, _pk) in pk_cols.iter().enumerate() {
        if i > 0 {
            pk_key.push('|');
        }
        let val: Option<String> = row.try_get::<_, Option<String>>(i + 1).unwrap_or(None);
        pk_key.push_str(&val.unwrap_or_else(|| "NULL".to_string()));
    }
    pk_key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_manifest_roundtrip() {
        let manifest = ChecksumManifest {
            db_name: Some("test_db".to_string()),
            schema: Some("public".to_string()),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 100,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "abc123".to_string()),
                        ("2".to_string(), "def456".to_string()),
                    ]),
                }],
            }],
        };

        let yaml = serde_yaml::to_string(&manifest).unwrap();
        let deserialized: ChecksumManifest = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.db_name, manifest.db_name);
        assert_eq!(deserialized.schema, manifest.schema);
        assert_eq!(deserialized.tables.len(), 1);
        assert_eq!(deserialized.tables[0].table_name, "users");
        assert_eq!(deserialized.tables[0].chunks[0].row_hashes.len(), 2);
    }

    #[test]
    fn test_compare_checksums_with_diffs() {
        let ref_manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 3,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),
                        ("2".to_string(), "bbb".to_string()),
                        ("3".to_string(), "ccc".to_string()),
                    ]),
                }],
            }],
        };

        let target_manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "users".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 3,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),     // same
                        ("2".to_string(), "xxx".to_string()),     // modified
                        ("4".to_string(), "ddd".to_string()),     // extra in target
                    ]),
                }],
            }],
        };

        let summaries = compare_checksums(&ref_manifest, &target_manifest);
        assert_eq!(summaries.len(), 1);

        let summary = &summaries[0];
        assert_eq!(summary.table_name, "users");
        assert_eq!(summary.differing_chunks, vec![0]);
        assert!(summary.missing_in_target.contains(&"3".to_string()));
        assert!(summary.missing_in_reference.contains(&"4".to_string()));
        assert!(summary.modified.contains(&"2".to_string()));
    }

    #[test]
    fn test_compare_checksums_identical() {
        let manifest = ChecksumManifest {
            db_name: None,
            schema: None,
            created_at: String::new(),
            tables: vec![TableChecksum {
                table_name: "orders".to_string(),
                strategy: "IdChunkSmall".to_string(),
                row_count: 2,
                timestamp_column: None,
                chunks: vec![ChunkChecksum {
                    chunk_index: 0,
                    chunk_start: String::new(),
                    chunk_end: String::new(),
                    row_hashes: HashMap::from([
                        ("1".to_string(), "aaa".to_string()),
                        ("2".to_string(), "bbb".to_string()),
                    ]),
                }],
            }],
        };

        let summaries = compare_checksums(&manifest, &manifest);
        assert!(summaries.is_empty());
    }
}
