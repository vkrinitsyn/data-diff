use std::io::Write;

use tokio_postgres::Client;
use tracing::debug;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::metadata::{get_columns, resolve_pk_columns};
use crate::sql_gen::{build_delete_check_query, build_pk_select, generate_delete};
use crate::types::{extract_row_value, ColumnInfo, Row};

/// Generates DELETE statements for rows that exist in the target table
/// but are missing from the reference table.
pub struct TableDeleteSync<'a> {
    target_client: &'a Client,
    ref_client: &'a Client,
    target_table: String,
    ref_table: String,
    chunk_size: usize,
    alternate_keys: Option<Vec<String>>,
    cancelled: bool,
}

impl<'a> TableDeleteSync<'a> {
    pub fn new(
        target_client: &'a Client,
        ref_client: &'a Client,
        target_table: String,
        ref_table: String,
    ) -> Self {
        Self {
            target_client,
            ref_client,
            target_table,
            ref_table,
            chunk_size: 50,
            alternate_keys: None,
            cancelled: false,
        }
    }

    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size.max(1);
    }

    pub fn set_alternate_keys(&mut self, keys: Option<Vec<String>>) {
        self.alternate_keys = keys;
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Execute the delete sync, writing DELETE SQL statements for rows
    /// in the target that are missing from the reference.
    pub async fn execute(&mut self, writer: &mut impl Write) -> Result<u64> {
        let target_columns = get_columns(self.target_client, &self.target_table).await?;
        let ref_columns = get_columns(self.ref_client, &self.ref_table).await?;

        // Determine PK columns from the target table
        let (pk_cols, _) = resolve_pk_columns(&target_columns, self.alternate_keys.as_deref());

        if pk_cols.is_empty() {
            return Err(DataDiffError::NoPrimaryKey(self.target_table.clone()));
        }

        // Resolve PK columns in the reference table (they should have the same names)
        let ref_pk_cols: Vec<ColumnInfo> = pk_cols
            .iter()
            .filter_map(|pk| {
                ref_columns
                    .iter()
                    .find(|rc| rc.name.eq_ignore_ascii_case(&pk.name))
                    .cloned()
            })
            .collect();

        if ref_pk_cols.len() != pk_cols.len() {
            return Err(DataDiffError::ColumnMismatch {
                ref_table: self.ref_table.clone(),
                target_table: self.target_table.clone(),
            });
        }

        let select_sql = build_pk_select(&self.target_table, &pk_cols);
        debug!("Delete sync select: {}", select_sql);

        let target_pk_rows = self.target_client.query(&select_sql, &[]).await?;

        let mut deleted: u64 = 0;
        let mut first_delete = true;
        let mut chunk: Vec<Row> = Vec::with_capacity(self.chunk_size);

        for pg_row in &target_pk_rows {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let row: Row = pk_cols
                .iter()
                .enumerate()
                .map(|(idx, col)| extract_row_value(pg_row, idx, &col.pg_type))
                .collect();
            chunk.push(row);

            if chunk.len() == self.chunk_size {
                deleted += self
                    .check_chunk(
                        &chunk,
                        &pk_cols,
                        &ref_pk_cols,
                        &target_columns,
                        writer,
                        &mut first_delete,
                    )
                    .await?;
                chunk.clear();
            }
        }

        if !chunk.is_empty() && !self.cancelled {
            deleted += self
                .check_chunk(
                    &chunk,
                    &pk_cols,
                    &ref_pk_cols,
                    &target_columns,
                    writer,
                    &mut first_delete,
                )
                .await?;
        }

        Ok(deleted)
    }

    /// Check a chunk of target PK rows against the reference table.
    /// Returns the number of DELETE statements written.
    async fn check_chunk(
        &self,
        target_pk_rows: &[Row],
        pk_cols: &[ColumnInfo],
        ref_pk_cols: &[ColumnInfo],
        _target_columns: &[ColumnInfo],
        writer: &mut impl Write,
        first_delete: &mut bool,
    ) -> Result<u64> {
        let check_sql =
            build_delete_check_query(&self.ref_table, ref_pk_cols, target_pk_rows);

        debug!("Delete check SQL: {}", check_sql);

        let ref_rows_result = self.ref_client.query(&check_sql, &[]).await?;
        let ref_rows: Vec<Row> = ref_rows_result
            .iter()
            .map(|r| {
                ref_pk_cols
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| extract_row_value(r, idx, &col.pg_type))
                    .collect()
            })
            .collect();

        // If counts match, no rows need to be deleted
        if ref_rows.len() == target_pk_rows.len() {
            return Ok(0);
        }

        let mut deleted: u64 = 0;

        for target_row in target_pk_rows {
            if self.cancelled {
                return Err(DataDiffError::Cancelled);
            }

            let exists_in_ref = ref_rows.iter().any(|ref_row| {
                pk_cols.iter().enumerate().all(|(idx, _)| {
                    target_row[idx] == ref_row[idx]
                })
            });

            if !exists_in_ref {
                if *first_delete {
                    *first_delete = false;
                    write_delete_header(writer, &self.target_table)?;
                }

                let sql = generate_delete(
                    &self.target_table,
                    pk_cols, // for delete, columns = pk_cols since rows only have PK values
                    pk_cols,
                    target_row,
                );
                writeln!(writer, "{}", sql)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }
}

fn write_delete_header(writer: &mut impl Write, table_name: &str) -> Result<()> {
    let (_, tname) = parse_table_name(table_name);
    writeln!(
        writer,
        "-- ----------------------------------------------------------------"
    )?;
    writeln!(writer, "-- DELETE statements for table: {}", tname)?;
    writeln!(
        writer,
        "-- ----------------------------------------------------------------"
    )?;
    Ok(())
}
