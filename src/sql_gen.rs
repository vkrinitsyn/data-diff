use std::collections::HashSet;
use std::fmt::Write as FmtWrite;

use crate::types::{ColumnInfo, Row, RowValue};

/// Quote a PostgreSQL identifier (table/column name) if needed.
pub fn quote_identifier(name: &str) -> String {
    // Must quote if: contains uppercase, spaces, special chars, or is a reserved word.
    // For safety, always quote with double quotes.
    let needs_quoting = name.chars().any(|c| c.is_ascii_uppercase() || !c.is_ascii_alphanumeric() && c != '_')
        || name.is_empty()
        || name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false);

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Format a schema-qualified table name for use in SQL.
pub fn format_table_name(name: &str) -> String {
    if let Some(dot_pos) = name.find('.') {
        let schema = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        format!("{}.{}", quote_identifier(schema), quote_identifier(table))
    } else {
        quote_identifier(name)
    }
}

/// Format a `RowValue` as a PostgreSQL literal.
pub fn format_literal(value: &RowValue) -> String {
    match value {
        RowValue::Null => "NULL".to_string(),
        RowValue::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        RowValue::I16(v) => v.to_string(),
        RowValue::I32(v) => v.to_string(),
        RowValue::I64(v) => v.to_string(),
        RowValue::F32(v) => {
            if v.is_nan() {
                "'NaN'::real".to_string()
            } else if v.is_infinite() {
                if v.is_sign_positive() {
                    "'Infinity'::real".to_string()
                } else {
                    "'-Infinity'::real".to_string()
                }
            } else {
                v.to_string()
            }
        }
        RowValue::F64(v) => {
            if v.is_nan() {
                "'NaN'::double precision".to_string()
            } else if v.is_infinite() {
                if v.is_sign_positive() {
                    "'Infinity'::double precision".to_string()
                } else {
                    "'-Infinity'::double precision".to_string()
                }
            } else {
                v.to_string()
            }
        }
        RowValue::Numeric(v) => v.clone(),
        RowValue::Text(s) => escape_string_literal(s),
        RowValue::Bytes(b) => {
            let mut result = String::with_capacity(b.len() * 2 + 5);
            result.push_str("'\\x");
            for byte in b {
                write!(result, "{:02x}", byte).unwrap();
            }
            result.push('\'');
            result
        }
        RowValue::Timestamp(ts) => {
            format!("'{}'::timestamp", ts.format("%Y-%m-%d %H:%M:%S%.f"))
        }
        RowValue::TimestampTz(ts) => {
            format!("'{}'::timestamptz", ts.format("%Y-%m-%d %H:%M:%S%.f%:z"))
        }
        RowValue::Date(d) => {
            format!("'{}'::date", d.format("%Y-%m-%d"))
        }
        RowValue::Time(t) => {
            format!("'{}'::time", t.format("%H:%M:%S%.f"))
        }
        RowValue::Uuid(u) => {
            format!("'{}'::uuid", u)
        }
        RowValue::Json(v) => {
            let json_str = serde_json::to_string(v).unwrap_or_default();
            format!("{}::jsonb", escape_string_literal(&json_str))
        }
    }
}

/// Escape a string for use as a PostgreSQL string literal.
fn escape_string_literal(s: &str) -> String {
    if s.contains('\\') {
        // Use E'' syntax for strings with backslashes
        let escaped = s.replace('\'', "''").replace('\\', "\\\\");
        format!("E'{}'", escaped)
    } else {
        format!("'{}'", s.replace('\'', "''"))
    }
}

/// Generate an INSERT statement.
pub fn generate_insert(
    table: &str,
    columns: &[ColumnInfo],
    row: &[RowValue],
    exclude_cols: &HashSet<String>,
) -> String {
    let table_sql = format_table_name(table);

    let mut col_names = Vec::new();
    let mut values = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        if exclude_cols.contains(&col.name.to_lowercase()) {
            continue;
        }
        col_names.push(quote_identifier(&col.name));
        values.push(format_literal(&row[i]));
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({});",
        table_sql,
        col_names.join(", "),
        values.join(", "),
    )
}

/// Generate an UPDATE statement.
///
/// `modified_indices` are the column indices where reference differs from target.
pub fn generate_update(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    row: &[RowValue],
    modified_indices: &[usize],
) -> Option<String> {
    if modified_indices.is_empty() {
        return None;
    }

    let table_sql = format_table_name(table);

    let set_clauses: Vec<String> = modified_indices
        .iter()
        .map(|&i| {
            format!(
                "{} = {}",
                quote_identifier(&columns[i].name),
                format_literal(&row[i])
            )
        })
        .collect();

    let where_clauses: Vec<String> = pk_cols
        .iter()
        .map(|pk| {
            let idx = columns.iter().position(|c| c.name.eq_ignore_ascii_case(&pk.name)).unwrap();
            let val = &row[idx];
            if matches!(val, RowValue::Null) {
                format!("{} IS NULL", quote_identifier(&pk.name))
            } else {
                format!("{} = {}", quote_identifier(&pk.name), format_literal(val))
            }
        })
        .collect();

    Some(format!(
        "UPDATE {} SET {} WHERE {};",
        table_sql,
        set_clauses.join(", "),
        where_clauses.join(" AND "),
    ))
}

/// Generate a DELETE statement.
pub fn generate_delete(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    row: &[RowValue],
) -> String {
    let table_sql = format_table_name(table);

    let where_clauses: Vec<String> = pk_cols
        .iter()
        .map(|pk| {
            let idx = columns.iter().position(|c| c.name.eq_ignore_ascii_case(&pk.name)).unwrap();
            let val = &row[idx];
            if matches!(val, RowValue::Null) {
                format!("{} IS NULL", quote_identifier(&pk.name))
            } else {
                format!("{} = {}", quote_identifier(&pk.name), format_literal(val))
            }
        })
        .collect();

    format!("DELETE FROM {} WHERE {};", table_sql, where_clauses.join(" AND "))
}

/// Build a SELECT query to retrieve matching rows from a target table,
/// given a chunk of reference rows and their PK values.
///
/// Produces: `SELECT col1, col2, ... FROM target WHERE (pk1 = v1 AND pk2 = v2) OR ...`
pub fn build_check_query(
    table: &str,
    columns: &[ColumnInfo],
    pk_cols: &[ColumnInfo],
    rows: &[Row],
) -> String {
    let table_sql = format_table_name(table);

    let select_cols: String = columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = format!("SELECT {} FROM {} WHERE ", select_cols, table_sql);

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push('(');
        for (pk_idx, pk) in pk_cols.iter().enumerate() {
            if pk_idx > 0 {
                sql.push_str(" AND ");
            }
            let col_idx = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&pk.name))
                .unwrap();
            let val = &row[col_idx];
            sql.push_str(&quote_identifier(&pk.name));
            if matches!(val, RowValue::Null) {
                sql.push_str(" IS NULL");
            } else {
                sql.push_str(" = ");
                sql.push_str(&format_literal(val));
            }
        }
        sql.push(')');
    }

    sql
}

/// Build a SELECT query for PK-only columns from a table (used by delete sync).
pub fn build_pk_select(table: &str, pk_cols: &[ColumnInfo]) -> String {
    let table_sql = format_table_name(table);
    let select_cols: String = pk_cols
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {} FROM {}", select_cols, table_sql)
}

/// Build a check query for the delete sync: checks which PK values exist in the reference table.
pub fn build_delete_check_query(
    reference_table: &str,
    pk_cols: &[ColumnInfo],
    rows: &[Row],
) -> String {
    let table_sql = format_table_name(reference_table);

    let select_cols: String = pk_cols
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let mut sql = format!("SELECT {} FROM {} WHERE ", select_cols, table_sql);

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push('(');
        for (pk_idx, pk) in pk_cols.iter().enumerate() {
            if pk_idx > 0 {
                sql.push_str(" AND ");
            }
            let val = &row[pk_idx]; // rows only contain PK values in order
            sql.push_str(&quote_identifier(&pk.name));
            if matches!(val, RowValue::Null) {
                sql.push_str(" IS NULL");
            } else {
                sql.push_str(" = ");
                sql.push_str(&format_literal(val));
            }
        }
        sql.push(')');
    }

    sql
}
