use std::collections::{HashMap, HashSet};

use postgres_types::Type;
use tokio_postgres::Client;

use crate::config::parse_table_name;
use crate::error::{DataDiffError, Result};
use crate::types::ColumnInfo;

/// Map a PostgreSQL type OID + type name to a `postgres_types::Type`.
fn resolve_pg_type(type_name: &str) -> Type {
    match type_name {
        "bool" | "boolean" => Type::BOOL,
        "int2" | "smallint" => Type::INT2,
        "int4" | "integer" | "int" | "serial" => Type::INT4,
        "int8" | "bigint" | "bigserial" => Type::INT8,
        "float4" | "real" => Type::FLOAT4,
        "float8" | "double precision" => Type::FLOAT8,
        "numeric" | "decimal" => Type::NUMERIC,
        "text" => Type::TEXT,
        "varchar" | "character varying" => Type::VARCHAR,
        "char" | "character" | "bpchar" => Type::BPCHAR,
        "bytea" => Type::BYTEA,
        "timestamp" | "timestamp without time zone" => Type::TIMESTAMP,
        "timestamptz" | "timestamp with time zone" => Type::TIMESTAMPTZ,
        "date" => Type::DATE,
        "time" | "time without time zone" => Type::TIME,
        "uuid" => Type::UUID,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        "oid" => Type::OID,
        "name" => Type::NAME,
        _ => Type::TEXT, // fallback
    }
}

/// Resolve the schema name, falling back to the current schema if not provided.
async fn resolve_schema(client: &Client, schema_opt: Option<&str>) -> Result<String> {
    if let Some(s) = schema_opt {
        Ok(s.to_string())
    } else {
        let row = client.query_one("SELECT current_schema()", &[]).await?;
        Ok(row.get(0))
    }
}

/// Retrieve column metadata for a table, including PK and identity information.
///
/// Queries `information_schema.columns` for column details and
/// `pg_constraint` + `pg_attribute` for primary key membership.
pub async fn get_columns(
    client: &Client,
    schema_qualified_name: &str,
) -> Result<Vec<ColumnInfo>> {
    let (schema_opt, table_name) = parse_table_name(schema_qualified_name);
    let schema = resolve_schema(client, schema_opt).await?;

    // Get column info from information_schema
    let col_rows = client
        .query(
            "SELECT column_name, udt_name, ordinal_position, is_identity, \
             column_default \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema, &table_name],
        )
        .await?;

    if col_rows.is_empty() {
        return Err(DataDiffError::TableNotFound(
            schema_qualified_name.to_string(),
        ));
    }

    // Get PK columns from pg_constraint
    let pk_rows = client
        .query(
            "SELECT a.attname \
             FROM pg_constraint c \
             JOIN pg_namespace n ON n.oid = c.connamespace \
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) \
             WHERE c.contype = 'p' \
             AND n.nspname = $1 \
             AND c.conrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass",
            &[&schema, &table_name],
        )
        .await?;

    let pk_names: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let columns: Vec<ColumnInfo> = col_rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let name: String = row.get("column_name");
            let udt_name: String = row.get("udt_name");
            let is_identity_str: String = row.get("is_identity");
            let col_default: Option<String> = row.get("column_default");

            let is_pk = pk_names.iter().any(|pk| pk.eq_ignore_ascii_case(&name));
            let is_identity = is_identity_str == "YES"
                || col_default
                    .as_deref()
                    .map(|d| d.starts_with("nextval("))
                    .unwrap_or(false);

            ColumnInfo {
                name,
                pg_type: resolve_pg_type(&udt_name),
                is_pk,
                is_identity,
                ordinal: idx,
            }
        })
        .collect();

    Ok(columns)
}

/// Check if a table exists.
pub async fn table_exists(client: &Client, schema_qualified_name: &str) -> Result<bool> {
    let (schema_opt, table_name) = parse_table_name(schema_qualified_name);
    let schema = resolve_schema(client, schema_opt).await?;

    let row = client
        .query_one(
            "SELECT EXISTS ( \
             SELECT 1 FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2)",
            &[&schema, &table_name],
        )
        .await?;

    Ok(row.get::<_, bool>(0))
}

/// List all table names in a given schema.
pub async fn list_tables(client: &Client, schema: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
             ORDER BY table_name",
            &[&schema],
        )
        .await?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Foreign key dependency: `table` depends on `references_table`.
#[derive(Debug, Clone)]
pub struct FkDependency {
    pub table: String,
    pub references_table: String,
}

/// Retrieve foreign key dependencies for all tables in a schema.
/// Returns a list of (child_table, parent_table) pairs.
pub async fn get_fk_dependencies(
    client: &Client,
    schema: &str,
) -> Result<Vec<FkDependency>> {
    let rows = client
        .query(
            "SELECT DISTINCT \
                 tc.table_name AS child_table, \
                 ccu.table_name AS parent_table \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.constraint_column_usage ccu \
                 ON ccu.constraint_name = tc.constraint_name \
                 AND ccu.constraint_schema = tc.constraint_schema \
             WHERE tc.constraint_type = 'FOREIGN KEY' \
             AND tc.table_schema = $1 \
             AND ccu.table_schema = $1 \
             AND tc.table_name <> ccu.table_name",
            &[&schema],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| FkDependency {
            table: r.get(0),
            references_table: r.get(1),
        })
        .collect())
}

/// Sort tables for INSERT order: parents before children (topological sort).
/// Tables without FK dependencies come first.
pub fn sort_for_insert(tables: &[String], deps: &[FkDependency]) -> Vec<String> {
    topological_sort(tables, deps, false)
}

/// Sort tables for DELETE order: children before parents (reverse topological sort).
pub fn sort_for_delete(tables: &[String], deps: &[FkDependency]) -> Vec<String> {
    topological_sort(tables, deps, true)
}

/// Topological sort using Kahn's algorithm.
/// If `reverse` is true, returns delete order (children first).
fn topological_sort(tables: &[String], deps: &[FkDependency], reverse: bool) -> Vec<String> {
    let table_set: HashSet<String> = tables.iter().map(|t| t.to_lowercase()).collect();

    // Build adjacency list: parent -> children (for insert order)
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

    for t in &table_set {
        in_degree.entry(t.clone()).or_insert(0);
        adjacency.entry(t.clone()).or_default();
    }

    for dep in deps {
        let child = dep.table.to_lowercase();
        let parent = dep.references_table.to_lowercase();

        if !table_set.contains(&child) || !table_set.contains(&parent) {
            continue;
        }

        // parent -> child edge: child depends on parent
        adjacency.entry(parent.clone()).or_default().push(child.clone());
        *in_degree.entry(child).or_insert(0) += 1;
    }

    // Kahn's algorithm
    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|&(_, deg)| *deg == 0)
        .map(|(t, _)| t.clone())
        .collect();
    queue.sort(); // deterministic order

    let mut result = Vec::new();

    while let Some(node) = queue.pop() {
        result.push(node.clone());
        if let Some(children) = adjacency.get(&node) {
            for child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(child.clone());
                        queue.sort();
                    }
                }
            }
        }
    }

    // Add any remaining tables (circular dependencies) at the end
    for t in &table_set {
        if !result.contains(t) {
            result.push(t.clone());
        }
    }

    // Map back to original casing
    let mut ordered: Vec<String> = Vec::with_capacity(result.len());
    for sorted_name in &result {
        if let Some(original) = tables.iter().find(|t| t.eq_ignore_ascii_case(sorted_name)) {
            ordered.push(original.clone());
        }
    }

    if reverse {
        ordered.reverse();
    }

    ordered
}

/// Resolve which columns to use as PK: alternate keys if provided, else real PK.
/// Returns (pk_columns, real_pk_column_names).
pub fn resolve_pk_columns(
    columns: &[ColumnInfo],
    alternate_keys: Option<&[String]>,
) -> (Vec<ColumnInfo>, Vec<String>) {
    let real_pk: Vec<String> = columns
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.clone())
        .collect();

    if let Some(alt_keys) = alternate_keys {
        if !alt_keys.is_empty() {
            let pk_cols: Vec<ColumnInfo> = columns
                .iter()
                .filter(|c| {
                    alt_keys
                        .iter()
                        .any(|k| k.eq_ignore_ascii_case(&c.name))
                })
                .cloned()
                .collect();
            return (pk_cols, real_pk);
        }
    }

    let pk_cols: Vec<ColumnInfo> = columns.iter().filter(|c| c.is_pk).cloned().collect();
    (pk_cols, real_pk)
}
