//! Lazy PostgreSQL catalog resolver for backend-side DataFusion planning.
//!
//! `df_catalog` intentionally stays narrow:
//!
//! - input: one DataFusion [`TableReference`]
//! - output: one owned [`ResolvedTable`] bundle with PostgreSQL identity and Arrow schema
//! - scope: live PostgreSQL catalog lookup only
//!
//! It does not handle snapshots, transport, DataFusion provider registration,
//! or plan serialization.
//!
//! Bare table references follow PostgreSQL name-resolution semantics, including
//! temporary tables and the current `search_path`.
//!
//! Identifiers longer than PostgreSQL's `NAMEDATALEN - 1` byte limit are
//! rejected explicitly instead of relying on PostgreSQL's implicit truncation
//! rules during planning.
//!
//! The current type surface intentionally rejects PostgreSQL `time with time zone`
//! (`timetz`) because this codebase does not yet have a lossless Arrow/DataFusion
//! representation for that type.
//!
//! # Execution Contract
//!
//! `df_catalog` is intended to feed backend planning for the
//! `scan_sql -> slot_scan` path.
//!
//! In that path:
//!
//! - [`ResolvedTable::relation`] and [`ResolvedTable::schema`] are used for
//!   DataFusion planning and PostgreSQL scan SQL compilation
//! - PostgreSQL text-like columns are exposed as Arrow `Utf8View` so the
//!   logical DataFusion schema matches page-backed scan batches without
//!   copying string payloads
//! - physical row execution happens later through `slot_scan`, which exposes
//!   the live run-time `TupleDesc` for the actual cursor result
//! - `ResolvedTable` is therefore **not** a heap-layout contract and must not
//!   be used to reconstruct physical PostgreSQL attribute layout or dropped
//!   column positions
//! - partitioned parents are acceptable because PostgreSQL expands them during
//!   SQL planning/execution, and `slot_scan` validates the resulting portal
//!   plan shape at run time
//!
//! # Examples
//!
//! ```no_run
//! use datafusion_common::TableReference;
//! use df_catalog::{CatalogResolver, PgrxCatalogResolver};
//!
//! let resolver = PgrxCatalogResolver::new();
//!
//! let bare = resolver.resolve_table(&TableReference::bare("orders"))?;
//! assert_eq!(bare.relation.table, "orders");
//!
//! let qualified = resolver.resolve_table(&TableReference::partial("public", "orders"))?;
//! assert_eq!(qualified.relation.schema.as_deref(), Some("public"));
//! # Ok::<(), df_catalog::ResolveError>(())
//! ```

mod error;

use std::ffi::{CStr, CString};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion_common::{Result as DataFusionResult, TableReference};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableSource};
use pg_type::{arrow_type_for_pg_type, PgTypeRef};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::{PgRelation, PgTryBuilder};
use scan_sql::PgRelation as ScanRelation;

pub use crate::error::ResolveError;

/// One fully resolved PostgreSQL relation for backend-side planning.
///
/// The returned relation identity is suitable for downstream planning:
///
/// - bare references are materialized to a resolved schema-qualified relation
/// - bare references that resolve to the current temporary schema are
///   normalized back to the logical `pg_temp` alias
/// - bare references preserve their logical input table identifier
/// - explicit schema-qualified references preserve their logical input
///   identity
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTable {
    /// PostgreSQL `pg_class.oid` for the resolved relation.
    ///
    /// This is relation identity for planning, diagnostics, and downstream
    /// PostgreSQL SQL generation. It is not a promise that callers can scan
    /// the relation by reading a heap fork directly.
    pub table_oid: u32,
    /// PostgreSQL relation identity for downstream planning and scan SQL compilation.
    pub relation: ScanRelation,
    /// PostgreSQL attribute numbers for [`schema`] fields in order.
    ///
    /// Dropped attributes are not present in [`schema`], so callers must use
    /// this mapping when they need catalog statistics keyed by `attnum`.
    pub column_attnums: Vec<i16>,
    /// Logical Arrow schema for downstream planning and scan SQL compilation.
    ///
    /// Relations containing unsupported PostgreSQL types, such as `timetz`,
    /// are rejected during resolution instead of being represented lossy.
    ///
    /// This schema is not physical heap metadata. Consumers that need the
    /// live PostgreSQL row layout must obtain it from the execution runtime,
    /// for example from `slot_scan`'s run-time `TupleDesc`.
    pub schema: SchemaRef,
    /// PostgreSQL column metadata matching [`schema`] fields in order.
    pub columns: Vec<ResolvedColumn>,
}

/// PostgreSQL column metadata preserved during catalog resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    /// PostgreSQL attribute number for this visible, non-dropped column.
    pub attnum: i16,
    /// PostgreSQL attribute name.
    pub name: String,
    /// PostgreSQL type identity, including typmod and collation when known.
    pub pg_type: PgTypeRef,
    /// Whether the column can contain NULL values.
    pub nullable: bool,
}

/// Narrow lazy resolver surface for backend planning code.
pub trait CatalogResolver {
    /// Resolve one PostgreSQL relation from a DataFusion table reference.
    ///
    /// Bare references use PostgreSQL's normal relation name resolver.
    /// Schema-qualified references resolve through PostgreSQL explicit-namespace
    /// lookup. Catalog-qualified references are rejected.
    fn resolve_table(&self, table: &TableReference) -> Result<ResolvedTable, ResolveError>;

    /// Resolve one PostgreSQL relation by already-analyzed relation OID.
    ///
    /// Typed PostgreSQL query-tree planning should use this path when the
    /// analyzed tree already carries `RTE_RELATION.relid`; schema/table names
    /// remain display and scan-SQL rendering metadata, not lookup authority.
    fn resolve_relation_oid(&self, relid: u32) -> Result<ResolvedTable, ResolveError>;
}

/// DataFusion planning source for a resolved PostgreSQL relation.
///
/// This type intentionally lives with catalog resolution rather than with any
/// query-tree frontend. Typed query-tree lowering uses it as the DataFusion
/// `TableSource` marker that later scan lowering recognizes and turns into a
/// PostgreSQL-owned scan stream.
#[derive(Debug)]
pub struct PgPlanningTableSource {
    resolved: ResolvedTable,
}

impl PgPlanningTableSource {
    pub fn new(resolved: ResolvedTable) -> Self {
        Self { resolved }
    }

    pub fn resolved(&self) -> &ResolvedTable {
        &self.resolved
    }
}

impl TableSource for PgPlanningTableSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.resolved.schema)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// pgrx-backed resolver against live PostgreSQL catalogs.
///
/// Resolution is lazy and per-table. The resolver does not cache catalog state
/// and does not materialize a whole catalog view up front.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgrxCatalogResolver;

impl PgrxCatalogResolver {
    /// Create a new live PostgreSQL catalog resolver.
    pub fn new() -> Self {
        Self
    }
}

impl CatalogResolver for PgrxCatalogResolver {
    fn resolve_table(&self, table: &TableReference) -> Result<ResolvedTable, ResolveError> {
        let rel_oid = resolve_relation_oid(table)?;
        resolve_relation_by_oid(Some(table), rel_oid)
    }

    fn resolve_relation_oid(&self, relid: u32) -> Result<ResolvedTable, ResolveError> {
        resolve_relation_by_oid(None, pg_sys::Oid::from(relid))
    }
}

fn resolve_relation_oid(table: &TableReference) -> Result<pg_sys::Oid, ResolveError> {
    match table {
        TableReference::Bare { table } => resolve_bare_relation_oid(table.as_ref()),
        TableReference::Partial { schema, table } => {
            resolve_qualified_relation_oid(schema.as_ref(), table.as_ref())
        }
        TableReference::Full { .. } => Err(ResolveError::FullReferenceUnsupported),
    }
}

fn resolve_bare_relation_oid(table: &str) -> Result<pg_sys::Oid, ResolveError> {
    let table_name = table.to_owned();
    let table_cstr = validate_lookup_identifier(table, "table")?;

    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let rel_oid = pg_sys::RelnameGetRelid(table_cstr.as_ptr());
        if rel_oid == pg_sys::InvalidOid {
            Err(ResolveError::TableNotFound {
                schema: None,
                table: table_name.clone(),
            })
        } else {
            Ok(rel_oid)
        }
    }))
    .catch_others(|error| Err(resolve_error_from_caught_error(error)))
    .execute()
}

fn resolve_qualified_relation_oid(schema: &str, table: &str) -> Result<pg_sys::Oid, ResolveError> {
    let schema_name = schema.to_owned();
    let table_name = table.to_owned();
    let schema_cstr = validate_lookup_identifier(schema, "schema")?;
    let table_cstr = validate_lookup_identifier(table, "table")?;

    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let schema_oid = pg_sys::LookupExplicitNamespace(schema_cstr.as_ptr(), true);
        if schema_oid == pg_sys::InvalidOid {
            return if schema == "pg_temp" {
                Err(ResolveError::TableNotFound {
                    schema: Some(schema_name.clone()),
                    table: table_name.clone(),
                })
            } else {
                Err(ResolveError::SchemaNotFound(schema_name.clone()))
            };
        }
        let rel_oid = pg_sys::get_relname_relid(table_cstr.as_ptr(), schema_oid);
        if rel_oid == pg_sys::InvalidOid {
            Err(ResolveError::TableNotFound {
                schema: Some(schema_name.clone()),
                table: table_name.clone(),
            })
        } else {
            Ok(rel_oid)
        }
    }))
    .catch_others(|error| Err(resolve_error_from_caught_error(error)))
    .execute()
}

fn resolve_relation_by_oid(
    input: Option<&TableReference>,
    rel_oid: pg_sys::Oid,
) -> Result<ResolvedTable, ResolveError> {
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let rel = PgRelation::with_lock(rel_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        validate_relkind(&rel)?;

        let relation = match input {
            Some(TableReference::Bare { table }) => bare_relation_identity(table.as_ref(), &rel),
            Some(TableReference::Partial { schema, table }) => {
                ScanRelation::new(Some(schema.as_ref()), table.as_ref())
            }
            Some(TableReference::Full { .. }) => {
                unreachable!("full references are rejected earlier")
            }
            None => oid_relation_identity(&rel)?,
        };
        let tuple_desc = rel.tuple_desc();
        let field_count = tuple_desc.iter().filter(|attr| !attr.is_dropped()).count();
        let mut fields = Vec::with_capacity(field_count);
        let mut column_attnums = Vec::with_capacity(field_count);
        let mut columns = Vec::with_capacity(field_count);
        for attr in tuple_desc.iter() {
            if attr.is_dropped() {
                continue;
            }
            let pg_type = PgTypeRef::new(attr.atttypid.to_u32(), attr.atttypmod, 0);
            let data_type =
                arrow_type_for_pg_type(pg_type).ok_or_else(|| ResolveError::UnsupportedType {
                    column: attr.name().to_owned(),
                    type_oid: attr.atttypid.to_u32(),
                })?;
            let name = attr.name().to_owned();
            let nullable = !attr.attnotnull;
            fields.push(Field::new(&name, data_type, nullable));
            column_attnums.push(attr.attnum);
            columns.push(ResolvedColumn {
                attnum: attr.attnum,
                name,
                pg_type,
                nullable,
            });
        }

        Ok(ResolvedTable {
            table_oid: rel.oid().to_u32(),
            relation,
            column_attnums,
            schema: Arc::new(Schema::new(fields)),
            columns,
        })
    }))
    .catch_others(|error| Err(resolve_error_from_caught_error(error)))
    .execute()
}

fn validate_lookup_identifier(
    identifier: &str,
    kind: &'static str,
) -> Result<CString, ResolveError> {
    if identifier.as_bytes().contains(&0) {
        return Err(ResolveError::InvalidIdentifier(kind));
    }

    let max_bytes = pg_identifier_max_bytes();
    if identifier.len() > max_bytes {
        return Err(ResolveError::OverlongIdentifier {
            kind,
            identifier: identifier.to_owned(),
            max_bytes,
        });
    }

    CString::new(identifier).map_err(|_| ResolveError::InvalidIdentifier(kind))
}

fn bare_relation_identity(input_table: &str, rel: &PgRelation) -> ScanRelation {
    let namespace = if relation_is_temp(rel) {
        "pg_temp"
    } else {
        rel.namespace()
    };
    ScanRelation::new(Some(namespace), input_table)
}

unsafe fn oid_relation_identity(rel: &PgRelation) -> Result<ScanRelation, ResolveError> {
    let namespace = if relation_is_temp(rel) {
        "pg_temp".to_owned()
    } else {
        rel.namespace().to_owned()
    };
    let name = cstr_from_pg(pg_sys::get_rel_name(rel.oid()))?;
    Ok(ScanRelation::new(Some(namespace), name))
}

unsafe fn cstr_from_pg(ptr: *const std::ffi::c_char) -> Result<String, ResolveError> {
    if ptr.is_null() {
        return Err(ResolveError::Postgres(
            "PostgreSQL returned a null relation name".into(),
        ));
    }
    Ok(CStr::from_ptr(ptr).to_string_lossy().into_owned())
}

fn relation_is_temp(rel: &PgRelation) -> bool {
    unsafe {
        (*rel.as_ptr())
            .rd_rel
            .as_ref()
            .is_some_and(|rd_rel| pg_sys::isTempNamespace(rd_rel.relnamespace))
    }
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

fn validate_relkind(rel: &PgRelation) -> Result<(), ResolveError> {
    if rel.is_table() || rel.is_partitioned_table() || rel.is_matview() {
        return Ok(());
    }

    let relkind = unsafe { (*rel.as_ptr()).rd_rel.as_ref() }
        .map(|rd_rel| rd_rel.relkind as u8 as char)
        .unwrap_or('?');
    Err(ResolveError::UnsupportedRelationKind(relkind))
}

fn resolve_error_from_caught_error(error: CaughtError) -> ResolveError {
    let message = match error {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_owned(),
    };
    ResolveError::Postgres(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, IntervalUnit, TimeUnit};

    fn oid_to_arrow_type(oid: pg_sys::Oid, typmod: i32) -> Option<DataType> {
        arrow_type_for_pg_type(PgTypeRef::new(oid.to_u32(), typmod, 0))
    }

    #[test]
    fn oid_to_arrow_type_maps_supported_oids() {
        let cases = [
            (pg_sys::BOOLOID, DataType::Boolean),
            (pg_sys::TEXTOID, DataType::Utf8View),
            (pg_sys::VARCHAROID, DataType::Utf8View),
            (pg_sys::BPCHAROID, DataType::Utf8View),
            (pg_sys::NAMEOID, DataType::Utf8View),
            (pg_sys::INT2OID, DataType::Int16),
            (pg_sys::INT4OID, DataType::Int32),
            (pg_sys::INT8OID, DataType::Int64),
            (pg_sys::FLOAT4OID, DataType::Float32),
            (pg_sys::FLOAT8OID, DataType::Float64),
            (pg_sys::UUIDOID, DataType::FixedSizeBinary(16)),
            (pg_sys::BYTEAOID, DataType::BinaryView),
            (pg_sys::DATEOID, DataType::Date32),
            (pg_sys::TIMEOID, DataType::Time64(TimeUnit::Microsecond)),
            (
                pg_sys::TIMESTAMPOID,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                pg_sys::TIMESTAMPTZOID,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                pg_sys::INTERVALOID,
                DataType::Interval(IntervalUnit::MonthDayNano),
            ),
        ];

        for (oid, data_type) in cases {
            assert_eq!(oid_to_arrow_type(oid, -1), Some(data_type));
        }
    }

    #[test]
    fn oid_to_arrow_type_rejects_unsupported_oids() {
        assert_eq!(oid_to_arrow_type(pg_sys::TIMETZOID, -1), None);
        assert_eq!(oid_to_arrow_type(pg_sys::JSONBOID, -1), None);
    }

    #[test]
    fn oid_to_arrow_type_maps_numeric_typmods() {
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, -1),
            Some(DataType::Decimal128(38, 16))
        );
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, numeric_typmod(12, 3)),
            Some(DataType::Decimal128(12, 3))
        );
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, numeric_typmod(38, 0)),
            Some(DataType::Decimal128(38, 0))
        );
    }

    #[test]
    fn oid_to_arrow_type_rejects_unsupported_numeric_typmods() {
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, numeric_typmod(39, 0)),
            None
        );
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, numeric_typmod(3, 4)),
            None
        );
        assert_eq!(
            oid_to_arrow_type(pg_sys::NUMERICOID, numeric_typmod(3, -1)),
            None
        );
    }

    fn numeric_typmod(precision: i32, scale: i32) -> i32 {
        ((precision << 16) | (scale & 0x7ff)) + pg_sys::VARHDRSZ as i32
    }
}
