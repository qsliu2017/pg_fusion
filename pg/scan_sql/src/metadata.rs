use std::collections::BTreeMap;

use datafusion_common::metadata::FieldMetadata;

pub(crate) const PG_TYPE_OID_KEY: &str = "pg_fusion.pg_type_oid";
pub(crate) const PG_TYPE_TYPMOD_KEY: &str = "pg_fusion.pg_type_typmod";
pub(crate) const PG_TYPE_COLLATION_KEY: &str = "pg_fusion.pg_type_collation";

/// Attach PostgreSQL type provenance to a DataFusion literal.
///
/// `scan_sql` consumes this metadata when it must render a literal with
/// PostgreSQL type semantics that cannot be recovered from the Arrow scalar
/// alone, such as `bpchar` versus `text`.
pub fn pg_type_metadata(oid: u32, typmod: i32, collation: u32) -> FieldMetadata {
    FieldMetadata::from(BTreeMap::from([
        (PG_TYPE_OID_KEY.to_string(), oid.to_string()),
        (PG_TYPE_TYPMOD_KEY.to_string(), typmod.to_string()),
        (PG_TYPE_COLLATION_KEY.to_string(), collation.to_string()),
    ]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PgTypeMetadata {
    pub(crate) oid: u32,
    pub(crate) typmod: i32,
}

pub(crate) fn read_pg_type_metadata(metadata: &FieldMetadata) -> Option<Option<PgTypeMetadata>> {
    let values = metadata.inner();
    let has_pg_key = values.contains_key(PG_TYPE_OID_KEY)
        || values.contains_key(PG_TYPE_TYPMOD_KEY)
        || values.contains_key(PG_TYPE_COLLATION_KEY);
    if !has_pg_key {
        return Some(None);
    }

    let oid = values.get(PG_TYPE_OID_KEY)?.parse().ok()?;
    let typmod = values.get(PG_TYPE_TYPMOD_KEY)?.parse().ok()?;
    values.get(PG_TYPE_COLLATION_KEY)?.parse::<u32>().ok()?;
    Some(Some(PgTypeMetadata { oid, typmod }))
}
