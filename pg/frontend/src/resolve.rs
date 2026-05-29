use df_catalog::CatalogResolver;

use crate::error::PgFrontendError;
use crate::typed_query::{ColumnRef, TypedQuery};

/// Borrowed proof that a [`TypedQuery`] has been resolved against a PostgreSQL catalog.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedQuery<'a> {
    query: &'a TypedQuery,
}

impl<'a> ResolvedQuery<'a> {
    pub fn query(&self) -> &'a TypedQuery {
        self.query
    }
}

impl TypedQuery {
    /// Resolve catalog metadata in place and return a resolved query view.
    pub fn resolve_catalog<R>(&mut self, resolver: &R) -> Result<ResolvedQuery<'_>, PgFrontendError>
    where
        R: CatalogResolver + Send + Sync,
    {
        resolve_catalog(self, resolver)
    }
}

pub fn resolve_catalog<'a, R>(
    query: &'a mut TypedQuery,
    resolver: &R,
) -> Result<ResolvedQuery<'a>, PgFrontendError>
where
    R: CatalogResolver + Send + Sync,
{
    for relation in &mut query.relations {
        let resolved = resolver.resolve_relation_oid(relation.relid)?;
        if resolved.table_oid != relation.relid {
            return Err(PgFrontendError::unsupported(format!(
                "catalog resolver returned relation oid {} for Query relid {}",
                resolved.table_oid, relation.relid
            )));
        }

        relation.columns = resolved
            .columns
            .iter()
            .map(|column| ColumnRef {
                attnum: column.attnum,
                name: column.name.clone(),
                pg_type: column.pg_type,
                nullable: column.nullable,
            })
            .collect();
        relation.schema = resolved.relation.schema.unwrap_or_default();
        relation.name = resolved.relation.table;
        relation.catalog_resolved = true;
    }

    Ok(ResolvedQuery { query })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::TableReference;
    use df_catalog::{CatalogResolver, ResolveError, ResolvedColumn, ResolvedTable};
    use pg_type::PgTypeRef;

    use super::*;
    use crate::typed_query::{FromItem, QueryCommand, RelationRef};

    #[test]
    fn resolve_catalog_mutates_relation_metadata_in_place() {
        let mut query = TypedQuery {
            command: QueryCommand::Select,
            relations: vec![RelationRef {
                rtindex: 1,
                relid: 42,
                schema: "pg_temp_3".into(),
                name: "items".into(),
                alias: None,
                columns: Vec::new(),
                catalog_resolved: false,
            }],
            from: FromItem::Relation { rtindex: 1 },
            selection: None,
            targets: Vec::new(),
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            has_distinct: false,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
            has_limit: false,
            has_sort: false,
            has_row_marks: false,
        };

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("catalog metadata should resolve");
        assert!(resolved.query().relations[0].catalog_resolved);
        assert_eq!(resolved.query().relations[0].schema, "pg_temp");
        assert_eq!(resolved.query().relations[0].name, "items");
        assert_eq!(resolved.query().relations[0].columns.len(), 1);
        assert_eq!(resolved.query().relations[0].columns[0].name, "id");
    }

    #[test]
    fn resolve_catalog_clears_schema_for_bare_relation_identity() {
        let mut query = TypedQuery {
            command: QueryCommand::Select,
            relations: vec![RelationRef {
                rtindex: 1,
                relid: 42,
                schema: "public".into(),
                name: "items".into(),
                alias: None,
                columns: Vec::new(),
                catalog_resolved: false,
            }],
            from: FromItem::Relation { rtindex: 1 },
            selection: None,
            targets: Vec::new(),
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            has_distinct: false,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
            has_limit: false,
            has_sort: false,
            has_row_marks: false,
        };

        let resolved = query
            .resolve_catalog(&BareRelationResolver)
            .expect("catalog metadata should resolve");
        assert!(resolved.query().relations[0].schema.is_empty());
        assert_eq!(resolved.query().relations[0].name, "items");
    }

    #[derive(Debug)]
    struct FakeResolver;

    #[derive(Debug)]
    struct BareRelationResolver;

    impl CatalogResolver for FakeResolver {
        fn resolve_table(&self, table: &TableReference) -> Result<ResolvedTable, ResolveError> {
            Err(ResolveError::Postgres(format!(
                "unexpected name lookup for {table}"
            )))
        }

        fn resolve_relation_oid(&self, relid: u32) -> Result<ResolvedTable, ResolveError> {
            assert_eq!(relid, 42);
            let pg_type = PgTypeRef::new(u32::from(pgrx::pg_sys::INT4OID), -1, 0);
            Ok(ResolvedTable {
                table_oid: 42,
                relation: scan_sql::PgRelation::new(Some("pg_temp"), "items"),
                column_attnums: vec![1],
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                columns: vec![ResolvedColumn {
                    attnum: 1,
                    name: "id".into(),
                    pg_type,
                    nullable: false,
                }],
            })
        }
    }

    impl CatalogResolver for BareRelationResolver {
        fn resolve_table(&self, table: &TableReference) -> Result<ResolvedTable, ResolveError> {
            Err(ResolveError::Postgres(format!(
                "unexpected name lookup for {table}"
            )))
        }

        fn resolve_relation_oid(&self, relid: u32) -> Result<ResolvedTable, ResolveError> {
            assert_eq!(relid, 42);
            let pg_type = PgTypeRef::new(u32::from(pgrx::pg_sys::INT4OID), -1, 0);
            Ok(ResolvedTable {
                table_oid: 42,
                relation: scan_sql::PgRelation::new(None::<String>, "items"),
                column_attnums: vec![1],
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                columns: vec![ResolvedColumn {
                    attnum: 1,
                    name: "id".into(),
                    pg_type,
                    nullable: false,
                }],
            })
        }
    }
}
