use df_catalog::CatalogResolver;

use crate::error::PgFrontendError;
use crate::typed_query::{ColumnRef, QueryExpr, TypedQuery};

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

    for cte in &mut query.ctes {
        resolve_catalog(&mut cte.query, resolver)?;
    }
    for subquery in &mut query.subqueries {
        resolve_catalog(&mut subquery.query, resolver)?;
    }
    for target in &mut query.targets {
        resolve_expr_catalog(&mut target.expr, resolver)?;
    }
    if let Some(selection) = &mut query.selection {
        resolve_expr_catalog(selection, resolver)?;
    }
    if let Some(having) = &mut query.having {
        resolve_expr_catalog(having, resolver)?;
    }
    if let Some(limit_count) = &mut query.limit_count {
        resolve_expr_catalog(limit_count, resolver)?;
    }
    if let Some(limit_offset) = &mut query.limit_offset {
        resolve_expr_catalog(limit_offset, resolver)?;
    }

    Ok(ResolvedQuery { query })
}

fn resolve_expr_catalog<R>(expr: &mut QueryExpr, resolver: &R) -> Result<(), PgFrontendError>
where
    R: CatalogResolver + Send + Sync,
{
    match expr {
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => resolve_expr_catalog(inner, resolver),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args
            .iter_mut()
            .try_for_each(|arg| resolve_expr_catalog(arg, resolver)),
        QueryExpr::ArraySubscript { array, index, .. } => {
            resolve_expr_catalog(array, resolver)?;
            resolve_expr_catalog(index, resolver)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                resolve_expr_catalog(operand, resolver)?;
            }
            for (when, then) in when_then {
                resolve_expr_catalog(when, resolver)?;
                resolve_expr_catalog(then, resolver)?;
            }
            if let Some(else_expr) = else_expr {
                resolve_expr_catalog(else_expr, resolver)?;
            }
            Ok(())
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            resolve_expr_catalog(left, resolver)?;
            resolve_expr_catalog(right, resolver)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                resolve_expr_catalog(arg, resolver)?;
            }
            if let Some(filter) = filter {
                resolve_expr_catalog(filter, resolver)?;
            }
            Ok(())
        }
        QueryExpr::ScalarSubquery(query)
        | QueryExpr::ExistsSubquery {
            subquery: query, ..
        } => {
            resolve_catalog(query, resolver)?;
            Ok(())
        }
        QueryExpr::InSubquery { expr, subquery, .. } => {
            resolve_expr_catalog(expr, resolver)?;
            resolve_catalog(subquery, resolver)?;
            Ok(())
        }
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::TableReference;
    use df_catalog::{CatalogResolver, ResolveError, ResolvedColumn, ResolvedTable};
    use pg_type::PgTypeRef;

    use super::*;
    use crate::typed_query::{DistinctSpec, FromItem, QueryCommand, RelationRef};

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
            values: Vec::new(),
            ctes: Vec::new(),
            cte_refs: Vec::new(),
            subqueries: Vec::new(),
            from: FromItem::Relation { rtindex: 1 },
            selection: None,
            having: None,
            targets: Vec::new(),
            group_refs: Vec::new(),
            grouping_sets: Vec::new(),
            windows: Vec::new(),
            set_operation: None,
            sort: Vec::new(),
            limit_count: None,
            limit_offset: None,
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            distinct: DistinctSpec::None,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
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
            values: Vec::new(),
            ctes: Vec::new(),
            cte_refs: Vec::new(),
            subqueries: Vec::new(),
            from: FromItem::Relation { rtindex: 1 },
            selection: None,
            having: None,
            targets: Vec::new(),
            group_refs: Vec::new(),
            grouping_sets: Vec::new(),
            windows: Vec::new(),
            set_operation: None,
            sort: Vec::new(),
            limit_count: None,
            limit_offset: None,
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            distinct: DistinctSpec::None,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
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
