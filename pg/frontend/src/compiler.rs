use std::sync::Arc;

#[cfg(test)]
use arrow_schema::DataType;
use arrow_schema::{Field, Schema};
use datafusion_common::{Column, ScalarValue, TableReference};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::logical_plan::{LogicalPlan, Projection, TableScan};
use datafusion_expr::{Expr, Operator, TableSource};
use df_catalog::{PgPlanningTableSource, ResolvedColumn, ResolvedTable};
use pg_type::{arrow_type_for_pg_type, is_text_like_type, scalar_for_pg_const};
use scan_sql::pg_type_metadata;

use crate::error::PgFrontendError;
use crate::resolve::ResolvedQuery;
#[cfg(test)]
use crate::typed_query::PgTypeRef;
use crate::typed_query::{
    BoolOp, Const, FromItem, QueryExpr, QueryOperator, RelationRef, Target, TypedQuery, Var,
};

#[derive(Debug)]
pub struct CompiledQuery {
    pub logical_plan: LogicalPlan,
}

#[derive(Debug, Clone, Copy)]
pub struct CompileConfig {
    pub identifier_max_bytes: usize,
}

pub fn compile_query(
    query: ResolvedQuery<'_>,
    config: CompileConfig,
) -> Result<CompiledQuery, PgFrontendError> {
    let query = query.query();
    validate_supported_query_shape(query)?;
    let relation = single_relation(query)?;
    let resolved = resolved_table_for_relation(relation)?;
    if let Some(schema) = resolved.relation.schema.as_deref() {
        validate_identifier_len(schema, config.identifier_max_bytes, "schema")?;
    }
    validate_identifier_len(
        &resolved.relation.table,
        config.identifier_max_bytes,
        "table",
    )?;
    let table_ref = table_reference_for_resolved_relation(&resolved.relation);

    let filter = query
        .selection
        .as_ref()
        .map(|expr| compile_expr(expr, query, &resolved))
        .transpose()?;
    let filters = filter.into_iter().collect::<Vec<_>>();
    let scan_projection = visible_target_projection(query, &resolved)?;
    let source = Arc::new(PgPlanningTableSource::new(resolved.clone())) as Arc<dyn TableSource>;
    let table_scan = TableScan::try_new(table_ref, source, Some(scan_projection), filters, None)?;
    let mut plan = LogicalPlan::TableScan(table_scan);

    let projection = visible_targets(query)
        .map(|target| compile_target_expr(target, query, &resolved))
        .collect::<Result<Vec<_>, _>>()?;
    plan = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(plan))?);

    Ok(CompiledQuery { logical_plan: plan })
}

fn table_reference_for_resolved_relation(relation: &scan_sql::PgRelation) -> TableReference {
    match relation.schema.as_deref() {
        Some(schema) => TableReference::partial(schema, relation.table.as_str()),
        None => TableReference::bare(relation.table.as_str()),
    }
}

fn validate_identifier_len(
    identifier: &str,
    max_bytes: usize,
    kind: &'static str,
) -> Result<(), PgFrontendError> {
    if identifier.len() > max_bytes {
        return Err(PgFrontendError::unsupported(format!(
            "{kind} identifier `{identifier}` exceeds PostgreSQL limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

fn resolved_table_for_relation(relation: &RelationRef) -> Result<ResolvedTable, PgFrontendError> {
    if !relation.catalog_resolved {
        return Err(PgFrontendError::unsupported(format!(
            "relation rtindex {} was not resolved before compilation",
            relation.rtindex
        )));
    }

    let mut fields = Vec::with_capacity(relation.columns.len());
    let mut column_attnums = Vec::with_capacity(relation.columns.len());
    let mut columns = Vec::with_capacity(relation.columns.len());
    for column in &relation.columns {
        let data_type = arrow_type_for_pg_type(column.pg_type).ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "resolved column {} has unsupported PostgreSQL type oid {}",
                column.name, column.pg_type.oid
            ))
        })?;
        fields.push(Field::new(&column.name, data_type, column.nullable));
        column_attnums.push(column.attnum);
        columns.push(ResolvedColumn {
            attnum: column.attnum,
            name: column.name.clone(),
            pg_type: column.pg_type,
            nullable: column.nullable,
        });
    }

    let schema = (!relation.schema.is_empty()).then_some(relation.schema.as_str());
    Ok(ResolvedTable {
        table_oid: relation.relid,
        relation: scan_sql::PgRelation::new(schema, relation.name.as_str()),
        column_attnums,
        schema: Arc::new(Schema::new(fields)),
        columns,
    })
}

fn validate_supported_query_shape(query: &TypedQuery) -> Result<(), PgFrontendError> {
    if query.has_aggregates {
        return Err(PgFrontendError::unsupported(
            "aggregates are not supported by pg_frontend v1",
        ));
    }
    if query.has_windows {
        return Err(PgFrontendError::unsupported(
            "window functions are not supported by pg_frontend v1",
        ));
    }
    if query.has_sublinks {
        return Err(PgFrontendError::unsupported(
            "subqueries are not supported by pg_frontend v1",
        ));
    }
    if query.has_distinct {
        return Err(PgFrontendError::unsupported(
            "DISTINCT is not supported by pg_frontend v1",
        ));
    }
    if query.has_group_by {
        return Err(PgFrontendError::unsupported(
            "GROUP BY is not supported by pg_frontend v1",
        ));
    }
    if query.has_having {
        return Err(PgFrontendError::unsupported(
            "HAVING is not supported by pg_frontend v1",
        ));
    }
    if query.has_grouping_sets {
        return Err(PgFrontendError::unsupported(
            "grouping sets are not supported by pg_frontend v1",
        ));
    }
    if query.has_set_operations {
        return Err(PgFrontendError::unsupported(
            "set operations are not supported by pg_frontend v1",
        ));
    }
    if query.has_limit {
        return Err(PgFrontendError::unsupported(
            "LIMIT/OFFSET is not supported by pg_frontend v1",
        ));
    }
    if query.has_sort {
        return Err(PgFrontendError::unsupported(
            "ORDER BY is not supported by pg_frontend v1",
        ));
    }
    if query.has_row_marks {
        return Err(PgFrontendError::unsupported(
            "row-locking clauses are not supported by pg_frontend v1",
        ));
    }
    Ok(())
}

fn single_relation(query: &TypedQuery) -> Result<&RelationRef, PgFrontendError> {
    let FromItem::Relation { rtindex } = query.from;
    query
        .relations
        .iter()
        .find(|relation| relation.rtindex == rtindex)
        .ok_or_else(|| PgFrontendError::unsupported(format!("missing rtable index {rtindex}")))
}

fn visible_targets(query: &TypedQuery) -> impl Iterator<Item = &Target> {
    query.targets.iter().filter(|target| !target.resjunk)
}

fn visible_target_projection(
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<Vec<usize>, PgFrontendError> {
    let mut projection = Vec::new();
    for target in visible_targets(query) {
        validate_target_expr(&target.expr)?;
        collect_target_var_indices(&target.expr, query, resolved, &mut projection)?;
    }
    Ok(projection)
}

fn collect_target_var_indices(
    expr: &QueryExpr,
    query: &TypedQuery,
    resolved: &ResolvedTable,
    projection: &mut Vec<usize>,
) -> Result<(), PgFrontendError> {
    match expr {
        QueryExpr::Var(var) => {
            let index = var_column_index(*var, query, resolved)?;
            if !projection.contains(&index) {
                projection.push(index);
            }
            Ok(())
        }
        QueryExpr::RelabelType(inner) => {
            collect_target_var_indices(inner, query, resolved, projection)
        }
        QueryExpr::Bool { args, .. } => args
            .iter()
            .try_for_each(|arg| collect_target_var_indices(arg, query, resolved, projection)),
        QueryExpr::NullTest { arg, .. } => {
            collect_target_var_indices(arg, query, resolved, projection)
        }
        QueryExpr::BinaryOp { .. } => Err(PgFrontendError::unsupported(
            "PostgreSQL operators in SELECT targets are not supported by pg_frontend v1",
        )),
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::Const(_) => Ok(()),
    }
}

fn compile_target_expr(
    target: &Target,
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    validate_target_expr(&target.expr)?;
    let expr = compile_expr(&target.expr, query, resolved)?;
    Ok(match &target.name {
        Some(name) => expr.alias(name.clone()),
        None => expr,
    })
}

fn validate_target_expr(expr: &QueryExpr) -> Result<(), PgFrontendError> {
    match expr {
        QueryExpr::BinaryOp { .. } => Err(PgFrontendError::unsupported(
            "PostgreSQL operators in SELECT targets are not supported by pg_frontend v1",
        )),
        QueryExpr::RelabelType(inner) => validate_target_expr(inner),
        QueryExpr::Bool { args, .. } => args.iter().try_for_each(validate_target_expr),
        QueryExpr::NullTest { arg, .. } => validate_target_expr(arg),
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::Var(_) | QueryExpr::Const(_) => Ok(()),
    }
}

fn compile_expr(
    expr: &QueryExpr,
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    match expr {
        QueryExpr::Var(var) => compile_var(*var, query, resolved),
        QueryExpr::Const(constant) => compile_const_expr(constant),
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::RelabelType(inner) => compile_expr(inner, query, resolved),
        QueryExpr::Bool { op, args } => compile_bool(*op, args, query, resolved),
        QueryExpr::BinaryOp {
            op, left, right, ..
        } => Ok(binary_expr(
            compile_expr(left, query, resolved)?,
            operator(*op),
            compile_expr(right, query, resolved)?,
        )),
        QueryExpr::NullTest { arg, is_null } => {
            let arg = Box::new(compile_expr(arg, query, resolved)?);
            Ok(if *is_null {
                Expr::IsNull(arg)
            } else {
                Expr::IsNotNull(arg)
            })
        }
    }
}

fn compile_var(
    var: Var,
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    let index = var_column_index(var, query, resolved)?;
    Ok(Expr::Column(Column::from_name(
        resolved.schema.field(index).name(),
    )))
}

fn var_column_index(
    var: Var,
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<usize, PgFrontendError> {
    let relation = query
        .relations
        .iter()
        .find(|relation| relation.rtindex == var.rtindex)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!("Var references missing rtindex {}", var.rtindex))
        })?;
    if relation.relid != resolved.table_oid {
        return Err(PgFrontendError::unsupported(
            "multi-relation Vars are not supported by pg_frontend v1",
        ));
    }
    let index = resolved
        .column_attnums
        .iter()
        .position(|attnum| *attnum == var.attnum)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "attribute {} is not present in resolved relation",
                var.attnum
            ))
        })?;
    Ok(index)
}

fn compile_bool(
    op: BoolOp,
    args: &[QueryExpr],
    query: &TypedQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    match op {
        BoolOp::And | BoolOp::Or => {
            if args.is_empty() {
                return Err(PgFrontendError::unsupported(
                    "empty boolean expression is not supported",
                ));
            }
            let operator = if op == BoolOp::And {
                Operator::And
            } else {
                Operator::Or
            };
            let mut compiled = args
                .iter()
                .map(|arg| compile_expr(arg, query, resolved))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();
            let first = compiled.next().expect("checked non-empty args");
            Ok(compiled.fold(first, |left, right| binary_expr(left, operator, right)))
        }
        BoolOp::Not => {
            if args.len() != 1 {
                return Err(PgFrontendError::unsupported(
                    "NOT expressions must have exactly one argument",
                ));
            }
            Ok(Expr::Not(Box::new(compile_expr(
                &args[0], query, resolved,
            )?)))
        }
    }
}

fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn operator(op: QueryOperator) -> Operator {
    match op {
        QueryOperator::Eq => Operator::Eq,
        QueryOperator::NotEq => Operator::NotEq,
        QueryOperator::Lt => Operator::Lt,
        QueryOperator::LtEq => Operator::LtEq,
        QueryOperator::Gt => Operator::Gt,
        QueryOperator::GtEq => Operator::GtEq,
        QueryOperator::Plus => Operator::Plus,
        QueryOperator::Minus => Operator::Minus,
        QueryOperator::Multiply => Operator::Multiply,
        QueryOperator::Divide => Operator::Divide,
    }
}

fn compile_const_scalar(constant: &Const) -> Result<ScalarValue, PgFrontendError> {
    scalar_for_pg_const(constant.value.as_ref(), constant.pg_type)
        .map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

fn compile_const_expr(constant: &Const) -> Result<Expr, PgFrontendError> {
    let literal = compile_const_scalar(constant)?;
    let metadata = is_text_like_type(constant.pg_type.oid).then(|| {
        pg_type_metadata(
            constant.pg_type.oid,
            constant.pg_type.typmod,
            constant.pg_type.collation,
        )
    });
    Ok(Expr::Literal(literal, metadata))
}

#[cfg(test)]
fn typed_null(pg_type: PgTypeRef) -> Result<ScalarValue, PgFrontendError> {
    pg_type::typed_null_scalar(pg_type).map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

#[cfg(test)]
fn arrow_type(pg_type: PgTypeRef) -> Option<DataType> {
    arrow_type_for_pg_type(pg_type)
}

#[cfg(test)]
fn oid_u32(oid: pgrx::pg_sys::Oid) -> u32 {
    u32::from(oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_query::{
        Const, FromItem, Param, ParamKind, PgConstValue, QueryCommand, QueryOperator, Var,
    };
    use arrow_schema::IntervalUnit;
    use df_catalog::CatalogResolver;

    #[test]
    fn rejects_group_by_having_and_row_locks() {
        let mut query = base_query();
        query.has_group_by = true;
        assert_unsupported_contains(&query, "GROUP BY");

        let mut query = base_query();
        query.has_having = true;
        assert_unsupported_contains(&query, "HAVING");

        let mut query = base_query();
        query.has_row_marks = true;
        assert_unsupported_contains(&query, "row-locking");
    }

    #[test]
    fn validates_projection_expression_shapes() {
        assert!(validate_target_expr(&target_var()).is_ok());

        let binary = target_binary_op();
        assert_target_expr_unsupported_contains(&binary, "SELECT targets");

        let relabeled_binary = QueryExpr::RelabelType(Box::new(binary));
        assert_target_expr_unsupported_contains(&relabeled_binary, "SELECT targets");
    }

    #[test]
    fn rejects_parameters_in_targets_and_filters() {
        let param = QueryExpr::Param(Param {
            kind: ParamKind::External,
            id: 1,
            pg_type: int4_type(),
        });

        assert_target_expr_unsupported_contains(&param, "parameters");

        let resolved = resolved_table();
        let query = query_for_resolved_table();
        let err = compile_expr(&param, &query, &resolved).expect_err("Param must be rejected");
        assert!(
            err.to_string().contains("parameters"),
            "error {err} must mention parameters"
        );
    }

    #[test]
    fn scan_projection_uses_only_visible_target_vars() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.targets = vec![
            target("second", target_var_attnum(2)),
            target(
                "is_first_null",
                QueryExpr::NullTest {
                    arg: Box::new(target_var_attnum(1)),
                    is_null: true,
                },
            ),
            target("second_again", target_var_attnum(2)),
            Target {
                expr: target_var_attnum(3),
                name: Some("hidden".into()),
                pg_type: int4_type(),
                resno: 4,
                resjunk: true,
            },
        ];

        let projection = visible_target_projection(&query, &resolved).unwrap();
        assert_eq!(projection, vec![1, 0]);
    }

    #[test]
    fn scan_projection_is_empty_for_constant_only_targets() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.targets = vec![target(
            "one",
            QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            }),
        )];

        let projection = visible_target_projection(&query, &resolved).unwrap();
        assert!(projection.is_empty());
    }

    #[test]
    fn compile_query_builds_typed_table_scan_plan() {
        let mut query = query_for_resolved_table();
        query.targets = vec![target("second", target_var_attnum(2))];
        query.selection = Some(QueryExpr::BinaryOp {
            op: QueryOperator::Eq,
            left: Box::new(target_var_attnum(1)),
            right: Box::new(QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: type_ref(pgrx::pg_sys::BOOLOID),
        });

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("frontend query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(rendered.contains("TableScan"), "{rendered}");
        assert!(rendered.contains("first"), "{rendered}");
        assert!(rendered.contains("second"), "{rendered}");
    }

    #[test]
    fn resolve_catalog_rejects_resolver_oid_mismatch() {
        #[derive(Debug)]
        struct MismatchResolver;

        impl CatalogResolver for MismatchResolver {
            fn resolve_table(
                &self,
                table: &datafusion_common::TableReference,
            ) -> Result<ResolvedTable, df_catalog::ResolveError> {
                Err(df_catalog::ResolveError::Postgres(format!(
                    "unexpected name lookup for {table}"
                )))
            }

            fn resolve_relation_oid(
                &self,
                _relid: u32,
            ) -> Result<ResolvedTable, df_catalog::ResolveError> {
                let mut resolved = resolved_table();
                resolved.table_oid = 99;
                Ok(resolved)
            }
        }

        let mut query = query_for_resolved_table();
        let err = query
            .resolve_catalog(&MismatchResolver)
            .expect_err("oid mismatch must fail closed");
        assert!(
            err.to_string().contains("relation oid"),
            "error {err} should mention relation oid mismatch"
        );
    }

    #[test]
    fn typed_null_and_arrow_types_cover_uuid_and_interval() {
        assert_eq!(
            typed_null(type_ref(pgrx::pg_sys::UUIDOID)).unwrap(),
            ScalarValue::FixedSizeBinary(16, None)
        );
        assert_eq!(
            arrow_type(type_ref(pgrx::pg_sys::UUIDOID)),
            Some(DataType::FixedSizeBinary(16))
        );

        assert_eq!(
            typed_null(type_ref(pgrx::pg_sys::INTERVALOID)).unwrap(),
            ScalarValue::IntervalMonthDayNano(None)
        );
        assert_eq!(
            arrow_type(type_ref(pgrx::pg_sys::INTERVALOID)),
            Some(DataType::Interval(IntervalUnit::MonthDayNano))
        );
    }

    #[test]
    fn text_like_constants_keep_pg_type_metadata() {
        let constant = Const {
            pg_type: PgTypeRef {
                oid: oid_u32(pgrx::pg_sys::BPCHAROID),
                typmod: pgrx::pg_sys::VARHDRSZ as i32 + 2,
                collation: oid_u32(pgrx::pg_sys::DEFAULT_COLLATION_OID),
            },
            value: Some(PgConstValue::Text("a ".into())),
        };

        let expr = compile_const_expr(&constant).unwrap();
        let Expr::Literal(ScalarValue::Utf8View(Some(value)), Some(metadata)) = expr else {
            panic!("bpchar constant must compile to Utf8View literal with PostgreSQL metadata");
        };

        assert_eq!(value, "a ");
        assert_eq!(
            metadata.inner().get("pg_fusion.pg_type_oid"),
            Some(&oid_u32(pgrx::pg_sys::BPCHAROID).to_string())
        );
        assert_eq!(
            metadata.inner().get("pg_fusion.pg_type_typmod"),
            Some(&(pgrx::pg_sys::VARHDRSZ as i32 + 2).to_string())
        );
    }

    fn assert_unsupported_contains(query: &TypedQuery, expected: &str) {
        let err = validate_supported_query_shape(query).expect_err("query must be rejected");
        assert!(
            err.to_string().contains(expected),
            "error {err} must contain {expected}"
        );
    }

    fn assert_target_expr_unsupported_contains(expr: &QueryExpr, expected: &str) {
        let err = validate_target_expr(expr).expect_err("target expression must be rejected");
        assert!(
            err.to_string().contains(expected),
            "error {err} must contain {expected}"
        );
    }

    fn target_binary_op() -> QueryExpr {
        QueryExpr::BinaryOp {
            op: QueryOperator::Plus,
            left: Box::new(target_var()),
            right: Box::new(QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: int4_type(),
        }
    }

    fn target_var() -> QueryExpr {
        target_var_attnum(1)
    }

    fn target_var_attnum(attnum: i16) -> QueryExpr {
        QueryExpr::Var(Var {
            rtindex: 1,
            attnum,
            pg_type: int4_type(),
        })
    }

    fn target(name: &str, expr: QueryExpr) -> Target {
        Target {
            expr,
            name: Some(name.into()),
            pg_type: int4_type(),
            resno: 1,
            resjunk: false,
        }
    }

    fn query_for_resolved_table() -> TypedQuery {
        let mut query = base_query();
        query.relations = vec![RelationRef {
            rtindex: 1,
            relid: 42,
            schema: "public".into(),
            name: "t".into(),
            alias: None,
            columns: Vec::new(),
            catalog_resolved: false,
        }];
        query
    }

    fn resolved_table() -> ResolvedTable {
        ResolvedTable {
            table_oid: 42,
            relation: scan_sql::PgRelation::new(Some("public"), "t"),
            column_attnums: vec![1, 2, 3],
            schema: Arc::new(arrow_schema::Schema::new(vec![
                Field::new("first", DataType::Int32, true),
                Field::new("second", DataType::Int32, true),
                Field::new("unused", DataType::Int32, true),
            ])),
            columns: vec![
                ResolvedColumn {
                    attnum: 1,
                    name: "first".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
                ResolvedColumn {
                    attnum: 2,
                    name: "second".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
                ResolvedColumn {
                    attnum: 3,
                    name: "unused".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
            ],
        }
    }

    #[derive(Debug)]
    struct FakeResolver;

    impl CatalogResolver for FakeResolver {
        fn resolve_table(
            &self,
            table: &datafusion_common::TableReference,
        ) -> Result<ResolvedTable, df_catalog::ResolveError> {
            Err(df_catalog::ResolveError::Postgres(format!(
                "unexpected name lookup for {table}"
            )))
        }

        fn resolve_relation_oid(
            &self,
            relid: u32,
        ) -> Result<ResolvedTable, df_catalog::ResolveError> {
            if relid == 42 {
                Ok(resolved_table())
            } else {
                Err(df_catalog::ResolveError::Postgres(format!(
                    "relation oid {relid} not found"
                )))
            }
        }
    }

    fn int4_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::INT4OID)
    }

    fn type_ref(oid: pgrx::pg_sys::Oid) -> PgTypeRef {
        PgTypeRef {
            oid: oid_u32(oid),
            typmod: -1,
            collation: 0,
        }
    }

    fn base_query() -> TypedQuery {
        TypedQuery {
            command: QueryCommand::Select,
            relations: Vec::new(),
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
        }
    }
}
