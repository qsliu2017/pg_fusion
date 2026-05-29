use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Placeholder};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::{Expr, Operator};
use df_catalog::{CatalogResolver, ResolvedTable};
use scan_node::{PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, pg_type_metadata, CompileScanInput, LimitLowering};

use crate::error::PgFrontendError;
use crate::ir::{
    PgBoolOp, PgConst, PgConstValue, PgExpr, PgFromItem, PgOperator, PgParamKind, PgQuery,
    PgRelationRef, PgTarget, PgTypeRef, PgVar,
};

#[derive(Debug)]
pub struct LoweredQuery {
    pub logical_plan: LogicalPlan,
    pub scans: Vec<Arc<PgScanSpec>>,
}

#[derive(Debug, Clone, Copy)]
pub struct LowerConfig {
    pub identifier_max_bytes: usize,
    pub first_scan_id: u64,
}

pub fn lower_query<R: CatalogResolver + Send + Sync>(
    query: PgQuery,
    resolver: &R,
    config: LowerConfig,
) -> Result<LoweredQuery, PgFrontendError> {
    validate_supported_query_shape(&query)?;
    let relation = single_relation(&query)?;
    let table_ref = datafusion_common::TableReference::partial(
        relation.schema.as_str(),
        relation.name.as_str(),
    );
    let resolved = resolver.resolve_table(&table_ref)?;
    let source_schema =
        DFSchema::try_from_qualified_schema(table_ref.clone(), resolved.schema.as_ref())?;

    let filter = query
        .selection
        .as_ref()
        .map(|expr| lower_expr(expr, &query, &resolved))
        .transpose()?;
    let filters = filter.into_iter().collect::<Vec<_>>();
    let scan_projection = visible_target_projection(&query, &resolved)?;
    let compiled = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: config.identifier_max_bytes,
        projection: Some(&scan_projection),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })?;
    ensure_no_residual_filters(&compiled.residual_filters)?;
    let spec = Arc::new(PgScanSpec::try_new(
        PgScanId::new(config.first_scan_id),
        resolved.table_oid,
        resolved.relation.clone(),
        &source_schema,
        compiled,
    )?);
    let mut plan = PgScanNode::new(Arc::clone(&spec)).into_logical_plan();

    let projection = visible_targets(&query)
        .map(|target| lower_target_expr(target, &query, &resolved))
        .collect::<Result<Vec<_>, _>>()?;
    plan = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(plan))?);

    Ok(LoweredQuery {
        logical_plan: plan,
        scans: vec![spec],
    })
}

fn validate_supported_query_shape(query: &PgQuery) -> Result<(), PgFrontendError> {
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

fn single_relation(query: &PgQuery) -> Result<&PgRelationRef, PgFrontendError> {
    let PgFromItem::Relation { rtindex } = query.from;
    query
        .relations
        .iter()
        .find(|relation| relation.rtindex == rtindex)
        .ok_or_else(|| PgFrontendError::unsupported(format!("missing rtable index {rtindex}")))
}

fn visible_targets(query: &PgQuery) -> impl Iterator<Item = &PgTarget> {
    query.targets.iter().filter(|target| !target.resjunk)
}

fn ensure_no_residual_filters(filters: &[Expr]) -> Result<(), PgFrontendError> {
    if filters.is_empty() {
        Ok(())
    } else {
        Err(PgFrontendError::unsupported(format!(
            "pg_frontend v1 requires all WHERE filters to execute inside PostgreSQL scan SQL; {} residual filter(s) would execute in DataFusion",
            filters.len()
        )))
    }
}

fn visible_target_projection(
    query: &PgQuery,
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
    expr: &PgExpr,
    query: &PgQuery,
    resolved: &ResolvedTable,
    projection: &mut Vec<usize>,
) -> Result<(), PgFrontendError> {
    match expr {
        PgExpr::Var(var) => {
            let index = var_column_index(*var, query, resolved)?;
            if !projection.contains(&index) {
                projection.push(index);
            }
            Ok(())
        }
        PgExpr::RelabelType(inner) => {
            collect_target_var_indices(inner, query, resolved, projection)
        }
        PgExpr::Bool { args, .. } => args
            .iter()
            .try_for_each(|arg| collect_target_var_indices(arg, query, resolved, projection)),
        PgExpr::NullTest { arg, .. } => {
            collect_target_var_indices(arg, query, resolved, projection)
        }
        PgExpr::BinaryOp { .. } => Err(PgFrontendError::unsupported(
            "PostgreSQL operators in SELECT targets are not supported by pg_frontend v1",
        )),
        PgExpr::Const(_) | PgExpr::Param(_) => Ok(()),
    }
}

fn lower_target_expr(
    target: &PgTarget,
    query: &PgQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    validate_target_expr(&target.expr)?;
    let expr = lower_expr(&target.expr, query, resolved)?;
    Ok(match &target.name {
        Some(name) => expr.alias(name.clone()),
        None => expr,
    })
}

fn validate_target_expr(expr: &PgExpr) -> Result<(), PgFrontendError> {
    match expr {
        PgExpr::BinaryOp { .. } => Err(PgFrontendError::unsupported(
            "PostgreSQL operators in SELECT targets are not supported by pg_frontend v1",
        )),
        PgExpr::RelabelType(inner) => validate_target_expr(inner),
        PgExpr::Bool { args, .. } => args.iter().try_for_each(validate_target_expr),
        PgExpr::NullTest { arg, .. } => validate_target_expr(arg),
        PgExpr::Var(_) | PgExpr::Const(_) | PgExpr::Param(_) => Ok(()),
    }
}

fn lower_expr(
    expr: &PgExpr,
    query: &PgQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    match expr {
        PgExpr::Var(var) => lower_var(*var, query, resolved),
        PgExpr::Const(constant) => lower_const_expr(constant),
        PgExpr::Param(param) if param.kind == PgParamKind::External => {
            let data_type = arrow_type(param.pg_type).ok_or_else(|| {
                PgFrontendError::unsupported(format!(
                    "parameter type oid {} cannot be represented in Arrow",
                    param.pg_type.oid
                ))
            })?;
            Ok(Expr::Placeholder(Placeholder::new_with_field(
                format!("${}", param.id),
                Some(Arc::new(Field::new("", data_type, true))),
            )))
        }
        PgExpr::Param(param) => Err(PgFrontendError::unsupported(format!(
            "parameter kind {:?} is not supported by pg_frontend v1",
            param.kind
        ))),
        PgExpr::RelabelType(inner) => lower_expr(inner, query, resolved),
        PgExpr::Bool { op, args } => lower_bool(*op, args, query, resolved),
        PgExpr::BinaryOp {
            op, left, right, ..
        } => Ok(binary_expr(
            lower_expr(left, query, resolved)?,
            operator(*op),
            lower_expr(right, query, resolved)?,
        )),
        PgExpr::NullTest { arg, is_null } => {
            let arg = Box::new(lower_expr(arg, query, resolved)?);
            Ok(if *is_null {
                Expr::IsNull(arg)
            } else {
                Expr::IsNotNull(arg)
            })
        }
    }
}

fn lower_var(
    var: PgVar,
    query: &PgQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    let index = var_column_index(var, query, resolved)?;
    Ok(Expr::Column(Column::from_name(
        resolved.schema.field(index).name(),
    )))
}

fn var_column_index(
    var: PgVar,
    query: &PgQuery,
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

fn lower_bool(
    op: PgBoolOp,
    args: &[PgExpr],
    query: &PgQuery,
    resolved: &ResolvedTable,
) -> Result<Expr, PgFrontendError> {
    match op {
        PgBoolOp::And | PgBoolOp::Or => {
            if args.is_empty() {
                return Err(PgFrontendError::unsupported(
                    "empty boolean expression is not supported",
                ));
            }
            let operator = if op == PgBoolOp::And {
                Operator::And
            } else {
                Operator::Or
            };
            let mut lowered = args
                .iter()
                .map(|arg| lower_expr(arg, query, resolved))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();
            let first = lowered.next().expect("checked non-empty args");
            Ok(lowered.fold(first, |left, right| binary_expr(left, operator, right)))
        }
        PgBoolOp::Not => {
            if args.len() != 1 {
                return Err(PgFrontendError::unsupported(
                    "NOT expressions must have exactly one argument",
                ));
            }
            Ok(Expr::Not(Box::new(lower_expr(&args[0], query, resolved)?)))
        }
    }
}

fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn operator(op: PgOperator) -> Operator {
    match op {
        PgOperator::Eq => Operator::Eq,
        PgOperator::NotEq => Operator::NotEq,
        PgOperator::Lt => Operator::Lt,
        PgOperator::LtEq => Operator::LtEq,
        PgOperator::Gt => Operator::Gt,
        PgOperator::GtEq => Operator::GtEq,
        PgOperator::Plus => Operator::Plus,
        PgOperator::Minus => Operator::Minus,
        PgOperator::Multiply => Operator::Multiply,
        PgOperator::Divide => Operator::Divide,
    }
}

fn lower_const(constant: &PgConst) -> Result<ScalarValue, PgFrontendError> {
    match (&constant.value, constant.pg_type.oid) {
        (None, _) => typed_null(constant.pg_type),
        (Some(PgConstValue::Bool(value)), _) => Ok(ScalarValue::Boolean(Some(*value))),
        (Some(PgConstValue::Int16(value)), _) => Ok(ScalarValue::Int16(Some(*value))),
        (Some(PgConstValue::Int32(value)), _) => Ok(ScalarValue::Int32(Some(*value))),
        (Some(PgConstValue::Int64(value)), _) => Ok(ScalarValue::Int64(Some(*value))),
        (Some(PgConstValue::Float32(value)), _) => Ok(ScalarValue::Float32(Some(*value))),
        (Some(PgConstValue::Float64(value)), _) => Ok(ScalarValue::Float64(Some(*value))),
        (Some(PgConstValue::Text(value)), _) => Ok(ScalarValue::Utf8View(Some(value.clone()))),
        (Some(PgConstValue::Binary(value)), _) => Ok(ScalarValue::BinaryView(Some(value.clone()))),
        (Some(PgConstValue::Time64Microsecond(value)), _) => {
            Ok(ScalarValue::Time64Microsecond(Some(*value)))
        }
    }
}

fn lower_const_expr(constant: &PgConst) -> Result<Expr, PgFrontendError> {
    let literal = lower_const(constant)?;
    let metadata = text_like_pg_type(constant.pg_type.oid).then(|| {
        pg_type_metadata(
            constant.pg_type.oid,
            constant.pg_type.typmod,
            constant.pg_type.collation,
        )
    });
    Ok(Expr::Literal(literal, metadata))
}

fn text_like_pg_type(oid: u32) -> bool {
    oid == oid_u32(pgrx::pg_sys::TEXTOID)
        || oid == oid_u32(pgrx::pg_sys::VARCHAROID)
        || oid == oid_u32(pgrx::pg_sys::BPCHAROID)
        || oid == oid_u32(pgrx::pg_sys::NAMEOID)
}

fn typed_null(pg_type: PgTypeRef) -> Result<ScalarValue, PgFrontendError> {
    let oid = pg_type.oid;
    if oid == oid_u32(pgrx::pg_sys::BOOLOID) {
        Ok(ScalarValue::Boolean(None))
    } else if oid == oid_u32(pgrx::pg_sys::INT2OID) {
        Ok(ScalarValue::Int16(None))
    } else if oid == oid_u32(pgrx::pg_sys::INT4OID) {
        Ok(ScalarValue::Int32(None))
    } else if oid == oid_u32(pgrx::pg_sys::INT8OID) {
        Ok(ScalarValue::Int64(None))
    } else if oid == oid_u32(pgrx::pg_sys::FLOAT4OID) {
        Ok(ScalarValue::Float32(None))
    } else if oid == oid_u32(pgrx::pg_sys::FLOAT8OID) {
        Ok(ScalarValue::Float64(None))
    } else if oid == oid_u32(pgrx::pg_sys::TEXTOID)
        || oid == oid_u32(pgrx::pg_sys::VARCHAROID)
        || oid == oid_u32(pgrx::pg_sys::BPCHAROID)
        || oid == oid_u32(pgrx::pg_sys::NAMEOID)
    {
        Ok(ScalarValue::Utf8View(None))
    } else if oid == oid_u32(pgrx::pg_sys::BYTEAOID) {
        Ok(ScalarValue::BinaryView(None))
    } else if oid == oid_u32(pgrx::pg_sys::UUIDOID) {
        Ok(ScalarValue::FixedSizeBinary(16, None))
    } else if oid == oid_u32(pgrx::pg_sys::DATEOID) {
        Ok(ScalarValue::Date32(None))
    } else if oid == oid_u32(pgrx::pg_sys::TIMEOID) {
        Ok(ScalarValue::Time64Microsecond(None))
    } else if oid == oid_u32(pgrx::pg_sys::TIMESTAMPOID)
        || oid == oid_u32(pgrx::pg_sys::TIMESTAMPTZOID)
    {
        Ok(ScalarValue::TimestampMicrosecond(None, None))
    } else if oid == oid_u32(pgrx::pg_sys::INTERVALOID) {
        Ok(ScalarValue::IntervalMonthDayNano(None))
    } else if oid == oid_u32(pgrx::pg_sys::NUMERICOID) {
        let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod).ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "numeric typmod {} cannot be represented as Arrow Decimal128",
                pg_type.typmod
            ))
        })?;
        Ok(ScalarValue::Decimal128(None, precision, scale))
    } else {
        Err(PgFrontendError::unsupported(format!(
            "type oid {oid} cannot be represented as a typed NULL"
        )))
    }
}

fn arrow_type(pg_type: PgTypeRef) -> Option<DataType> {
    let oid = pg_type.oid;
    if oid == oid_u32(pgrx::pg_sys::BOOLOID) {
        Some(DataType::Boolean)
    } else if oid == oid_u32(pgrx::pg_sys::INT2OID) {
        Some(DataType::Int16)
    } else if oid == oid_u32(pgrx::pg_sys::INT4OID) {
        Some(DataType::Int32)
    } else if oid == oid_u32(pgrx::pg_sys::INT8OID) {
        Some(DataType::Int64)
    } else if oid == oid_u32(pgrx::pg_sys::FLOAT4OID) {
        Some(DataType::Float32)
    } else if oid == oid_u32(pgrx::pg_sys::FLOAT8OID) {
        Some(DataType::Float64)
    } else if oid == oid_u32(pgrx::pg_sys::TEXTOID)
        || oid == oid_u32(pgrx::pg_sys::VARCHAROID)
        || oid == oid_u32(pgrx::pg_sys::BPCHAROID)
        || oid == oid_u32(pgrx::pg_sys::NAMEOID)
    {
        Some(DataType::Utf8View)
    } else if oid == oid_u32(pgrx::pg_sys::BYTEAOID) {
        Some(DataType::BinaryView)
    } else if oid == oid_u32(pgrx::pg_sys::UUIDOID) {
        Some(DataType::FixedSizeBinary(16))
    } else if oid == oid_u32(pgrx::pg_sys::DATEOID) {
        Some(DataType::Date32)
    } else if oid == oid_u32(pgrx::pg_sys::TIMEOID) {
        Some(DataType::Time64(TimeUnit::Microsecond))
    } else if oid == oid_u32(pgrx::pg_sys::TIMESTAMPOID)
        || oid == oid_u32(pgrx::pg_sys::TIMESTAMPTZOID)
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else if oid == oid_u32(pgrx::pg_sys::INTERVALOID) {
        Some(DataType::Interval(IntervalUnit::MonthDayNano))
    } else if oid == oid_u32(pgrx::pg_sys::NUMERICOID) {
        let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod)?;
        Some(DataType::Decimal128(precision, scale))
    } else {
        None
    }
}

fn numeric_shape_from_typmod(atttypmod: i32) -> Option<(u8, i8)> {
    if atttypmod < 0 {
        return Some((38, 16));
    }

    let typmod = atttypmod.checked_sub(pgrx::pg_sys::VARHDRSZ as i32)?;
    let precision = (typmod >> 16) & 0xffff;
    let scale = ((typmod & 0x7ff) ^ 1024) - 1024;
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
        return None;
    }

    Some((precision as u8, scale as i8))
}

fn oid_u32(oid: pgrx::pg_sys::Oid) -> u32 {
    u32::from(oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{PgCommand, PgConst, PgConstValue, PgFromItem, PgOperator, PgVar};

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

        let relabeled_binary = PgExpr::RelabelType(Box::new(binary));
        assert_target_expr_unsupported_contains(&relabeled_binary, "SELECT targets");
    }

    #[test]
    fn rejects_residual_filters_before_datafusion_filtering() {
        assert!(ensure_no_residual_filters(&[]).is_ok());

        let residual = Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        let err = ensure_no_residual_filters(&[residual]).expect_err("residual must be rejected");
        assert!(
            err.to_string().contains("residual filter"),
            "error {err} must mention residual filters"
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
                PgExpr::NullTest {
                    arg: Box::new(target_var_attnum(1)),
                    is_null: true,
                },
            ),
            target("second_again", target_var_attnum(2)),
            PgTarget {
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
            PgExpr::Const(PgConst {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            }),
        )];

        let projection = visible_target_projection(&query, &resolved).unwrap();
        assert!(projection.is_empty());
    }

    #[test]
    fn typed_null_and_param_types_cover_uuid_and_interval() {
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
        let constant = PgConst {
            pg_type: PgTypeRef {
                oid: oid_u32(pgrx::pg_sys::BPCHAROID),
                typmod: pgrx::pg_sys::VARHDRSZ as i32 + 2,
                collation: oid_u32(pgrx::pg_sys::DEFAULT_COLLATION_OID),
            },
            value: Some(PgConstValue::Text("a ".into())),
        };

        let expr = lower_const_expr(&constant).unwrap();
        let Expr::Literal(ScalarValue::Utf8View(Some(value)), Some(metadata)) = expr else {
            panic!("bpchar constant must lower to Utf8View literal with PostgreSQL metadata");
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

    fn assert_unsupported_contains(query: &PgQuery, expected: &str) {
        let err = validate_supported_query_shape(query).expect_err("query must be rejected");
        assert!(
            err.to_string().contains(expected),
            "error {err} must contain {expected}"
        );
    }

    fn assert_target_expr_unsupported_contains(expr: &PgExpr, expected: &str) {
        let err = validate_target_expr(expr).expect_err("target expression must be rejected");
        assert!(
            err.to_string().contains(expected),
            "error {err} must contain {expected}"
        );
    }

    fn target_binary_op() -> PgExpr {
        PgExpr::BinaryOp {
            op: PgOperator::Plus,
            left: Box::new(target_var()),
            right: Box::new(PgExpr::Const(PgConst {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: int4_type(),
        }
    }

    fn target_var() -> PgExpr {
        target_var_attnum(1)
    }

    fn target_var_attnum(attnum: i16) -> PgExpr {
        PgExpr::Var(PgVar {
            rtindex: 1,
            attnum,
            pg_type: int4_type(),
        })
    }

    fn target(name: &str, expr: PgExpr) -> PgTarget {
        PgTarget {
            expr,
            name: Some(name.into()),
            pg_type: int4_type(),
            resno: 1,
            resjunk: false,
        }
    }

    fn query_for_resolved_table() -> PgQuery {
        let mut query = base_query();
        query.relations = vec![PgRelationRef {
            rtindex: 1,
            relid: 42,
            schema: "public".into(),
            name: "t".into(),
            alias: None,
            columns: Vec::new(),
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

    fn base_query() -> PgQuery {
        PgQuery {
            command: PgCommand::Select,
            relations: Vec::new(),
            from: PgFromItem::Relation { rtindex: 1 },
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
