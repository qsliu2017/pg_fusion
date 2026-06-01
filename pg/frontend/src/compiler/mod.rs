use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{
    metadata::FieldMetadata, Column, DFSchema, NullEquality, ScalarValue, Spans, TableReference,
};
use datafusion_expr::expr::{
    AggregateFunction as DfAggregateFunction, Alias, BinaryExpr, Case, Cast, GroupingSet,
    InSubquery, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::logical_plan::{
    Aggregate, Distinct, DistinctOn, EmptyRelation, Join, Limit, LogicalPlan, Projection, Sort,
    SubqueryAlias, TableScan, Union, Values, Window,
};
use datafusion_expr::{
    AggregateUDF, Expr, JoinConstraint, JoinType, Operator, ScalarUDF, Subquery, TableSource,
    WindowFrame, WindowFrameBound as DfWindowFrameBound, WindowFrameUnits as DfWindowFrameUnits,
    WindowFunctionDefinition,
};
use df_catalog::{PgPlanningTableSource, ResolvedColumn, ResolvedTable};
use pg_type::{
    arrow_type_for_pg_type, is_text_like_type, scalar_for_pg_const, PgConstValue,
    PG_NUMERIC_TRIM_TRAILING_ZEROS_METADATA_KEY,
};
use scan_sql::pg_type_metadata;

use crate::error::PgFrontendError;
use crate::resolve::ResolvedQuery;
use crate::typed_query::{
    AggregateFunction, BoolOp, BooleanTestKind, Const, CteRangeRef, DistinctSpec, FromItem,
    GroupingSetSpec, JoinKind, OuterVar, QueryExpr, QueryOperator, QueryUnaryOperator, RelationRef,
    ScalarFunction, SetOperationTree, SetOperator, SortKey, SubqueryRef, Target, TypedQuery,
    ValuesRef, Var, WindowFrameBound, WindowFrameSpec, WindowFrameUnits, WindowFunctionKind,
    WindowSpec,
};

#[derive(Debug)]
pub struct CompiledQuery {
    pub logical_plan: LogicalPlan,
}

pub(crate) fn result_schema_for_targets(targets: &[Target]) -> Result<SchemaRef, PgFrontendError> {
    let fields = targets
        .iter()
        .map(|target| {
            let data_type = arrow_type_for_pg_type(target.pg_type).ok_or_else(|| {
                PgFrontendError::unsupported(format!(
                    "target {} has unsupported PostgreSQL type oid {}",
                    target.resno, target.pg_type.oid
                ))
            })?;
            Ok(pg_output_field(
                target
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column{}", target.resno)),
                data_type,
                true,
                target.pg_type,
            ))
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

#[derive(Debug, Clone, Copy)]
pub struct CompileConfig {
    pub identifier_max_bytes: usize,
}

struct CompileContext {
    tables: HashMap<usize, ResolvedTable>,
    values: HashMap<usize, ValuesRef>,
    ctes: HashMap<usize, CteRangeRef>,
    subqueries: HashMap<usize, SubqueryRef>,
    config: CompileConfig,
}

#[derive(Debug, Clone)]
struct WindowBinding {
    expr: QueryExpr,
    column: Expr,
}

#[derive(Debug, Clone)]
struct AggregateBinding {
    expr: QueryExpr,
    column: Expr,
}

#[derive(Debug, Clone)]
struct ScalarSubqueryBinding {
    query: TypedQuery,
    column: Expr,
}

#[derive(Debug, Clone)]
struct CorrelatedInSubquery {
    expr: QueryExpr,
    subquery: TypedQuery,
}

impl CompileContext {
    fn table(&self, rtindex: usize) -> Result<&ResolvedTable, PgFrontendError> {
        self.tables.get(&rtindex).ok_or_else(|| {
            PgFrontendError::unsupported(format!("missing resolved relation for rtindex {rtindex}"))
        })
    }

    fn values(&self, rtindex: usize) -> Result<&ValuesRef, PgFrontendError> {
        self.values.get(&rtindex).ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "missing VALUES range table for rtindex {rtindex}"
            ))
        })
    }

    fn cte(&self, rtindex: usize) -> Result<&CteRangeRef, PgFrontendError> {
        self.ctes.get(&rtindex).ok_or_else(|| {
            PgFrontendError::unsupported(format!("missing CTE range table for rtindex {rtindex}"))
        })
    }

    fn subquery(&self, rtindex: usize) -> Result<&SubqueryRef, PgFrontendError> {
        self.subqueries.get(&rtindex).ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "missing subquery range table for rtindex {rtindex}"
            ))
        })
    }
}

pub fn compile_query(
    query: ResolvedQuery<'_>,
    config: CompileConfig,
) -> Result<CompiledQuery, PgFrontendError> {
    let query = query.query();
    compile_typed_query(query, config)
}

fn compile_typed_query(
    query: &TypedQuery,
    config: CompileConfig,
) -> Result<CompiledQuery, PgFrontendError> {
    validate_supported_query_shape(query)?;
    let ctx = resolved_tables_for_query(query, config)?;
    for resolved in ctx.tables.values() {
        if let Some(schema) = resolved.relation.schema.as_deref() {
            validate_identifier_len(schema, config.identifier_max_bytes, "schema")?;
        }
        validate_identifier_len(
            &resolved.relation.table,
            config.identifier_max_bytes,
            "table",
        )?;
    }

    let mut plan = base_plan(query, &ctx)?;
    let mut aggregate_bindings = Vec::new();
    if query.has_aggregates || query.has_group_by || query.has_having || query.has_grouping_sets {
        let (aggregate_plan, bindings) = aggregate_plan(query, plan, &ctx)?;
        plan = aggregate_plan;
        aggregate_bindings = bindings;
    }
    if let Some(having) = query.having.as_ref() {
        let mut scalar_bindings = Vec::new();
        plan = attach_scalar_subqueries(plan, having, &ctx, &mut scalar_bindings)?;
        let predicate = compile_expr_with_windows(
            having,
            query,
            &ctx,
            &[],
            &scalar_bindings,
            &aggregate_bindings,
        )?;
        plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
            predicate,
            Arc::new(plan),
        )?);
    }

    let window_bindings = if query.has_windows {
        let (window_plan, bindings) = window_plan(query, plan, &ctx, &aggregate_bindings)?;
        plan = window_plan;
        bindings
    } else {
        Vec::new()
    };

    let has_distinct_on = matches!(query.distinct, DistinctSpec::On { .. });
    let project_all_targets = has_distinct_on || !query.sort.is_empty();
    let projection_targets = if project_all_targets {
        query.targets.iter().collect::<Vec<_>>()
    } else {
        visible_targets(query).collect::<Vec<_>>()
    };
    let mut scalar_bindings = Vec::new();
    for target in &projection_targets {
        plan = attach_scalar_subqueries(plan, &target.expr, &ctx, &mut scalar_bindings)?;
    }
    let projection = projection_targets
        .into_iter()
        .map(|target| {
            compile_target_expr_with_bindings(
                target,
                query,
                &ctx,
                &window_bindings,
                &scalar_bindings,
                &aggregate_bindings,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    plan = LogicalPlan::Projection(Projection::try_new(
        deduplicate_projection_names(projection),
        Arc::new(plan),
    )?);

    match &query.distinct {
        DistinctSpec::None => {}
        DistinctSpec::FullRow => {
            if !distinct_is_redundant_after_global_aggregate(query) {
                plan = distinct_full_row_plan(plan);
            }
        }
        DistinctSpec::On { target_refs } => {
            plan = distinct_on_plan(query, plan, target_refs)?;
        }
    }

    if !query.sort.is_empty() && !has_distinct_on {
        let sort = compile_sort(query, &plan)?;
        plan = LogicalPlan::Sort(Sort {
            expr: sort,
            input: Arc::new(plan),
            fetch: None,
        });
    }

    let limit_offset = query
        .limit_offset
        .as_ref()
        .filter(|expr| !limit_bound_is_zero(expr));
    if query.limit_count.is_some() || limit_offset.is_some() {
        plan = LogicalPlan::Limit(Limit {
            skip: limit_offset
                .map(|expr| compile_limit_bound(expr).map(Box::new))
                .transpose()?,
            fetch: query
                .limit_count
                .as_ref()
                .map(|expr| compile_limit_bound(expr).map(Box::new))
                .transpose()?,
            input: Arc::new(plan),
        });
    }

    let plan_contains_all_targets = project_all_targets && !has_distinct_on;
    if plan_contains_all_targets && query.targets.iter().any(|target| target.resjunk) {
        let visible = visible_plan_columns(query, &plan);
        plan = LogicalPlan::Projection(Projection::try_new(visible, Arc::new(plan))?);
    }

    Ok(CompiledQuery { logical_plan: plan })
}

fn distinct_full_row_plan(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Distinct(Distinct::All(Arc::new(input)))
}

fn distinct_on_plan(
    query: &TypedQuery,
    input: LogicalPlan,
    target_refs: &[u32],
) -> Result<LogicalPlan, PgFrontendError> {
    let on_expr = target_refs
        .iter()
        .map(|target_ref| column_for_sort_group_ref(query, &input, *target_ref))
        .collect::<Result<Vec<_>, _>>()?;
    let select_expr = query
        .targets
        .iter()
        .enumerate()
        .filter(|(_, target)| !target.resjunk)
        .map(|(index, _)| column_for_plan_index(&input, index))
        .collect::<Result<Vec<_>, _>>()?;
    let sort_expr = if query.sort.is_empty() {
        None
    } else {
        Some(compile_sort(query, &input)?)
    };

    Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
        on_expr,
        select_expr,
        sort_expr,
        Arc::new(input),
    )?)))
}

fn column_for_sort_group_ref(
    query: &TypedQuery,
    input: &LogicalPlan,
    target_ref: u32,
) -> Result<Expr, PgFrontendError> {
    let index = target_index_by_sort_group_ref(query, target_ref)?;
    column_for_plan_index(input, index)
}

fn column_for_plan_index(input: &LogicalPlan, index: usize) -> Result<Expr, PgFrontendError> {
    if index >= input.schema().fields().len() {
        return Err(PgFrontendError::unsupported(format!(
            "target index {index} is outside projected input schema"
        )));
    }
    let (qualifier, field) = input.schema().qualified_field(index);
    Ok(Expr::Column(Column::from((qualifier, field))))
}

fn deduplicate_projection_names(exprs: Vec<Expr>) -> Vec<Expr> {
    let mut seen = HashMap::<String, usize>::new();
    exprs
        .into_iter()
        .map(|expr| {
            let name = expr.schema_name().to_string();
            let count = seen.entry(name.clone()).or_default();
            if *count == 0 {
                *count += 1;
                expr
            } else {
                *count += 1;
                expr.alias(format!("{name}__pg_fusion_{}", *count))
            }
        })
        .collect()
}

fn distinct_is_redundant_after_global_aggregate(query: &TypedQuery) -> bool {
    query.has_aggregates && !query.has_group_by
}

fn resolved_tables_for_query(
    query: &TypedQuery,
    config: CompileConfig,
) -> Result<CompileContext, PgFrontendError> {
    let mut tables = HashMap::new();
    for relation in &query.relations {
        tables.insert(relation.rtindex, resolved_table_for_relation(relation)?);
    }
    Ok(CompileContext {
        tables,
        values: query
            .values
            .iter()
            .map(|values| (values.rtindex, values.clone()))
            .collect(),
        ctes: query
            .cte_refs
            .iter()
            .map(|cte| (cte.rtindex, cte.clone()))
            .collect(),
        subqueries: query
            .subqueries
            .iter()
            .map(|subquery| (subquery.rtindex, subquery.clone()))
            .collect(),
        config,
    })
}

mod aggregate;
mod const_expr;
mod expr;
mod from;
mod join;
mod projection;
mod refs;
mod sort;
mod subquery;
mod window;

use aggregate::*;
use const_expr::*;
use expr::*;
use from::*;
use join::*;
use projection::*;
use refs::*;
use sort::*;
use subquery::*;
use window::*;

#[cfg(test)]
mod tests;
