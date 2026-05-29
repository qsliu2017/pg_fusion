//! Backend-side builder for DataFusion logical plans with PostgreSQL scan leaves.
//!
//! `plan_builder` accepts SQL plus DataFusion scalar parameters, resolves table
//! metadata through `df_catalog`, runs DataFusion logical optimization, and
//! builds PostgreSQL table scans as [`scan_node::PgScanNode`] leaves.
//!
//! The result contains no snapshot identity. Snapshot ownership stays in the
//! later backend execution state that serves scan requests.
//!
//! DataFusion logical optimization is pinned to one target partition by default.
//! This is a DataFusion-level contract only: PostgreSQL-side parallel plans can
//! still be produced later by `slot_scan`.
//!
//! Subquery expressions are accepted when DataFusion can decorrelate/rewrite
//! them into ordinary relational operators before scan building. Any subquery
//! nodes that survive optimization are rejected before `PgScanNode` building.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::SessionStateDefaults;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, TableReference};
use datafusion_common::{DFSchema, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::expr_fn::cast;
use datafusion_expr::logical_plan::{Filter, LogicalPlan, Projection, TableScan};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    AggregateUDF, Expr, ScalarUDF, TableProviderFilterPushDown, TableSource, WindowUDF,
};
use datafusion_sql::parser::{DFParser, Statement as DFStatement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::ast::{
    visit_expressions_mut, Cte, CteAsMaterialized, DataType as SqlDataType, Expr as SqlExpr, Ident,
    ObjectName, ObjectNamePart, Query, Statement as SqlStatement, Visit, Visitor, With,
};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::ParserError;
use df_catalog::{CatalogResolver, PgrxCatalogResolver, ResolveError};
use once_cell::sync::Lazy;
use pgrx::pg_sys;
use scan_node::{PgCteId, PgCteRefNode, PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, CompileError, CompileScanInput, LimitLowering};
use thiserror::Error;

mod join_reorder;

pub use df_catalog::PgPlanningTableSource;
pub use join_reorder::{JoinStatsProvider, LiveJoinStatsProvider};

const SPECIAL_NUMERIC_ERROR: &str =
    "pg_fusion Decimal128 avg cannot represent PostgreSQL numeric NaN/Infinity values";

static BUILTINS: Lazy<Arc<Builtins>> = Lazy::new(|| Arc::new(Builtins::new()));

/// Input for one backend logical-plan build.
#[derive(Debug, Clone)]
pub struct PlanBuildInput<'a> {
    /// SQL text. v1 accepts exactly one query-shaped statement.
    pub sql: &'a str,
    /// Positional DataFusion parameter values for `$1`, `$2`, ...
    pub params: Vec<ScalarValue>,
}

/// Output of a successful hybrid plan build.
#[derive(Debug)]
pub struct HybridPlan {
    /// Optimized logical plan with PostgreSQL table scans built as custom nodes.
    pub logical_plan: LogicalPlan,
    /// PostgreSQL scan plan referenced by custom scan leaves in `logical_plan`.
    pub scan_plan: PgScanPlan,
}

/// Query-local PostgreSQL scan plan built alongside a DataFusion logical plan.
#[derive(Debug)]
pub struct PgScanPlan {
    /// Query-local scan specs in allocation order.
    pub scans: Vec<Arc<PgScanSpec>>,
}

impl PgScanPlan {
    fn new(scans: Vec<Arc<PgScanSpec>>) -> Self {
        Self { scans }
    }

    pub fn scans(&self) -> &[Arc<PgScanSpec>] {
        &self.scans
    }

    pub fn into_scans(self) -> Vec<Arc<PgScanSpec>> {
        self.scans
    }
}

/// Build a pg_fusion execution plan from an already-typed DataFusion logical plan.
///
/// This is the shared post-frontend pipeline used by both SQL-text planning
/// and PostgreSQL query-tree planning: it validates supported plan shapes,
/// runs DataFusion optimization, applies pg_fusion join ordering, turns
/// PostgreSQL table leaves into [`scan_node::PgScanNode`], and normalizes root
/// output transport types.
pub fn build_preplanned_logical_plan(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
) -> Result<HybridPlan, PlanBuildError> {
    build_preplanned_logical_plan_with_stats(plan, config, &LiveJoinStatsProvider)
}

/// [`build_preplanned_logical_plan`] with an explicit statistics provider.
pub fn build_preplanned_logical_plan_with_stats<S>(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
) -> Result<HybridPlan, PlanBuildError>
where
    S: JoinStatsProvider,
{
    let mut scan_builder = PgScanBuilder::new(config);
    let logical_plan = build_preplanned_logical_plan_with_scan_builder(
        plan,
        config,
        stats_provider,
        &mut scan_builder,
    )?;
    Ok(HybridPlan {
        logical_plan,
        scan_plan: PgScanPlan::new(scan_builder.scans),
    })
}

/// Build a pg_fusion execution plan for PostgreSQL query-tree frontend output.
///
/// The frontend path intentionally builds PostgreSQL scan leaves without first
/// running generic DataFusion optimization. Frontend scan predicates carry
/// PostgreSQL type metadata and must reach `scan_sql` before DataFusion can
/// fold or rewrite them with DataFusion semantics.
pub fn build_frontend_logical_plan(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
) -> Result<HybridPlan, PlanBuildError> {
    let mut scan_builder = PgScanBuilder::new(config);
    let logical_plan = build_frontend_logical_plan_with_scan_builder(plan, &mut scan_builder)?;
    reject_residual_frontend_filters(&scan_builder.scans)?;
    Ok(HybridPlan {
        logical_plan,
        scan_plan: PgScanPlan::new(scan_builder.scans),
    })
}

fn build_preplanned_logical_plan_with_scan_builder<S>(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
    scan_builder: &mut PgScanBuilder,
) -> Result<LogicalPlan, PlanBuildError>
where
    S: JoinStatsProvider,
{
    validate_no_special_numeric_literals(&plan)?;
    let optimized = optimize_logical_plan(plan, config.target_partitions)?;
    validate_no_special_numeric_literals(&optimized)?;
    validate_supported_plan_shape(&optimized)?;
    let optimized = if config.join_reordering_enabled {
        join_reorder::rewrite_join_order(optimized, config, stats_provider)?
    } else {
        optimized
    };

    let logical_plan = scan_builder.build_scans(optimized)?;
    normalize_root_output_types(logical_plan).map_err(PlanBuildError::from)
}

fn build_frontend_logical_plan_with_scan_builder(
    plan: LogicalPlan,
    scan_builder: &mut PgScanBuilder,
) -> Result<LogicalPlan, PlanBuildError> {
    validate_no_special_numeric_literals(&plan)?;
    validate_supported_plan_shape(&plan)?;
    let logical_plan = scan_builder.build_scans(plan)?;
    normalize_root_output_types(logical_plan).map_err(PlanBuildError::from)
}

fn reject_residual_frontend_filters(scans: &[Arc<PgScanSpec>]) -> Result<(), PlanBuildError> {
    let count = scans
        .iter()
        .map(|scan| scan.compiled_scan.residual_filters.len())
        .sum::<usize>();
    if count == 0 {
        Ok(())
    } else {
        Err(PlanBuildError::Plan(format!(
            "pg_frontend v1 requires all WHERE filters to execute inside PostgreSQL scan SQL; {count} residual filter(s) would execute in DataFusion"
        )))
    }
}

/// Configuration for [`PlanBuilder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanBuilderConfig {
    /// DataFusion optimizer target partitions. v1 keeps this at one unless
    /// explicitly overridden by tests or future integration code.
    pub target_partitions: usize,
    /// Live PostgreSQL identifier byte limit for this backend.
    pub identifier_max_bytes: usize,
    /// First query-local scan id to allocate.
    pub first_scan_id: u64,
    /// Enable statistics-based join reordering for eligible inner/cross joins.
    pub join_reordering_enabled: bool,
    /// Search limits and cross-join policy for the compact join-order optimizer.
    pub join_order_config: join_order::JoinOrderConfig,
}

impl Default for PlanBuilderConfig {
    fn default() -> Self {
        let join_order_config = join_order::JoinOrderConfig {
            allow_cross_joins: true,
            ..join_order::JoinOrderConfig::default()
        };
        Self {
            target_partitions: 1,
            identifier_max_bytes: pg_identifier_max_bytes(),
            first_scan_id: 1,
            join_reordering_enabled: true,
            join_order_config,
        }
    }
}

/// Build errors for SQL planning and PostgreSQL scan building.
#[derive(Debug, Error)]
pub enum PlanBuildError {
    #[error("failed to parse SQL: {0}")]
    Parse(#[from] ParserError),
    #[error("expected exactly one SQL statement, got {count}")]
    MultipleStatements { count: usize },
    #[error("unsupported statement for PostgreSQL scan planning: {0}")]
    UnsupportedStatement(String),
    #[error("catalog resolution failed: {0}")]
    Catalog(#[from] ResolveError),
    #[error("DataFusion planning failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("PostgreSQL scan SQL compilation failed: {0}")]
    ScanSql(#[from] CompileError),
    #[error("PostgreSQL statistics failed: {0}")]
    Statistics(String),
    #[error("join order optimization failed: {0}")]
    JoinOrder(#[from] join_order::OptimizeError),
    #[error("join order rewrite failed: {0}")]
    JoinReorder(String),
    #[error("unsupported SQL shape for PostgreSQL scan planning: {0}")]
    UnsupportedSubquery(String),
    #[error("{0}")]
    Plan(String),
}

/// Backend-side SQL-to-logical-plan builder.
#[derive(Debug, Clone)]
pub struct PlanBuilder<R = PgrxCatalogResolver, S = LiveJoinStatsProvider> {
    resolver: R,
    stats_provider: S,
    config: PlanBuilderConfig,
}

impl PlanBuilder<PgrxCatalogResolver, LiveJoinStatsProvider> {
    /// Create a builder backed by live PostgreSQL catalogs.
    pub fn new() -> Self {
        Self::with_resolver(PgrxCatalogResolver::new())
    }
}

impl Default for PlanBuilder<PgrxCatalogResolver, LiveJoinStatsProvider> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> PlanBuilder<R, LiveJoinStatsProvider> {
    /// Create a builder with a custom catalog resolver.
    pub fn with_resolver(resolver: R) -> Self {
        Self {
            resolver,
            stats_provider: LiveJoinStatsProvider,
            config: PlanBuilderConfig::default(),
        }
    }
}

impl<R, S> PlanBuilder<R, S> {
    /// Override the default statistics provider.
    pub fn with_stats_provider<T>(self, stats_provider: T) -> PlanBuilder<R, T> {
        PlanBuilder {
            resolver: self.resolver,
            stats_provider,
            config: self.config,
        }
    }

    /// Override the default builder configuration.
    pub fn with_config(mut self, config: PlanBuilderConfig) -> Self {
        self.config = config;
        self
    }

    /// Return the effective configuration.
    pub fn config(&self) -> PlanBuilderConfig {
        self.config
    }
}

impl<R, S> PlanBuilder<R, S>
where
    R: CatalogResolver + Send + Sync,
    S: JoinStatsProvider,
{
    /// Build an optimized logical plan and lower PostgreSQL table scans.
    pub fn build(&self, input: PlanBuildInput<'_>) -> Result<HybridPlan, PlanBuildError> {
        let mut statement = parse_one_query(input.sql)?;
        let context = PgPlanningContext::new(&self.resolver, self.config);
        let mut scan_builder = PgScanBuilder::new(self.config);
        prepare_materialized_ctes(
            &mut statement,
            &context,
            &input.params,
            &mut scan_builder,
            &self.stats_provider,
        )?;
        let planner = SqlToRel::new(&context);
        let plan = planner.statement_to_plan(statement)?;
        let plan = plan.with_param_values(input.params)?;
        let logical_plan = build_preplanned_logical_plan_with_scan_builder(
            plan,
            self.config,
            &self.stats_provider,
            &mut scan_builder,
        )?;
        Ok(HybridPlan {
            logical_plan,
            scan_plan: PgScanPlan::new(scan_builder.scans),
        })
    }
}

fn parse_one_query(sql: &str) -> Result<DFStatement, PlanBuildError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;
    let count = statements.len();
    if count != 1 {
        return Err(PlanBuildError::MultipleStatements { count });
    }
    let mut statement = statements.pop_front().expect("checked count above");
    normalize_postgres_unknown_casts(&mut statement);
    ensure_query_statement(&statement)?;
    Ok(statement)
}

fn normalize_postgres_unknown_casts(statement: &mut DFStatement) {
    let DFStatement::Statement(statement) = statement else {
        return;
    };
    let _ = visit_expressions_mut(statement.as_mut(), |expr| {
        if let SqlExpr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
        {
            if is_postgres_unknown_type(data_type) {
                *expr = (**inner).clone();
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

fn is_postgres_unknown_type(data_type: &SqlDataType) -> bool {
    let SqlDataType::Custom(name, modifiers) = data_type else {
        return false;
    };
    modifiers.is_empty()
        && name.0.len() == 1
        && matches!(
            &name.0[0],
            ObjectNamePart::Identifier(ident)
                if ident.value.eq_ignore_ascii_case("unknown") && ident.quote_style.is_none()
        )
}

fn ensure_query_statement(statement: &DFStatement) -> Result<(), PlanBuildError> {
    match statement {
        DFStatement::Statement(inner) if matches!(inner.as_ref(), SqlStatement::Query(_)) => Ok(()),
        other => Err(PlanBuildError::UnsupportedStatement(other.to_string())),
    }
}

fn optimize_logical_plan(
    plan: LogicalPlan,
    target_partitions: usize,
) -> Result<LogicalPlan, DataFusionError> {
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = target_partitions;
    let mut state = SessionStateBuilder::new()
        .with_config(options.into())
        .with_optimizer_rules(pg_fusion_optimizer_rules())
        .build();
    let _ = state.register_udf(df_functions::pg_format_udf());
    let _ = state.register_udf(df_functions::pg_quote_literal_udf());
    let _ = state.register_udaf(df_functions::pg_avg_udaf());
    state.optimize(&plan)
}

fn pg_fusion_optimizer_rules() -> Vec<Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>> {
    datafusion::optimizer::Optimizer::new()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != "single_distinct_aggregation_to_group_by")
        .collect()
}

fn normalize_root_output_types(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
    let fields = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.cloned(),
                field.name().to_owned(),
                field.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let columns = plan.schema().columns();

    let mut changed = false;
    let expr = columns
        .into_iter()
        .zip(fields)
        .map(|(column, (qualifier, name, data_type))| {
            let expr = Expr::Column(column);
            match data_type {
                DataType::UInt64 => {
                    changed = true;
                    cast(expr, DataType::Int64).alias_qualified(qualifier, name)
                }
                DataType::LargeUtf8 => {
                    changed = true;
                    cast(expr, DataType::Utf8).alias_qualified(qualifier, name)
                }
                _ => expr,
            }
        })
        .collect::<Vec<_>>();

    if !changed {
        return Ok(plan);
    }

    Projection::try_new(expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

fn validate_no_special_numeric_literals(plan: &LogicalPlan) -> Result<(), PlanBuildError> {
    let special_string_columns = special_numeric_string_columns(plan);
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }

        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if casts_special_numeric_literal(expr)
                    || casts_special_numeric_column(expr, &special_string_columns)
                {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })?;

        if found {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;

    if found {
        Err(PlanBuildError::Plan(SPECIAL_NUMERIC_ERROR.to_owned()))
    } else {
        Ok(())
    }
}

fn special_numeric_string_columns(plan: &LogicalPlan) -> HashSet<Column> {
    match plan {
        LogicalPlan::Values(values) => {
            let mut columns = HashSet::new();
            for (column_index, field) in values.schema.fields().iter().enumerate() {
                if values.values.iter().any(|row| {
                    row.get(column_index)
                        .is_some_and(string_literal_is_special_numeric)
                }) {
                    columns.insert(Column::new_unqualified(field.name().as_str()));
                }
            }
            columns
        }
        LogicalPlan::Projection(projection) => {
            let input_columns = special_numeric_string_columns(&projection.input);
            projection
                .expr
                .iter()
                .zip(projection.schema.fields())
                .filter(|(expr, _field)| {
                    expr_contains_special_numeric_string_column(expr, &input_columns)
                })
                .map(|(_expr, field)| Column::new_unqualified(field.name().as_str()))
                .collect()
        }
        LogicalPlan::SubqueryAlias(alias) => special_numeric_string_columns(&alias.input)
            .into_iter()
            .map(|column| Column::new(Some(alias.alias.clone()), column.name))
            .collect(),
        LogicalPlan::Filter(filter) => special_numeric_string_columns(&filter.input),
        LogicalPlan::Repartition(repartition) => special_numeric_string_columns(&repartition.input),
        LogicalPlan::Sort(sort) => special_numeric_string_columns(&sort.input),
        LogicalPlan::Limit(limit) => special_numeric_string_columns(&limit.input),
        LogicalPlan::Join(join) => {
            let mut columns = special_numeric_string_columns(&join.left);
            columns.extend(special_numeric_string_columns(&join.right));
            columns
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .flat_map(|input| special_numeric_string_columns(input))
            .collect(),
        _ => plan
            .inputs()
            .into_iter()
            .flat_map(special_numeric_string_columns)
            .collect(),
    }
}

fn casts_special_numeric_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => {
            is_decimal_type(&cast.data_type) && string_literal_is_special_numeric(&cast.expr)
        }
        Expr::TryCast(cast) => {
            is_decimal_type(&cast.data_type) && string_literal_is_special_numeric(&cast.expr)
        }
        _ => false,
    }
}

fn casts_special_numeric_column(expr: &Expr, special_string_columns: &HashSet<Column>) -> bool {
    match expr {
        Expr::Cast(cast) => {
            is_decimal_type(&cast.data_type)
                && expr_contains_special_numeric_string_column(&cast.expr, special_string_columns)
        }
        Expr::TryCast(cast) => {
            is_decimal_type(&cast.data_type)
                && expr_contains_special_numeric_string_column(&cast.expr, special_string_columns)
        }
        _ => false,
    }
}

fn expr_contains_special_numeric_string_column(
    expr: &Expr,
    special_string_columns: &HashSet<Column>,
) -> bool {
    let mut found = false;
    let _ = expr.apply(|expr| {
        if string_literal_is_special_numeric(expr) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Expr::Column(column) = expr {
            if column_matches_special_numeric_string(column, special_string_columns) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn column_matches_special_numeric_string(
    column: &Column,
    special_string_columns: &HashSet<Column>,
) -> bool {
    special_string_columns.contains(column)
        || special_string_columns.contains(&Column::new_unqualified(&column.name))
}

fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}

fn string_literal_is_special_numeric(expr: &Expr) -> bool {
    let value = match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => value,
        _ => return false,
    };
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "nan" | "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
    )
}

#[derive(Debug, Error)]
enum UnsupportedSubqueryShape {
    #[error("EXISTS(...) expressions")]
    Exists,
    #[error("IN (SELECT ...) expressions")]
    InSubquery,
    #[error("scalar subquery expressions")]
    ScalarSubquery,
    #[error("correlated subqueries")]
    OuterReferenceColumn,
    #[error("logical subquery plan nodes")]
    SubqueryPlan,
}

fn validate_supported_plan_shape(plan: &LogicalPlan) -> Result<(), PlanBuildError> {
    plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::Subquery(_)) {
            return Err(DataFusionError::External(Box::new(
                UnsupportedSubqueryShape::SubqueryPlan,
            )));
        }

        node.apply_expressions(|expr| {
            expr.apply(|expr| match expr {
                Expr::Exists(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::Exists,
                ))),
                Expr::InSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::InSubquery,
                ))),
                Expr::ScalarSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::ScalarSubquery,
                ))),
                Expr::OuterReferenceColumn(_, _) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::OuterReferenceColumn,
                ))),
                _ => Ok(TreeNodeRecursion::Continue),
            })
        })
    })
    .map_err(map_subquery_validation_error)?;
    Ok(())
}

fn map_subquery_validation_error(error: DataFusionError) -> PlanBuildError {
    match recover_subquery_validation_error(error) {
        Ok(shape) => PlanBuildError::UnsupportedSubquery(shape.to_string()),
        Err(error) => PlanBuildError::DataFusion(error),
    }
}

fn recover_subquery_validation_error(
    error: DataFusionError,
) -> Result<UnsupportedSubqueryShape, DataFusionError> {
    match error {
        DataFusionError::External(source) => match source.downcast::<UnsupportedSubqueryShape>() {
            Ok(shape) => Ok(*shape),
            Err(source) => Err(DataFusionError::External(source)),
        },
        DataFusionError::Context(context, source) => {
            match recover_subquery_validation_error(*source) {
                Ok(shape) => Ok(shape),
                Err(source) => Err(DataFusionError::Context(context, Box::new(source))),
            }
        }
        other => Err(other),
    }
}

fn prepare_materialized_ctes<R, S>(
    statement: &mut DFStatement,
    context: &PgPlanningContext<'_, R>,
    params: &[ScalarValue],
    scan_builder: &mut PgScanBuilder,
    stats_provider: &S,
) -> Result<(), PlanBuildError>
where
    R: CatalogResolver + Send + Sync,
    S: JoinStatsProvider,
{
    let DFStatement::Statement(statement_inner) = statement else {
        return Ok(());
    };
    let SqlStatement::Query(query) = statement_inner.as_mut() else {
        return Ok(());
    };

    let Some(with_ref) = query.with.as_ref() else {
        return Ok(());
    };
    if with_ref.recursive {
        return Err(PlanBuildError::UnsupportedStatement(
            "recursive CTEs are not supported by pg_fusion".into(),
        ));
    }

    let ref_counts = count_top_level_cte_references(query);
    let mut prefix_ctes = Vec::new();
    let mut next_cte_id = 1_u64;
    let with_template = With {
        with_token: with_ref.with_token.clone(),
        recursive: false,
        cte_tables: Vec::new(),
    };
    let Some(with_mut) = query.with.as_mut() else {
        return Ok(());
    };

    for cte in &mut with_mut.cte_tables {
        let cte_name = normalize_ident(cte.alias.name.clone());
        let ref_count = ref_counts.get(&cte_name).copied().unwrap_or(0);
        let should_materialize = match cte.materialized {
            Some(CteAsMaterialized::Materialized) => true,
            Some(CteAsMaterialized::NotMaterialized) => false,
            None => ref_count > 1,
        };

        let original_cte = cte.clone();
        if should_materialize {
            let cte_id = PgCteId::new(next_cte_id);
            next_cte_id = next_cte_id.checked_add(1).ok_or_else(|| {
                PlanBuildError::Plan("materialized CTE id counter overflowed".into())
            })?;
            let definition_statement =
                cte_definition_statement(&with_template, &prefix_ctes, original_cte)?;
            let definition = build_materialized_cte_definition(
                definition_statement,
                context,
                params,
                scan_builder,
                stats_provider,
            )?;
            let synthetic_table = synthetic_cte_table_name(cte_id);
            context.register_cte_source(
                TableReference::bare(synthetic_table.clone()),
                PgPlanningCteSource::new(cte_id, cte_name, definition),
            )?;
            cte.query = synthetic_cte_query(&synthetic_table)?;
        }

        prefix_ctes.push(cte.clone());
    }

    Ok(())
}

fn count_top_level_cte_references(query: &Query) -> HashMap<String, usize> {
    let Some(with) = query.with.as_ref() else {
        return HashMap::new();
    };
    let names = with
        .cte_tables
        .iter()
        .map(|cte| normalize_ident(cte.alias.name.clone()))
        .collect::<HashSet<_>>();
    if names.is_empty() {
        return HashMap::new();
    }

    let mut body_query = query.clone();
    body_query.with = None;
    let mut visitor = CteReferenceCounter {
        names: &names,
        counts: HashMap::new(),
    };
    let _ = body_query.visit(&mut visitor);
    visitor.counts
}

struct CteReferenceCounter<'a> {
    names: &'a HashSet<String>,
    counts: HashMap<String, usize>,
}

impl Visitor for CteReferenceCounter<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let [ObjectNamePart::Identifier(ident)] = relation.0.as_slice() {
            let name = normalize_ident(ident.clone());
            if self.names.contains(&name) {
                *self.counts.entry(name).or_default() += 1;
            }
        }
        ControlFlow::Continue(())
    }
}

fn cte_definition_statement(
    with_template: &With,
    prefix_ctes: &[Cte],
    cte: Cte,
) -> Result<DFStatement, PlanBuildError> {
    let mut statement = parse_select_star_statement(&cte.alias.name)?;
    let DFStatement::Statement(statement_inner) = &mut statement else {
        return Err(PlanBuildError::Plan(
            "synthetic CTE definition statement was not a SQL statement".into(),
        ));
    };
    let SqlStatement::Query(query) = statement_inner.as_mut() else {
        return Err(PlanBuildError::Plan(
            "synthetic CTE definition statement was not a query".into(),
        ));
    };

    let mut cte_tables = prefix_ctes.to_vec();
    cte_tables.push(cte);
    query.with = Some(With {
        with_token: with_template.with_token.clone(),
        recursive: false,
        cte_tables,
    });
    Ok(statement)
}

fn parse_select_star_statement(cte_name: &Ident) -> Result<DFStatement, PlanBuildError> {
    parse_one_query(&format!("SELECT * FROM {cte_name}"))
}

fn synthetic_cte_query(synthetic_table: &str) -> Result<Box<Query>, PlanBuildError> {
    let mut statement = parse_one_query(&format!("SELECT * FROM {synthetic_table}"))?;
    let DFStatement::Statement(statement_inner) = &mut statement else {
        return Err(PlanBuildError::Plan(
            "synthetic CTE statement was not a SQL statement".into(),
        ));
    };
    let SqlStatement::Query(query) = statement_inner.as_mut() else {
        return Err(PlanBuildError::Plan(
            "synthetic CTE statement was not a query".into(),
        ));
    };
    Ok(query.clone())
}

fn synthetic_cte_table_name(cte_id: PgCteId) -> String {
    format!("__pg_fusion_cte_{}", cte_id.get())
}

fn build_materialized_cte_definition<R, S>(
    statement: DFStatement,
    context: &PgPlanningContext<'_, R>,
    params: &[ScalarValue],
    scan_builder: &mut PgScanBuilder,
    stats_provider: &S,
) -> Result<LogicalPlan, PlanBuildError>
where
    R: CatalogResolver + Send + Sync,
    S: JoinStatsProvider,
{
    let planner = SqlToRel::new(context);
    let plan = planner.statement_to_plan(statement)?;
    let plan = plan.with_param_values(params.to_vec())?;
    let optimized = optimize_logical_plan(plan, context.config.target_partitions)?;
    validate_supported_plan_shape(&optimized)?;
    let optimized = if context.config.join_reordering_enabled {
        join_reorder::rewrite_join_order(optimized, context.config, stats_provider)?
    } else {
        optimized
    };
    scan_builder.build_scans(optimized)
}

fn normalize_ident(ident: Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value
    } else {
        ident.value.to_ascii_lowercase()
    }
}

#[derive(Debug)]
struct PgPlanningContext<'a, R> {
    resolver: &'a R,
    config: PlanBuilderConfig,
    options: ConfigOptions,
    builtins: Arc<Builtins>,
    tables: Mutex<HashMap<TableReference, Arc<PgPlanningTableSource>>>,
    cte_sources: Mutex<HashMap<TableReference, Arc<PgPlanningCteSource>>>,
}

impl<'a, R> PgPlanningContext<'a, R> {
    fn new(resolver: &'a R, config: PlanBuilderConfig) -> Self {
        let mut options = ConfigOptions::default();
        options.execution.target_partitions = config.target_partitions;
        Self {
            resolver,
            config,
            options,
            builtins: Arc::clone(&BUILTINS),
            tables: Mutex::new(HashMap::new()),
            cte_sources: Mutex::new(HashMap::new()),
        }
    }

    fn register_cte_source(
        &self,
        table: TableReference,
        source: PgPlanningCteSource,
    ) -> DataFusionResult<()> {
        let mut cte_sources = self.cte_sources.lock().map_err(|error| {
            DataFusionError::Plan(format!("CTE source cache lock poisoned: {error}"))
        })?;
        cte_sources.insert(table, Arc::new(source));
        Ok(())
    }
}

impl<R> ContextProvider for PgPlanningContext<'_, R>
where
    R: CatalogResolver + Send + Sync,
{
    fn get_table_source(&self, table: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        validate_table_reference_identifiers(&table, self.config.identifier_max_bytes)?;

        if let Some(source) = self
            .cte_sources
            .lock()
            .map_err(|error| {
                DataFusionError::Plan(format!("CTE source cache lock poisoned: {error}"))
            })?
            .get(&table)
            .cloned()
        {
            return Ok(source);
        }

        let mut tables = self.tables.lock().map_err(|error| {
            DataFusionError::Plan(format!("catalog cache lock poisoned: {error}"))
        })?;
        if let Some(source) = tables.get(&table) {
            return Ok(Arc::clone(source) as Arc<dyn TableSource>);
        }

        let resolved = self
            .resolver
            .resolve_table(&table)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let source = Arc::new(PgPlanningTableSource::new(resolved));
        tables.insert(table, Arc::clone(&source));
        Ok(source)
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.builtins.scalar_udf.get(name).map(Arc::clone)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.builtins.agg_udf.get(name).map(Arc::clone)
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.builtins.window_udf.get(name).map(Arc::clone)
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow_schema::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        self.builtins.scalar_udf.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.builtins.agg_udf.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.builtins.window_udf.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.builtins.expr_planners
    }
}

fn validate_table_reference_identifiers(
    table: &TableReference,
    max_bytes: usize,
) -> DataFusionResult<()> {
    validate_identifier(table.table(), max_bytes, "table")?;
    if let Some(schema) = table.schema() {
        validate_identifier(schema, max_bytes, "schema")?;
    }
    Ok(())
}

fn validate_identifier(
    identifier: &str,
    max_bytes: usize,
    kind: &'static str,
) -> DataFusionResult<()> {
    if identifier.len() > max_bytes {
        return Err(DataFusionError::Plan(format!(
            "{kind} identifier `{identifier}` exceeds PostgreSQL limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct PgPlanningCteSource {
    cte_id: PgCteId,
    name: String,
    definition: LogicalPlan,
    schema: SchemaRef,
}

impl PgPlanningCteSource {
    fn new(cte_id: PgCteId, name: String, definition: LogicalPlan) -> Self {
        let schema = definition.schema().inner().clone();
        Self {
            cte_id,
            name,
            definition,
            schema,
        }
    }
}

impl TableSource for PgPlanningCteSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

#[derive(Debug)]
struct Builtins {
    agg_udf: HashMap<String, Arc<AggregateUDF>>,
    scalar_udf: HashMap<String, Arc<ScalarUDF>>,
    window_udf: HashMap<String, Arc<WindowUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl Builtins {
    fn new() -> Self {
        let mut agg_udf = HashMap::new();
        for function in SessionStateDefaults::default_aggregate_functions() {
            register_aggregate_udf(&mut agg_udf, function);
        }
        register_aggregate_udf(&mut agg_udf, df_functions::pg_avg_udaf());
        register_existing_aggregate_alias(&mut agg_udf, "var_samp", "variance");

        let mut scalar_udf = HashMap::new();
        for function in SessionStateDefaults::default_scalar_functions() {
            register_scalar_udf(&mut scalar_udf, function);
        }
        register_scalar_udf(&mut scalar_udf, df_functions::pg_format_udf());
        register_scalar_udf(&mut scalar_udf, df_functions::pg_quote_literal_udf());
        register_existing_scalar_alias(&mut scalar_udf, "ceil", "ceiling");

        let mut window_udf = HashMap::new();
        for function in SessionStateDefaults::default_window_functions() {
            register_window_udf(&mut window_udf, function);
        }

        Self {
            agg_udf,
            scalar_udf,
            window_udf,
            expr_planners: SessionStateDefaults::default_expr_planners(),
        }
    }
}

fn register_scalar_udf(registry: &mut HashMap<String, Arc<ScalarUDF>>, udf: Arc<ScalarUDF>) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

fn register_existing_scalar_alias(
    registry: &mut HashMap<String, Arc<ScalarUDF>>,
    existing: &str,
    alias: &str,
) {
    if let Some(udf) = registry.get(existing).cloned() {
        registry.insert(alias.to_owned(), udf);
    }
}

fn register_aggregate_udf(
    registry: &mut HashMap<String, Arc<AggregateUDF>>,
    udf: Arc<AggregateUDF>,
) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

fn register_existing_aggregate_alias(
    registry: &mut HashMap<String, Arc<AggregateUDF>>,
    existing: &str,
    alias: &str,
) {
    if let Some(udf) = registry.get(existing).cloned() {
        registry.insert(alias.to_owned(), udf);
    }
}

fn register_window_udf(registry: &mut HashMap<String, Arc<WindowUDF>>, udf: Arc<WindowUDF>) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

#[derive(Debug)]
struct PgScanBuilder {
    config: PlanBuilderConfig,
    next_scan_id: u64,
    scans: Vec<Arc<PgScanSpec>>,
}

impl PgScanBuilder {
    fn new(config: PlanBuilderConfig) -> Self {
        Self {
            next_scan_id: config.first_scan_id,
            config,
            scans: Vec::new(),
        }
    }

    fn build_scans(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, PlanBuildError> {
        let transformed = plan.transform_up(|node| self.build_node(node))?;
        Ok(transformed.data)
    }

    fn build_node(&mut self, plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                self.build_table_scan(table_scan).map(Transformed::yes)
            }
            other => Ok(Transformed::no(other)),
        }
    }

    fn build_table_scan(&mut self, table_scan: TableScan) -> DataFusionResult<LogicalPlan> {
        let cte_source = table_scan
            .source
            .as_any()
            .downcast_ref::<PgPlanningCteSource>()
            .map(|source| {
                (
                    source.cte_id,
                    source.name.clone(),
                    source.definition.clone(),
                )
            });
        if let Some((cte_id, name, definition)) = cte_source {
            return self.build_cte_ref(table_scan, cte_id, name, definition);
        }

        let source = table_scan
            .source
            .as_any()
            .downcast_ref::<PgPlanningTableSource>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "TableScan {} is not backed by pg/plan_builder catalog source",
                    table_scan.table_name
                ))
            })?;
        let resolved = source.resolved();
        let source_schema = DFSchema::try_from_qualified_schema(
            table_scan.table_name.clone(),
            resolved.schema.as_ref(),
        )?;

        let compiled = compile_scan(CompileScanInput {
            relation: &resolved.relation,
            schema: resolved.schema.as_ref(),
            identifier_max_bytes: self.config.identifier_max_bytes,
            projection: table_scan.projection.as_deref(),
            filters: &table_scan.filters,
            requested_limit: table_scan.fetch,
            limit_lowering: LimitLowering::ExternalHint,
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let residual_filters = compiled.residual_filters.clone();
        let selected_output_len = compiled.selected_columns.len();
        let needs_output_projection = !compiled.residual_filter_columns.is_empty();
        let scan_id = self.allocate_scan_id()?;
        let spec = Arc::new(PgScanSpec::try_new(
            scan_id,
            resolved.table_oid,
            resolved.relation.clone(),
            &source_schema,
            compiled,
        )?);

        let mut plan = PgScanNode::new(Arc::clone(&spec)).into_logical_plan();
        self.scans.push(spec);

        if let Some(predicate) = conjunction(residual_filters) {
            plan = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(plan))?);
        }

        if needs_output_projection {
            let expr = (0..selected_output_len)
                .map(|index| {
                    let (qualifier, field) = plan.schema().qualified_field(index);
                    Expr::Column(datafusion_common::Column::from((qualifier, field)))
                })
                .collect::<Vec<_>>();
            plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(plan))?);
        }

        Ok(plan)
    }

    fn build_cte_ref(
        &mut self,
        table_scan: TableScan,
        cte_id: PgCteId,
        name: String,
        definition: LogicalPlan,
    ) -> DataFusionResult<LogicalPlan> {
        if !table_scan.filters.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "materialized CTE {} unexpectedly received pushed filters",
                name
            )));
        }

        Ok(PgCteRefNode::new(
            cte_id,
            name,
            definition,
            table_scan.projected_schema,
            table_scan.projection,
            table_scan.fetch,
        )
        .into_logical_plan())
    }

    fn allocate_scan_id(&mut self) -> DataFusionResult<PgScanId> {
        let scan_id = self.next_scan_id;
        self.next_scan_id = self
            .next_scan_id
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Plan("PgScanId counter overflowed".into()))?;
        Ok(PgScanId::new(scan_id))
    }
}

#[cfg(test)]
mod tests;
