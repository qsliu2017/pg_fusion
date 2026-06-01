use super::*;

pub(super) fn aggregate_plan(
    query: &TypedQuery,
    input: LogicalPlan,
    ctx: &CompileContext,
) -> Result<(LogicalPlan, Vec<AggregateBinding>), PgFrontendError> {
    if query.targets.iter().any(|target| {
        !is_group_target(query, target)
            && !contains_aggregate_call(&target.expr)
            && !expr_is_grouping_invariant(&target.expr)
    }) {
        return Err(PgFrontendError::unsupported(
            "pg_frontend v1 supports only GROUP BY expressions and aggregate-call SELECT targets in aggregate queries",
        ));
    }

    let group_targets = aggregate_group_targets(query)?;
    let group_exprs = compile_group_exprs(query, ctx)?;
    let mut aggregate_calls = Vec::new();
    for target in &query.targets {
        collect_aggregate_calls(&target.expr, &mut aggregate_calls);
    }
    if let Some(having) = query.having.as_ref() {
        collect_aggregate_calls(having, &mut aggregate_calls);
    }
    if aggregate_calls.is_empty() && group_exprs.is_empty() {
        return Err(PgFrontendError::unsupported(
            "pg_frontend v1 aggregate queries must contain GROUP BY expressions or aggregate calls",
        ));
    }
    let aggregate_exprs = aggregate_calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            compile_aggregate_expr(call, query, ctx).map(|expr| {
                if is_grouping_call(call) {
                    expr
                } else {
                    expr.alias(format!("__pgfusion_aggregate_{index}"))
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate_expr_count = aggregate_exprs.len();
    let plan = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(input),
        group_exprs,
        aggregate_exprs,
    )?);
    let mut bindings = Vec::new();
    for (index, target) in group_targets.iter().enumerate() {
        let (qualifier, field) = plan.schema().qualified_field(index);
        bindings.push(AggregateBinding {
            expr: target.expr.clone(),
            column: Expr::Column(Column::from((qualifier, field))),
        });
    }
    let aggregate_offset = plan.schema().fields().len() - aggregate_expr_count;
    for (offset, call) in aggregate_calls.into_iter().enumerate() {
        let (qualifier, field) = plan.schema().qualified_field(aggregate_offset + offset);
        let column =
            aggregate_binding_column(&call, Expr::Column(Column::from((qualifier, field))));
        bindings.push(AggregateBinding { expr: call, column });
    }
    Ok((plan, bindings))
}

pub(super) fn is_grouping_call(expr: &QueryExpr) -> bool {
    matches!(
        expr,
        QueryExpr::AggregateCall {
            func: AggregateFunction::Grouping,
            ..
        }
    )
}

pub(super) fn aggregate_group_targets(query: &TypedQuery) -> Result<Vec<&Target>, PgFrontendError> {
    let refs = aggregate_group_refs(query);
    refs.iter()
        .map(|group_ref| {
            query
                .targets
                .iter()
                .find(|target| target.ressortgroupref == *group_ref)
                .ok_or_else(|| {
                    PgFrontendError::unsupported(format!(
                        "GROUP BY target ref {group_ref} was not found in target list"
                    ))
                })
        })
        .collect()
}

pub(super) fn aggregate_group_refs(query: &TypedQuery) -> Vec<u32> {
    if query.grouping_sets.is_empty() {
        return query.group_refs.clone();
    }
    let mut refs = Vec::new();
    for spec in &query.grouping_sets {
        collect_grouping_set_refs(spec, &mut refs);
    }
    refs
}

pub(super) fn collect_grouping_set_refs(spec: &GroupingSetSpec, refs: &mut Vec<u32>) {
    match spec {
        GroupingSetSpec::Empty => {}
        GroupingSetSpec::Simple(group_refs) => {
            for group_ref in group_refs {
                push_grouping_ref(refs, *group_ref);
            }
        }
        GroupingSetSpec::Rollup(atoms) | GroupingSetSpec::Cube(atoms) => {
            for atom in atoms {
                for group_ref in atom {
                    push_grouping_ref(refs, *group_ref);
                }
            }
        }
        GroupingSetSpec::Sets(sets) => {
            for spec in sets {
                collect_grouping_set_refs(spec, refs);
            }
        }
    }
}

pub(super) fn push_grouping_ref(refs: &mut Vec<u32>, group_ref: u32) {
    if !refs.contains(&group_ref) {
        refs.push(group_ref);
    }
}

pub(super) fn compile_group_exprs(
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Vec<Expr>, PgFrontendError> {
    if query.grouping_sets.is_empty() {
        return query
            .group_refs
            .iter()
            .map(|group_ref| compile_sort_group_target(*group_ref, query, ctx, &[]))
            .collect();
    }

    query
        .grouping_sets
        .iter()
        .map(|spec| compile_grouping_set_expr(spec, query, ctx))
        .collect()
}

pub(super) fn compile_grouping_set_expr(
    spec: &GroupingSetSpec,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    Ok(Expr::GroupingSet(GroupingSet::GroupingSets(
        compile_grouping_set_lists(spec, query, ctx)?,
    )))
}

pub(super) fn compile_grouping_set_lists(
    spec: &GroupingSetSpec,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Vec<Vec<Expr>>, PgFrontendError> {
    match spec {
        GroupingSetSpec::Empty => Ok(vec![vec![]]),
        GroupingSetSpec::Simple(refs) => Ok(vec![compile_grouping_atom(refs, query, ctx)?]),
        GroupingSetSpec::Rollup(atoms) => {
            let atoms = compile_grouping_atoms(atoms, query, ctx)?;
            let mut sets = Vec::with_capacity(atoms.len() + 1);
            for len in (0..=atoms.len()).rev() {
                sets.push(flatten_grouping_atoms(&atoms[..len]));
            }
            Ok(sets)
        }
        GroupingSetSpec::Cube(atoms) => {
            let atoms = compile_grouping_atoms(atoms, query, ctx)?;
            let count = 1usize.checked_shl(atoms.len() as u32).ok_or_else(|| {
                PgFrontendError::unsupported("CUBE contains too many grouping expressions")
            })?;
            let mut sets = Vec::with_capacity(count);
            for mask in (0..count).rev() {
                let mut set = Vec::new();
                for (index, atom) in atoms.iter().enumerate() {
                    if mask & (1usize << index) != 0 {
                        set.extend(atom.iter().cloned());
                    }
                }
                sets.push(set);
            }
            Ok(sets)
        }
        GroupingSetSpec::Sets(sets) => {
            let mut out = Vec::new();
            for spec in sets {
                out.extend(compile_grouping_set_lists(spec, query, ctx)?);
            }
            Ok(out)
        }
    }
}

pub(super) fn compile_grouping_atoms(
    atoms: &[Vec<u32>],
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Vec<Vec<Expr>>, PgFrontendError> {
    atoms
        .iter()
        .map(|atom| compile_grouping_atom(atom, query, ctx))
        .collect()
}

pub(super) fn compile_grouping_atom(
    refs: &[u32],
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Vec<Expr>, PgFrontendError> {
    refs.iter()
        .map(|group_ref| compile_sort_group_target(*group_ref, query, ctx, &[]))
        .collect()
}

pub(super) fn flatten_grouping_atoms(atoms: &[Vec<Expr>]) -> Vec<Expr> {
    atoms.iter().flat_map(|atom| atom.iter().cloned()).collect()
}

pub(super) fn aggregate_binding_column(expr: &QueryExpr, column: Expr) -> Expr {
    if matches!(
        expr,
        QueryExpr::AggregateCall {
            func: AggregateFunction::Sum,
            pg_type,
            ..
        } if pg_type.oid == u32::from(pgrx::pg_sys::FLOAT4OID)
    ) {
        Expr::Cast(Cast::new(Box::new(column), DataType::Float32))
    } else if matches!(
        expr,
        QueryExpr::AggregateCall {
            func: AggregateFunction::RegrCount,
            ..
        }
    ) {
        Expr::Cast(Cast::new(Box::new(column), DataType::Int64))
    } else if matches!(
        expr,
        QueryExpr::AggregateCall {
            func: AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp
                | AggregateFunction::VarPop
                | AggregateFunction::VarSamp,
            pg_type,
            ..
        } if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
    ) {
        Expr::Cast(Cast::new(Box::new(column), DataType::Decimal128(38, 0)))
    } else {
        column
    }
}

pub(super) fn is_group_target(query: &TypedQuery, target: &Target) -> bool {
    target.ressortgroupref != 0 && query.group_refs.contains(&target.ressortgroupref)
}

pub(super) fn contains_aggregate_call(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::AggregateCall { .. } => true,
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => contains_aggregate_call(inner),
        QueryExpr::WindowCall { args, filter, .. } => {
            args.iter().any(contains_aggregate_call)
                || filter.as_deref().is_some_and(contains_aggregate_call)
        }
        QueryExpr::FunctionCall { args, .. } => args.iter().any(contains_aggregate_call),
        QueryExpr::Array { elements, .. } => elements.iter().any(contains_aggregate_call),
        QueryExpr::ArraySubscript { array, index, .. } => {
            contains_aggregate_call(array) || contains_aggregate_call(index)
        }
        QueryExpr::Coalesce { args, .. } => args.iter().any(contains_aggregate_call),
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate_call)
                || when_then.iter().any(|(when, then)| {
                    contains_aggregate_call(when) || contains_aggregate_call(then)
                })
                || else_expr.as_deref().is_some_and(contains_aggregate_call)
        }
        QueryExpr::Bool { args, .. } => args.iter().any(contains_aggregate_call),
        QueryExpr::BinaryOp { left, right, .. } => {
            contains_aggregate_call(left) || contains_aggregate_call(right)
        }
        QueryExpr::InSubquery { expr, .. } => contains_aggregate_call(expr),
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => {
            false
        }
        QueryExpr::ScalarSubquery(_) | QueryExpr::ExistsSubquery { .. } => false,
    }
}

pub(super) fn expr_is_grouping_invariant(expr: &QueryExpr) -> bool {
    !expr_contains_current_row_var(expr)
}

pub(super) fn expr_contains_current_row_var(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::Var(_) => true,
        QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => false,
        QueryExpr::ScalarSubquery(_) | QueryExpr::ExistsSubquery { .. } => false,
        QueryExpr::InSubquery { expr, .. } => expr_contains_current_row_var(expr),
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => expr_contains_current_row_var(inner),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args.iter().any(expr_contains_current_row_var),
        QueryExpr::ArraySubscript { array, index, .. } => {
            expr_contains_current_row_var(array) || expr_contains_current_row_var(index)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(expr_contains_current_row_var)
                || when_then.iter().any(|(when, then)| {
                    expr_contains_current_row_var(when) || expr_contains_current_row_var(then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(expr_contains_current_row_var)
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            expr_contains_current_row_var(left) || expr_contains_current_row_var(right)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            args.iter().any(expr_contains_current_row_var)
                || filter.as_deref().is_some_and(expr_contains_current_row_var)
        }
    }
}

pub(super) fn collect_aggregate_calls(expr: &QueryExpr, calls: &mut Vec<QueryExpr>) {
    if matches!(expr, QueryExpr::AggregateCall { .. }) {
        if !calls.contains(expr) {
            calls.push(expr.clone());
        }
        return;
    }
    match expr {
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => collect_aggregate_calls(inner, calls),
        QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                collect_aggregate_calls(arg, calls);
            }
            if let Some(filter) = filter {
                collect_aggregate_calls(filter, calls);
            }
        }
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => {
            for arg in args {
                collect_aggregate_calls(arg, calls);
            }
        }
        QueryExpr::ArraySubscript { array, index, .. } => {
            collect_aggregate_calls(array, calls);
            collect_aggregate_calls(index, calls);
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_aggregate_calls(operand, calls);
            }
            for (when, then) in when_then {
                collect_aggregate_calls(when, calls);
                collect_aggregate_calls(then, calls);
            }
            if let Some(else_expr) = else_expr {
                collect_aggregate_calls(else_expr, calls);
            }
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            collect_aggregate_calls(left, calls);
            collect_aggregate_calls(right, calls);
        }
        QueryExpr::InSubquery { expr, .. } => collect_aggregate_calls(expr, calls),
        QueryExpr::AggregateCall { .. } => {}
        QueryExpr::ScalarSubquery(_) | QueryExpr::ExistsSubquery { .. } => {}
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => {}
    }
}

pub(super) fn compile_aggregate_expr(
    expr: &QueryExpr,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    let QueryExpr::AggregateCall {
        func,
        args,
        distinct,
        filter,
        ..
    } = expr
    else {
        return Err(PgFrontendError::unsupported(
            "internal aggregate compiler received a non-aggregate expression",
        ));
    };
    compile_aggregate_call(*func, args, *distinct, filter.as_deref(), query, ctx)
}

pub(super) fn compile_aggregate_call(
    func: AggregateFunction,
    args: &[QueryExpr],
    distinct: bool,
    filter: Option<&QueryExpr>,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    let compiled_args = args
        .iter()
        .map(|arg| compile_expr(arg, query, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = filter
        .map(|expr| compile_expr(expr, query, ctx).map(Box::new))
        .transpose()?;

    let expr = match (func, distinct, compiled_args.as_slice()) {
        (AggregateFunction::Count, false, []) => {
            datafusion::functions_aggregate::count::count_all()
        }
        (AggregateFunction::Count, false, [arg]) => {
            datafusion::functions_aggregate::expr_fn::count(arg.clone())
        }
        (AggregateFunction::Count, true, [arg]) => {
            datafusion::functions_aggregate::expr_fn::count_distinct(arg.clone())
        }
        (AggregateFunction::Sum, false, [arg]) => {
            let arg = compile_pg_sum_arg(arg.clone(), &args[0])?;
            datafusion::functions_aggregate::expr_fn::sum(arg.clone())
        }
        (AggregateFunction::Sum, true, [arg]) => {
            let arg = compile_pg_sum_arg(arg.clone(), &args[0])?;
            datafusion::functions_aggregate::expr_fn::sum_distinct(arg.clone())
        }
        (AggregateFunction::Avg, distinct, [arg]) => {
            let arg = compile_pg_avg_arg(arg.clone(), &args[0])?;
            Expr::AggregateFunction(DfAggregateFunction::new_udf(
                df_functions::pg_avg_udaf(),
                vec![arg],
                distinct,
                None,
                vec![],
                None,
            ))
        }
        (AggregateFunction::Min, _, [arg]) => {
            datafusion::functions_aggregate::expr_fn::min(arg.clone())
        }
        (AggregateFunction::Max, _, [arg]) => {
            datafusion::functions_aggregate::expr_fn::max(arg.clone())
        }
        (AggregateFunction::StddevPop, distinct, [arg]) => {
            let arg = compile_pg_float8_stat_arg(arg.clone());
            aggregate_udf_expr(
                datafusion::functions_aggregate::stddev::stddev_pop_udaf(),
                arg,
                distinct,
            )
        }
        (AggregateFunction::StddevSamp, distinct, [arg]) => {
            let arg = compile_pg_float8_stat_arg(arg.clone());
            aggregate_udf_expr(
                datafusion::functions_aggregate::stddev::stddev_udaf(),
                arg,
                distinct,
            )
        }
        (AggregateFunction::VarPop, distinct, [arg]) => {
            let arg = compile_pg_float8_stat_arg(arg.clone());
            aggregate_udf_expr(
                datafusion::functions_aggregate::variance::var_pop_udaf(),
                arg,
                distinct,
            )
        }
        (AggregateFunction::VarSamp, distinct, [arg]) => {
            let arg = compile_pg_float8_stat_arg(arg.clone());
            aggregate_udf_expr(
                datafusion::functions_aggregate::variance::var_samp_udaf(),
                arg,
                distinct,
            )
        }
        (AggregateFunction::RegrCount, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_count_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrSxx, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_sxx_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrSyy, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_syy_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrSxy, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_sxy_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrAvgX, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_avgx_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrAvgY, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_avgy_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrR2, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_r2_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrSlope, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_slope_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::RegrIntercept, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::regr::regr_intercept_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::CovarPop, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::covariance::covar_pop_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::CovarSamp, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::covariance::covar_samp_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::Corr, distinct, [left, right]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::correlation::corr_udaf(),
            compile_pg_float8_stat_args([left.clone(), right.clone()]),
            distinct,
        ),
        (AggregateFunction::StringAgg, distinct, [value, delimiter]) => aggregate_udf_expr_args(
            datafusion::functions_aggregate::string_agg::string_agg_udaf(),
            vec![value.clone(), delimiter.clone()],
            distinct,
        ),
        (AggregateFunction::Grouping, false, args) if !args.is_empty() => aggregate_udf_expr_args(
            datafusion::functions_aggregate::grouping::grouping_udaf(),
            args.to_vec(),
            false,
        ),
        _ => {
            return Err(PgFrontendError::unsupported(format!(
                "aggregate argument shape is not supported by pg_frontend v1: {func:?} distinct={distinct} args={}",
                compiled_args.len()
            )))
        }
    };
    with_aggregate_filter(expr, filter)
}

pub(super) fn with_aggregate_filter(
    expr: Expr,
    filter: Option<Box<Expr>>,
) -> Result<Expr, PgFrontendError> {
    let Some(filter) = filter else {
        return Ok(expr);
    };
    match expr {
        Expr::AggregateFunction(mut aggregate) => {
            aggregate.params.filter = Some(filter);
            Ok(Expr::AggregateFunction(aggregate))
        }
        other => Err(PgFrontendError::unsupported(format!(
            "aggregate FILTER cannot be applied to expression {other:?}"
        ))),
    }
}

pub(super) fn aggregate_udf_expr(udf: Arc<AggregateUDF>, arg: Expr, distinct: bool) -> Expr {
    aggregate_udf_expr_args(udf, vec![arg], distinct)
}

pub(super) fn aggregate_udf_expr_args(
    udf: Arc<AggregateUDF>,
    args: Vec<Expr>,
    distinct: bool,
) -> Expr {
    Expr::AggregateFunction(DfAggregateFunction::new_udf(
        udf,
        args,
        distinct,
        None,
        vec![],
        None,
    ))
}

pub(super) fn compile_pg_avg_arg(expr: Expr, source: &QueryExpr) -> Result<Expr, PgFrontendError> {
    if expr_pg_type(source)
        .map(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::FLOAT4OID))
        .unwrap_or(false)
    {
        Ok(Expr::Cast(Cast::new(Box::new(expr), DataType::Float64)))
    } else {
        Ok(expr)
    }
}

pub(super) fn compile_pg_sum_arg(expr: Expr, source: &QueryExpr) -> Result<Expr, PgFrontendError> {
    let Some(pg_type) = expr_pg_type(source) else {
        return Ok(expr);
    };
    let data_type = if pg_type.oid == u32::from(pgrx::pg_sys::INT2OID)
        || pg_type.oid == u32::from(pgrx::pg_sys::INT4OID)
    {
        Some(DataType::Int64)
    } else if pg_type.oid == u32::from(pgrx::pg_sys::FLOAT4OID) {
        Some(DataType::Float64)
    } else if pg_type.oid == u32::from(pgrx::pg_sys::INT8OID) {
        Some(DataType::Decimal128(38, 0))
    } else {
        None
    };
    Ok(match data_type {
        Some(data_type) => Expr::Cast(Cast::new(Box::new(expr), data_type)),
        None => expr,
    })
}

pub(super) fn compile_pg_float8_stat_arg(expr: Expr) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), DataType::Float64))
}

pub(super) fn compile_pg_float8_stat_args<const N: usize>(exprs: [Expr; N]) -> Vec<Expr> {
    exprs.into_iter().map(compile_pg_float8_stat_arg).collect()
}
