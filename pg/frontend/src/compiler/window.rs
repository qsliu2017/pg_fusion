use super::*;

pub(super) fn window_plan(
    query: &TypedQuery,
    input: LogicalPlan,
    ctx: &CompileContext,
    aggregate_bindings: &[AggregateBinding],
) -> Result<(LogicalPlan, Vec<WindowBinding>), PgFrontendError> {
    let mut calls = Vec::new();
    for target in &query.targets {
        collect_window_calls(&target.expr, &mut calls);
    }
    if calls.is_empty() {
        return Err(PgFrontendError::unsupported(
            "query is marked as containing window functions but no window call was found",
        ));
    }

    let mut plan = input;
    let mut bindings = Vec::with_capacity(calls.len());
    let mut alias_counts = HashMap::<&'static str, usize>::new();
    let mut used_aliases = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<HashSet<_>>();
    for expr in calls {
        let input_len = plan.schema().fields().len();
        let QueryExpr::WindowCall { func, .. } = &expr else {
            unreachable!("window calls contains only window expressions");
        };
        let window_expr = compile_window_call(&expr, query, ctx, aggregate_bindings)?.alias(
            readable_internal_alias(
                &mut alias_counts,
                &mut used_aliases,
                window_alias_base(*func),
            ),
        );
        plan = LogicalPlan::Window(Window::try_new(vec![window_expr], Arc::new(plan))?);
        let (qualifier, field) = plan.schema().qualified_field(input_len);
        let column =
            aggregate_binding_column(&expr, Expr::Column(Column::from((qualifier, field))));
        bindings.push(WindowBinding { expr, column });
    }
    Ok((plan, bindings))
}

pub(super) fn collect_window_calls(expr: &QueryExpr, calls: &mut Vec<QueryExpr>) {
    if matches!(expr, QueryExpr::WindowCall { .. }) {
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
        | QueryExpr::BooleanTest { arg: inner, .. } => collect_window_calls(inner, calls),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => {
            for arg in args {
                collect_window_calls(arg, calls);
            }
        }
        QueryExpr::ArraySubscript { array, index, .. } => {
            collect_window_calls(array, calls);
            collect_window_calls(index, calls);
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_window_calls(operand, calls);
            }
            for (when, then) in when_then {
                collect_window_calls(when, calls);
                collect_window_calls(then, calls);
            }
            if let Some(else_expr) = else_expr {
                collect_window_calls(else_expr, calls);
            }
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            collect_window_calls(left, calls);
            collect_window_calls(right, calls);
        }
        QueryExpr::AggregateCall { args, filter, .. } => {
            for arg in args {
                collect_window_calls(arg, calls);
            }
            if let Some(filter) = filter {
                collect_window_calls(filter, calls);
            }
        }
        QueryExpr::InSubquery { expr, .. } => collect_window_calls(expr, calls),
        QueryExpr::ExistsSubquery { .. } => {}
        QueryExpr::ScalarSubquery(_) => {}
        QueryExpr::WindowCall { .. } => {}
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => {}
    }
}

pub(super) fn compile_window_call(
    call: &QueryExpr,
    query: &TypedQuery,
    ctx: &CompileContext,
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let QueryExpr::WindowCall {
        func,
        args,
        winref,
        filter,
        distinct,
        ..
    } = call
    else {
        return Err(PgFrontendError::unsupported(
            "internal window compiler received a non-window expression",
        ));
    };
    let spec = window_spec(query, *winref)?;
    let args = compile_window_args(*func, args, query, ctx, aggregate_bindings)?;
    let filter = filter
        .as_deref()
        .map(|expr| {
            compile_expr_with_windows(expr, query, ctx, &[], &[], aggregate_bindings).map(Box::new)
        })
        .transpose()?;
    if filter.is_some() && !matches!(func, WindowFunctionKind::Aggregate(_)) {
        return Err(PgFrontendError::unsupported(
            "FILTER is only supported for aggregate window functions",
        ));
    }
    let partition_by = spec
        .partition_refs
        .iter()
        .map(|target_ref| compile_sort_group_target(*target_ref, query, ctx, aggregate_bindings))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = spec
        .order
        .iter()
        .map(|key| compile_window_sort_key(key, query, ctx, aggregate_bindings))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Expr::from(WindowFunction {
        fun: window_function_definition(*func)?,
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame: compile_window_frame(&spec.frame, spec.order.is_empty()),
            filter,
            null_treatment: None,
            distinct: *distinct,
        },
    }))
}

pub(super) fn compile_window_args(
    func: WindowFunctionKind,
    args: &[QueryExpr],
    query: &TypedQuery,
    ctx: &CompileContext,
    aggregate_bindings: &[AggregateBinding],
) -> Result<Vec<Expr>, PgFrontendError> {
    if matches!(
        func,
        WindowFunctionKind::CumeDist
            | WindowFunctionKind::DenseRank
            | WindowFunctionKind::PercentRank
            | WindowFunctionKind::Rank
            | WindowFunctionKind::RowNumber
    ) && !args.is_empty()
    {
        return Err(PgFrontendError::unsupported(
            "rank-like window function does not accept arguments",
        ));
    }
    if matches!(
        func,
        WindowFunctionKind::FirstValue | WindowFunctionKind::LastValue
    ) && args.len() != 1
    {
        return Err(PgFrontendError::unsupported(
            "first_value/last_value window function requires exactly one argument",
        ));
    }
    if matches!(func, WindowFunctionKind::Lag | WindowFunctionKind::Lead)
        && (args.is_empty() || args.len() > 3)
    {
        return Err(PgFrontendError::unsupported(
            "lag/lead window function requires one to three arguments",
        ));
    }
    if func == WindowFunctionKind::Ntile && args.len() != 1 {
        return Err(PgFrontendError::unsupported(
            "ntile window function requires exactly one argument",
        ));
    }
    if func == WindowFunctionKind::NthValue && args.len() != 2 {
        return Err(PgFrontendError::unsupported(
            "nth_value window function requires exactly two arguments",
        ));
    }
    if matches!(
        func,
        WindowFunctionKind::Aggregate(AggregateFunction::Count)
    ) && args.is_empty()
    {
        return Ok(vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)]);
    }
    args.iter()
        .map(|arg| {
            let expr = compile_expr_with_windows(arg, query, ctx, &[], &[], aggregate_bindings)?;
            match func {
                WindowFunctionKind::Aggregate(AggregateFunction::Avg) => {
                    compile_pg_avg_arg(expr, arg)
                }
                WindowFunctionKind::Aggregate(AggregateFunction::Sum) => {
                    compile_pg_sum_arg(expr, arg)
                }
                WindowFunctionKind::Aggregate(
                    AggregateFunction::StddevPop
                    | AggregateFunction::StddevSamp
                    | AggregateFunction::VarPop
                    | AggregateFunction::VarSamp
                    | AggregateFunction::RegrCount
                    | AggregateFunction::RegrSxx
                    | AggregateFunction::RegrSyy
                    | AggregateFunction::RegrSxy
                    | AggregateFunction::RegrAvgX
                    | AggregateFunction::RegrAvgY
                    | AggregateFunction::RegrR2
                    | AggregateFunction::RegrSlope
                    | AggregateFunction::RegrIntercept
                    | AggregateFunction::CovarPop
                    | AggregateFunction::CovarSamp
                    | AggregateFunction::Corr,
                ) => Ok(compile_pg_float8_stat_arg(expr)),
                _ => Ok(expr),
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

pub(super) fn window_function_definition(
    func: WindowFunctionKind,
) -> Result<WindowFunctionDefinition, PgFrontendError> {
    Ok(match func {
        WindowFunctionKind::CumeDist => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::cume_dist::cume_dist_udwf(),
        ),
        WindowFunctionKind::DenseRank => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::rank::dense_rank_udwf(),
        ),
        WindowFunctionKind::FirstValue => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::nth_value::first_value_udwf(),
        ),
        WindowFunctionKind::Lag => {
            WindowFunctionDefinition::WindowUDF(datafusion::functions_window::lead_lag::lag_udwf())
        }
        WindowFunctionKind::LastValue => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::nth_value::last_value_udwf(),
        ),
        WindowFunctionKind::Lead => {
            WindowFunctionDefinition::WindowUDF(datafusion::functions_window::lead_lag::lead_udwf())
        }
        WindowFunctionKind::NthValue => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::nth_value::nth_value_udwf(),
        ),
        WindowFunctionKind::Ntile => {
            WindowFunctionDefinition::WindowUDF(datafusion::functions_window::ntile::ntile_udwf())
        }
        WindowFunctionKind::PercentRank => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::rank::percent_rank_udwf(),
        ),
        WindowFunctionKind::Rank => {
            WindowFunctionDefinition::WindowUDF(datafusion::functions_window::rank::rank_udwf())
        }
        WindowFunctionKind::RowNumber => WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::row_number::row_number_udwf(),
        ),
        WindowFunctionKind::Aggregate(AggregateFunction::Count) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::count::count_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Sum) => {
            WindowFunctionDefinition::AggregateUDF(datafusion::functions_aggregate::sum::sum_udaf())
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Avg) => {
            WindowFunctionDefinition::AggregateUDF(df_functions::pg_avg_udaf())
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Min) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::min_max::min_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Max) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::min_max::max_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::StddevPop) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::stddev::stddev_pop_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::StddevSamp) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::stddev::stddev_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::VarPop) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::variance::var_pop_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::VarSamp) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::variance::var_samp_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrCount) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_count_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrSxx) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_sxx_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrSyy) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_syy_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrSxy) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_sxy_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrAvgX) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_avgx_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrAvgY) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_avgy_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrR2) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_r2_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrSlope) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_slope_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::RegrIntercept) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::regr::regr_intercept_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::CovarPop) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::covariance::covar_pop_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::CovarSamp) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::covariance::covar_samp_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Corr) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::correlation::corr_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::StringAgg) => {
            WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::string_agg::string_agg_udaf(),
            )
        }
        WindowFunctionKind::Aggregate(AggregateFunction::Grouping) => {
            return Err(PgFrontendError::unsupported(
                "GROUPING() is not supported as a window function",
            ))
        }
    })
}

pub(super) fn compile_window_sort_key(
    key: &SortKey,
    query: &TypedQuery,
    ctx: &CompileContext,
    aggregate_bindings: &[AggregateBinding],
) -> Result<datafusion_expr::expr::Sort, PgFrontendError> {
    Ok(
        compile_sort_group_target(key.target_ref, query, ctx, aggregate_bindings)?
            .sort(key.asc, key.nulls_first),
    )
}

pub(super) fn compile_sort_group_target(
    target_ref: u32,
    query: &TypedQuery,
    ctx: &CompileContext,
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let target = target_by_sort_group_ref(query, target_ref)?;
    compile_expr_with_windows(&target.expr, query, ctx, &[], &[], aggregate_bindings)
}

pub(super) fn compile_window_frame(frame: &WindowFrameSpec, order_by_empty: bool) -> WindowFrame {
    if order_by_empty && frame.units == WindowFrameUnits::Range {
        return WindowFrame::new_bounds(
            DfWindowFrameUnits::Rows,
            DfWindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            DfWindowFrameBound::Following(ScalarValue::UInt64(None)),
        );
    }
    WindowFrame::new_bounds(
        match frame.units {
            WindowFrameUnits::Rows => DfWindowFrameUnits::Rows,
            WindowFrameUnits::Range => DfWindowFrameUnits::Range,
            WindowFrameUnits::Groups => DfWindowFrameUnits::Groups,
        },
        compile_window_frame_bound(&frame.start),
        compile_window_frame_bound(&frame.end),
    )
}

pub(super) fn compile_window_frame_bound(bound: &WindowFrameBound) -> DfWindowFrameBound {
    match bound {
        WindowFrameBound::UnboundedPreceding => {
            DfWindowFrameBound::Preceding(ScalarValue::UInt64(None))
        }
        WindowFrameBound::UnboundedFollowing => {
            DfWindowFrameBound::Following(ScalarValue::UInt64(None))
        }
        WindowFrameBound::CurrentRow => DfWindowFrameBound::CurrentRow,
        WindowFrameBound::Preceding(value) => DfWindowFrameBound::Preceding(value.clone()),
        WindowFrameBound::Following(value) => DfWindowFrameBound::Following(value.clone()),
    }
}

pub(super) fn window_spec(query: &TypedQuery, winref: u32) -> Result<&WindowSpec, PgFrontendError> {
    query
        .windows
        .iter()
        .find(|spec| spec.ref_id == winref)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!("window specification ref {winref} was not found"))
        })
}
