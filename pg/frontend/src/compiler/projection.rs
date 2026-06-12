use super::*;

pub(super) fn validate_supported_query_shape(query: &TypedQuery) -> Result<(), PgFrontendError> {
    let _ = query;
    Ok(())
}

pub(super) fn visible_targets(query: &TypedQuery) -> impl Iterator<Item = &Target> {
    query.targets.iter().filter(|target| !target.resjunk)
}

pub(super) fn visible_plan_columns(query: &TypedQuery, plan: &LogicalPlan) -> Vec<Expr> {
    query
        .targets
        .iter()
        .enumerate()
        .filter(|(_, target)| !target.resjunk)
        .map(|(index, _)| {
            let (qualifier, field) = plan.schema().qualified_field(index);
            Expr::Column(Column::from((qualifier, field)))
        })
        .collect()
}

pub(super) fn target_projection(
    query: &TypedQuery,
    rtindex: usize,
    resolved: &ResolvedTable,
    include_resjunk: bool,
) -> Result<Vec<usize>, PgFrontendError> {
    let mut projection = Vec::new();
    for target in query
        .targets
        .iter()
        .filter(|target| include_resjunk || !target.resjunk)
    {
        validate_target_expr(&target.expr)?;
        collect_var_indices_for_relation(&target.expr, rtindex, resolved, &mut projection)?;
    }
    Ok(projection)
}

pub(super) fn relation_projection(
    query: &TypedQuery,
    rtindex: usize,
    resolved: &ResolvedTable,
    include_resjunk: bool,
) -> Result<Vec<usize>, PgFrontendError> {
    let mut projection = target_projection(query, rtindex, resolved, include_resjunk)?;
    if let Some(selection) = &query.selection {
        collect_var_indices_for_relation(selection, rtindex, resolved, &mut projection)?;
    }
    collect_from_item_var_indices(&query.from, rtindex, resolved, &mut projection)?;
    Ok(projection)
}

pub(super) fn collect_from_item_var_indices(
    item: &FromItem,
    rtindex: usize,
    resolved: &ResolvedTable,
    projection: &mut Vec<usize>,
) -> Result<(), PgFrontendError> {
    match item {
        FromItem::Join {
            left, right, quals, ..
        } => {
            if let Some(quals) = quals {
                collect_var_indices_for_relation(quals, rtindex, resolved, projection)?;
            }
            collect_from_item_var_indices(left, rtindex, resolved, projection)?;
            collect_from_item_var_indices(right, rtindex, resolved, projection)
        }
        FromItem::Empty
        | FromItem::Relation { .. }
        | FromItem::Values { .. }
        | FromItem::Cte { .. }
        | FromItem::Subquery { .. } => Ok(()),
    }
}

pub(super) fn collect_var_indices_for_relation(
    expr: &QueryExpr,
    rtindex: usize,
    resolved: &ResolvedTable,
    projection: &mut Vec<usize>,
) -> Result<(), PgFrontendError> {
    match expr {
        QueryExpr::Var(var) => {
            if var.rtindex == rtindex {
                let index = var_column_index(*var, resolved)?;
                if !projection.contains(&index) {
                    projection.push(index);
                }
            }
            Ok(())
        }
        QueryExpr::OuterVar(_) => Ok(()),
        QueryExpr::RelabelType(inner) => {
            collect_var_indices_for_relation(inner, rtindex, resolved, projection)
        }
        QueryExpr::Cast { arg, .. } => {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }
        QueryExpr::UnaryOp { arg, .. } => {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }
        QueryExpr::FunctionCall { args, .. } => args.iter().try_for_each(|arg| {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }),
        QueryExpr::Array { elements, .. } => elements.iter().try_for_each(|element| {
            collect_var_indices_for_relation(element, rtindex, resolved, projection)
        }),
        QueryExpr::ArraySubscript { array, index, .. } => {
            collect_var_indices_for_relation(array, rtindex, resolved, projection)?;
            collect_var_indices_for_relation(index, rtindex, resolved, projection)
        }
        QueryExpr::Coalesce { args, .. } => args.iter().try_for_each(|arg| {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }),
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_var_indices_for_relation(operand, rtindex, resolved, projection)?;
            }
            for (when, then) in when_then {
                collect_var_indices_for_relation(when, rtindex, resolved, projection)?;
                collect_var_indices_for_relation(then, rtindex, resolved, projection)?;
            }
            if let Some(else_expr) = else_expr {
                collect_var_indices_for_relation(else_expr, rtindex, resolved, projection)?;
            }
            Ok(())
        }
        QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                collect_var_indices_for_relation(arg, rtindex, resolved, projection)?;
            }
            if let Some(filter) = filter {
                collect_var_indices_for_relation(filter, rtindex, resolved, projection)?;
            }
            Ok(())
        }
        QueryExpr::InSubquery { expr, .. } => {
            collect_var_indices_for_relation(expr, rtindex, resolved, projection)
        }
        QueryExpr::ScalarSubquery(_) | QueryExpr::ExistsSubquery { .. } => Ok(()),
        QueryExpr::Bool { args, .. } => args.iter().try_for_each(|arg| {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }),
        QueryExpr::NullTest { arg, .. } | QueryExpr::BooleanTest { arg, .. } => {
            collect_var_indices_for_relation(arg, rtindex, resolved, projection)
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            collect_var_indices_for_relation(left, rtindex, resolved, projection)?;
            collect_var_indices_for_relation(right, rtindex, resolved, projection)
        }
        QueryExpr::AggregateCall { args, filter, .. } => {
            for arg in args {
                collect_var_indices_for_relation(arg, rtindex, resolved, projection)?;
            }
            if let Some(filter) = filter {
                collect_var_indices_for_relation(filter, rtindex, resolved, projection)?;
            }
            Ok(())
        }
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::Const(_) => Ok(()),
    }
}

pub(super) fn compile_target_expr_with_bindings(
    target: &Target,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    validate_target_expr(&target.expr)?;
    let expr = compile_expr_with_windows(
        &target.expr,
        query,
        ctx,
        window_bindings,
        scalar_bindings,
        aggregate_bindings,
    )?;
    Ok(match &target.name {
        Some(name) => expr.alias(name.clone()),
        None => expr,
    })
}

pub(super) fn validate_target_expr(expr: &QueryExpr) -> Result<(), PgFrontendError> {
    match expr {
        QueryExpr::BinaryOp { left, right, .. } => {
            validate_target_expr(left)?;
            validate_target_expr(right)
        }
        QueryExpr::UnaryOp { arg, .. } => validate_target_expr(arg),
        QueryExpr::RelabelType(inner) => validate_target_expr(inner),
        QueryExpr::Cast { arg, .. } => validate_target_expr(arg),
        QueryExpr::FunctionCall { args, .. } => args.iter().try_for_each(validate_target_expr),
        QueryExpr::Array { elements, .. } => elements.iter().try_for_each(validate_target_expr),
        QueryExpr::ArraySubscript { array, index, .. } => {
            validate_target_expr(array)?;
            validate_target_expr(index)
        }
        QueryExpr::Coalesce { args, .. } => args.iter().try_for_each(validate_target_expr),
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                validate_target_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_target_expr(when)?;
                validate_target_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_target_expr(else_expr)?;
            }
            Ok(())
        }
        QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                validate_target_expr(arg)?;
            }
            if let Some(filter) = filter {
                validate_target_expr(filter)?;
            }
            Ok(())
        }
        QueryExpr::InSubquery { expr, .. } => validate_target_expr(expr),
        QueryExpr::ScalarSubquery(_) | QueryExpr::ExistsSubquery { .. } => Ok(()),
        QueryExpr::Bool { args, .. } => args.iter().try_for_each(validate_target_expr),
        QueryExpr::NullTest { arg, .. } | QueryExpr::BooleanTest { arg, .. } => {
            validate_target_expr(arg)
        }
        QueryExpr::AggregateCall { args, filter, .. } => {
            for arg in args {
                validate_target_expr(arg)?;
            }
            if let Some(filter) = filter {
                validate_target_expr(filter)?;
            }
            Ok(())
        }
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) => Ok(()),
    }
}
