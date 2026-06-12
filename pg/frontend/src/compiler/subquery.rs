use super::*;

pub(super) fn extract_top_level_correlated_exists(
    query: &TypedQuery,
) -> Option<(Option<QueryExpr>, Vec<TypedQuery>)> {
    let selection = query.selection.as_ref()?;
    let mut predicates = split_and_predicates(selection);
    let mut exists_subqueries = Vec::new();
    predicates.retain(|predicate| match predicate {
        QueryExpr::ExistsSubquery { subquery, .. } if query_contains_outer_var(subquery) => {
            exists_subqueries.push((**subquery).clone());
            false
        }
        _ => true,
    });
    (!exists_subqueries.is_empty()).then(|| (combine_and_predicates(predicates), exists_subqueries))
}

pub(super) fn extract_top_level_correlated_in(
    query: &TypedQuery,
) -> Option<(Option<QueryExpr>, Vec<CorrelatedInSubquery>)> {
    let selection = query.selection.as_ref()?;
    let mut predicates = split_and_predicates(selection);
    let mut in_subqueries = Vec::new();
    predicates.retain(|predicate| match predicate {
        QueryExpr::InSubquery { expr, subquery, .. } if query_contains_outer_var(subquery) => {
            in_subqueries.push(CorrelatedInSubquery {
                expr: (**expr).clone(),
                subquery: (**subquery).clone(),
            });
            false
        }
        _ => true,
    });
    (!in_subqueries.is_empty()).then(|| (combine_and_predicates(predicates), in_subqueries))
}

pub(super) fn split_and_predicates(expr: &QueryExpr) -> Vec<QueryExpr> {
    match expr {
        QueryExpr::Bool {
            op: BoolOp::And,
            args,
        } => args.iter().flat_map(split_and_predicates).collect(),
        _ => vec![expr.clone()],
    }
}

pub(super) fn combine_and_predicates(mut predicates: Vec<QueryExpr>) -> Option<QueryExpr> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(QueryExpr::Bool {
            op: BoolOp::And,
            args: predicates,
        }),
    }
}

pub(super) fn apply_correlated_exists_join(
    outer_plan: LogicalPlan,
    subquery: &TypedQuery,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let mut inner_query = subquery.clone();
    let mut local_predicates = Vec::new();
    let mut correlated_predicates = Vec::new();
    if let Some(selection) = subquery.selection.as_ref() {
        for predicate in split_and_predicates(selection) {
            if expr_contains_outer_var(&predicate) {
                correlated_predicates.push(predicate);
            } else {
                local_predicates.push(predicate);
            }
        }
    }
    if correlated_predicates.is_empty() {
        return Err(PgFrontendError::unsupported(
            "correlated EXISTS subquery has no extractable correlation predicates",
        ));
    }
    inner_query.selection = combine_and_predicates(local_predicates);
    add_correlated_inner_targets(&mut inner_query, &correlated_predicates)?;

    let inner_ctx = resolved_tables_for_query(&inner_query, ctx.config)?;
    let inner_plan = base_plan(&inner_query, &inner_ctx)?;
    let mut on = Vec::new();
    let mut filters = Vec::new();
    for predicate in &correlated_predicates {
        if let Some((outer, inner)) = correlated_equi_pair(predicate) {
            let (outer_expr, inner_expr) = coerce_binary_operands(
                QueryOperator::Eq,
                pg_type::PgTypeRef::new(u32::from(pgrx::pg_sys::BOOLOID), -1, 0),
                compile_outer_join_expr(outer)?,
                compile_expr(inner, &inner_query, &inner_ctx)?,
                outer,
                inner,
            );
            on.push((outer_expr, inner_expr));
        } else {
            filters.push(compile_correlated_join_filter(
                predicate,
                &inner_query,
                &inner_ctx,
            )?);
        }
    }
    let filter = filters
        .into_iter()
        .reduce(|left, right| binary_expr(left, Operator::And, right));
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(outer_plan),
        Arc::new(inner_plan),
        on,
        filter,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

pub(super) fn apply_correlated_in_join(
    outer_plan: LogicalPlan,
    subquery: &CorrelatedInSubquery,
    outer_query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let mut inner_query = subquery.subquery.clone();
    let mut local_predicates = Vec::new();
    let mut correlated_predicates = Vec::new();
    if let Some(selection) = inner_query.selection.as_ref() {
        for predicate in split_and_predicates(selection) {
            if expr_contains_outer_var(&predicate) {
                correlated_predicates.push(predicate);
            } else {
                local_predicates.push(predicate);
            }
        }
    }
    inner_query.selection = combine_and_predicates(local_predicates);
    add_correlated_inner_targets(&mut inner_query, &correlated_predicates)?;

    let inner_ctx = resolved_tables_for_query(&inner_query, ctx.config)?;
    let inner_plan = base_plan(&inner_query, &inner_ctx)?;
    let inner_target = visible_targets(&inner_query)
        .next()
        .ok_or_else(|| PgFrontendError::unsupported("IN subquery must return one column"))?;
    let mut on = Vec::new();
    let (outer_expr, inner_expr) = coerce_binary_operands(
        QueryOperator::Eq,
        pg_type::PgTypeRef::new(u32::from(pgrx::pg_sys::BOOLOID), -1, 0),
        compile_expr(&subquery.expr, outer_query, ctx)?,
        compile_expr(&inner_target.expr, &inner_query, &inner_ctx)?,
        &subquery.expr,
        &inner_target.expr,
    );
    on.push((outer_expr, inner_expr));

    let mut filters = Vec::new();
    for predicate in &correlated_predicates {
        if let Some((outer, inner)) = correlated_equi_pair(predicate) {
            let (outer_expr, inner_expr) = coerce_binary_operands(
                QueryOperator::Eq,
                pg_type::PgTypeRef::new(u32::from(pgrx::pg_sys::BOOLOID), -1, 0),
                compile_outer_join_expr(outer)?,
                compile_expr(inner, &inner_query, &inner_ctx)?,
                outer,
                inner,
            );
            on.push((outer_expr, inner_expr));
        } else {
            filters.push(compile_correlated_join_filter(
                predicate,
                &inner_query,
                &inner_ctx,
            )?);
        }
    }
    let filter = filters
        .into_iter()
        .reduce(|left, right| binary_expr(left, Operator::And, right));
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(outer_plan),
        Arc::new(inner_plan),
        on,
        filter,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

pub(super) fn add_correlated_inner_targets(
    query: &mut TypedQuery,
    predicates: &[QueryExpr],
) -> Result<(), PgFrontendError> {
    let mut next_resno = query
        .targets
        .iter()
        .map(|target| target.resno)
        .max()
        .unwrap_or(0)
        + 1;
    for predicate in predicates {
        if let Some(inner) = correlated_inner_expr(predicate) {
            let pg_type = expr_pg_type(inner).ok_or_else(|| {
                PgFrontendError::unsupported(
                    "correlated EXISTS inner predicate has unknown expression type",
                )
            })?;
            query.targets.push(Target {
                expr: inner.clone(),
                name: Some(format!("__pg_fusion_corr_{}", next_resno)),
                pg_type,
                resno: next_resno,
                ressortgroupref: 0,
                resjunk: false,
            });
            next_resno += 1;
        }
    }
    Ok(())
}

pub(super) fn add_correlated_outer_targets(
    query: &mut TypedQuery,
    exists_subqueries: &[TypedQuery],
) -> Result<(), PgFrontendError> {
    let mut next_resno = query
        .targets
        .iter()
        .map(|target| target.resno)
        .max()
        .unwrap_or(0)
        + 1;
    for subquery in exists_subqueries {
        let Some(selection) = subquery.selection.as_ref() else {
            continue;
        };
        for predicate in split_and_predicates(selection) {
            let Some(outer) = correlated_outer_expr(&predicate) else {
                continue;
            };
            let expr = outer_expr_to_query_expr(query, outer)?;
            let pg_type = expr_pg_type(&expr).ok_or_else(|| {
                PgFrontendError::unsupported(
                    "correlated EXISTS outer predicate has unknown expression type",
                )
            })?;
            query.targets.push(Target {
                expr,
                name: Some(format!("__pg_fusion_outer_corr_{}", next_resno)),
                pg_type,
                resno: next_resno,
                ressortgroupref: 0,
                resjunk: false,
            });
            next_resno += 1;
        }
    }
    Ok(())
}

pub(super) fn add_correlated_in_outer_targets(
    query: &mut TypedQuery,
    in_subqueries: &[CorrelatedInSubquery],
) -> Result<(), PgFrontendError> {
    let mut next_resno = query
        .targets
        .iter()
        .map(|target| target.resno)
        .max()
        .unwrap_or(0)
        + 1;
    for in_subquery in in_subqueries {
        let pg_type = expr_pg_type(&in_subquery.expr).ok_or_else(|| {
            PgFrontendError::unsupported("correlated IN outer expression has unknown type")
        })?;
        query.targets.push(Target {
            expr: in_subquery.expr.clone(),
            name: Some(format!("__pg_fusion_in_outer_{}", next_resno)),
            pg_type,
            resno: next_resno,
            ressortgroupref: 0,
            resjunk: false,
        });
        next_resno += 1;

        let Some(selection) = in_subquery.subquery.selection.as_ref() else {
            continue;
        };
        for predicate in split_and_predicates(selection) {
            let Some(outer) = correlated_outer_expr(&predicate) else {
                continue;
            };
            let expr = outer_expr_to_query_expr(query, outer)?;
            let pg_type = expr_pg_type(&expr).ok_or_else(|| {
                PgFrontendError::unsupported(
                    "correlated IN outer predicate has unknown expression type",
                )
            })?;
            query.targets.push(Target {
                expr,
                name: Some(format!("__pg_fusion_outer_corr_{}", next_resno)),
                pg_type,
                resno: next_resno,
                ressortgroupref: 0,
                resjunk: false,
            });
            next_resno += 1;
        }
    }
    Ok(())
}

pub(super) fn correlated_equi_pair(expr: &QueryExpr) -> Option<(&QueryExpr, &QueryExpr)> {
    let QueryExpr::BinaryOp {
        op: QueryOperator::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    split_outer_inner_expr(left, right)
}

pub(super) fn correlated_inner_expr(expr: &QueryExpr) -> Option<&QueryExpr> {
    let QueryExpr::BinaryOp { left, right, .. } = expr else {
        return None;
    };
    split_outer_inner_expr(left, right).map(|(_outer, inner)| inner)
}

pub(super) fn correlated_outer_expr(expr: &QueryExpr) -> Option<&QueryExpr> {
    let QueryExpr::BinaryOp { left, right, .. } = expr else {
        return None;
    };
    split_outer_inner_expr(left, right).map(|(outer, _inner)| outer)
}

pub(super) fn outer_expr_to_query_expr(
    query: &TypedQuery,
    expr: &QueryExpr,
) -> Result<QueryExpr, PgFrontendError> {
    match expr {
        QueryExpr::OuterVar(var) => outer_var_to_query_var(query, var).map(QueryExpr::Var),
        QueryExpr::RelabelType(inner) => Ok(QueryExpr::RelabelType(Box::new(
            outer_expr_to_query_expr(query, inner)?,
        ))),
        QueryExpr::Cast { arg, pg_type } => Ok(QueryExpr::Cast {
            arg: Box::new(outer_expr_to_query_expr(query, arg)?),
            pg_type: *pg_type,
        }),
        _ => Err(PgFrontendError::unsupported(
            "correlated EXISTS outer predicate must reference an outer column",
        )),
    }
}

pub(super) fn outer_var_to_query_var(
    query: &TypedQuery,
    var: &OuterVar,
) -> Result<Var, PgFrontendError> {
    let mut matches = Vec::new();
    for relation in &query.relations {
        let relation_matches = match var.relation.as_deref() {
            Some(name) => relation.alias.as_deref() == Some(name) || relation.name == name,
            None => true,
        };
        if !relation_matches {
            continue;
        }
        if let Some(column) = relation
            .columns
            .iter()
            .find(|column| column.name == var.name)
        {
            matches.push(Var {
                rtindex: relation.rtindex,
                attnum: column.attnum,
                pg_type: var.pg_type,
            });
        }
    }
    match matches.as_slice() {
        [var] => Ok(*var),
        [] => Err(PgFrontendError::unsupported(format!(
            "correlated outer column {} was not found in outer query",
            var.name
        ))),
        _ => Err(PgFrontendError::unsupported(format!(
            "correlated outer column {} is ambiguous in outer query",
            var.name
        ))),
    }
}

pub(super) fn split_outer_inner_expr<'a>(
    left: &'a QueryExpr,
    right: &'a QueryExpr,
) -> Option<(&'a QueryExpr, &'a QueryExpr)> {
    match (
        expr_contains_outer_var(left),
        expr_contains_outer_var(right),
    ) {
        (true, false) => Some((left, right)),
        (false, true) => Some((right, left)),
        _ => None,
    }
}

pub(super) fn compile_outer_join_expr(expr: &QueryExpr) -> Result<Expr, PgFrontendError> {
    match expr {
        QueryExpr::OuterVar(var) => Ok(Expr::Column(match var.relation.as_deref() {
            Some(relation) => Column::new(Some(relation), var.name.as_str()),
            None => Column::from_name(var.name.as_str()),
        })),
        QueryExpr::RelabelType(inner) => compile_outer_join_expr(inner),
        QueryExpr::Cast { arg, pg_type } => {
            let data_type = arrow_type_for_pg_type(*pg_type).ok_or_else(|| {
                PgFrontendError::unsupported(format!(
                    "cast target PostgreSQL type oid {} is not supported by pg_frontend v1",
                    pg_type.oid
                ))
            })?;
            Ok(Expr::Cast(Cast::new(
                Box::new(compile_outer_join_expr(arg)?),
                data_type,
            )))
        }
        _ => Err(PgFrontendError::unsupported(
            "correlated EXISTS join key must be an outer column expression",
        )),
    }
}

pub(super) fn compile_correlated_join_filter(
    expr: &QueryExpr,
    inner_query: &TypedQuery,
    inner_ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    if !expr_contains_outer_var(expr) {
        return compile_expr(expr, inner_query, inner_ctx);
    }
    match expr {
        QueryExpr::OuterVar(_) | QueryExpr::RelabelType(_) | QueryExpr::Cast { .. } => {
            compile_outer_join_expr(expr)
        }
        QueryExpr::BinaryOp {
            op,
            left,
            right,
            pg_type,
        } => {
            let left_expr = compile_correlated_join_filter(left, inner_query, inner_ctx)?;
            let right_expr = compile_correlated_join_filter(right, inner_query, inner_ctx)?;
            compile_binary_expr(*op, *pg_type, left_expr, right_expr, left, right)
        }
        QueryExpr::Bool { op, args } => match op {
            BoolOp::And | BoolOp::Or => {
                if args.is_empty() {
                    return Err(PgFrontendError::unsupported(
                        "empty boolean expression is not supported",
                    ));
                }
                let operator = if *op == BoolOp::And {
                    Operator::And
                } else {
                    Operator::Or
                };
                let mut compiled = args
                    .iter()
                    .map(|arg| compile_correlated_join_filter(arg, inner_query, inner_ctx))
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
                Ok(Expr::Not(Box::new(compile_correlated_join_filter(
                    &args[0],
                    inner_query,
                    inner_ctx,
                )?)))
            }
        },
        QueryExpr::NullTest { arg, is_null } => {
            let arg = Box::new(compile_correlated_join_filter(arg, inner_query, inner_ctx)?);
            Ok(if *is_null {
                Expr::IsNull(arg)
            } else {
                Expr::IsNotNull(arg)
            })
        }
        QueryExpr::BooleanTest { arg, kind } => {
            let arg = Box::new(compile_correlated_join_filter(arg, inner_query, inner_ctx)?);
            Ok(match kind {
                BooleanTestKind::IsTrue => Expr::IsTrue(arg),
                BooleanTestKind::IsNotTrue => Expr::IsNotTrue(arg),
                BooleanTestKind::IsFalse => Expr::IsFalse(arg),
                BooleanTestKind::IsNotFalse => Expr::IsNotFalse(arg),
                BooleanTestKind::IsUnknown => Expr::IsUnknown(arg),
                BooleanTestKind::IsNotUnknown => Expr::IsNotUnknown(arg),
            })
        }
        _ => Err(PgFrontendError::unsupported(
            "correlated EXISTS join filter shape is not supported by pg_frontend v1",
        )),
    }
}

pub(super) fn attach_scalar_subqueries(
    mut plan: LogicalPlan,
    expr: &QueryExpr,
    ctx: &CompileContext,
    bindings: &mut Vec<ScalarSubqueryBinding>,
) -> Result<LogicalPlan, PgFrontendError> {
    let mut subqueries = Vec::new();
    collect_scalar_subqueries(expr, &mut subqueries);
    for subquery in subqueries {
        if bindings.iter().any(|binding| binding.query == *subquery) {
            continue;
        }
        if query_contains_outer_var(subquery) {
            continue;
        }

        let subquery_plan = compile_typed_query(subquery, ctx.config)?.logical_plan;
        let subquery_plan =
            scalar_subquery_value_plan(subquery_plan, scalar_subquery_value_alias(bindings.len()))?;
        let alias = TableReference::bare(format!("scalar_subquery_{}", bindings.len() + 1));
        let aliased =
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(subquery_plan), alias)?);
        let (qualifier, field) = aliased.schema().qualified_field(0);
        let column = Expr::Column(Column::from((qualifier, field)));
        plan = LogicalPlan::Join(Join::try_new(
            Arc::new(plan),
            Arc::new(aliased),
            Vec::new(),
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        bindings.push(ScalarSubqueryBinding {
            query: subquery.clone(),
            column,
        });
    }
    Ok(plan)
}

pub(super) fn scalar_subquery_value_plan(
    subquery_plan: LogicalPlan,
    output_alias: String,
) -> Result<LogicalPlan, PgFrontendError> {
    if subquery_plan.schema().fields().len() != 1 {
        return Err(PgFrontendError::unsupported(
            "scalar subquery must return exactly one column",
        ));
    }
    let (qualifier, field) = subquery_plan.schema().qualified_field(0);
    let value = Expr::Column(Column::from((qualifier, field)));
    let aggregate = df_functions::pg_scalar_subquery_value_udaf()
        .call(vec![value])
        .alias(output_alias);
    Ok(LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(subquery_plan),
        Vec::new(),
        vec![aggregate],
    )?))
}

pub(super) fn scalar_subquery_value_alias(index: usize) -> String {
    if index == 0 {
        "scalar_subquery_value".into()
    } else {
        format!("scalar_subquery_value_{}", index + 1)
    }
}

pub(super) fn collect_scalar_subqueries<'a>(expr: &'a QueryExpr, out: &mut Vec<&'a TypedQuery>) {
    match expr {
        QueryExpr::ScalarSubquery(subquery) => out.push(subquery),
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. } => {
            collect_scalar_subqueries(inner, out);
        }
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => {
            for arg in args {
                collect_scalar_subqueries(arg, out);
            }
        }
        QueryExpr::ArraySubscript { array, index, .. } => {
            collect_scalar_subqueries(array, out);
            collect_scalar_subqueries(index, out);
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            collect_scalar_subqueries(left, out);
            collect_scalar_subqueries(right, out);
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                collect_scalar_subqueries(arg, out);
            }
            if let Some(filter) = filter {
                collect_scalar_subqueries(filter, out);
            }
        }
        QueryExpr::NullTest { arg, .. } | QueryExpr::BooleanTest { arg, .. } => {
            collect_scalar_subqueries(arg, out)
        }
        QueryExpr::InSubquery { expr, .. } => collect_scalar_subqueries(expr, out),
        QueryExpr::ExistsSubquery { .. } => {}
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_scalar_subqueries(operand, out);
            }
            for (when, then) in when_then {
                collect_scalar_subqueries(when, out);
                collect_scalar_subqueries(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_scalar_subqueries(else_expr, out);
            }
        }
        QueryExpr::Var(_) | QueryExpr::OuterVar(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => {}
    }
}

pub(super) fn contains_scalar_subquery(expr: &QueryExpr) -> bool {
    let mut subqueries = Vec::new();
    collect_scalar_subqueries(expr, &mut subqueries);
    !subqueries.is_empty()
}

pub(super) fn contains_predicate_subquery(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::ExistsSubquery { .. } | QueryExpr::InSubquery { .. } => true,
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => contains_predicate_subquery(inner),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args.iter().any(contains_predicate_subquery),
        QueryExpr::ArraySubscript { array, index, .. } => {
            contains_predicate_subquery(array) || contains_predicate_subquery(index)
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            contains_predicate_subquery(left) || contains_predicate_subquery(right)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            args.iter().any(contains_predicate_subquery)
                || filter.as_deref().is_some_and(contains_predicate_subquery)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(contains_predicate_subquery)
                || when_then.iter().any(|(when, then)| {
                    contains_predicate_subquery(when) || contains_predicate_subquery(then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(contains_predicate_subquery)
        }
        QueryExpr::ScalarSubquery(_)
        | QueryExpr::Var(_)
        | QueryExpr::OuterVar(_)
        | QueryExpr::Const(_)
        | QueryExpr::Param(_) => false,
    }
}

pub(super) fn query_contains_outer_var(query: &TypedQuery) -> bool {
    query
        .selection
        .as_ref()
        .is_some_and(expr_contains_outer_var)
        || query
            .targets
            .iter()
            .any(|target| expr_contains_outer_var(&target.expr))
        || query
            .limit_count
            .as_ref()
            .is_some_and(expr_contains_outer_var)
        || query
            .limit_offset
            .as_ref()
            .is_some_and(expr_contains_outer_var)
        || query.having.as_ref().is_some_and(expr_contains_outer_var)
        || from_item_contains_outer_var(&query.from)
        || query
            .ctes
            .iter()
            .any(|cte| query_contains_outer_var(&cte.query))
        || query
            .subqueries
            .iter()
            .any(|subquery| query_contains_outer_var(&subquery.query))
}

pub(super) fn from_item_contains_outer_var(item: &FromItem) -> bool {
    match item {
        FromItem::Join {
            left, right, quals, ..
        } => {
            quals.as_ref().is_some_and(expr_contains_outer_var)
                || from_item_contains_outer_var(left)
                || from_item_contains_outer_var(right)
        }
        FromItem::Empty
        | FromItem::Relation { .. }
        | FromItem::Values { .. }
        | FromItem::Cte { .. }
        | FromItem::Subquery { .. } => false,
    }
}

pub(super) fn expr_contains_outer_var(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::OuterVar(_) => true,
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => expr_contains_outer_var(inner),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args.iter().any(expr_contains_outer_var),
        QueryExpr::ArraySubscript { array, index, .. } => {
            expr_contains_outer_var(array) || expr_contains_outer_var(index)
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            expr_contains_outer_var(left) || expr_contains_outer_var(right)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            args.iter().any(expr_contains_outer_var)
                || filter.as_deref().is_some_and(expr_contains_outer_var)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand.as_deref().is_some_and(expr_contains_outer_var)
                || when_then.iter().any(|(when, then)| {
                    expr_contains_outer_var(when) || expr_contains_outer_var(then)
                })
                || else_expr.as_deref().is_some_and(expr_contains_outer_var)
        }
        QueryExpr::ScalarSubquery(query)
        | QueryExpr::ExistsSubquery {
            subquery: query, ..
        } => query_contains_outer_var(query),
        QueryExpr::InSubquery { expr, subquery, .. } => {
            expr_contains_outer_var(expr) || query_contains_outer_var(subquery)
        }
        QueryExpr::Var(_) | QueryExpr::Const(_) | QueryExpr::Param(_) => false,
    }
}
