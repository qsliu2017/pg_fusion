use super::*;

pub(super) fn base_plan(
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    if let Some(set_operation) = query.set_operation.as_ref() {
        let plan = compile_set_operation_tree(set_operation, ctx)?;
        let rtindex = first_set_operation_rtindex(set_operation)?;
        let subquery = ctx.subquery(rtindex)?;
        return Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            table_reference_for_subquery(subquery),
        )?));
    }

    if let Some((selection, exists_subqueries)) = extract_top_level_correlated_exists(query) {
        let mut outer_query = query.clone();
        outer_query.selection = selection.clone();
        add_correlated_outer_targets(&mut outer_query, &exists_subqueries)?;
        let mut plan = build_from_item(&outer_query, &outer_query.from, ctx, None)?;
        if let Some(selection) = selection.as_ref() {
            let mut scalar_bindings = Vec::new();
            plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
            let predicate = compile_expr_with_windows(
                selection,
                &outer_query,
                ctx,
                &[],
                &scalar_bindings,
                &[],
            )?;
            plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
                predicate,
                Arc::new(plan),
            )?);
        }
        for subquery in exists_subqueries {
            plan = apply_correlated_exists_join(plan, &subquery, ctx)?;
        }
        return Ok(plan);
    }

    if let Some((selection, in_subqueries)) = extract_top_level_correlated_in(query) {
        let mut outer_query = query.clone();
        outer_query.selection = selection.clone();
        add_correlated_in_outer_targets(&mut outer_query, &in_subqueries)?;
        let mut plan = build_from_item(&outer_query, &outer_query.from, ctx, None)?;
        if let Some(selection) = selection.as_ref() {
            let mut scalar_bindings = Vec::new();
            plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
            let predicate = compile_expr_with_windows(
                selection,
                &outer_query,
                ctx,
                &[],
                &scalar_bindings,
                &[],
            )?;
            plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
                predicate,
                Arc::new(plan),
            )?);
        }
        for subquery in in_subqueries {
            plan = apply_correlated_in_join(plan, &subquery, &outer_query, ctx)?;
        }
        return Ok(plan);
    }

    let push_selection_into_scan = matches!(query.from, FromItem::Relation { .. })
        && !query.selection.as_ref().is_some_and(|expr| {
            contains_scalar_subquery(expr)
                || contains_predicate_subquery(expr)
                || expr_contains_outer_var(expr)
        });
    let mut plan = build_from_item(
        query,
        &query.from,
        ctx,
        push_selection_into_scan
            .then_some(query.selection.as_ref())
            .flatten(),
    )?;
    if !push_selection_into_scan {
        if let Some(selection) = query.selection.as_ref() {
            let mut scalar_bindings = Vec::new();
            plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
            let predicate =
                compile_expr_with_windows(selection, query, ctx, &[], &scalar_bindings, &[])?;
            plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
                predicate,
                Arc::new(plan),
            )?);
        }
    }
    Ok(plan)
}

pub(super) fn first_set_operation_rtindex(
    operation: &SetOperationTree,
) -> Result<usize, PgFrontendError> {
    match operation {
        SetOperationTree::Range { rtindex } => Ok(*rtindex),
        SetOperationTree::Operation { left, .. } => first_set_operation_rtindex(left),
    }
}

pub(super) fn compile_set_operation_tree(
    operation: &SetOperationTree,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    match operation {
        SetOperationTree::Range { rtindex } => build_subquery_scan(*rtindex, ctx),
        SetOperationTree::Operation {
            op,
            all,
            left,
            right,
        } => {
            let left = compile_set_operation_tree(left, ctx)?;
            let right = compile_set_operation_tree(right, ctx)?;
            match op {
                SetOperator::Union => {
                    let union = LogicalPlan::Union(Union::try_new_with_loose_types(vec![
                        Arc::new(left),
                        Arc::new(right),
                    ])?);
                    if *all {
                        Ok(union)
                    } else {
                        Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(union))))
                    }
                }
            }
        }
    }
}

pub(super) fn build_from_item(
    query: &TypedQuery,
    item: &FromItem,
    ctx: &CompileContext,
    scan_filter: Option<&QueryExpr>,
) -> Result<LogicalPlan, PgFrontendError> {
    match item {
        FromItem::Empty => empty_plan(query, ctx),
        FromItem::Relation { rtindex } => build_relation_scan(query, *rtindex, ctx, scan_filter),
        FromItem::Values { rtindex } => build_values_scan(query, *rtindex, ctx),
        FromItem::Cte { rtindex } => build_cte_scan(query, *rtindex, ctx),
        FromItem::Subquery { rtindex } => build_subquery_scan(*rtindex, ctx),
        FromItem::Join {
            kind,
            left,
            right,
            quals,
        } => build_join(query, *kind, left, right, quals.as_ref(), ctx),
    }
}

pub(super) fn build_subquery_scan(
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let subquery = ctx.subquery(rtindex)?;
    let input = compile_typed_query(&subquery.query, ctx.config)?.logical_plan;
    if input.schema().fields().len() != subquery.columns.len() {
        return Err(PgFrontendError::unsupported(format!(
            "subquery range table has {} column(s) but compiled subquery has {} field(s)",
            subquery.columns.len(),
            input.schema().fields().len()
        )));
    }
    let projection = subquery
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (qualifier, field) = input.schema().qualified_field(index);
            Expr::Column(Column::from((qualifier, field))).alias(column.name.clone())
        })
        .collect::<Vec<_>>();
    let projected = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(projected),
        table_reference_for_subquery(subquery),
    )?))
}

pub(super) fn build_cte_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let cte_ref = ctx.cte(rtindex)?;
    let cte_def = query
        .ctes
        .iter()
        .find(|cte| cte.id == cte_ref.cte_id)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "CTE range table {} has no matching CTE definition",
                cte_ref.name
            ))
        })?;
    let input = compile_typed_query(&cte_def.query, ctx.config)?.logical_plan;
    if input.schema().fields().len() != cte_ref.columns.len() {
        return Err(PgFrontendError::unsupported(format!(
            "CTE {} has {} column(s) but compiled CTE query has {} field(s)",
            cte_ref.name,
            cte_ref.columns.len(),
            input.schema().fields().len()
        )));
    }
    let projection = cte_ref
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (qualifier, field) = input.schema().qualified_field(index);
            Expr::Column(Column::from((qualifier, field))).alias(column.name.clone())
        })
        .collect::<Vec<_>>();
    let input = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
    let fields = cte_ref
        .columns
        .iter()
        .map(|column| {
            let data_type = arrow_type_for_pg_type(column.pg_type).ok_or_else(|| {
                PgFrontendError::unsupported(format!(
                    "CTE column {} has unsupported PostgreSQL type oid {}",
                    column.name, column.pg_type.oid
                ))
            })?;
            Ok(pg_output_field(
                &column.name,
                data_type,
                column.nullable,
                column.pg_type,
            ))
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let schema = Schema::new(fields);
    let schema = Arc::new(DFSchema::try_from_qualified_schema(
        table_reference_for_cte(cte_ref),
        &schema,
    )?);
    Ok(scan_node::PgCteRefNode::new(
        cte_ref.cte_id,
        cte_ref.name.clone(),
        input,
        schema,
        None,
        None,
    )
    .into_logical_plan())
}

pub(super) fn build_values_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let values = ctx.values(rtindex)?;
    let fields = values
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let data_type =
                values_column_data_type(values, index, column.pg_type).ok_or_else(|| {
                    PgFrontendError::unsupported(format!(
                        "VALUES column {} has unsupported PostgreSQL type oid {}",
                        column.name, column.pg_type.oid
                    ))
                })?;
            Ok(pg_output_field(
                column.name.clone(),
                data_type,
                column.nullable,
                column.pg_type,
            ))
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let table_ref = table_reference_for_values(values);
    let data_types = fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let schema = Schema::new(fields);
    let schema = Arc::new(DFSchema::try_from(schema)?);
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, expr)| {
                    let expr = compile_expr(expr, query, ctx)?;
                    Ok::<_, PgFrontendError>(Expr::Cast(Cast::new(
                        Box::new(expr),
                        data_types[index].clone(),
                    )))
                })
                .collect::<Result<Vec<_>, PgFrontendError>>()
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let values_plan = LogicalPlan::Values(Values {
        schema,
        values: rows,
    });
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(values_plan),
        table_ref,
    )?))
}

pub(super) fn values_column_data_type(
    values: &ValuesRef,
    index: usize,
    pg_type: pg_type::PgTypeRef,
) -> Option<DataType> {
    if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) && pg_type.typmod < 0 {
        if let Some(scale) = values_numeric_column_scale(values, index) {
            return Some(DataType::Decimal128(38, scale));
        }
    }
    arrow_type_for_pg_type(pg_type)
}

pub(super) fn values_numeric_column_scale(values: &ValuesRef, index: usize) -> Option<i8> {
    values
        .rows
        .iter()
        .map(|row| numeric_expr_scale(row.get(index)?))
        .try_fold(0_i8, |max_scale, scale| {
            scale.map(|scale| max_scale.max(scale))
        })
}

pub(super) fn numeric_expr_scale(expr: &QueryExpr) -> Option<i8> {
    match expr {
        QueryExpr::Const(constant) => numeric_const_scale(constant),
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            numeric_expr_scale(inner)
        }
        QueryExpr::UnaryOp { arg, .. } => numeric_expr_scale(arg),
        QueryExpr::BinaryOp {
            op: QueryOperator::Plus | QueryOperator::Minus | QueryOperator::Multiply,
            left,
            right,
            ..
        } => Some(numeric_expr_scale(left)?.max(numeric_expr_scale(right)?)),
        QueryExpr::BinaryOp {
            op:
                QueryOperator::Divide
                | QueryOperator::Modulo
                | QueryOperator::Eq
                | QueryOperator::NotEq
                | QueryOperator::IsDistinctFrom
                | QueryOperator::IsNotDistinctFrom
                | QueryOperator::Lt
                | QueryOperator::LtEq
                | QueryOperator::Gt
                | QueryOperator::GtEq
                | QueryOperator::BitwiseShiftLeft
                | QueryOperator::BitwiseShiftRight
                | QueryOperator::StringConcat
                | QueryOperator::RegexMatch
                | QueryOperator::RegexNotMatch,
            ..
        } => None,
        _ => None,
    }
}

pub(super) fn numeric_const_scale(constant: &Const) -> Option<i8> {
    match constant.value.as_ref()? {
        pg_type::PgConstValue::Int16(_)
        | pg_type::PgConstValue::Int32(_)
        | pg_type::PgConstValue::Int64(_) => Some(0),
        pg_type::PgConstValue::Numeric(value) => value
            .split_once('.')
            .map(|(_, fraction)| i8::try_from(fraction.trim_end_matches('0').len()).ok())
            .unwrap_or(Some(0)),
        _ => None,
    }
}

pub(super) fn empty_plan(
    _query: &TypedQuery,
    _ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(DFSchema::empty()),
    }))
}

pub(super) fn build_relation_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
    scan_filter: Option<&QueryExpr>,
) -> Result<LogicalPlan, PgFrontendError> {
    let relation = relation_by_rtindex(query, rtindex)?;
    let resolved = ctx.table(rtindex)?;
    let table_ref = table_reference_for_query_relation(relation, resolved);
    let filter = scan_filter
        .map(|expr| compile_expr(expr, query, ctx))
        .transpose()?;
    let filters = filter.into_iter().collect::<Vec<_>>();
    let scan_projection = if query.has_aggregates {
        None
    } else {
        let include_resjunk = !query.sort.is_empty()
            || query.has_windows
            || matches!(query.distinct, DistinctSpec::On { .. });
        Some(relation_projection(
            query,
            rtindex,
            resolved,
            include_resjunk,
        )?)
    };
    let source = Arc::new(PgPlanningTableSource::new(resolved.clone())) as Arc<dyn TableSource>;
    let table_scan = TableScan::try_new(table_ref, source, scan_projection, filters, None)?;
    Ok(LogicalPlan::TableScan(table_scan))
}

pub(super) fn build_join(
    query: &TypedQuery,
    kind: JoinKind,
    left: &FromItem,
    right: &FromItem,
    quals: Option<&QueryExpr>,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let left_plan = build_from_item(query, left, ctx, None)?;
    let right_plan = build_from_item(query, right, ctx, None)?;
    let left_relations = from_rtindexes(left);
    let right_relations = from_rtindexes(right);
    let mut on = Vec::new();
    let mut filters = Vec::new();
    if let Some(quals) = quals {
        collect_join_quals(
            quals,
            &left_relations,
            &right_relations,
            query,
            ctx,
            &mut on,
            &mut filters,
        )?;
    }
    let filter = filters
        .into_iter()
        .reduce(|left, right| binary_expr(left, Operator::And, right));
    if kind == JoinKind::Right {
        let projection = schema_projection_exprs(left_plan.schema())
            .into_iter()
            .chain(schema_projection_exprs(right_plan.schema()))
            .collect::<Vec<_>>();
        let join = Join::try_new(
            Arc::new(right_plan),
            Arc::new(left_plan),
            on.into_iter().map(|(left, right)| (right, left)).collect(),
            filter,
            JoinType::Left,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join = LogicalPlan::Join(join);
        return Ok(LogicalPlan::Projection(Projection::try_new(
            projection,
            Arc::new(join),
        )?));
    }
    let join = Join::try_new(
        Arc::new(left_plan),
        Arc::new(right_plan),
        on,
        filter,
        join_type(kind),
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;
    Ok(LogicalPlan::Join(join))
}

pub(super) fn schema_projection_exprs(schema: &DFSchema) -> Vec<Expr> {
    (0..schema.fields().len())
        .map(|index| {
            let (qualifier, field) = schema.qualified_field(index);
            Expr::Column(Column::from((qualifier, field)))
        })
        .collect()
}
