use super::*;

pub(super) fn join_kind(kind: pg_sys::JoinType::Type) -> Result<JoinKind, PgFrontendError> {
    match kind {
        pg_sys::JoinType::JOIN_INNER => Ok(JoinKind::Inner),
        pg_sys::JoinType::JOIN_LEFT => Ok(JoinKind::Left),
        pg_sys::JoinType::JOIN_RIGHT => Ok(JoinKind::Right),
        pg_sys::JoinType::JOIN_FULL => Ok(JoinKind::Full),
        _ => Err(PgFrontendError::unsupported(format!(
            "join type {kind} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) fn boolean_test_kind(
    kind: pg_sys::BoolTestType::Type,
) -> Result<BooleanTestKind, PgFrontendError> {
    match kind {
        pg_sys::BoolTestType::IS_TRUE => Ok(BooleanTestKind::IsTrue),
        pg_sys::BoolTestType::IS_NOT_TRUE => Ok(BooleanTestKind::IsNotTrue),
        pg_sys::BoolTestType::IS_FALSE => Ok(BooleanTestKind::IsFalse),
        pg_sys::BoolTestType::IS_NOT_FALSE => Ok(BooleanTestKind::IsNotFalse),
        pg_sys::BoolTestType::IS_UNKNOWN => Ok(BooleanTestKind::IsUnknown),
        pg_sys::BoolTestType::IS_NOT_UNKNOWN => Ok(BooleanTestKind::IsNotUnknown),
        _ => Err(PgFrontendError::unsupported(format!(
            "boolean test type {kind} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) unsafe fn read_scalar_array_op_expr(
    expr: &pg_sys::ScalarArrayOpExpr,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if unsafe { list_len(expr.args) } != 2 {
        return Err(PgFrontendError::unsupported(
            "ScalarArrayOpExpr must have exactly two arguments",
        ));
    }
    let left = unsafe { read_expr(list_ptr_at(expr.args, 0).cast(), scope) }?;
    let array = unsafe { list_ptr_at(expr.args, 1) as *mut pg_sys::Node };
    if array.is_null() || unsafe { (*array).type_ } != pg_sys::NodeTag::T_ArrayExpr {
        return Err(PgFrontendError::unsupported(
            "ScalarArrayOpExpr only supports constant ARRAY expressions in pg_frontend v1",
        ));
    }
    let array = unsafe { &*array.cast::<pg_sys::ArrayExpr>() };
    if array.multidims {
        return Err(PgFrontendError::unsupported(
            "multidimensional ARRAY expressions are not supported by pg_frontend v1",
        ));
    }
    let op = read_operator(expr.opno)?;
    let mut args = Vec::new();
    for index in 0..unsafe { list_len(array.elements) } {
        let right = unsafe { read_expr(list_ptr_at(array.elements, index).cast(), scope) }?;
        args.push(QueryExpr::BinaryOp {
            op,
            left: Box::new(left.clone()),
            right: Box::new(right),
            pg_type: type_ref(pg_sys::BOOLOID, -1, 0.into()),
        });
    }
    if args.is_empty() {
        return Ok(QueryExpr::Const(Const {
            pg_type: type_ref(pg_sys::BOOLOID, -1, 0.into()),
            value: Some(PgConstValue::Bool(!expr.useOr)),
        }));
    }
    Ok(QueryExpr::Bool {
        op: if expr.useOr { BoolOp::Or } else { BoolOp::And },
        args,
    })
}

pub(super) unsafe fn read_array_expr(
    expr: &pg_sys::ArrayExpr,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if expr.multidims {
        return Err(PgFrontendError::unsupported(
            "multidimensional ARRAY expressions are not supported by pg_frontend v1",
        ));
    }
    let mut elements = Vec::new();
    for index in 0..unsafe { list_len(expr.elements) } {
        elements.push(unsafe { read_expr(list_ptr_at(expr.elements, index).cast(), scope) }?);
    }
    Ok(QueryExpr::Array {
        elements,
        pg_type: type_ref(expr.array_typeid, -1, expr.array_collid),
    })
}

pub(super) unsafe fn read_subscripting_ref(
    expr: &pg_sys::SubscriptingRef,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if !expr.refassgnexpr.is_null() {
        return Err(PgFrontendError::unsupported(
            "array assignment subscripting is not supported by pg_frontend v1",
        ));
    }
    if !expr.reflowerindexpr.is_null() || unsafe { list_len(expr.refupperindexpr) } != 1 {
        return Err(PgFrontendError::unsupported(
            "only single array element subscripting is supported by pg_frontend v1",
        ));
    }
    let array = unsafe { read_expr(expr.refexpr.cast(), scope) }?;
    let index = unsafe { read_expr(list_ptr_at(expr.refupperindexpr, 0).cast(), scope) }?;
    let pg_type = type_ref(expr.refrestype, expr.reftypmod, expr.refcollid);
    supported_value_type(pg_type).map_err(|reason| PgFrontendError::unsupported(reason.message))?;
    Ok(QueryExpr::ArraySubscript {
        array: Box::new(array),
        index: Box::new(index),
        pg_type,
    })
}

pub(super) unsafe fn read_null_if_expr(
    node: *mut pg_sys::Node,
    expr: &pg_sys::NullIfExpr,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if unsafe { list_len(expr.args) } != 2 {
        return Err(PgFrontendError::unsupported(
            "NULLIF expression must have exactly two arguments",
        ));
    }
    read_operator(expr.opno)?;
    let args = vec![
        unsafe { read_expr(list_ptr_at(expr.args, 0).cast(), scope) }?,
        unsafe { read_expr(list_ptr_at(expr.args, 1).cast(), scope) }?,
    ];
    let pg_type = unsafe { expr_type_ref(node) };
    supported_value_type(pg_type).map_err(|reason| PgFrontendError::unsupported(reason.message))?;
    Ok(QueryExpr::FunctionCall {
        func: ScalarFunction::NullIf,
        args,
        pg_type,
    })
}

pub(super) unsafe fn read_sublink(
    sublink: &pg_sys::SubLink,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    let subquery = unsafe { read_sublink_query(sublink, scope) }?;
    let bool_type = type_ref(pg_sys::BOOLOID, -1, 0.into());
    match sublink.subLinkType {
        pg_sys::SubLinkType::EXPR_SUBLINK => {
            if !sublink.testexpr.is_null() {
                return Err(PgFrontendError::unsupported(
                    "scalar subquery test expressions are not supported by pg_frontend v1",
                ));
            }
            Ok(QueryExpr::ScalarSubquery(subquery))
        }
        pg_sys::SubLinkType::EXISTS_SUBLINK => Ok(QueryExpr::ExistsSubquery {
            subquery,
            pg_type: bool_type,
        }),
        pg_sys::SubLinkType::ANY_SUBLINK => {
            let expr = unsafe { read_in_sublink_testexpr(sublink.testexpr, scope) }?;
            Ok(QueryExpr::InSubquery {
                expr: Box::new(expr),
                subquery,
                pg_type: bool_type,
            })
        }
        other => Err(PgFrontendError::unsupported(format!(
            "subquery type {other} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) unsafe fn read_sublink_query(
    sublink: &pg_sys::SubLink,
    scope: &CteScope,
) -> Result<Box<TypedQuery>, PgFrontendError> {
    if sublink.subselect.is_null()
        || unsafe { (*sublink.subselect).type_ } != pg_sys::NodeTag::T_Query
    {
        return Err(PgFrontendError::unsupported(
            "subquery is not a SELECT query tree",
        ));
    }
    Ok(Box::new(unsafe {
        read_query_with_scope(sublink.subselect.cast(), &scope.for_child_query())
    }?))
}

pub(super) unsafe fn read_in_sublink_testexpr(
    testexpr: *mut pg_sys::Node,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if testexpr.is_null() {
        return Err(PgFrontendError::unsupported(
            "IN subquery has no test expression",
        ));
    }
    let testexpr = unsafe { read_expr(testexpr, scope) }?;
    let QueryExpr::BinaryOp {
        op: QueryOperator::Eq,
        left,
        right,
        ..
    } = testexpr
    else {
        return Err(PgFrontendError::unsupported(
            "only equality IN subqueries are supported by pg_frontend v1",
        ));
    };
    match (
        expr_is_sublink_param(left.as_ref()),
        expr_is_sublink_param(right.as_ref()),
    ) {
        (false, true) => Ok(*left),
        (true, false) => Ok(*right),
        _ => Err(PgFrontendError::unsupported(
            "IN subquery test expression must compare one outer expression with one sublink parameter",
        )),
    }
}

pub(super) fn expr_is_sublink_param(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::Param(param) => param.kind == crate::typed_query::ParamKind::Sublink,
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            expr_is_sublink_param(inner)
        }
        _ => false,
    }
}

pub(super) unsafe fn read_row_compare_expr(
    expr: &pg_sys::RowCompareExpr,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    let len = unsafe { list_len(expr.largs) };
    if len == 0 || len != unsafe { list_len(expr.rargs) } {
        return Err(PgFrontendError::unsupported(
            "row comparison must have equally sized non-empty sides",
        ));
    }
    let mut left = Vec::new();
    let mut right = Vec::new();
    for index in 0..len {
        left.push(unsafe { read_expr(list_ptr_at(expr.largs, index).cast(), scope) }?);
        right.push(unsafe { read_expr(list_ptr_at(expr.rargs, index).cast(), scope) }?);
    }
    Ok(row_compare_expr(expr.rctype, left, right))
}

pub(super) fn row_compare_expr(
    rctype: pg_sys::RowCompareType::Type,
    left: Vec<QueryExpr>,
    right: Vec<QueryExpr>,
) -> QueryExpr {
    match rctype {
        pg_sys::RowCompareType::ROWCOMPARE_EQ => {
            row_pairwise_bool(left, right, QueryOperator::Eq, BoolOp::And)
        }
        pg_sys::RowCompareType::ROWCOMPARE_NE => {
            row_pairwise_bool(left, right, QueryOperator::NotEq, BoolOp::Or)
        }
        pg_sys::RowCompareType::ROWCOMPARE_LT => {
            row_lexicographic_expr(left, right, QueryOperator::Lt, false)
        }
        pg_sys::RowCompareType::ROWCOMPARE_LE => {
            row_lexicographic_expr(left, right, QueryOperator::Lt, true)
        }
        pg_sys::RowCompareType::ROWCOMPARE_GT => {
            row_lexicographic_expr(left, right, QueryOperator::Gt, false)
        }
        pg_sys::RowCompareType::ROWCOMPARE_GE => {
            row_lexicographic_expr(left, right, QueryOperator::Gt, true)
        }
        _ => QueryExpr::Const(Const {
            pg_type: type_ref(pg_sys::BOOLOID, -1, 0.into()),
            value: Some(PgConstValue::Bool(false)),
        }),
    }
}

pub(super) fn row_pairwise_bool(
    left: Vec<QueryExpr>,
    right: Vec<QueryExpr>,
    op: QueryOperator,
    bool_op: BoolOp,
) -> QueryExpr {
    let args = left
        .into_iter()
        .zip(right)
        .map(|(left, right)| bool_binary_op(left, op, right))
        .collect();
    QueryExpr::Bool { op: bool_op, args }
}

pub(super) fn row_lexicographic_expr(
    left: Vec<QueryExpr>,
    right: Vec<QueryExpr>,
    cmp: QueryOperator,
    include_equal: bool,
) -> QueryExpr {
    let mut disjuncts = Vec::new();
    let mut equal_prefix = Vec::new();
    for (left_expr, right_expr) in left.into_iter().zip(right) {
        let cmp_expr = bool_binary_op(left_expr.clone(), cmp, right_expr.clone());
        let branch = if equal_prefix.is_empty() {
            cmp_expr
        } else {
            let mut args = equal_prefix.clone();
            args.push(cmp_expr);
            QueryExpr::Bool {
                op: BoolOp::And,
                args,
            }
        };
        disjuncts.push(branch);
        equal_prefix.push(bool_binary_op(left_expr, QueryOperator::Eq, right_expr));
    }
    if include_equal {
        disjuncts.push(QueryExpr::Bool {
            op: BoolOp::And,
            args: equal_prefix,
        });
    }
    QueryExpr::Bool {
        op: BoolOp::Or,
        args: disjuncts,
    }
}

pub(super) fn bool_binary_op(left: QueryExpr, op: QueryOperator, right: QueryExpr) -> QueryExpr {
    QueryExpr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
        pg_type: type_ref(pg_sys::BOOLOID, -1, 0.into()),
    }
}

pub(super) unsafe fn read_expr(
    node: *mut pg_sys::Node,
    scope: &CteScope,
) -> Result<QueryExpr, PgFrontendError> {
    if node.is_null() {
        return Err(PgFrontendError::unsupported("null expression node"));
    }

    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            let var = unsafe { &*node.cast::<pg_sys::Var>() };
            if var.varlevelsup != 0 {
                if var.varattno <= 0 {
                    return Err(PgFrontendError::unsupported(
                        "whole-row and system-column outer-reference Vars are not supported",
                    ));
                }
                return Ok(QueryExpr::OuterVar(unsafe { scope.outer_var(var) }?));
            }
            if var.varattno <= 0 {
                return Err(PgFrontendError::unsupported(
                    "whole-row and system-column Vars are not supported",
                ));
            }
            if let Some(expr) = scope.join_aliases.get(&(var.varno as usize, var.varattno)) {
                return Ok(expr.clone());
            }
            Ok(QueryExpr::Var(Var {
                rtindex: var.varno as usize,
                attnum: var.varattno,
                pg_type: type_ref(var.vartype, var.vartypmod, var.varcollid),
            }))
        }
        pg_sys::NodeTag::T_Const => {
            let constant = unsafe { &*node.cast::<pg_sys::Const>() };
            Ok(QueryExpr::Const(unsafe { read_const(constant) }?))
        }
        pg_sys::NodeTag::T_Param => {
            let param = unsafe { &*node.cast::<pg_sys::Param>() };
            Ok(QueryExpr::Param(Param {
                kind: param_kind(param.paramkind),
                id: param.paramid,
                pg_type: type_ref(param.paramtype, param.paramtypmod, param.paramcollid),
            }))
        }
        pg_sys::NodeTag::T_SubLink => {
            let sublink = unsafe { &*node.cast::<pg_sys::SubLink>() };
            unsafe { read_sublink(sublink, scope) }
        }
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = unsafe { &*node.cast::<pg_sys::RelabelType>() };
            Ok(QueryExpr::RelabelType(Box::new(unsafe {
                read_expr(relabel.arg.cast(), scope)
            }?)))
        }
        pg_sys::NodeTag::T_CoerceViaIO => {
            let cast = unsafe { &*node.cast::<pg_sys::CoerceViaIO>() };
            let pg_type = unsafe { expr_type_ref(node) };
            supported_value_type(pg_type)
                .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
            Ok(QueryExpr::Cast {
                arg: Box::new(unsafe { read_expr(cast.arg.cast(), scope) }?),
                pg_type,
            })
        }
        pg_sys::NodeTag::T_FuncExpr => {
            let func = unsafe { &*node.cast::<pg_sys::FuncExpr>() };
            if func.funcretset {
                return Err(PgFrontendError::unsupported(
                    "set-returning functions are not supported by pg_frontend v1",
                ));
            }
            let mut args = Vec::new();
            for index in 0..unsafe { list_len(func.args) } {
                args.push(unsafe { read_expr(list_ptr_at(func.args, index).cast(), scope) }?);
            }
            let pg_type = unsafe { expr_type_ref(node) };
            supported_value_type(pg_type)
                .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
            let func_name = unsafe { read_pg_catalog_function_name(func.funcid) }?;
            if is_cast_function_name(&func_name) && !args.is_empty() {
                return Ok(QueryExpr::Cast {
                    arg: Box::new(args.remove(0)),
                    pg_type,
                });
            }
            Ok(QueryExpr::FunctionCall {
                func: read_scalar_function(func.funcid, &func_name, &args, pg_type)?,
                args,
                pg_type,
            })
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = unsafe { &*node.cast::<pg_sys::BoolExpr>() };
            let mut args = Vec::new();
            for index in 0..unsafe { list_len(bool_expr.args) } {
                args.push(unsafe { read_expr(list_ptr_at(bool_expr.args, index).cast(), scope) }?);
            }
            Ok(QueryExpr::Bool {
                op: bool_op(bool_expr.boolop)?,
                args,
            })
        }
        pg_sys::NodeTag::T_RowCompareExpr => {
            let row_compare = unsafe { &*node.cast::<pg_sys::RowCompareExpr>() };
            unsafe { read_row_compare_expr(row_compare, scope) }
        }
        pg_sys::NodeTag::T_BooleanTest => {
            let test = unsafe { &*node.cast::<pg_sys::BooleanTest>() };
            Ok(QueryExpr::BooleanTest {
                arg: Box::new(unsafe { read_expr(test.arg.cast(), scope) }?),
                kind: boolean_test_kind(test.booltesttype)?,
            })
        }
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = unsafe { &*node.cast::<pg_sys::OpExpr>() };
            match unsafe { list_len(op_expr.args) } {
                1 => {
                    let arg = unsafe { read_expr(list_ptr_at(op_expr.args, 0).cast(), scope) }?;
                    Ok(QueryExpr::UnaryOp {
                        op: read_unary_operator(op_expr.opno)?,
                        arg: Box::new(arg),
                        pg_type: type_ref(op_expr.opresulttype, -1, op_expr.opcollid),
                    })
                }
                2 => {
                    let left = unsafe { read_expr(list_ptr_at(op_expr.args, 0).cast(), scope) }?;
                    let right = unsafe { read_expr(list_ptr_at(op_expr.args, 1).cast(), scope) }?;
                    Ok(QueryExpr::BinaryOp {
                        op: read_operator(op_expr.opno)?,
                        left: Box::new(left),
                        right: Box::new(right),
                        pg_type: type_ref(op_expr.opresulttype, -1, op_expr.opcollid),
                    })
                }
                _ => Err(PgFrontendError::unsupported(
                    "only unary and binary operator expressions are supported",
                )),
            }
        }
        pg_sys::NodeTag::T_DistinctExpr => {
            let op_expr = unsafe { &*node.cast::<pg_sys::DistinctExpr>() };
            if unsafe { list_len(op_expr.args) } != 2 {
                return Err(PgFrontendError::unsupported(
                    "only binary DISTINCT expressions are supported",
                ));
            }
            let left = unsafe { read_expr(list_ptr_at(op_expr.args, 0).cast(), scope) }?;
            let right = unsafe { read_expr(list_ptr_at(op_expr.args, 1).cast(), scope) }?;
            Ok(QueryExpr::BinaryOp {
                op: QueryOperator::IsDistinctFrom,
                left: Box::new(left),
                right: Box::new(right),
                pg_type: type_ref(op_expr.opresulttype, -1, op_expr.opcollid),
            })
        }
        pg_sys::NodeTag::T_NullIfExpr => {
            let expr = unsafe { &*node.cast::<pg_sys::NullIfExpr>() };
            unsafe { read_null_if_expr(node, expr, scope) }
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => {
            let expr = unsafe { &*node.cast::<pg_sys::ScalarArrayOpExpr>() };
            unsafe { read_scalar_array_op_expr(expr, scope) }
        }
        pg_sys::NodeTag::T_ArrayExpr => {
            let expr = unsafe { &*node.cast::<pg_sys::ArrayExpr>() };
            unsafe { read_array_expr(expr, scope) }
        }
        pg_sys::NodeTag::T_SubscriptingRef => {
            let expr = unsafe { &*node.cast::<pg_sys::SubscriptingRef>() };
            unsafe { read_subscripting_ref(expr, scope) }
        }
        pg_sys::NodeTag::T_GroupingFunc => {
            let grouping = unsafe { &*node.cast::<pg_sys::GroupingFunc>() };
            if grouping.agglevelsup != 0 {
                return Err(PgFrontendError::unsupported(
                    "outer-reference GROUPING() calls are not supported",
                ));
            }
            let mut args = Vec::new();
            for index in 0..unsafe { list_len(grouping.args) } {
                args.push(unsafe { read_expr(list_ptr_at(grouping.args, index).cast(), scope) }?);
            }
            Ok(QueryExpr::AggregateCall {
                func: AggregateFunction::Grouping,
                args,
                distinct: false,
                filter: None,
                pg_type: type_ref(pg_sys::INT4OID, -1, pg_sys::InvalidOid),
            })
        }
        pg_sys::NodeTag::T_Aggref => {
            let agg = unsafe { &*node.cast::<pg_sys::Aggref>() };
            if agg.agglevelsup != 0 {
                return Err(PgFrontendError::unsupported(
                    "outer-reference aggregate calls are not supported",
                ));
            }
            if !agg.aggdirectargs.is_null() {
                return Err(PgFrontendError::unsupported(
                    "ordered-set aggregate direct args are not supported",
                ));
            }
            let func = unsafe { read_aggregate_function(agg.aggfnoid) }?;
            if !agg.aggorder.is_null()
                && !matches!(func, AggregateFunction::Min | AggregateFunction::Max)
            {
                return Err(PgFrontendError::unsupported(
                    "aggregate ORDER BY is not supported by pg_frontend v1",
                ));
            }
            let mut args = Vec::new();
            if !agg.aggstar {
                for index in 0..unsafe { list_len(agg.args) } {
                    let entry = unsafe { list_ptr_at(agg.args, index) as *mut pg_sys::TargetEntry };
                    if entry.is_null() {
                        return Err(PgFrontendError::unsupported("null aggregate argument"));
                    }
                    let entry_ref = unsafe { &*entry };
                    if entry_ref.resjunk {
                        continue;
                    }
                    args.push(unsafe { read_expr(entry_ref.expr.cast(), scope) }?);
                }
            }
            let filter = if agg.aggfilter.is_null() {
                None
            } else {
                Some(Box::new(unsafe { read_expr(agg.aggfilter.cast(), scope) }?))
            };
            Ok(QueryExpr::AggregateCall {
                func,
                args,
                distinct: !agg.aggdistinct.is_null(),
                filter,
                pg_type: type_ref(agg.aggtype, -1, agg.aggcollid),
            })
        }
        pg_sys::NodeTag::T_WindowFunc => {
            let window = unsafe { &*node.cast::<pg_sys::WindowFunc>() };
            if !window.runCondition.is_null() {
                return Err(PgFrontendError::unsupported(
                    "window run conditions are not supported by pg_frontend v1",
                ));
            }
            let mut args = Vec::new();
            if !window.winstar {
                for index in 0..unsafe { list_len(window.args) } {
                    args.push(unsafe { read_expr(list_ptr_at(window.args, index).cast(), scope) }?);
                }
            }
            let filter = if window.aggfilter.is_null() {
                None
            } else {
                Some(Box::new(unsafe {
                    read_expr(window.aggfilter.cast(), scope)
                }?))
            };
            let pg_type = type_ref(window.wintype, -1, window.wincollid);
            supported_value_type(pg_type)
                .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
            Ok(QueryExpr::WindowCall {
                func: unsafe { read_window_function(window.winfnoid, window.winagg) }?,
                args,
                winref: window.winref,
                filter,
                distinct: false,
                pg_type,
            })
        }
        pg_sys::NodeTag::T_CoalesceExpr => {
            let coalesce = unsafe { &*node.cast::<pg_sys::CoalesceExpr>() };
            let mut args = Vec::new();
            for index in 0..unsafe { list_len(coalesce.args) } {
                args.push(unsafe { read_expr(list_ptr_at(coalesce.args, index).cast(), scope) }?);
            }
            let pg_type = type_ref(coalesce.coalescetype, -1, coalesce.coalescecollid);
            supported_value_type(pg_type)
                .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
            Ok(QueryExpr::Coalesce { args, pg_type })
        }
        pg_sys::NodeTag::T_CaseExpr => {
            let case_expr = unsafe { &*node.cast::<pg_sys::CaseExpr>() };
            let case_scope = if case_expr.arg.is_null() {
                scope.clone()
            } else {
                scope.with_case_operand(unsafe { read_expr(case_expr.arg.cast(), scope) }?)
            };
            let mut when_then = Vec::new();
            for index in 0..unsafe { list_len(case_expr.args) } {
                let when = unsafe { list_ptr_at(case_expr.args, index) as *mut pg_sys::CaseWhen };
                if when.is_null() {
                    return Err(PgFrontendError::unsupported("null CASE WHEN entry"));
                }
                let when = unsafe { &*when };
                when_then.push((
                    unsafe { read_expr(when.expr.cast(), &case_scope) }?,
                    unsafe { read_expr(when.result.cast(), scope) }?,
                ));
            }
            let else_expr = if case_expr.defresult.is_null() {
                None
            } else {
                Some(Box::new(unsafe {
                    read_expr(case_expr.defresult.cast(), scope)
                }?))
            };
            let pg_type = type_ref(case_expr.casetype, -1, case_expr.casecollid);
            supported_value_type(pg_type)
                .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
            Ok(QueryExpr::Case {
                operand: None,
                when_then,
                else_expr,
                pg_type,
            })
        }
        pg_sys::NodeTag::T_NullTest => {
            let null_test = unsafe { &*node.cast::<pg_sys::NullTest>() };
            if null_test.argisrow {
                return Err(PgFrontendError::unsupported(
                    "row-valued NULL tests are not supported",
                ));
            }
            Ok(QueryExpr::NullTest {
                arg: Box::new(unsafe { read_expr(null_test.arg.cast(), scope) }?),
                is_null: null_test.nulltesttype == pg_sys::NullTestType::IS_NULL,
            })
        }
        pg_sys::NodeTag::T_CaseTestExpr => scope.case_operand.clone().ok_or_else(|| {
            PgFrontendError::unsupported("CASE test expression has no active CASE operand")
        }),
        tag => Err(PgFrontendError::unsupported(format!(
            "expression node {:?} is not supported by pg_frontend v1",
            tag
        ))),
    }
}
