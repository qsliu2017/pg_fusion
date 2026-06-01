use super::*;

pub(super) unsafe fn read_pg_catalog_function_name(
    funcid: pg_sys::Oid,
) -> Result<String, PgFrontendError> {
    if u32::from(unsafe { pg_sys::get_func_namespace(funcid) }) != pg_sys::PG_CATALOG_NAMESPACE {
        return Err(PgFrontendError::unsupported(format!(
            "function oid {} is not in pg_catalog",
            u32::from(funcid)
        )));
    }
    unsafe { cstr_from_pg(pg_sys::get_func_name(funcid)) }
}

pub(super) fn read_scalar_function_name(name: &str) -> Result<ScalarFunction, PgFrontendError> {
    match name {
        "abs" => Ok(ScalarFunction::Abs),
        "acosh" => Ok(ScalarFunction::Acosh),
        "asinh" => Ok(ScalarFunction::Asinh),
        "atanh" => Ok(ScalarFunction::Atanh),
        "ceil" | "ceiling" => Ok(ScalarFunction::Ceil),
        "concat" => Ok(ScalarFunction::Concat),
        "concat_ws" => Ok(ScalarFunction::ConcatWs),
        "cosh" => Ok(ScalarFunction::Cosh),
        "exp" => Ok(ScalarFunction::Exp),
        "floor" => Ok(ScalarFunction::Floor),
        "format" => Ok(ScalarFunction::Format),
        "length" => Ok(ScalarFunction::Length),
        "ln" => Ok(ScalarFunction::Ln),
        "power" => Ok(ScalarFunction::Power),
        "quote_literal" => Ok(ScalarFunction::QuoteLiteral),
        "random" => Ok(ScalarFunction::Random),
        "reverse" => Ok(ScalarFunction::Reverse),
        "round" => Ok(ScalarFunction::Round),
        "sinh" => Ok(ScalarFunction::Sinh),
        "sqrt" => Ok(ScalarFunction::Sqrt),
        "tanh" => Ok(ScalarFunction::Tanh),
        "trunc" => Ok(ScalarFunction::Trunc),
        _ => Err(PgFrontendError::unsupported(format!(
            "function {name} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) fn is_cast_function_name(name: &str) -> bool {
    matches!(
        name,
        "bool"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "text"
            | "varchar"
            | "bpchar"
    )
}

pub(super) unsafe fn read_aggregate_function(
    funcid: pg_sys::Oid,
) -> Result<AggregateFunction, PgFrontendError> {
    if u32::from(unsafe { pg_sys::get_func_namespace(funcid) }) != pg_sys::PG_CATALOG_NAMESPACE {
        return Err(PgFrontendError::unsupported(format!(
            "aggregate function oid {} is not in pg_catalog",
            u32::from(funcid)
        )));
    }
    let name = unsafe { cstr_from_pg(pg_sys::get_func_name(funcid)) }?;
    match name.as_str() {
        "count" => Ok(AggregateFunction::Count),
        "sum" => Ok(AggregateFunction::Sum),
        "avg" => Ok(AggregateFunction::Avg),
        "min" => Ok(AggregateFunction::Min),
        "max" => Ok(AggregateFunction::Max),
        "stddev_pop" => Ok(AggregateFunction::StddevPop),
        "stddev_samp" | "stddev" => Ok(AggregateFunction::StddevSamp),
        "var_pop" => Ok(AggregateFunction::VarPop),
        "var_samp" | "variance" => Ok(AggregateFunction::VarSamp),
        "regr_count" => Ok(AggregateFunction::RegrCount),
        "regr_sxx" => Ok(AggregateFunction::RegrSxx),
        "regr_syy" => Ok(AggregateFunction::RegrSyy),
        "regr_sxy" => Ok(AggregateFunction::RegrSxy),
        "regr_avgx" => Ok(AggregateFunction::RegrAvgX),
        "regr_avgy" => Ok(AggregateFunction::RegrAvgY),
        "regr_r2" => Ok(AggregateFunction::RegrR2),
        "regr_slope" => Ok(AggregateFunction::RegrSlope),
        "regr_intercept" => Ok(AggregateFunction::RegrIntercept),
        "covar_pop" => Ok(AggregateFunction::CovarPop),
        "covar_samp" => Ok(AggregateFunction::CovarSamp),
        "corr" => Ok(AggregateFunction::Corr),
        "string_agg" => Ok(AggregateFunction::StringAgg),
        _ => Err(PgFrontendError::unsupported(format!(
            "aggregate function {name} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) unsafe fn read_window_function(
    funcid: pg_sys::Oid,
    winagg: bool,
) -> Result<WindowFunctionKind, PgFrontendError> {
    if winagg {
        return Ok(WindowFunctionKind::Aggregate(unsafe {
            read_aggregate_function(funcid)
        }?));
    }
    if u32::from(unsafe { pg_sys::get_func_namespace(funcid) }) != pg_sys::PG_CATALOG_NAMESPACE {
        return Err(PgFrontendError::unsupported(format!(
            "window function oid {} is not in pg_catalog",
            u32::from(funcid)
        )));
    }
    let name = unsafe { cstr_from_pg(pg_sys::get_func_name(funcid)) }?;
    match name.as_str() {
        "cume_dist" => Ok(WindowFunctionKind::CumeDist),
        "dense_rank" => Ok(WindowFunctionKind::DenseRank),
        "first_value" => Ok(WindowFunctionKind::FirstValue),
        "lag" => Ok(WindowFunctionKind::Lag),
        "last_value" => Ok(WindowFunctionKind::LastValue),
        "lead" => Ok(WindowFunctionKind::Lead),
        "nth_value" => Ok(WindowFunctionKind::NthValue),
        "ntile" => Ok(WindowFunctionKind::Ntile),
        "percent_rank" => Ok(WindowFunctionKind::PercentRank),
        "rank" => Ok(WindowFunctionKind::Rank),
        "row_number" => Ok(WindowFunctionKind::RowNumber),
        _ => Err(PgFrontendError::unsupported(format!(
            "window function {name} is not supported by pg_frontend v1"
        ))),
    }
}
