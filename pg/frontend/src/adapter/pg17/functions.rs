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

pub(super) fn read_scalar_function(
    funcid: pg_sys::Oid,
    name: &str,
    args: &[QueryExpr],
    result_pg_type: PgTypeRef,
) -> Result<ScalarFunction, PgFrontendError> {
    let arg_oids = args
        .iter()
        .map(expr_pg_type)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "function {name} oid {} has an argument with unknown PostgreSQL type",
                u32::from(funcid)
            ))
        })?;
    let signature = ScalarFunctionSignature {
        name,
        args: &arg_oids,
        result: result_pg_type.oid,
    };
    classify_scalar_function_signature(&signature).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "function {name} oid {} with argument type OIDs {:?} and result type OID {} is not supported by pg_frontend v1",
            u32::from(funcid),
            arg_oids,
            result_pg_type.oid
        ))
    })
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

#[derive(Debug)]
struct ScalarFunctionSignature<'a> {
    name: &'a str,
    args: &'a [u32],
    result: u32,
}

fn classify_scalar_function_signature(
    signature: &ScalarFunctionSignature<'_>,
) -> Option<ScalarFunction> {
    let args = signature.args;
    let result = signature.result;
    match signature.name {
        "abs" if unary_numeric(args, result) => Some(ScalarFunction::Abs),
        "acosh" if unary_float(args, result) => Some(ScalarFunction::Acosh),
        "asinh" if unary_float(args, result) => Some(ScalarFunction::Asinh),
        "atanh" if unary_float(args, result) => Some(ScalarFunction::Atanh),
        "ceil" | "ceiling" if unary_numeric(args, result) => Some(ScalarFunction::Ceil),
        "concat" if !args.is_empty() && text_result(result) && supported_args(args) => {
            Some(ScalarFunction::Concat)
        }
        "concat_ws"
            if args.len() >= 2
                && text_arg(args[0])
                && text_result(result)
                && supported_args(args) =>
        {
            Some(ScalarFunction::ConcatWs)
        }
        "cosh" if unary_float(args, result) => Some(ScalarFunction::Cosh),
        "exp" if unary_float(args, result) => Some(ScalarFunction::Exp),
        "floor" if unary_numeric(args, result) => Some(ScalarFunction::Floor),
        "format" if !args.is_empty() && text_arg(args[0]) && text_result(result) => {
            Some(ScalarFunction::Format)
        }
        "length"
            if args.len() == 1 && text_arg(args[0]) && result == u32::from(pg_sys::INT4OID) =>
        {
            Some(ScalarFunction::Length)
        }
        "ln" if unary_float(args, result) => Some(ScalarFunction::Ln),
        "power"
            if args.len() == 2
                && args.iter().all(|oid| float_arg(*oid))
                && float_result(result) =>
        {
            Some(ScalarFunction::Power)
        }
        "quote_literal" if args.len() == 1 && text_arg(args[0]) && text_result(result) => {
            Some(ScalarFunction::QuoteLiteral)
        }
        "random" if args.is_empty() && result == u32::from(pg_sys::FLOAT8OID) => {
            Some(ScalarFunction::Random)
        }
        "reverse" if args.len() == 1 && text_arg(args[0]) && text_result(result) => {
            Some(ScalarFunction::Reverse)
        }
        "round" if unary_numeric(args, result) => Some(ScalarFunction::Round),
        "sinh" if unary_float(args, result) => Some(ScalarFunction::Sinh),
        "sqrt" if unary_float(args, result) => Some(ScalarFunction::Sqrt),
        "tanh" if unary_float(args, result) => Some(ScalarFunction::Tanh),
        "trunc" if unary_numeric(args, result) => Some(ScalarFunction::Trunc),
        _ => None,
    }
}

fn unary_numeric(args: &[u32], result: u32) -> bool {
    args.len() == 1 && numeric_arg(args[0]) && numeric_result(result)
}

fn unary_float(args: &[u32], result: u32) -> bool {
    args.len() == 1 && float_arg(args[0]) && float_result(result)
}

fn supported_args(args: &[u32]) -> bool {
    args.iter()
        .all(|oid| pg_type::is_supported_scalar_type(*oid))
}

fn numeric_arg(oid: u32) -> bool {
    oid == u32::from(pg_sys::INT2OID)
        || oid == u32::from(pg_sys::INT4OID)
        || oid == u32::from(pg_sys::INT8OID)
        || oid == u32::from(pg_sys::FLOAT4OID)
        || oid == u32::from(pg_sys::FLOAT8OID)
        || oid == u32::from(pg_sys::NUMERICOID)
}

fn numeric_result(oid: u32) -> bool {
    numeric_arg(oid)
}

fn float_arg(oid: u32) -> bool {
    oid == u32::from(pg_sys::FLOAT4OID) || oid == u32::from(pg_sys::FLOAT8OID)
}

fn float_result(oid: u32) -> bool {
    oid == u32::from(pg_sys::FLOAT4OID) || oid == u32::from(pg_sys::FLOAT8OID)
}

fn text_arg(oid: u32) -> bool {
    pg_type::is_text_like_type(oid)
}

fn text_result(oid: u32) -> bool {
    pg_type::is_text_like_type(oid)
}

fn expr_pg_type(expr: &QueryExpr) -> Option<u32> {
    match expr {
        QueryExpr::Var(var) => Some(var.pg_type.oid),
        QueryExpr::OuterVar(var) => Some(var.pg_type.oid),
        QueryExpr::Const(constant) => Some(constant.pg_type.oid),
        QueryExpr::Param(param) => Some(param.pg_type.oid),
        QueryExpr::RelabelType(inner) => expr_pg_type(inner),
        QueryExpr::Cast { pg_type, .. }
        | QueryExpr::Array { pg_type, .. }
        | QueryExpr::ArraySubscript { pg_type, .. }
        | QueryExpr::FunctionCall { pg_type, .. }
        | QueryExpr::BinaryOp { pg_type, .. }
        | QueryExpr::UnaryOp { pg_type, .. }
        | QueryExpr::AggregateCall { pg_type, .. }
        | QueryExpr::WindowCall { pg_type, .. }
        | QueryExpr::Coalesce { pg_type, .. }
        | QueryExpr::Case { pg_type, .. }
        | QueryExpr::ExistsSubquery { pg_type, .. }
        | QueryExpr::InSubquery { pg_type, .. } => Some(pg_type.oid),
        QueryExpr::Bool { .. }
        | QueryExpr::NullTest { .. }
        | QueryExpr::BooleanTest { .. }
        | QueryExpr::ScalarSubquery(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_function_classifier_accepts_text_length_only() {
        let text_args = [oid(pg_sys::TEXTOID)];
        assert_eq!(
            classify_scalar_function_signature(&signature("length", &text_args, pg_sys::INT4OID)),
            Some(ScalarFunction::Length)
        );

        let bytea_args = [oid(pg_sys::BYTEAOID)];
        assert_eq!(
            classify_scalar_function_signature(&signature("length", &bytea_args, pg_sys::INT4OID)),
            None,
            "bytea length overload must not lower to character_length"
        );
    }

    #[test]
    fn scalar_function_classifier_keeps_supported_variadic_text_functions() {
        let concat_args = [oid(pg_sys::TEXTOID), oid(pg_sys::INT4OID)];
        assert_eq!(
            classify_scalar_function_signature(&signature(
                "concat_ws",
                &concat_args,
                pg_sys::TEXTOID
            )),
            Some(ScalarFunction::ConcatWs)
        );

        let format_args = [oid(pg_sys::TEXTOID), oid(pg_sys::INT4OID)];
        assert_eq!(
            classify_scalar_function_signature(&signature("format", &format_args, pg_sys::TEXTOID)),
            Some(ScalarFunction::Format)
        );
    }

    #[test]
    fn scalar_function_classifier_rejects_wrong_named_overloads() {
        let quote_literal_args = [oid(pg_sys::INT4OID)];
        assert_eq!(
            classify_scalar_function_signature(&signature(
                "quote_literal",
                &quote_literal_args,
                pg_sys::TEXTOID
            )),
            None
        );

        let power_args = [oid(pg_sys::INT4OID), oid(pg_sys::INT4OID)];
        assert_eq!(
            classify_scalar_function_signature(&signature("power", &power_args, pg_sys::FLOAT8OID)),
            None,
            "integer overloads must not be accepted for float power lowering"
        );
    }

    fn signature<'a>(
        name: &'a str,
        args: &'a [u32],
        result: pg_sys::Oid,
    ) -> ScalarFunctionSignature<'a> {
        ScalarFunctionSignature {
            name,
            args,
            result: oid(result),
        }
    }

    fn oid(oid: pg_sys::Oid) -> u32 {
        u32::from(oid)
    }
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
