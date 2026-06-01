use super::*;

pub(super) fn compile_const_scalar(constant: &Const) -> Result<ScalarValue, PgFrontendError> {
    scalar_for_pg_const(constant.value.as_ref(), constant.pg_type)
        .map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

pub(super) fn compile_const_expr(constant: &Const) -> Result<Expr, PgFrontendError> {
    let literal = compile_const_scalar(constant)?;
    let metadata = is_text_like_type(constant.pg_type.oid).then(|| {
        pg_type_metadata(
            constant.pg_type.oid,
            constant.pg_type.typmod,
            constant.pg_type.collation,
        )
    });
    Ok(Expr::Literal(literal, metadata))
}
