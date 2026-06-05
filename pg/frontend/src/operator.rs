use std::ffi::c_char;
use std::slice;
use std::str;

use pg_type::is_supported_scalar_type;
use pg_type::is_text_like_type;
use pgrx::pg_sys;

use crate::error::PgFrontendError;
use crate::typed_query::{QueryOperator, QueryUnaryOperator};

/// Return the DataFusion operator for PostgreSQL comparison operators that v1
/// can compile.
///
/// PostgreSQL operator names are user-extensible, so matching only by spelling
/// would silently turn a user-defined `=` into DataFusion's builtin operator.
/// PostgreSQL already resolved `OpExpr.opno`; this function validates that the
/// resolved operator is a safe `pg_catalog` binary comparison over supported
/// scalar types before compiling it.
pub(crate) unsafe fn supported_operator(
    opno: pg_sys::Oid,
) -> Result<QueryOperator, PgFrontendError> {
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID as i32,
            pg_sys::ObjectIdGetDatum(opno),
        )
    };
    if tuple.is_null() {
        return Err(PgFrontendError::unsupported(format!(
            "operator oid {} is not present in pg_operator",
            u32::from(opno)
        )));
    }

    let form = unsafe { pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_operator };
    let metadata = unsafe { OperatorMetadata::from_pg_operator(&*form) };
    unsafe { pg_sys::ReleaseSysCache(tuple) };

    classify_operator(&metadata).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "operator oid {} ({}) is not supported by pg_frontend v1",
            u32::from(opno),
            metadata.describe()
        ))
    })
}

pub(crate) unsafe fn supported_unary_operator(
    opno: pg_sys::Oid,
) -> Result<QueryUnaryOperator, PgFrontendError> {
    let tuple = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID as i32,
            pg_sys::ObjectIdGetDatum(opno),
        )
    };
    if tuple.is_null() {
        return Err(PgFrontendError::unsupported(format!(
            "operator oid {} is not present in pg_operator",
            u32::from(opno)
        )));
    }

    let form = unsafe { pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_operator };
    let metadata = unsafe { OperatorMetadata::from_pg_operator(&*form) };
    unsafe { pg_sys::ReleaseSysCache(tuple) };

    classify_unary_operator(&metadata).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "unary operator oid {} ({}) is not supported by pg_frontend v1",
            u32::from(opno),
            metadata.describe()
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperatorMetadata {
    name: String,
    namespace: u32,
    kind: c_char,
    left: u32,
    right: u32,
    result: u32,
}

impl OperatorMetadata {
    unsafe fn from_pg_operator(form: &pg_sys::FormData_pg_operator) -> Self {
        Self {
            name: decode_name_data(&form.oprname),
            namespace: u32::from(form.oprnamespace),
            kind: form.oprkind,
            left: u32::from(form.oprleft),
            right: u32::from(form.oprright),
            result: u32::from(form.oprresult),
        }
    }

    fn describe(&self) -> String {
        format!(
            "name {:?}, namespace {}, left {}, right {}, result {}",
            self.name, self.namespace, self.left, self.right, self.result
        )
    }
}

fn classify_operator(metadata: &OperatorMetadata) -> Option<QueryOperator> {
    if metadata.namespace != pg_sys::PG_CATALOG_NAMESPACE {
        return None;
    }
    if metadata.kind != b'b' as c_char {
        return None;
    }
    if metadata.name == "||" {
        return (is_text_like_type(metadata.left)
            && is_text_like_type(metadata.right)
            && is_text_like_type(metadata.result))
        .then_some(QueryOperator::StringConcat);
    }
    if metadata.name == "~" || metadata.name == "!~" {
        return (is_text_like_type(metadata.left)
            && is_text_like_type(metadata.right)
            && metadata.result == u32::from(pg_sys::BOOLOID))
        .then_some(if metadata.name == "~" {
            QueryOperator::RegexMatch
        } else {
            QueryOperator::RegexNotMatch
        });
    }
    if matches!(metadata.name.as_str(), "~~" | "!~~" | "~~*" | "!~~*") {
        return (is_text_like_type(metadata.left)
            && is_text_like_type(metadata.right)
            && metadata.result == u32::from(pg_sys::BOOLOID))
        .then_some(match metadata.name.as_str() {
            "~~" => QueryOperator::LikeMatch,
            "!~~" => QueryOperator::NotLikeMatch,
            "~~*" => QueryOperator::ILikeMatch,
            "!~~*" => QueryOperator::NotILikeMatch,
            _ => unreachable!("LIKE operator name was matched above"),
        });
    }
    if !is_supported_binary_operands(metadata.left, metadata.right) {
        return None;
    }
    match metadata.name.as_str() {
        "=" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::Eq),
        "<>" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::NotEq),
        "<" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::Lt),
        "<=" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::LtEq),
        ">" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::Gt),
        ">=" if metadata.result == u32::from(pg_sys::BOOLOID) => Some(QueryOperator::GtEq),
        "+" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::Plus),
        "-" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::Minus),
        "*" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::Multiply),
        "/" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::Divide),
        "%" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::Modulo),
        "<<" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::BitwiseShiftLeft),
        ">>" if is_supported_scalar_type(metadata.result) => Some(QueryOperator::BitwiseShiftRight),
        _ => None,
    }
}

fn classify_unary_operator(metadata: &OperatorMetadata) -> Option<QueryUnaryOperator> {
    if metadata.namespace != pg_sys::PG_CATALOG_NAMESPACE {
        return None;
    }
    if metadata.kind == b'b' as c_char {
        return None;
    }
    if !is_supported_scalar_type(metadata.result) {
        return None;
    }
    match metadata.name.as_str() {
        "+" => Some(QueryUnaryOperator::Plus),
        "-" => Some(QueryUnaryOperator::Minus),
        _ => None,
    }
}

fn is_supported_binary_operands(left: u32, right: u32) -> bool {
    if left == right {
        return is_supported_scalar_type(left);
    }
    is_numeric_type(left) && is_numeric_type(right)
}

fn is_numeric_type(oid: u32) -> bool {
    oid == u32::from(pg_sys::INT2OID)
        || oid == u32::from(pg_sys::INT4OID)
        || oid == u32::from(pg_sys::INT8OID)
        || oid == u32::from(pg_sys::FLOAT4OID)
        || oid == u32::from(pg_sys::FLOAT8OID)
        || oid == u32::from(pg_sys::NUMERICOID)
}

fn decode_name_data(name: &pg_sys::NameData) -> String {
    let bytes = &name.data;
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    let raw = unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end) };
    str::from_utf8(raw).unwrap_or("").to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_catalog_comparison_over_same_supported_type() {
        assert_eq!(
            classify_operator(&operator(
                "=",
                pg_sys::INT4OID,
                pg_sys::INT4OID,
                pg_sys::BOOLOID
            )),
            Some(QueryOperator::Eq)
        );
        assert_eq!(
            classify_operator(&operator(
                "<>",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::NotEq)
        );
        assert_eq!(
            classify_operator(&operator(
                "<",
                pg_sys::FLOAT8OID,
                pg_sys::FLOAT8OID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::Lt)
        );
        assert_eq!(
            classify_operator(&operator(
                ">=",
                pg_sys::DATEOID,
                pg_sys::DATEOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::GtEq)
        );
    }

    #[test]
    fn rejects_non_catalog_operator_namespace() {
        let metadata = OperatorMetadata {
            namespace: 999_999,
            ..operator("=", pg_sys::INT4OID, pg_sys::INT4OID, pg_sys::BOOLOID)
        };
        assert_eq!(classify_operator(&metadata), None);
    }

    #[test]
    fn accepts_catalog_comparison_over_mixed_numeric_types() {
        assert_eq!(
            classify_operator(&operator(
                ">=",
                pg_sys::INT8OID,
                pg_sys::INT4OID,
                pg_sys::BOOLOID
            )),
            Some(QueryOperator::GtEq)
        );
    }

    #[test]
    fn rejects_mixed_non_numeric_operand_types_for_v1() {
        assert_eq!(
            classify_operator(&operator(
                "=",
                pg_sys::INT4OID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID
            )),
            None
        );
    }

    #[test]
    fn rejects_unsupported_operand_type() {
        assert_eq!(
            classify_operator(&operator(
                "=",
                pg_sys::REGCLASSOID,
                pg_sys::REGCLASSOID,
                pg_sys::BOOLOID
            )),
            None
        );
    }

    #[test]
    fn rejects_non_boolean_result() {
        assert_eq!(
            classify_operator(&operator(
                "=",
                pg_sys::INT4OID,
                pg_sys::INT4OID,
                pg_sys::INT4OID
            )),
            None
        );
    }

    #[test]
    fn accepts_catalog_arithmetic_over_supported_type() {
        assert_eq!(
            classify_operator(&operator(
                "+",
                pg_sys::INT4OID,
                pg_sys::INT4OID,
                pg_sys::INT4OID,
            )),
            Some(QueryOperator::Plus)
        );
        assert_eq!(
            classify_operator(&operator(
                "/",
                pg_sys::FLOAT8OID,
                pg_sys::FLOAT8OID,
                pg_sys::FLOAT8OID,
            )),
            Some(QueryOperator::Divide)
        );
        assert_eq!(
            classify_operator(&operator(
                "%",
                pg_sys::INT2OID,
                pg_sys::INT2OID,
                pg_sys::INT2OID,
            )),
            Some(QueryOperator::Modulo)
        );
        assert_eq!(
            classify_operator(&operator(
                "<<",
                pg_sys::INT2OID,
                pg_sys::INT4OID,
                pg_sys::INT2OID,
            )),
            Some(QueryOperator::BitwiseShiftLeft)
        );
    }

    #[test]
    fn accepts_catalog_text_concat() {
        assert_eq!(
            classify_operator(&operator(
                "||",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
            )),
            Some(QueryOperator::StringConcat)
        );
    }

    #[test]
    fn accepts_catalog_like_operators_over_text_like_types() {
        assert_eq!(
            classify_operator(&operator(
                "~~",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::LikeMatch)
        );
        assert_eq!(
            classify_operator(&operator(
                "!~~",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::NotLikeMatch)
        );
        assert_eq!(
            classify_operator(&operator(
                "~~*",
                pg_sys::VARCHAROID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::ILikeMatch)
        );
        assert_eq!(
            classify_operator(&operator(
                "!~~*",
                pg_sys::BPCHAROID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(QueryOperator::NotILikeMatch)
        );
    }

    #[test]
    fn rejects_like_operators_over_non_text_like_types() {
        assert_eq!(
            classify_operator(&operator(
                "~~",
                pg_sys::INT4OID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            None
        );
        assert_eq!(
            classify_operator(&operator(
                "~~",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::INT4OID,
            )),
            None
        );
    }

    #[test]
    fn accepts_catalog_unary_arithmetic_over_supported_type() {
        let metadata = OperatorMetadata {
            kind: b'l' as c_char,
            left: 0,
            ..operator("-", pg_sys::INT2OID, pg_sys::INT2OID, pg_sys::INT2OID)
        };
        assert_eq!(
            classify_unary_operator(&metadata),
            Some(QueryUnaryOperator::Minus)
        );
    }

    #[test]
    fn rejects_unsupported_unary_operator_kind() {
        let metadata = OperatorMetadata {
            kind: b'l' as c_char,
            ..operator("@", pg_sys::INT4OID, pg_sys::INT4OID, pg_sys::INT4OID)
        };
        assert_eq!(classify_unary_operator(&metadata), None);
    }

    fn operator(
        name: &str,
        left: pg_sys::Oid,
        right: pg_sys::Oid,
        result: pg_sys::Oid,
    ) -> OperatorMetadata {
        OperatorMetadata {
            name: name.into(),
            namespace: pg_sys::PG_CATALOG_NAMESPACE,
            kind: b'b' as c_char,
            left: u32::from(left),
            right: u32::from(right),
            result: u32::from(result),
        }
    }
}
