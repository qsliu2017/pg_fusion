use std::ffi::c_char;
use std::slice;
use std::str;

use pg_type::is_supported_scalar_type;
use pgrx::pg_sys;

use crate::error::PgFrontendError;
use crate::ir::PgOperator;

/// Return the DataFusion operator for PostgreSQL comparison operators that v1
/// can compile.
///
/// PostgreSQL operator names are user-extensible, so matching only by spelling
/// would silently turn a user-defined `=` into DataFusion's builtin operator.
/// PostgreSQL already resolved `OpExpr.opno`; this function validates that the
/// resolved operator is a safe `pg_catalog` binary comparison over supported
/// scalar types before compiling it.
pub(crate) unsafe fn supported_operator(opno: pg_sys::Oid) -> Result<PgOperator, PgFrontendError> {
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

fn classify_operator(metadata: &OperatorMetadata) -> Option<PgOperator> {
    if metadata.namespace != pg_sys::PG_CATALOG_NAMESPACE {
        return None;
    }
    if metadata.kind != b'b' as c_char {
        return None;
    }
    if metadata.left != metadata.right {
        return None;
    }
    if !is_supported_scalar_type(metadata.left) {
        return None;
    }
    if metadata.result != u32::from(pg_sys::BOOLOID) {
        return None;
    }

    match metadata.name.as_str() {
        "=" => Some(PgOperator::Eq),
        "<>" => Some(PgOperator::NotEq),
        "<" => Some(PgOperator::Lt),
        "<=" => Some(PgOperator::LtEq),
        ">" => Some(PgOperator::Gt),
        ">=" => Some(PgOperator::GtEq),
        _ => None,
    }
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
            Some(PgOperator::Eq)
        );
        assert_eq!(
            classify_operator(&operator(
                "<>",
                pg_sys::TEXTOID,
                pg_sys::TEXTOID,
                pg_sys::BOOLOID,
            )),
            Some(PgOperator::NotEq)
        );
        assert_eq!(
            classify_operator(&operator(
                "<",
                pg_sys::FLOAT8OID,
                pg_sys::FLOAT8OID,
                pg_sys::BOOLOID,
            )),
            Some(PgOperator::Lt)
        );
        assert_eq!(
            classify_operator(&operator(
                ">=",
                pg_sys::DATEOID,
                pg_sys::DATEOID,
                pg_sys::BOOLOID,
            )),
            Some(PgOperator::GtEq)
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
    fn rejects_mixed_operand_types_for_v1() {
        assert_eq!(
            classify_operator(&operator(
                "=",
                pg_sys::INT2OID,
                pg_sys::INT4OID,
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
    fn rejects_arithmetic_operator_names() {
        for name in ["+", "-", "*", "/"] {
            assert_eq!(
                classify_operator(&operator(
                    name,
                    pg_sys::INT4OID,
                    pg_sys::INT4OID,
                    pg_sys::INT4OID,
                )),
                None
            );
        }
    }

    #[test]
    fn rejects_unary_operator_kind() {
        let metadata = OperatorMetadata {
            kind: b'l' as c_char,
            ..operator("=", pg_sys::INT4OID, pg_sys::INT4OID, pg_sys::BOOLOID)
        };
        assert_eq!(classify_operator(&metadata), None);
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
