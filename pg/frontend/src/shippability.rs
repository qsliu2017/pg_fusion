use crate::typed_query::PgTypeRef;
use pg_type::{
    is_supported_non_null_const_type as pg_is_supported_non_null_const_type,
    is_supported_scalar_type as pg_is_supported_scalar_type,
    is_supported_value_type as pg_is_supported_value_type, validate_supported_non_null_const_type,
    validate_supported_value_type,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedReason {
    pub message: String,
}

impl UnsupportedReason {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn supported_type(pg_type: PgTypeRef) -> Result<(), UnsupportedReason> {
    supported_value_type(pg_type)
}

pub fn supported_value_type(pg_type: PgTypeRef) -> Result<(), UnsupportedReason> {
    validate_supported_value_type(pg_type).map_err(|err| UnsupportedReason::new(err.to_string()))
}

pub fn supported_non_null_const_type(pg_type: PgTypeRef) -> Result<(), UnsupportedReason> {
    validate_supported_non_null_const_type(pg_type)
        .map_err(|err| UnsupportedReason::new(err.to_string()))
}

pub fn is_supported_scalar_type(oid: u32) -> bool {
    pg_is_supported_scalar_type(oid)
}

pub fn is_supported_value_type(oid: u32) -> bool {
    pg_is_supported_value_type(oid)
}

pub fn is_supported_non_null_const_type(oid: u32) -> bool {
    pg_is_supported_non_null_const_type(oid)
}

#[cfg(test)]
fn oid_u32(oid: pgrx::pg_sys::Oid) -> u32 {
    u32::from(oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_types_match_frontend_scalar_boundary() {
        for oid in [
            pgrx::pg_sys::BOOLOID,
            pgrx::pg_sys::INT2OID,
            pgrx::pg_sys::INT4OID,
            pgrx::pg_sys::INT8OID,
            pgrx::pg_sys::FLOAT4OID,
            pgrx::pg_sys::FLOAT8OID,
            pgrx::pg_sys::NUMERICOID,
            pgrx::pg_sys::TEXTOID,
            pgrx::pg_sys::VARCHAROID,
            pgrx::pg_sys::BPCHAROID,
            pgrx::pg_sys::NAMEOID,
            pgrx::pg_sys::BYTEAOID,
            pgrx::pg_sys::UUIDOID,
            pgrx::pg_sys::DATEOID,
            pgrx::pg_sys::TIMEOID,
            pgrx::pg_sys::TIMESTAMPOID,
            pgrx::pg_sys::TIMESTAMPTZOID,
            pgrx::pg_sys::INTERVALOID,
            pgrx::pg_sys::NUMERICOID,
        ] {
            supported_value_type(type_ref(oid)).expect("value type must be supported");
        }
    }

    #[test]
    fn non_null_const_types_match_pg_const_value_carriers() {
        for oid in [
            pgrx::pg_sys::BOOLOID,
            pgrx::pg_sys::INT2OID,
            pgrx::pg_sys::INT4OID,
            pgrx::pg_sys::INT8OID,
            pgrx::pg_sys::FLOAT4OID,
            pgrx::pg_sys::FLOAT8OID,
            pgrx::pg_sys::TEXTOID,
            pgrx::pg_sys::VARCHAROID,
            pgrx::pg_sys::BPCHAROID,
            pgrx::pg_sys::NAMEOID,
            pgrx::pg_sys::BYTEAOID,
            pgrx::pg_sys::TIMEOID,
        ] {
            supported_non_null_const_type(type_ref(oid))
                .expect("non-null const type must be supported");
        }
    }

    #[test]
    fn non_null_const_types_reject_value_only_types() {
        for (oid, expected) in [
            (pgrx::pg_sys::DATEOID, "date"),
            (pgrx::pg_sys::TIMESTAMPOID, "timestamp"),
            (pgrx::pg_sys::TIMESTAMPTZOID, "timestamptz"),
            (pgrx::pg_sys::UUIDOID, "uuid"),
            (pgrx::pg_sys::INTERVALOID, "interval"),
        ] {
            let err = supported_non_null_const_type(type_ref(oid))
                .expect_err("value-only type must reject non-null constants");
            assert!(
                err.message.contains(expected),
                "error {:?} must mention {expected}",
                err.message
            );
        }
    }

    #[test]
    fn name_values_accept_builtin_c_collation() {
        supported_value_type(type_ref_with_collation(
            pgrx::pg_sys::NAMEOID,
            pgrx::pg_sys::C_COLLATION_OID,
        ))
        .expect("name values use built-in C collation");
        supported_non_null_const_type(type_ref_with_collation(
            pgrx::pg_sys::NAMEOID,
            pgrx::pg_sys::C_COLLATION_OID,
        ))
        .expect("name constants use built-in C collation");
    }

    #[test]
    fn text_values_still_reject_c_collation() {
        let err = supported_value_type(type_ref_with_collation(
            pgrx::pg_sys::TEXTOID,
            pgrx::pg_sys::C_COLLATION_OID,
        ))
        .expect_err("text with non-default collation remains unsupported");
        assert!(err.message.contains("non-default collation"));
    }

    fn type_ref(oid: pgrx::pg_sys::Oid) -> PgTypeRef {
        type_ref_with_collation(oid, pgrx::pg_sys::Oid::INVALID)
    }

    fn type_ref_with_collation(oid: pgrx::pg_sys::Oid, collation: pgrx::pg_sys::Oid) -> PgTypeRef {
        PgTypeRef {
            oid: oid_u32(oid),
            typmod: -1,
            collation: oid_u32(collation),
        }
    }
}
