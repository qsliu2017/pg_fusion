use crate::ir::PgTypeRef;

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
    validate_collation(pg_type)?;

    if is_supported_value_type(pg_type.oid) {
        Ok(())
    } else {
        Err(UnsupportedReason::new(format!(
            "PostgreSQL type oid {} is not supported by pg_frontend v1",
            pg_type.oid
        )))
    }
}

pub fn supported_non_null_const_type(pg_type: PgTypeRef) -> Result<(), UnsupportedReason> {
    validate_collation(pg_type)?;

    if is_supported_non_null_const_type(pg_type.oid) {
        Ok(())
    } else {
        Err(UnsupportedReason::new(format!(
            "non-null PostgreSQL constant type {} is not supported by pg_frontend v1",
            type_name(pg_type.oid)
        )))
    }
}

fn validate_collation(pg_type: PgTypeRef) -> Result<(), UnsupportedReason> {
    if pg_type.collation != 0
        && !is_default_collation(pg_type.collation)
        && !is_name_c_collation(pg_type)
    {
        return Err(UnsupportedReason::new(format!(
            "non-default collation oid {} is not supported by pg_frontend v1",
            pg_type.collation
        )));
    }
    Ok(())
}

fn is_default_collation(collation: u32) -> bool {
    collation == oid_u32(pgrx::pg_sys::DEFAULT_COLLATION_OID)
}

fn is_name_c_collation(pg_type: PgTypeRef) -> bool {
    pg_type.oid == oid_u32(pgrx::pg_sys::NAMEOID)
        && pg_type.collation == oid_u32(pgrx::pg_sys::C_COLLATION_OID)
}

pub fn is_supported_scalar_type(oid: u32) -> bool {
    is_supported_value_type(oid)
}

pub fn is_supported_value_type(oid: u32) -> bool {
    oid == oid_u32(pgrx::pg_sys::BOOLOID)
        || oid == oid_u32(pgrx::pg_sys::INT2OID)
        || oid == oid_u32(pgrx::pg_sys::INT4OID)
        || oid == oid_u32(pgrx::pg_sys::INT8OID)
        || oid == oid_u32(pgrx::pg_sys::FLOAT4OID)
        || oid == oid_u32(pgrx::pg_sys::FLOAT8OID)
        || oid == oid_u32(pgrx::pg_sys::TEXTOID)
        || oid == oid_u32(pgrx::pg_sys::VARCHAROID)
        || oid == oid_u32(pgrx::pg_sys::BPCHAROID)
        || oid == oid_u32(pgrx::pg_sys::NAMEOID)
        || oid == oid_u32(pgrx::pg_sys::BYTEAOID)
        || oid == oid_u32(pgrx::pg_sys::UUIDOID)
        || oid == oid_u32(pgrx::pg_sys::DATEOID)
        || oid == oid_u32(pgrx::pg_sys::TIMEOID)
        || oid == oid_u32(pgrx::pg_sys::TIMESTAMPOID)
        || oid == oid_u32(pgrx::pg_sys::TIMESTAMPTZOID)
        || oid == oid_u32(pgrx::pg_sys::INTERVALOID)
        || oid == oid_u32(pgrx::pg_sys::NUMERICOID)
}

pub fn is_supported_non_null_const_type(oid: u32) -> bool {
    oid == oid_u32(pgrx::pg_sys::BOOLOID)
        || oid == oid_u32(pgrx::pg_sys::INT2OID)
        || oid == oid_u32(pgrx::pg_sys::INT4OID)
        || oid == oid_u32(pgrx::pg_sys::INT8OID)
        || oid == oid_u32(pgrx::pg_sys::FLOAT4OID)
        || oid == oid_u32(pgrx::pg_sys::FLOAT8OID)
        || oid == oid_u32(pgrx::pg_sys::TEXTOID)
        || oid == oid_u32(pgrx::pg_sys::VARCHAROID)
        || oid == oid_u32(pgrx::pg_sys::BPCHAROID)
        || oid == oid_u32(pgrx::pg_sys::NAMEOID)
        || oid == oid_u32(pgrx::pg_sys::BYTEAOID)
        || oid == oid_u32(pgrx::pg_sys::TIMEOID)
}

fn type_name(oid: u32) -> String {
    if oid == oid_u32(pgrx::pg_sys::NUMERICOID) {
        "numeric".into()
    } else if oid == oid_u32(pgrx::pg_sys::DATEOID) {
        "date".into()
    } else if oid == oid_u32(pgrx::pg_sys::TIMESTAMPOID) {
        "timestamp".into()
    } else if oid == oid_u32(pgrx::pg_sys::TIMESTAMPTZOID) {
        "timestamptz".into()
    } else if oid == oid_u32(pgrx::pg_sys::UUIDOID) {
        "uuid".into()
    } else if oid == oid_u32(pgrx::pg_sys::INTERVALOID) {
        "interval".into()
    } else {
        format!("oid {oid}")
    }
}

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
            (pgrx::pg_sys::NUMERICOID, "numeric"),
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
