use std::ffi::CStr;
use std::ptr::null_mut;

use pgrx::pg_sys;

use crate::error::PgFrontendError;
use crate::ir::{PgBoolOp, PgOperator, PgParamKind, PgTypeRef};
use crate::operator::supported_operator;

const USECS_PER_DAY: i64 = 86_400_000_000;

pub(super) fn finite_float32_const(value: f32) -> Result<f32, PgFrontendError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(PgFrontendError::unsupported(
            "non-finite float4 constants are not supported by pg_frontend v1",
        ))
    }
}

pub(super) fn finite_float64_const(value: f64) -> Result<f64, PgFrontendError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(PgFrontendError::unsupported(
            "non-finite float8 constants are not supported by pg_frontend v1",
        ))
    }
}

pub(super) fn time_const(value: i64) -> Result<i64, PgFrontendError> {
    if (0..USECS_PER_DAY).contains(&value) {
        Ok(value)
    } else if value == USECS_PER_DAY {
        Err(PgFrontendError::unsupported(
            "TIME '24:00:00' constants are not supported by pg_frontend v1",
        ))
    } else {
        Err(PgFrontendError::unsupported(format!(
            "time constant value {value} is outside the supported time-of-day range"
        )))
    }
}

pub(super) fn unsupported_temporal_const(type_name: &str) -> PgFrontendError {
    PgFrontendError::unsupported(format!(
        "non-null {type_name} constants are not supported by pg_frontend v1"
    ))
}

pub(super) fn read_operator(opno: pg_sys::Oid) -> Result<PgOperator, PgFrontendError> {
    supported_operator(u32::from(opno)).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "operator oid {} is not supported by pg_frontend v1",
            u32::from(opno)
        ))
    })
}

pub(super) fn bool_op(op: pg_sys::BoolExprType::Type) -> Result<PgBoolOp, PgFrontendError> {
    match op {
        pg_sys::BoolExprType::AND_EXPR => Ok(PgBoolOp::And),
        pg_sys::BoolExprType::OR_EXPR => Ok(PgBoolOp::Or),
        pg_sys::BoolExprType::NOT_EXPR => Ok(PgBoolOp::Not),
        other => Err(PgFrontendError::unsupported(format!(
            "boolean expression kind {other} is not supported"
        ))),
    }
}

pub(super) fn param_kind(kind: pg_sys::ParamKind::Type) -> PgParamKind {
    match kind {
        pg_sys::ParamKind::PARAM_EXTERN => PgParamKind::External,
        pg_sys::ParamKind::PARAM_EXEC => PgParamKind::Exec,
        pg_sys::ParamKind::PARAM_SUBLINK => PgParamKind::Sublink,
        pg_sys::ParamKind::PARAM_MULTIEXPR => PgParamKind::Multiexpr,
        _ => PgParamKind::Exec,
    }
}

pub(super) unsafe fn expr_type_ref(expr: *const pg_sys::Node) -> PgTypeRef {
    type_ref(
        unsafe { pg_sys::exprType(expr) },
        unsafe { pg_sys::exprTypmod(expr) },
        unsafe { pg_sys::exprCollation(expr) },
    )
}

pub(super) fn type_ref(oid: pg_sys::Oid, typmod: i32, collation: pg_sys::Oid) -> PgTypeRef {
    PgTypeRef {
        oid: u32::from(oid),
        typmod,
        collation: u32::from(collation),
    }
}

pub(super) unsafe fn list_len(list: *mut pg_sys::List) -> i32 {
    if list.is_null() {
        0
    } else {
        unsafe { (*list).length }
    }
}

pub(super) unsafe fn list_ptr_at(list: *mut pg_sys::List, index: i32) -> *mut std::ffi::c_void {
    if list.is_null() || index < 0 || index >= unsafe { (*list).length } {
        return null_mut();
    }
    unsafe { (*(*list).elements.offset(index as isize)).ptr_value }
}

pub(super) unsafe fn cstr_from_pg(ptr: *mut std::ffi::c_char) -> Result<String, PgFrontendError> {
    if ptr.is_null() {
        return Err(PgFrontendError::unsupported(
            "PostgreSQL returned null name",
        ));
    }
    Ok(unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_float_constants_are_accepted() {
        assert_eq!(finite_float32_const(1.5).unwrap(), 1.5);
        assert_eq!(finite_float64_const(-2.25).unwrap(), -2.25);
    }

    #[test]
    fn non_finite_float_constants_are_rejected() {
        assert!(finite_float32_const(f32::NAN).is_err());
        assert!(finite_float32_const(f32::INFINITY).is_err());
        assert!(finite_float32_const(f32::NEG_INFINITY).is_err());
        assert!(finite_float64_const(f64::NAN).is_err());
        assert!(finite_float64_const(f64::INFINITY).is_err());
        assert!(finite_float64_const(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn time_constants_reject_24_hour_sentinel() {
        assert_eq!(time_const(0).unwrap(), 0);
        assert_eq!(time_const(43_200_000_000).unwrap(), 43_200_000_000);
        assert_eq!(time_const(USECS_PER_DAY - 1).unwrap(), USECS_PER_DAY - 1);

        assert!(time_const(USECS_PER_DAY).is_err());
        assert!(time_const(-1).is_err());
        assert!(time_const(USECS_PER_DAY + 1).is_err());
    }

    #[test]
    fn temporal_constants_are_rejected_until_representation_is_lossless() {
        assert!(unsupported_temporal_const("date")
            .to_string()
            .contains("non-null date constants"));
    }
}
