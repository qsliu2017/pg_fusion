use crate::error::oid_u32;
use crate::{ConfigError, EncodeError};
use arrow_layout::TypeTag;
use pgrx_pg_sys as pg_sys;
use std::ffi::CStr;
use std::ptr;
use std::slice;

#[cfg(test)]
use std::sync::atomic::{AtomicI32, Ordering};

const NUMERIC_FALLBACK_PRECISION: u8 = 38;
const NUMERIC_FALLBACK_SCALE: i8 = 16;

#[cfg(target_endian = "little")]
const VARLENA_1B_FLAG: u8 = 0x01;

#[cfg(target_endian = "big")]
const VARLENA_1B_FLAG: u8 = 0x80;

#[cfg(target_endian = "little")]
const VARLENA_4B_COMPRESSED_FLAG: u32 = 0x02;

#[cfg(target_endian = "big")]
const VARLENA_4B_COMPRESSED_FLAG: u32 = 0x4000_0000;

pub(crate) fn validate_pg_layout_type(
    index: usize,
    oid: pg_sys::Oid,
    atttypmod: i32,
    type_tag: TypeTag,
) -> Result<(), ConfigError> {
    let ok = match type_tag {
        TypeTag::Boolean => oid == pg_sys::BOOLOID,
        TypeTag::Int16 => oid == pg_sys::INT2OID,
        TypeTag::Int32 => oid == pg_sys::INT4OID,
        TypeTag::Int64 => oid == pg_sys::INT8OID,
        TypeTag::Float32 => oid == pg_sys::FLOAT4OID,
        TypeTag::Float64 => oid == pg_sys::FLOAT8OID,
        TypeTag::Uuid => oid == pg_sys::UUIDOID,
        TypeTag::Decimal128 => {
            oid == pg_sys::NUMERICOID && numeric_shape_from_typmod(atttypmod).is_some()
        }
        TypeTag::IntervalMonthDayNano => oid == pg_sys::INTERVALOID,
        TypeTag::Date32 => oid == pg_sys::DATEOID,
        TypeTag::Time64Microsecond => oid == pg_sys::TIMEOID,
        TypeTag::TimestampMicrosecond => {
            oid == pg_sys::TIMESTAMPOID || oid == pg_sys::TIMESTAMPTZOID
        }
        TypeTag::Utf8View => {
            oid == pg_sys::TEXTOID
                || oid == pg_sys::VARCHAROID
                || oid == pg_sys::BPCHAROID
                || oid == pg_sys::NAMEOID
        }
        TypeTag::BinaryView => oid == pg_sys::BYTEAOID,
    };
    if ok {
        Ok(())
    } else {
        Err(ConfigError::PgLayoutTypeMismatch {
            index,
            oid: oid_u32(oid),
            type_tag,
        })
    }
}

pub(crate) fn pg_oid_needs_detoast(oid: pg_sys::Oid) -> bool {
    oid == pg_sys::TEXTOID
        || oid == pg_sys::VARCHAROID
        || oid == pg_sys::BPCHAROID
        || oid == pg_sys::BYTEAOID
}

#[cfg(not(test))]
pub(crate) fn with_detoasted_slot_datum<R, F>(
    datum: pg_sys::Datum,
    index: usize,
    f: F,
) -> Result<R, EncodeError>
where
    F: FnOnce(pg_sys::Datum) -> Result<R, EncodeError>,
{
    let original = datum.cast_mut_ptr::<pg_sys::varlena>();
    if original.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(original) };
    if detoasted.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let detoasted_datum = pg_sys::Datum::from(detoasted);
    let result = f(detoasted_datum);
    if detoasted != original {
        unsafe { pg_sys::pfree(detoasted.cast()) };
    }
    result
}

#[cfg(test)]
pub(crate) fn with_detoasted_slot_datum<R, F>(
    datum: pg_sys::Datum,
    index: usize,
    f: F,
) -> Result<R, EncodeError>
where
    F: FnOnce(pg_sys::Datum) -> Result<R, EncodeError>,
{
    if datum.cast_mut_ptr::<pg_sys::varlena>().is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    f(datum)
}

#[cfg(not(test))]
pub(crate) fn database_encoding() -> i32 {
    unsafe { pg_sys::GetDatabaseEncoding() }
}

#[cfg(test)]
static TEST_DATABASE_ENCODING: AtomicI32 = AtomicI32::new(pg_sys::pg_enc::PG_UTF8 as i32);

#[cfg(test)]
pub(crate) fn database_encoding() -> i32 {
    TEST_DATABASE_ENCODING.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_test_database_encoding(encoding: i32) -> i32 {
    TEST_DATABASE_ENCODING.swap(encoding, Ordering::Relaxed)
}

pub(crate) unsafe fn read_bool(datum: pg_sys::Datum, byval: bool) -> bool {
    if byval {
        datum.value() != 0
    } else {
        unsafe { *datum.cast_mut_ptr::<bool>() }
    }
}

pub(crate) unsafe fn read_i16(datum: pg_sys::Datum, byval: bool) -> i16 {
    if byval {
        datum.value() as i16
    } else {
        unsafe { *datum.cast_mut_ptr::<i16>() }
    }
}

pub(crate) unsafe fn read_i32(datum: pg_sys::Datum, byval: bool) -> i32 {
    if byval {
        datum.value() as i32
    } else {
        unsafe { *datum.cast_mut_ptr::<i32>() }
    }
}

pub(crate) unsafe fn read_i64(datum: pg_sys::Datum, byval: bool) -> i64 {
    if byval {
        datum.value() as i64
    } else {
        unsafe { *datum.cast_mut_ptr::<i64>() }
    }
}

pub(crate) unsafe fn read_f32(datum: pg_sys::Datum, byval: bool) -> f32 {
    let bits = if byval {
        datum.value() as u32
    } else {
        unsafe { ptr::read(datum.cast_mut_ptr::<u32>()) }
    };
    f32::from_bits(bits)
}

pub(crate) unsafe fn read_f64(datum: pg_sys::Datum, byval: bool) -> f64 {
    let bits = if byval {
        datum.value() as u64
    } else {
        unsafe { ptr::read(datum.cast_mut_ptr::<u64>()) }
    };
    f64::from_bits(bits)
}

pub(crate) unsafe fn read_name_bytes<'a>(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let bytes = unsafe { &(*ptr).data };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end) })
}

pub(crate) unsafe fn read_fixed_bytes<'a>(
    datum: pg_sys::Datum,
    width: usize,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    Ok(unsafe { slice::from_raw_parts(ptr, width) })
}

pub(crate) unsafe fn read_interval_month_day_nano(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<(i32, i32, i64), EncodeError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::Interval>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let interval = unsafe { *ptr };
    if interval_is_infinite(interval) {
        return Err(EncodeError::UnsupportedInfiniteInterval { index });
    }
    let nanoseconds = interval
        .time
        .checked_mul(1_000)
        .ok_or(EncodeError::IntervalTimeOverflow { index })?;
    Ok((interval.month, interval.day, nanoseconds))
}

fn interval_is_infinite(interval: pg_sys::Interval) -> bool {
    (interval.month == i32::MIN && interval.day == i32::MIN && interval.time == i64::MIN)
        || (interval.month == i32::MAX && interval.day == i32::MAX && interval.time == i64::MAX)
}

pub(crate) unsafe fn read_numeric_decimal128(
    datum: pg_sys::Datum,
    atttypmod: i32,
    index: usize,
) -> Result<i128, EncodeError> {
    unsafe { read_numeric_decimal128_with_scale(datum, atttypmod, index) }
        .map(|(value, _scale)| value)
}

pub(crate) unsafe fn read_numeric_decimal128_with_scale(
    datum: pg_sys::Datum,
    atttypmod: i32,
    index: usize,
) -> Result<(i128, i8), EncodeError> {
    let (precision, scale) = numeric_shape_from_typmod(atttypmod)
        .ok_or(EncodeError::UnsupportedNumericTypmod { index, atttypmod })?;
    let original = datum.cast_mut_ptr::<pg_sys::varlena>();
    if original.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }

    let detoasted = unsafe { pg_sys::pg_detoast_datum(original) };
    if detoasted.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let is_copy = !ptr::eq(detoasted, original);
    let result = unsafe { numeric_text(detoasted, index) }
        .and_then(|text| parse_numeric_text_to_decimal128(&text, precision, scale, index))
        .map(|value| (value, scale));
    if is_copy {
        unsafe { pg_sys::pfree(detoasted.cast()) };
    }
    result
}

unsafe fn numeric_text(
    numeric_varlena: *mut pg_sys::varlena,
    index: usize,
) -> Result<String, EncodeError> {
    let numeric = numeric_varlena.cast::<pg_sys::NumericData>();
    if unsafe { pg_sys::numeric_is_nan(numeric) || pg_sys::numeric_is_inf(numeric) } {
        return Err(EncodeError::UnsupportedSpecialNumeric { index });
    }

    let cstr_ptr = unsafe {
        pg_sys::OidOutputFunctionCall(
            pg_sys::Oid::from_u32(pg_sys::F_NUMERIC_OUT),
            pg_sys::Datum::from(numeric_varlena),
        )
    };
    if cstr_ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let text = unsafe { CStr::from_ptr(cstr_ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|_| EncodeError::MalformedNumericText {
            index,
            value: "<non-utf8>".to_owned(),
        });
    unsafe { pg_sys::pfree(cstr_ptr.cast()) };
    text
}

fn numeric_shape_from_typmod(atttypmod: i32) -> Option<(u8, i8)> {
    if atttypmod < 0 {
        return Some((NUMERIC_FALLBACK_PRECISION, NUMERIC_FALLBACK_SCALE));
    }

    let typmod = atttypmod.checked_sub(pg_sys::VARHDRSZ as i32)?;
    let precision = (typmod >> 16) & 0xffff;
    let scale = ((typmod & 0x7ff) ^ 1024) - 1024;
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
        return None;
    }

    Some((precision as u8, scale as i8))
}

fn parse_numeric_text_to_decimal128(
    text: &str,
    precision: u8,
    scale: i8,
    index: usize,
) -> Result<i128, EncodeError> {
    if scale < 0 {
        return Err(EncodeError::NumericValueOutOfRange {
            index,
            precision,
            scale,
        });
    }

    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if rest.is_empty() {
        return Err(EncodeError::MalformedNumericText {
            index,
            value: text.to_owned(),
        });
    }

    let mut parts = rest.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(EncodeError::MalformedNumericText {
            index,
            value: text.to_owned(),
        });
    }

    let target_scale = scale as usize;
    if fraction.len() > target_scale
        && fraction.as_bytes()[target_scale..]
            .iter()
            .any(|byte| *byte != b'0')
    {
        return Err(EncodeError::NumericValueOutOfRange {
            index,
            precision,
            scale,
        });
    }

    let mut digits = String::with_capacity(integer.len() + target_scale);
    digits.push_str(integer);
    if fraction.len() >= target_scale {
        digits.push_str(&fraction[..target_scale]);
    } else {
        digits.push_str(fraction);
        digits.extend(std::iter::repeat_n('0', target_scale - fraction.len()));
    }

    let significant_digits = digits.trim_start_matches('0').len().max(1);
    if significant_digits > usize::from(precision) {
        return Err(EncodeError::NumericValueOutOfRange {
            index,
            precision,
            scale,
        });
    }

    let mut value = digits
        .parse::<i128>()
        .map_err(|_| EncodeError::NumericValueOutOfRange {
            index,
            precision,
            scale,
        })?;
    if negative {
        value = value
            .checked_neg()
            .ok_or(EncodeError::NumericValueOutOfRange {
                index,
                precision,
                scale,
            })?;
    }
    Ok(value)
}

pub(crate) unsafe fn read_packed_varlena<'a>(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }

    let b0 = unsafe { *ptr };
    if varlena_is_1b(b0) {
        if varlena_is_1b_external(b0) {
            return Err(EncodeError::ExternalVarlena { index });
        }
        let total_len = varlena_1b_total_len(b0);
        if total_len == 0 {
            return Err(EncodeError::MalformedVarlena { index });
        }
        let data_len = total_len
            .checked_sub(1)
            .ok_or(EncodeError::MalformedVarlena { index })?;
        return Ok(unsafe { slice::from_raw_parts(ptr.add(1), data_len) });
    }

    let header = unsafe { ptr::read_unaligned(ptr.cast::<u32>()) };
    if varlena_is_4b_compressed(header) {
        return Err(EncodeError::CompressedVarlena { index });
    }
    let total_len = varlena_4b_total_len(header);
    if total_len < std::mem::size_of::<u32>() {
        return Err(EncodeError::MalformedVarlena { index });
    }
    let data_len = total_len - std::mem::size_of::<u32>();
    Ok(unsafe { slice::from_raw_parts(ptr.add(std::mem::size_of::<u32>()), data_len) })
}

#[cfg(target_endian = "little")]
fn varlena_is_1b(b0: u8) -> bool {
    (b0 & VARLENA_1B_FLAG) == VARLENA_1B_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_1b(b0: u8) -> bool {
    (b0 & VARLENA_1B_FLAG) == VARLENA_1B_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_is_1b_external(b0: u8) -> bool {
    b0 == VARLENA_1B_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_1b_external(b0: u8) -> bool {
    b0 == VARLENA_1B_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_1b_total_len(b0: u8) -> usize {
    (b0 as usize) >> 1
}

#[cfg(target_endian = "big")]
fn varlena_1b_total_len(b0: u8) -> usize {
    (b0 & 0x7F) as usize
}

#[cfg(target_endian = "little")]
fn varlena_is_4b_compressed(header: u32) -> bool {
    (header & VARLENA_4B_COMPRESSED_FLAG) == VARLENA_4B_COMPRESSED_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_4b_compressed(header: u32) -> bool {
    (header & VARLENA_4B_COMPRESSED_FLAG) == VARLENA_4B_COMPRESSED_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_4b_total_len(header: u32) -> usize {
    (header >> 2) as usize
}

#[cfg(target_endian = "big")]
fn varlena_4b_total_len(header: u32) -> usize {
    (header & 0x3FFF_FFFF) as usize
}

#[cfg(test)]
mod tests {
    use super::{numeric_shape_from_typmod, parse_numeric_text_to_decimal128};
    use crate::EncodeError;
    use pgrx_pg_sys as pg_sys;

    #[test]
    fn numeric_typmod_decode_supports_pg_numeric_shapes() {
        assert_eq!(numeric_shape_from_typmod(-1), Some((38, 16)));
        assert_eq!(
            numeric_shape_from_typmod(numeric_typmod(12, 3)),
            Some((12, 3))
        );
        assert_eq!(
            numeric_shape_from_typmod(numeric_typmod(38, 0)),
            Some((38, 0))
        );
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(39, 0)), None);
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(4, 5)), None);
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(4, -1)), None);
    }

    #[test]
    fn decimal128_parser_scales_finite_numeric_text() {
        assert_eq!(parse("123.45", 10, 2), 12345);
        assert_eq!(parse("-123.4", 10, 2), -12340);
        assert_eq!(parse("0.0001", 10, 6), 100);
        assert_eq!(parse("+42", 10, 3), 42000);
        assert_eq!(parse("1.230000", 10, 2), 123);
        assert_eq!(
            parse("99999999999999999999999999999999999999", 38, 0),
            99_999_999_999_999_999_999_999_999_999_999_999_999_i128
        );
    }

    #[test]
    fn decimal128_parser_rejects_unsupported_numeric_text() {
        assert!(matches!(
            parse_numeric_text_to_decimal128("1.234", 10, 2, 0),
            Err(EncodeError::NumericValueOutOfRange { .. })
        ));
        assert!(matches!(
            parse_numeric_text_to_decimal128("100000", 5, 0, 0),
            Err(EncodeError::NumericValueOutOfRange { .. })
        ));
        assert!(matches!(
            parse_numeric_text_to_decimal128("NaN", 38, 16, 0),
            Err(EncodeError::MalformedNumericText { .. })
        ));
        assert!(matches!(
            parse_numeric_text_to_decimal128("1e3", 38, 16, 0),
            Err(EncodeError::MalformedNumericText { .. })
        ));
    }

    fn parse(text: &str, precision: u8, scale: i8) -> i128 {
        parse_numeric_text_to_decimal128(text, precision, scale, 0).expect("parse decimal")
    }

    fn numeric_typmod(precision: i32, scale: i32) -> i32 {
        ((precision << 16) | (scale & 0x7ff)) + pg_sys::VARHDRSZ as i32
    }
}
