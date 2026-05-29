use arrow_schema::DataType;
use datafusion_common::{metadata::FieldMetadata, ScalarValue};

use crate::metadata::{read_pg_type_metadata, PgTypeMetadata};
use crate::quote::encode_hex;

const TEXT_OID: u32 = 25;
const BPCHAR_OID: u32 = 1042;
const VARCHAR_OID: u32 = 1043;
const NAME_OID: u32 = 19;
const VARHDRSZ: i32 = 4;

pub(crate) fn render_literal(
    literal: &ScalarValue,
    metadata: Option<&FieldMetadata>,
) -> Option<String> {
    if let Some(metadata) = metadata {
        if let Some(pg_type) = read_pg_type_metadata(metadata)? {
            return render_pg_typed_literal(literal, pg_type);
        }
    }

    match literal {
        ScalarValue::Null => Some("NULL".into()),
        ScalarValue::Boolean(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| Some(if value { "TRUE" } else { "FALSE" }.into()),
        ),
        ScalarValue::Float16(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| {
                let value = f32::from(value);
                value.is_finite().then(|| value.to_string())
            },
        ),
        ScalarValue::Float32(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| value.is_finite().then(|| value.to_string()),
        ),
        ScalarValue::Float64(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| value.is_finite().then(|| value.to_string()),
        ),
        ScalarValue::Decimal32(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Decimal64(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Decimal128(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Decimal256(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Int8(value) => render_option_number(*value),
        ScalarValue::Int16(value) => render_option_number(*value),
        ScalarValue::Int32(value) => render_option_number(*value),
        ScalarValue::Int64(value) => render_option_number(*value),
        ScalarValue::UInt8(value) => render_option_number(*value),
        ScalarValue::UInt16(value) => render_option_number(*value),
        ScalarValue::UInt32(value) => render_option_number(*value),
        ScalarValue::UInt64(value) => render_option_number(*value),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value
                .as_ref()
                .map(|value| render_string_literal(value))
                .or_else(|| Some("NULL".into()))
        }
        ScalarValue::Binary(value)
        | ScalarValue::BinaryView(value)
        | ScalarValue::LargeBinary(value) => value
            .as_ref()
            .map(|value| render_bytea_literal(value))
            .or_else(|| Some("NULL".into())),
        ScalarValue::FixedSizeBinary(_, value) => value
            .as_ref()
            .map(|value| render_bytea_literal(value))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Date32(value) => value
            .map(render_date32_literal)
            .or_else(|| Some("NULL".into())),
        ScalarValue::Date64(value) => value
            .map(render_date64_literal)
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time32Second(value) => value
            .map(|value| render_time_literal(i64::from(value), "second"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time32Millisecond(value) => value
            .map(|value| render_time_literal(i64::from(value), "millisecond"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time64Microsecond(value) => value
            .map(|value| render_time_literal(value, "microsecond"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time64Nanosecond(_) => None,
        ScalarValue::TimestampSecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "second")
        }
        ScalarValue::TimestampMillisecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "millisecond")
        }
        ScalarValue::TimestampMicrosecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "microsecond")
        }
        ScalarValue::TimestampNanosecond(_, _) => None,
        ScalarValue::Dictionary(_, value) => render_literal(value, None),
        ScalarValue::IntervalYearMonth(_)
        | ScalarValue::IntervalDayTime(_)
        | ScalarValue::IntervalMonthDayNano(_)
        | ScalarValue::DurationSecond(_)
        | ScalarValue::DurationMillisecond(_)
        | ScalarValue::DurationMicrosecond(_)
        | ScalarValue::DurationNanosecond(_)
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::RunEndEncoded(_, _, _) => None,
    }
}

fn render_pg_typed_literal(literal: &ScalarValue, pg_type: PgTypeMetadata) -> Option<String> {
    let target = render_pg_text_cast_target(pg_type)?;
    match literal {
        ScalarValue::Null => Some(format!("CAST(NULL AS {target})")),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value
                .as_ref()
                .map(|value| format!("CAST({} AS {target})", render_string_literal(value)))
                .or_else(|| Some(format!("CAST(NULL AS {target})")))
        }
        ScalarValue::Dictionary(_, value) => render_pg_typed_literal(value, pg_type),
        _ => None,
    }
}

fn render_pg_text_cast_target(pg_type: PgTypeMetadata) -> Option<String> {
    match pg_type.oid {
        TEXT_OID => Some("TEXT".into()),
        VARCHAR_OID => {
            if pg_type.typmod == -1 {
                Some("CHARACTER VARYING".into())
            } else {
                render_typmod_length(pg_type.typmod)
                    .map(|length| format!("CHARACTER VARYING({length})"))
            }
        }
        BPCHAR_OID => {
            if pg_type.typmod == -1 {
                Some("pg_catalog.bpchar".into())
            } else {
                render_typmod_length(pg_type.typmod).map(|length| format!("CHARACTER({length})"))
            }
        }
        NAME_OID => Some("NAME".into()),
        _ => None,
    }
}

fn render_typmod_length(typmod: i32) -> Option<i32> {
    if typmod > VARHDRSZ {
        Some(typmod - VARHDRSZ)
    } else {
        None
    }
}

pub(crate) fn render_cast_target(data_type: &DataType) -> Option<String> {
    Some(match data_type {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 | DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float16 | DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("NUMERIC({precision}, {scale})")
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".into(),
        DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => "BYTEA".into(),
        _ => return None,
    })
}

pub(crate) fn render_string_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

fn render_decimal_literal(value: Option<String>, precision: u8, scale: i8) -> Option<String> {
    value
        .map(|value| {
            let decimal = apply_decimal_scale(value, scale);
            format!(
                "CAST({} AS NUMERIC({precision}, {scale}))",
                render_string_literal(&decimal)
            )
        })
        .or_else(|| Some("NULL".into()))
}

fn apply_decimal_scale(value: String, scale: i8) -> String {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(stripped) => (true, stripped.to_string()),
        None => (false, value),
    };
    let scaled = if scale >= 0 {
        let scale = scale as usize;
        if scale == 0 {
            digits
        } else if digits.len() <= scale {
            let zeros = "0".repeat(scale - digits.len());
            format!("0.{zeros}{digits}")
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        format!("{digits}{}", "0".repeat((-scale) as usize))
    };

    if negative {
        format!("-{scaled}")
    } else {
        scaled
    }
}

fn render_option_number<T: ToString>(value: Option<T>) -> Option<String> {
    value
        .map(|value| value.to_string())
        .or_else(|| Some("NULL".into()))
}

fn render_bytea_literal(bytes: &[u8]) -> String {
    format!("'\\\\x{}'::bytea", encode_hex(bytes))
}

fn render_date32_literal(days: i32) -> String {
    format!("(DATE '1970-01-01' + ({days}))")
}

fn render_date64_literal(milliseconds: i64) -> String {
    format!(
        "((TIMESTAMP '1970-01-01 00:00:00' + ({milliseconds}) * INTERVAL '1 millisecond')::date)"
    )
}

fn render_time_literal(value: i64, unit: &str) -> String {
    format!("((TIME '00:00:00' + ({value}) * INTERVAL '1 {unit}')::time)")
}

fn render_timestamp_literal(
    value: Option<i64>,
    timezone: Option<&str>,
    unit: &str,
) -> Option<String> {
    match (value, timezone) {
        (None, _) => Some("NULL".into()),
        (Some(_), Some(_)) => None,
        (Some(value), None) => Some(format!(
            "(TIMESTAMP '1970-01-01 00:00:00' + ({value}) * INTERVAL '1 {unit}')"
        )),
    }
}
