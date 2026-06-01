use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringViewBuilder;
use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{Array, ArrayRef, IntervalMonthDayNanoArray};
use arrow_schema::{DataType, IntervalUnit};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

const NANOS_PER_MICRO: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

/// PostgreSQL-compatible text output for finite `interval` values represented
/// as Arrow `Interval(MonthDayNano)`.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgIntervalOut {
    signature: Signature,
}

impl PgIntervalOut {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Interval(IntervalUnit::MonthDayNano)],
                Volatility::Stable,
            ),
        }
    }
}

impl Default for PgIntervalOut {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_interval_out_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgIntervalOut::new()))
}

impl ScalarUDFImpl for PgIntervalOut {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pg_fusion_interval_out"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Interval(IntervalUnit::MonthDayNano)] => Ok(DataType::Utf8View),
            _ => plan_err!("pg_fusion_interval_out expects one interval argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("pg_fusion_interval_out expects one interval argument");
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(value)) => {
                let value = value.map(format_interval).transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(value)))
            }
            ColumnarValue::Scalar(_) => {
                exec_err!("pg_fusion_interval_out expects one interval argument")
            }
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(format_interval_array(array)?)),
        }
    }
}

fn format_interval_array(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "pg_fusion_interval_out expects Interval(MonthDayNano) array".into(),
            )
        })?;
    let mut builder = StringViewBuilder::new();
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(format_interval(values.value(row))?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_interval(
    value: <IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native,
) -> Result<String> {
    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(value);
    if nanos % NANOS_PER_MICRO != 0 {
        return exec_err!(
            "pg_fusion_interval_out cannot format interval with sub-microsecond precision"
        );
    }
    Ok(format_interval_parts(months, days, nanos / NANOS_PER_MICRO))
}

fn format_interval_parts(months: i32, days: i32, micros: i64) -> String {
    let mut parts = Vec::new();
    let years = months / 12;
    let remaining_months = months % 12;
    if years != 0 {
        parts.push(format_unit(i64::from(years), "year", "years"));
    }
    if remaining_months != 0 {
        parts.push(format_unit(i64::from(remaining_months), "mon", "mons"));
    }
    if days != 0 {
        parts.push(format_unit(i64::from(days), "day", "days"));
    }
    if micros != 0 || parts.is_empty() {
        parts.push(format_time(micros));
    }
    parts.join(" ")
}

fn format_unit(value: i64, singular: &str, plural: &str) -> String {
    let suffix = if value.abs() == 1 { singular } else { plural };
    format!("{value} {suffix}")
}

fn format_time(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs = micros.abs();
    let hours = abs / MICROS_PER_HOUR;
    let minutes = (abs % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let seconds = (abs % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let fraction = abs % MICROS_PER_SECOND;
    if fraction == 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        let mut fraction = format!("{fraction:06}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_postgres_style_interval_text() {
        let value = IntervalMonthDayNanoType::make_value(1, 0, 8 * MICROS_PER_HOUR * 1_000);
        assert_eq!(format_interval(value).unwrap(), "1 mon 08:00:00");

        let value = IntervalMonthDayNanoType::make_value(0, 1, 0);
        assert_eq!(format_interval(value).unwrap(), "1 day");

        let value = IntervalMonthDayNanoType::make_value(0, 0, 0);
        assert_eq!(format_interval(value).unwrap(), "00:00:00");
    }

    #[test]
    fn formats_interval_arrays() {
        let values = Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
            IntervalMonthDayNanoType::make_value(2, 0, 0),
        )])) as ArrayRef;
        let formatted = format_interval_array(&values).unwrap();
        let formatted = formatted
            .as_any()
            .downcast_ref::<arrow_array::StringViewArray>()
            .unwrap();
        assert_eq!(formatted.value(0), "2 mons");
    }
}
