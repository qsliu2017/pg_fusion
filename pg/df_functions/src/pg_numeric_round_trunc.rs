use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Decimal128Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum NumericRoundTruncOp {
    Round,
    Trunc,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgNumericRoundTrunc {
    name: &'static str,
    op: NumericRoundTruncOp,
    signature: Signature,
}

impl PgNumericRoundTrunc {
    fn new(name: &'static str, op: NumericRoundTruncOp) -> Self {
        Self {
            name,
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

pub fn pg_numeric_round_scale_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgNumericRoundTrunc::new(
        "pg_fusion_numeric_round_scale",
        NumericRoundTruncOp::Round,
    )))
}

pub fn pg_numeric_trunc_scale_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgNumericRoundTrunc::new(
        "pg_fusion_numeric_trunc_scale",
        NumericRoundTruncOp::Trunc,
    )))
}

impl ScalarUDFImpl for PgNumericRoundTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        numeric_round_trunc_return_type(self.name, arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let value_type = numeric_round_trunc_return_type(self.name, arg_types)?;
        Ok(vec![value_type, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("{} expects exactly two arguments", self.name);
        }

        if let [ColumnarValue::Scalar(value), ColumnarValue::Scalar(decimal_places)] =
            &args.args[..]
        {
            return scalar_round_trunc(self.op, value, decimal_places);
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let DataType::Decimal128(precision, scale) = arrays[0].data_type() else {
            return exec_err!("{} expected Decimal128 value array", self.name);
        };
        let value = arrays[0]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "{} expected Decimal128 value array",
                    self.name
                ))
            })?;
        let mut output = Vec::with_capacity(value.len());
        for row in 0..value.len() {
            if value.is_null(row) || arrays[1].is_null(row) {
                output.push(None);
                continue;
            }
            let decimal_places = array_decimal_places(&arrays[1], row)?;
            output.push(Some(round_trunc_decimal128(
                self.op,
                value.value(row),
                *scale,
                decimal_places,
            )?));
        }
        let array = Decimal128Array::from(output)
            .with_precision_and_scale(*precision, *scale)
            .map_err(|err| datafusion_common::DataFusionError::ArrowError(Box::new(err), None))?;
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

fn numeric_round_trunc_return_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 2 {
        return plan_err!("{name} expects exactly two arguments");
    }
    match (&arg_types[0], &arg_types[1]) {
        (DataType::Decimal128(_, _), DataType::Int64 | DataType::Int32) => Ok(arg_types[0].clone()),
        _ => plan_err!("{name} expects Decimal128 and int4/int8 arguments"),
    }
}

fn scalar_round_trunc(
    op: NumericRoundTruncOp,
    value: &ScalarValue,
    decimal_places: &ScalarValue,
) -> Result<ColumnarValue> {
    let (value, precision, scale) = match value {
        ScalarValue::Decimal128(value, precision, scale) => (value, *precision, *scale),
        _ => return exec_err!("numeric round/trunc expected Decimal128 scalar"),
    };
    let decimal_places = scalar_decimal_places(decimal_places)?;
    let value = match (value, decimal_places) {
        (Some(value), Some(decimal_places)) => {
            Some(round_trunc_decimal128(op, *value, scale, decimal_places)?)
        }
        _ => None,
    };
    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
        value, precision, scale,
    )))
}

fn scalar_decimal_places(value: &ScalarValue) -> Result<Option<i64>> {
    match value {
        ScalarValue::Int64(value) => Ok(*value),
        ScalarValue::Int32(value) => Ok(value.map(i64::from)),
        _ => exec_err!("numeric round/trunc expected int4/int8 decimal places"),
    }
}

fn array_decimal_places(array: &ArrayRef, row: usize) -> Result<i64> {
    match array.data_type() {
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "numeric round/trunc expected Int64 decimal places array".into(),
                )
            })?;
            Ok(array.value(row))
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "numeric round/trunc expected Int32 decimal places array".into(),
                )
            })?;
            Ok(i64::from(array.value(row)))
        }
        _ => exec_err!("numeric round/trunc expected int4/int8 decimal places array"),
    }
}

fn round_trunc_decimal128(
    op: NumericRoundTruncOp,
    value: i128,
    value_scale: i8,
    decimal_places: i64,
) -> Result<i128> {
    let exponent = i64::from(value_scale) - decimal_places;
    if exponent <= 0 {
        return Ok(value);
    }
    if exponent > 38 {
        return Ok(0);
    }
    let factor = 10_i128
        .checked_pow(u32::try_from(exponent).expect("positive exponent fits u32"))
        .expect("10^38 fits i128");
    let quotient = value / factor;
    let remainder = value % factor;
    match op {
        NumericRoundTruncOp::Trunc => quotient.checked_mul(factor).ok_or_else(decimal_overflow),
        NumericRoundTruncOp::Round => {
            let twice_remainder = remainder
                .abs()
                .checked_mul(2)
                .ok_or_else(decimal_overflow)?;
            let adjustment = if twice_remainder >= factor {
                value.signum()
            } else {
                0
            };
            quotient
                .checked_add(adjustment)
                .and_then(|value| value.checked_mul(factor))
                .ok_or_else(decimal_overflow)
        }
    }
}

fn decimal_overflow() -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Execution(
        "numeric round/trunc result does not fit Decimal128".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    fn scalar_args(value: Option<i128>, decimal_places: Option<i64>) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(value, 38, 16)),
                ColumnarValue::Scalar(ScalarValue::Int64(decimal_places)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("value", DataType::Decimal128(38, 16), true)),
                Arc::new(Field::new("decimal_places", DataType::Int64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Decimal128(38, 16), true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn invoke_scalar(
        op: NumericRoundTruncOp,
        value: Option<i128>,
        decimal_places: Option<i64>,
    ) -> Option<i128> {
        let udf = PgNumericRoundTrunc::new("test_numeric_round_trunc", op);
        let result = udf
            .invoke_with_args(scalar_args(value, decimal_places))
            .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Decimal128(value, 38, 16)) = result else {
            panic!("numeric round/trunc should return Decimal128 scalar");
        };
        value
    }

    #[test]
    fn rounds_decimal128_half_away_from_zero() {
        assert_eq!(
            invoke_scalar(
                NumericRoundTruncOp::Round,
                Some(12_350_000_000_000_000),
                Some(2)
            ),
            Some(12_400_000_000_000_000)
        );
        assert_eq!(
            invoke_scalar(
                NumericRoundTruncOp::Round,
                Some(-12_350_000_000_000_000),
                Some(2)
            ),
            Some(-12_400_000_000_000_000)
        );
    }

    #[test]
    fn truncates_decimal128_toward_zero() {
        assert_eq!(
            invoke_scalar(
                NumericRoundTruncOp::Trunc,
                Some(12_345_000_000_000_000),
                Some(2)
            ),
            Some(12_300_000_000_000_000)
        );
        assert_eq!(
            invoke_scalar(
                NumericRoundTruncOp::Trunc,
                Some(-12_345_000_000_000_000),
                Some(2)
            ),
            Some(-12_300_000_000_000_000)
        );
    }

    #[test]
    fn preserves_nulls() {
        assert_eq!(
            invoke_scalar(NumericRoundTruncOp::Round, None, Some(2)),
            None
        );
        assert_eq!(
            invoke_scalar(
                NumericRoundTruncOp::Round,
                Some(12_345_000_000_000_000),
                None
            ),
            None
        );
    }

    #[test]
    fn rounds_arrays() {
        let udf = PgNumericRoundTrunc::new("test_numeric_round_trunc", NumericRoundTruncOp::Round);
        let values = Arc::new(
            Decimal128Array::from(vec![
                Some(12_345_000_000_000_000),
                Some(12_350_000_000_000_000),
                None,
            ])
            .with_precision_and_scale(38, 16)
            .unwrap(),
        ) as ArrayRef;
        let decimal_places =
            Arc::new(Int64Array::from(vec![Some(2), Some(2), Some(2)])) as ArrayRef;
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(values),
                    ColumnarValue::Array(decimal_places),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("value", DataType::Decimal128(38, 16), true)),
                    Arc::new(Field::new("decimal_places", DataType::Int64, true)),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new("result", DataType::Decimal128(38, 16), true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("numeric round/trunc should return array");
        };
        let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(result.value(0), 12_300_000_000_000_000);
        assert_eq!(result.value(1), 12_400_000_000_000_000);
        assert!(result.is_null(2));
    }
}
