use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

const NUMERIC_AVG_PRECISION: u8 = 38;
const NUMERIC_AVG_SCALE: i8 = 16;
const INT_AVG_SUM_PRECISION: u8 = 76;
const INT_AVG_SUM_SCALE: i8 = 0;
#[cfg(test)]
const INT_AVG_SCALE_FACTOR: i128 = 10_000_000_000_000_000;
const DECIMAL_AVG_SUM_PRECISION: u8 = 76;

/// PostgreSQL-compatible AVG aggregate for the type surface pg_fusion supports.
#[derive(Debug)]
pub struct PgAvg {
    signature: Signature,
}

impl PgAvg {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for PgAvg {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_avg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(PgAvg::new()))
}

impl AggregateUDFImpl for PgAvg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match single_arg_type(self.name(), arg_types)? {
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, _) => Ok(
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ),
            DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
            other => exec_err!("{} does not support {other:?}", self.name()),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return exec_err!("avg(DISTINCT) aggregations are not available");
        }

        let Some(expr) = acc_args.exprs.first() else {
            return exec_err!("avg expects one input expression");
        };
        let data_type = expr.data_type(acc_args.schema)?;
        match (&data_type, acc_args.return_type) {
            (
                DataType::Int16 | DataType::Int32 | DataType::Int64,
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => Ok(Box::<IntegerAvgAccumulator>::default()),
            (
                DataType::Decimal128(precision, scale),
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => Ok(Box::new(DecimalAvgAccumulator::try_new(
                *precision, *scale,
            )?)),
            (DataType::Float64, DataType::Float64) => Ok(Box::<FloatAvgAccumulator>::default()),
            _ => exec_err!(
                "{} accumulator does not support ({} -> {})",
                self.name(),
                data_type,
                acc_args.return_type
            ),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let input_type = single_arg_type(self.name(), args.input_types)?;
        let sum_type = match (input_type, args.return_type) {
            (
                DataType::Int16 | DataType::Int32 | DataType::Int64,
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => DataType::Decimal256(INT_AVG_SUM_PRECISION, INT_AVG_SUM_SCALE),
            (
                DataType::Decimal128(_, scale),
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => DataType::Decimal256(DECIMAL_AVG_SUM_PRECISION, decimal_avg_state_scale(*scale)),
            (DataType::Float64, DataType::Float64) => DataType::Float64,
            _ => {
                return exec_err!(
                    "{} state does not support ({} -> {})",
                    self.name(),
                    input_type,
                    args.return_type
                )
            }
        };

        Ok(vec![
            Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(format_state_name(args.name, "sum"), sum_type, true),
        ])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let arg_type = single_arg_type(self.name(), arg_types)?;
        let coerced = match arg_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => arg_type.clone(),
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, _) => arg_type.clone(),
            other => return exec_err!("{} does not support inputs of type {other:?}", self.name()),
        };
        Ok(vec![coerced])
    }
}

fn single_arg_type<'a>(name: &str, arg_types: &'a [DataType]) -> Result<&'a DataType> {
    if arg_types.len() != 1 {
        return exec_err!("{name} expects exactly one argument");
    }
    Ok(&arg_types[0])
}

#[derive(Debug, Default)]
struct IntegerAvgAccumulator {
    sum: i256,
    count: u64,
}

impl IntegerAvgAccumulator {
    fn add_i256(&mut self, value: i256) -> Result<()> {
        self.sum = self.sum.checked_add(value).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer transition sum overflowed i256".to_owned(),
            )
        })?;
        self.count = self.count.checked_add(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer transition count overflowed u64".to_owned(),
            )
        })?;
        Ok(())
    }

    fn merge_state(&mut self, count: u64, sum: i256) -> Result<()> {
        self.sum = self.sum.checked_add(sum).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer merged sum overflowed i256".to_owned(),
            )
        })?;
        self.count = self.count.checked_add(count).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer merged count overflowed u64".to_owned(),
            )
        })?;
        Ok(())
    }
}

impl Accumulator for IntegerAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };

        match values.data_type() {
            DataType::Int16 => {
                for value in values.as_primitive::<Int16Type>().iter().flatten() {
                    self.add_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            DataType::Int32 => {
                for value in values.as_primitive::<Int32Type>().iter().flatten() {
                    self.add_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            DataType::Int64 => {
                for value in values.as_primitive::<Int64Type>().iter().flatten() {
                    self.add_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            other => return exec_err!("avg integer accumulator got {other:?}"),
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Decimal128(
            scaled_integer_average(self.sum, self.count)?,
            NUMERIC_AVG_PRECISION,
            NUMERIC_AVG_SCALE,
        ))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Decimal256(Some(self.sum), INT_AVG_SUM_PRECISION, INT_AVG_SUM_SCALE),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return exec_err!("avg integer merge expects count and sum states");
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let sums = states[1].as_primitive::<Decimal256Type>();
        if counts.len() != sums.len() {
            return exec_err!("avg integer merge state arrays have different lengths");
        }

        for row in 0..counts.len() {
            if counts.is_null(row) || sums.is_null(row) {
                continue;
            }
            self.merge_state(counts.value(row), sums.value(row))?;
        }

        Ok(())
    }
}

fn scaled_integer_average(sum: i256, count: u64) -> Result<Option<i128>> {
    scaled_average(sum, count, INT_AVG_SUM_SCALE, NUMERIC_AVG_SCALE, "integer")
}

fn scaled_average(
    sum: i256,
    count: u64,
    sum_scale: i8,
    target_scale: i8,
    label: &str,
) -> Result<Option<i128>> {
    if count == 0 {
        return Ok(None);
    }

    let mut scaled_sum = sum;
    let mut divisor = i256::from_i128(i128::from(count));
    if target_scale >= sum_scale {
        let factor = ten_pow_i256((target_scale - sum_scale) as u32)?;
        scaled_sum = scaled_sum.checked_mul(factor).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "avg {label} final scaling overflowed i256"
            ))
        })?;
    } else {
        let factor = ten_pow_i256((sum_scale - target_scale) as u32)?;
        divisor = divisor.checked_mul(factor).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "avg {label} final divisor overflowed i256"
            ))
        })?;
    }

    let mut quotient = scaled_sum.checked_div(divisor).ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(format!("avg {label} division failed"))
    })?;
    let remainder = scaled_sum.checked_rem(divisor).ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(format!("avg {label} remainder failed"))
    })?;

    let doubled_abs_remainder = remainder
        .wrapping_abs()
        .checked_mul(i256::from_i128(2))
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "avg {label} rounding overflowed i256"
            ))
        })?;
    if doubled_abs_remainder >= divisor {
        let adjustment = if scaled_sum.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        quotient = quotient.checked_add(adjustment).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "avg {label} rounded result overflowed i256"
            ))
        })?;
    }

    quotient.to_i128().map(Some).ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(format!(
            "avg {label} result does not fit Decimal128(38, 16)"
        ))
    })
}

fn ten_pow_i256(power: u32) -> Result<i256> {
    let mut value = i256::ONE;
    for _ in 0..power {
        value = value.checked_mul(i256::from_i128(10)).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "decimal scale factor overflowed i256".to_owned(),
            )
        })?;
    }
    Ok(value)
}

fn decimal_avg_state_scale(input_scale: i8) -> i8 {
    input_scale.max(NUMERIC_AVG_SCALE)
}

#[derive(Debug)]
struct DecimalAvgAccumulator {
    sum: i256,
    count: u64,
    input_precision: u8,
    input_scale: i8,
    state_scale: i8,
}

impl DecimalAvgAccumulator {
    fn try_new(input_precision: u8, input_scale: i8) -> Result<Self> {
        let state_scale = decimal_avg_state_scale(input_scale);
        let scale_delta = state_scale.checked_sub(input_scale).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal input scale overflowed i8".to_owned(),
            )
        })?;
        if scale_delta < 0 {
            return exec_err!(
                "avg decimal state scale {state_scale} is smaller than input scale {input_scale}"
            );
        }
        Ok(Self {
            sum: i256::ZERO,
            count: 0,
            input_precision,
            input_scale,
            state_scale,
        })
    }

    fn add_decimal128(&mut self, value: i128) -> Result<()> {
        let scale_delta = (self.state_scale - self.input_scale) as u32;
        let scaled = i256::from_i128(value)
            .checked_mul(ten_pow_i256(scale_delta)?)
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg decimal transition scaling overflowed i256".to_owned(),
                )
            })?;
        self.sum = self.sum.checked_add(scaled).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal transition sum overflowed i256".to_owned(),
            )
        })?;
        self.count = self.count.checked_add(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal transition count overflowed u64".to_owned(),
            )
        })?;
        Ok(())
    }

    fn merge_state(&mut self, count: u64, sum: i256) -> Result<()> {
        self.sum = self.sum.checked_add(sum).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal merged sum overflowed i256".to_owned(),
            )
        })?;
        self.count = self.count.checked_add(count).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal merged count overflowed u64".to_owned(),
            )
        })?;
        Ok(())
    }
}

impl Accumulator for DecimalAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };

        match values.data_type() {
            DataType::Decimal128(precision, scale)
                if *precision == self.input_precision && *scale == self.input_scale =>
            {
                for value in values.as_primitive::<Decimal128Type>().iter().flatten() {
                    self.add_decimal128(value)?;
                }
            }
            other => return exec_err!("avg decimal accumulator got {other:?}"),
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Decimal128(
            scaled_average(
                self.sum,
                self.count,
                self.state_scale,
                NUMERIC_AVG_SCALE,
                "decimal",
            )?,
            NUMERIC_AVG_PRECISION,
            NUMERIC_AVG_SCALE,
        ))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Decimal256(Some(self.sum), DECIMAL_AVG_SUM_PRECISION, self.state_scale),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return exec_err!("avg decimal merge expects count and sum states");
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let sums = states[1].as_primitive::<Decimal256Type>();
        if counts.len() != sums.len() {
            return exec_err!("avg decimal merge state arrays have different lengths");
        }
        match sums.data_type() {
            DataType::Decimal256(DECIMAL_AVG_SUM_PRECISION, scale)
                if *scale == self.state_scale => {}
            other => return exec_err!("avg decimal merge got sum state {other:?}"),
        }

        for row in 0..counts.len() {
            if counts.is_null(row) || sums.is_null(row) {
                continue;
            }
            self.merge_state(counts.value(row), sums.value(row))?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
struct FloatAvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for FloatAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        let values = values.as_primitive::<Float64Type>();
        for value in values.iter().flatten() {
            self.sum = Some(self.sum.unwrap_or(0.0) + value);
            self.count = self.count.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float transition count overflowed u64".to_owned(),
                )
            })?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum.map(|sum| sum / self.count as f64),
        ))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float64(self.sum),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return exec_err!("avg float merge expects count and sum states");
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let sums = states[1].as_primitive::<Float64Type>();
        if counts.len() != sums.len() {
            return exec_err!("avg float merge state arrays have different lengths");
        }

        for row in 0..counts.len() {
            if counts.is_null(row) || sums.is_null(row) {
                continue;
            }
            self.count = self.count.checked_add(counts.value(row)).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float merged count overflowed u64".to_owned(),
                )
            })?;
            self.sum = Some(self.sum.unwrap_or(0.0) + sums.value(row));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Decimal128Array, Float64Array, Int32Array, Int64Array};

    fn decimal128_value(value: ScalarValue) -> i128 {
        match value {
            ScalarValue::Decimal128(Some(value), NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE) => value,
            other => panic!("unexpected scalar value {other:?}"),
        }
    }

    fn decimal128_array(
        values: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Arc<Decimal128Array> {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    #[test]
    fn integer_avg_returns_scaled_decimal() {
        let mut acc = IntegerAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Int32Array::from(vec![Some(1), Some(2)]))])
            .unwrap();

        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            INT_AVG_SCALE_FACTOR + INT_AVG_SCALE_FACTOR / 2
        );
    }

    #[test]
    fn integer_avg_preserves_values_above_f64_exact_range() {
        let mut acc = IntegerAvgAccumulator::default();
        let base = 9_007_199_254_740_992_i64;
        acc.update_batch(&[Arc::new(Int64Array::from(vec![base, base + 1]))])
            .unwrap();

        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            i128::from(base) * INT_AVG_SCALE_FACTOR + INT_AVG_SCALE_FACTOR / 2
        );
    }

    #[test]
    fn integer_avg_rounds_negative_half_away_from_zero() {
        let mut acc = IntegerAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Int32Array::from(vec![Some(-1), Some(0)]))])
            .unwrap();

        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            -(INT_AVG_SCALE_FACTOR / 2)
        );
    }

    #[test]
    fn integer_avg_ignores_nulls_and_returns_null_for_empty_input() {
        let mut acc = IntegerAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Int32Array::from(vec![None, None]))])
            .unwrap();

        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Decimal128(None, NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE)
        );
    }

    #[test]
    fn float_avg_stays_float64() {
        let mut acc = FloatAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)]))])
            .unwrap();

        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(1.5)));
    }

    #[test]
    fn decimal_avg_returns_scaled_decimal() {
        let mut acc = DecimalAvgAccumulator::try_new(10, 1).unwrap();
        acc.update_batch(&[decimal128_array(vec![Some(25), Some(35)], 10, 1)])
            .unwrap();

        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            3 * INT_AVG_SCALE_FACTOR
        );
    }

    #[test]
    fn decimal_avg_ignores_nulls_and_returns_null_for_empty_input() {
        let mut acc = DecimalAvgAccumulator::try_new(10, 2).unwrap();
        acc.update_batch(&[decimal128_array(vec![None, None], 10, 2)])
            .unwrap();

        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Decimal128(None, NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE)
        );
    }

    #[test]
    fn decimal_avg_rounds_half_away_from_zero() {
        let mut positive = DecimalAvgAccumulator::try_new(38, 17).unwrap();
        positive
            .update_batch(&[decimal128_array(vec![Some(5)], 38, 17)])
            .unwrap();
        assert_eq!(decimal128_value(positive.evaluate().unwrap()), 1);

        let mut negative = DecimalAvgAccumulator::try_new(38, 17).unwrap();
        negative
            .update_batch(&[decimal128_array(vec![Some(-5)], 38, 17)])
            .unwrap();
        assert_eq!(decimal128_value(negative.evaluate().unwrap()), -1);
    }

    #[test]
    fn decimal_avg_merge_combines_partial_states() {
        let mut left = DecimalAvgAccumulator::try_new(10, 1).unwrap();
        left.update_batch(&[decimal128_array(vec![Some(15), Some(25)], 10, 1)])
            .unwrap();
        let state = left.state().unwrap();

        let counts = state[0].to_array().unwrap();
        let sums = state[1].to_array().unwrap();
        let mut merged = DecimalAvgAccumulator::try_new(10, 1).unwrap();
        merged.merge_batch(&[counts, sums]).unwrap();

        assert_eq!(
            decimal128_value(merged.evaluate().unwrap()),
            2 * INT_AVG_SCALE_FACTOR
        );
    }

    #[test]
    fn pg_avg_type_rules_match_pg_integer_float_and_decimal_surface() {
        let avg = PgAvg::new();

        assert_eq!(
            avg.return_type(&[DataType::Int32]).unwrap(),
            DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE)
        );
        assert_eq!(
            avg.coerce_types(&[DataType::Float32]).unwrap(),
            vec![DataType::Float64]
        );
        assert_eq!(
            avg.return_type(&[DataType::Float64]).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            avg.return_type(&[DataType::Decimal128(10, 3)]).unwrap(),
            DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE)
        );
        assert_eq!(
            avg.coerce_types(&[DataType::Decimal128(10, 3)]).unwrap(),
            vec![DataType::Decimal128(10, 3)]
        );
        assert!(avg.return_type(&[DataType::Decimal256(76, 16)]).is_err());
    }

    #[test]
    fn integer_avg_merge_combines_partial_states() {
        let mut left = IntegerAvgAccumulator::default();
        left.update_batch(&[Arc::new(Int32Array::from(vec![1, 2]))])
            .unwrap();
        let state = left.state().unwrap();

        let counts = state[0].to_array().unwrap();
        let sums = state[1].to_array().unwrap();
        let mut merged = IntegerAvgAccumulator::default();
        merged.merge_batch(&[counts, sums]).unwrap();

        assert_eq!(
            decimal128_value(merged.evaluate().unwrap()),
            INT_AVG_SCALE_FACTOR + INT_AVG_SCALE_FACTOR / 2
        );
    }
}
