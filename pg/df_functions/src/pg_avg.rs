use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem::size_of_val;
use std::sync::Arc;

use ahash::AHashSet;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float64Type, Int16Type, Int32Type, Int64Type,
    IntervalMonthDayNanoType, UInt64Type,
};
use arrow_array::{Array, ArrayRef};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, FieldRef, IntervalUnit};
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
const NANOS_PER_MICRO: i64 = 1_000;
const DAYS_PER_MONTH: f64 = 30.0;
const SECS_PER_DAY: f64 = 86_400.0;
const USECS_PER_SEC: f64 = 1_000_000.0;
const TS_PREC_INV: f64 = 1_000_000.0;

/// PostgreSQL-compatible AVG aggregate for the type surface pg_fusion supports.
#[derive(Debug, Eq, Hash, PartialEq)]
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
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            other => exec_err!("{} does not support {other:?}", self.name()),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(expr) = acc_args.exprs.first() else {
            return exec_err!("avg expects one input expression");
        };
        let data_type = expr.data_type(acc_args.schema)?;
        if acc_args.is_distinct {
            return DistinctAvgAccumulator::try_new(&data_type).map(|acc| Box::new(acc) as _);
        }
        match (&data_type, acc_args.return_type()) {
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
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ) => Ok(Box::<IntervalAvgAccumulator>::default()),
            _ => exec_err!(
                "{} accumulator does not support ({} -> {})",
                self.name(),
                data_type,
                acc_args.return_type()
            ),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = single_arg_field_type(self.name(), args.input_fields)?;
        if args.is_distinct {
            return Ok(vec![Arc::new(Field::new_list(
                format_state_name(args.name, "distinct_values"),
                Field::new_list_field(input_type.clone(), true),
                false,
            ))]);
        }

        let count_field = || {
            Arc::new(Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                true,
            ))
        };

        match (input_type, args.return_type()) {
            (
                DataType::Int16 | DataType::Int32 | DataType::Int64,
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => Ok(vec![
                count_field(),
                Arc::new(Field::new(
                    format_state_name(args.name, "sum"),
                    DataType::Decimal256(INT_AVG_SUM_PRECISION, INT_AVG_SUM_SCALE),
                    true,
                )),
            ]),
            (
                DataType::Decimal128(_, scale),
                DataType::Decimal128(NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE),
            ) => Ok(vec![
                count_field(),
                Arc::new(Field::new(
                    format_state_name(args.name, "sum"),
                    DataType::Decimal256(
                        DECIMAL_AVG_SUM_PRECISION,
                        decimal_avg_state_scale(*scale),
                    ),
                    true,
                )),
            ]),
            (DataType::Float64, DataType::Float64) => Ok(vec![
                count_field(),
                Arc::new(Field::new(
                    format_state_name(args.name, "finite_sum"),
                    DataType::Float64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "finite_sxx"),
                    DataType::Float64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "nan_count"),
                    DataType::UInt64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "pos_inf_count"),
                    DataType::UInt64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "neg_inf_count"),
                    DataType::UInt64,
                    true,
                )),
            ]),
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ) => Ok(vec![
                count_field(),
                Arc::new(Field::new(
                    format_state_name(args.name, "months"),
                    DataType::Int64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "days"),
                    DataType::Int64,
                    true,
                )),
                Arc::new(Field::new(
                    format_state_name(args.name, "time_micros"),
                    DataType::Int64,
                    true,
                )),
            ]),
            _ => exec_err!(
                "{} state does not support ({} -> {})",
                self.name(),
                input_type,
                args.return_type()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let arg_type = single_arg_type(self.name(), arg_types)?;
        let coerced = match arg_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => arg_type.clone(),
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, _) => arg_type.clone(),
            DataType::Interval(IntervalUnit::MonthDayNano) => arg_type.clone(),
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

fn single_arg_field_type<'a>(name: &str, fields: &'a [FieldRef]) -> Result<&'a DataType> {
    if fields.len() != 1 {
        return exec_err!("{name} expects exactly one argument");
    }
    Ok(fields[0].data_type())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FloatDistinctKey(u64);

impl FloatDistinctKey {
    const NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

    fn new(value: f64) -> Self {
        if value.is_nan() {
            Self(Self::NAN_BITS)
        } else if value == 0.0 {
            Self(0.0_f64.to_bits())
        } else {
            Self(value.to_bits())
        }
    }

    fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl Hash for FloatDistinctKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct IntervalDistinctKey {
    months: i32,
    days: i32,
    time_micros: i64,
}

impl IntervalDistinctKey {
    fn try_new(
        value: <IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native,
        context: &str,
    ) -> Result<Self> {
        let (months, days, time_micros) = interval_parts_to_pg_micros(value, context)?;
        Ok(Self {
            months,
            days,
            time_micros,
        })
    }

    fn native(
        self,
    ) -> Result<<IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native> {
        let nanos = self
            .time_micros
            .checked_mul(NANOS_PER_MICRO)
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg distinct interval state cannot be represented as Arrow interval"
                        .to_owned(),
                )
            })?;
        Ok(IntervalMonthDayNanoType::make_value(
            self.months,
            self.days,
            nanos,
        ))
    }
}

#[derive(Debug)]
enum DistinctAvgAccumulator {
    Int16(AHashSet<i16>),
    Int32(AHashSet<i32>),
    Int64(AHashSet<i64>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: AHashSet<i128>,
    },
    Float64(AHashSet<FloatDistinctKey>),
    IntervalMonthDayNano(AHashSet<IntervalDistinctKey>),
}

impl DistinctAvgAccumulator {
    fn try_new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Int16 => Ok(Self::Int16(AHashSet::new())),
            DataType::Int32 => Ok(Self::Int32(AHashSet::new())),
            DataType::Int64 => Ok(Self::Int64(AHashSet::new())),
            DataType::Decimal128(precision, scale) => Ok(Self::Decimal128 {
                precision: *precision,
                scale: *scale,
                values: AHashSet::new(),
            }),
            DataType::Float64 => Ok(Self::Float64(AHashSet::new())),
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Ok(Self::IntervalMonthDayNano(AHashSet::new()))
            }
            other => exec_err!("avg(DISTINCT) does not support {other:?}"),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            Self::Float64(_) => DataType::Float64,
            Self::IntervalMonthDayNano(_) => DataType::Interval(IntervalUnit::MonthDayNano),
        }
    }

    fn state_values(&self) -> Result<Vec<ScalarValue>> {
        match self {
            Self::Int16(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                Ok(values
                    .into_iter()
                    .map(|value| ScalarValue::Int16(Some(value)))
                    .collect())
            }
            Self::Int32(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                Ok(values
                    .into_iter()
                    .map(|value| ScalarValue::Int32(Some(value)))
                    .collect())
            }
            Self::Int64(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                Ok(values
                    .into_iter()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect())
            }
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                Ok(values
                    .into_iter()
                    .map(|value| ScalarValue::Decimal128(Some(value), *precision, *scale))
                    .collect())
            }
            Self::Float64(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                Ok(values
                    .into_iter()
                    .map(|value| ScalarValue::Float64(Some(value.value())))
                    .collect())
            }
            Self::IntervalMonthDayNano(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                values
                    .into_iter()
                    .map(|value| {
                        value
                            .native()
                            .map(|value| ScalarValue::IntervalMonthDayNano(Some(value)))
                    })
                    .collect()
            }
        }
    }

    fn insert_batch(&mut self, values: &ArrayRef, context: &str) -> Result<()> {
        match (self, values.data_type()) {
            (Self::Int16(seen), DataType::Int16) => {
                for value in values.as_primitive::<Int16Type>().iter().flatten() {
                    seen.insert(value);
                }
            }
            (Self::Int32(seen), DataType::Int32) => {
                for value in values.as_primitive::<Int32Type>().iter().flatten() {
                    seen.insert(value);
                }
            }
            (Self::Int64(seen), DataType::Int64) => {
                for value in values.as_primitive::<Int64Type>().iter().flatten() {
                    seen.insert(value);
                }
            }
            (
                Self::Decimal128 {
                    precision,
                    scale,
                    values: seen,
                },
                DataType::Decimal128(value_precision, value_scale),
            ) if precision == value_precision && scale == value_scale => {
                for value in values.as_primitive::<Decimal128Type>().iter().flatten() {
                    seen.insert(value);
                }
            }
            (Self::Float64(seen), DataType::Float64) => {
                for value in values.as_primitive::<Float64Type>().iter().flatten() {
                    seen.insert(FloatDistinctKey::new(value));
                }
            }
            (Self::IntervalMonthDayNano(seen), DataType::Interval(IntervalUnit::MonthDayNano)) => {
                for value in values
                    .as_primitive::<IntervalMonthDayNanoType>()
                    .iter()
                    .flatten()
                {
                    seen.insert(IntervalDistinctKey::try_new(value, context)?);
                }
            }
            (_, other) => return exec_err!("avg(DISTINCT) accumulator got {other:?}"),
        }
        Ok(())
    }
}

impl Accumulator for DistinctAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        self.insert_batch(values, "transition")
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self {
            Self::Int16(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = IntegerAvgAccumulator::default();
                for value in values {
                    acc.add_i256(i256::from_i128(i128::from(value)))?;
                }
                acc.evaluate()
            }
            Self::Int32(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = IntegerAvgAccumulator::default();
                for value in values {
                    acc.add_i256(i256::from_i128(i128::from(value)))?;
                }
                acc.evaluate()
            }
            Self::Int64(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = IntegerAvgAccumulator::default();
                for value in values {
                    acc.add_i256(i256::from_i128(i128::from(value)))?;
                }
                acc.evaluate()
            }
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = DecimalAvgAccumulator::try_new(*precision, *scale)?;
                for value in values {
                    acc.add_decimal128(value)?;
                }
                acc.evaluate()
            }
            Self::Float64(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = FloatAvgAccumulator::default();
                for value in values {
                    acc.add_value(value.value())?;
                }
                acc.evaluate()
            }
            Self::IntervalMonthDayNano(values) => {
                let mut values = values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                let mut acc = IntervalAvgAccumulator::default();
                for value in values {
                    acc.add_interval(value.native()?)?;
                }
                acc.evaluate()
            }
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Int16(values) => size_of_val(self) + values.capacity() * size_of_val(&0_i16),
            Self::Int32(values) => size_of_val(self) + values.capacity() * size_of_val(&0_i32),
            Self::Int64(values) => size_of_val(self) + values.capacity() * size_of_val(&0_i64),
            Self::Decimal128 { values, .. } => {
                size_of_val(self) + values.capacity() * size_of_val(&0_i128)
            }
            Self::Float64(values) => {
                size_of_val(self) + values.capacity() * size_of_val(&FloatDistinctKey(0))
            }
            Self::IntervalMonthDayNano(values) => {
                size_of_val(self)
                    + values.capacity()
                        * size_of_val(&IntervalDistinctKey {
                            months: 0,
                            days: 0,
                            time_micros: 0,
                        })
            }
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &self.state_values()?,
            &self.data_type(),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 1 {
            return exec_err!("avg(DISTINCT) merge expects one distinct-values state");
        }
        for values in states[0].as_list::<i32>().iter().flatten() {
            self.insert_batch(&values, "merge")?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        exec_err!("avg(DISTINCT) does not support window retraction")
    }

    fn supports_retract_batch(&self) -> bool {
        false
    }
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

    fn retract_i256(&mut self, value: i256) -> Result<()> {
        let next_sum = self.sum.checked_sub(value).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer retraction sum overflowed i256".to_owned(),
            )
        })?;
        let next_count = self.count.checked_sub(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg integer retraction count underflowed u64".to_owned(),
            )
        })?;
        self.sum = next_sum;
        self.count = next_count;
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

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };

        match values.data_type() {
            DataType::Int16 => {
                for value in values.as_primitive::<Int16Type>().iter().flatten() {
                    self.retract_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            DataType::Int32 => {
                for value in values.as_primitive::<Int32Type>().iter().flatten() {
                    self.retract_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            DataType::Int64 => {
                for value in values.as_primitive::<Int64Type>().iter().flatten() {
                    self.retract_i256(i256::from_i128(i128::from(value)))?;
                }
            }
            other => return exec_err!("avg integer accumulator got {other:?}"),
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
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

    fn retract_decimal128(&mut self, value: i128) -> Result<()> {
        let scale_delta = (self.state_scale - self.input_scale) as u32;
        let scaled = i256::from_i128(value)
            .checked_mul(ten_pow_i256(scale_delta)?)
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg decimal retraction scaling overflowed i256".to_owned(),
                )
            })?;
        let next_sum = self.sum.checked_sub(scaled).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal retraction sum overflowed i256".to_owned(),
            )
        })?;
        let next_count = self.count.checked_sub(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg decimal retraction count underflowed u64".to_owned(),
            )
        })?;
        self.sum = next_sum;
        self.count = next_count;
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

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };

        match values.data_type() {
            DataType::Decimal128(precision, scale)
                if *precision == self.input_precision && *scale == self.input_scale =>
            {
                for value in values.as_primitive::<Decimal128Type>().iter().flatten() {
                    self.retract_decimal128(value)?;
                }
            }
            other => return exec_err!("avg decimal accumulator got {other:?}"),
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
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
struct IntervalAvgAccumulator {
    months: i64,
    days: i64,
    time_micros: i64,
    count: u64,
}

impl IntervalAvgAccumulator {
    fn add_interval(
        &mut self,
        value: <IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native,
    ) -> Result<()> {
        let (months, days, time_micros) = interval_parts_to_pg_micros(value, "transition")?;
        let next_months = self.months.checked_add(i64::from(months)).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval transition month sum overflowed i64".to_owned(),
            )
        })?;
        let next_days = self.days.checked_add(i64::from(days)).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval transition day sum overflowed i64".to_owned(),
            )
        })?;
        let next_time_micros = self.time_micros.checked_add(time_micros).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval transition time sum overflowed i64".to_owned(),
            )
        })?;
        let next_count = self.count.checked_add(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval transition count overflowed u64".to_owned(),
            )
        })?;
        validate_pg_finite_interval(next_months, next_days, next_time_micros, "transition")?;
        self.months = next_months;
        self.days = next_days;
        self.time_micros = next_time_micros;
        self.count = next_count;
        Ok(())
    }

    fn retract_interval(
        &mut self,
        value: <IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native,
    ) -> Result<()> {
        let (months, days, time_micros) = interval_parts_to_pg_micros(value, "retraction")?;
        let next_count = self.count.checked_sub(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval retraction count underflowed u64".to_owned(),
            )
        })?;
        if next_count == 0 {
            self.months = 0;
            self.days = 0;
            self.time_micros = 0;
            self.count = 0;
            return Ok(());
        }

        let next_months = self.months.checked_sub(i64::from(months)).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval retraction month sum overflowed i64".to_owned(),
            )
        })?;
        let next_days = self.days.checked_sub(i64::from(days)).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval retraction day sum overflowed i64".to_owned(),
            )
        })?;
        let next_time_micros = self.time_micros.checked_sub(time_micros).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval retraction time sum overflowed i64".to_owned(),
            )
        })?;
        validate_pg_finite_interval(next_months, next_days, next_time_micros, "retraction")?;
        self.months = next_months;
        self.days = next_days;
        self.time_micros = next_time_micros;
        self.count = next_count;
        Ok(())
    }

    fn merge_state(&mut self, count: u64, months: i64, days: i64, time_micros: i64) -> Result<()> {
        validate_pg_finite_interval(months, days, time_micros, "merge input")?;
        let next_months = self.months.checked_add(months).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval merged month sum overflowed i64".to_owned(),
            )
        })?;
        let next_days = self.days.checked_add(days).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval merged day sum overflowed i64".to_owned(),
            )
        })?;
        let next_time_micros = self.time_micros.checked_add(time_micros).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval merged time sum overflowed i64".to_owned(),
            )
        })?;
        let next_count = self.count.checked_add(count).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg interval merged count overflowed u64".to_owned(),
            )
        })?;
        validate_pg_finite_interval(next_months, next_days, next_time_micros, "merge")?;
        self.months = next_months;
        self.days = next_days;
        self.time_micros = next_time_micros;
        self.count = next_count;
        Ok(())
    }
}

impl Accumulator for IntervalAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        let values = values.as_primitive::<IntervalMonthDayNanoType>();
        for value in values.iter().flatten() {
            self.add_interval(value)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        let values = values.as_primitive::<IntervalMonthDayNanoType>();
        for value in values.iter().flatten() {
            self.retract_interval(value)?;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.count == 0 {
            None
        } else {
            Some(pg_interval_div(
                self.months,
                self.days,
                self.time_micros,
                self.count,
            )?)
        };
        Ok(ScalarValue::IntervalMonthDayNano(value))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Int64(Some(self.months)),
            ScalarValue::Int64(Some(self.days)),
            ScalarValue::Int64(Some(self.time_micros)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 4 {
            return exec_err!("avg interval merge expects count, month, day, and time states");
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let months = states[1].as_primitive::<Int64Type>();
        let days = states[2].as_primitive::<Int64Type>();
        let time_micros = states[3].as_primitive::<Int64Type>();
        if counts.len() != months.len()
            || counts.len() != days.len()
            || counts.len() != time_micros.len()
        {
            return exec_err!("avg interval merge state arrays have different lengths");
        }

        for row in 0..counts.len() {
            if counts.is_null(row)
                || months.is_null(row)
                || days.is_null(row)
                || time_micros.is_null(row)
            {
                continue;
            }
            self.merge_state(
                counts.value(row),
                months.value(row),
                days.value(row),
                time_micros.value(row),
            )?;
        }

        Ok(())
    }
}

fn interval_parts_to_pg_micros(
    value: <IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native,
    context: &str,
) -> Result<(i32, i32, i64)> {
    let (months, days, nanoseconds) = IntervalMonthDayNanoType::to_parts(value);
    if nanoseconds % NANOS_PER_MICRO != 0 {
        return Err(datafusion_common::DataFusionError::Execution(format!(
            "avg interval {context} value has nanosecond precision not representable by PostgreSQL interval"
        )));
    }
    let time_micros = nanoseconds / NANOS_PER_MICRO;
    if pg_interval_is_infinite(i64::from(months), i64::from(days), time_micros) {
        return Err(datafusion_common::DataFusionError::Execution(format!(
            "avg interval {context} value is PostgreSQL interval infinity, which pg_fusion does not support"
        )));
    }
    Ok((months, days, time_micros))
}

fn validate_pg_finite_interval(
    months: i64,
    days: i64,
    time_micros: i64,
    context: &str,
) -> Result<()> {
    if pg_interval_is_infinite(months, days, time_micros) {
        return Err(datafusion_common::DataFusionError::Execution(format!(
            "avg interval {context} state is PostgreSQL interval infinity, which pg_fusion does not support"
        )));
    }
    if months < i64::from(i32::MIN)
        || months > i64::from(i32::MAX)
        || days < i64::from(i32::MIN)
        || days > i64::from(i32::MAX)
    {
        return Err(datafusion_common::DataFusionError::Execution(format!(
            "avg interval {context} state is outside PostgreSQL finite interval range"
        )));
    }
    Ok(())
}

fn pg_interval_is_infinite(months: i64, days: i64, time_micros: i64) -> bool {
    (months == i64::from(i32::MIN) && days == i64::from(i32::MIN) && time_micros == i64::MIN)
        || (months == i64::from(i32::MAX) && days == i64::from(i32::MAX) && time_micros == i64::MAX)
}

fn pg_interval_div(
    months_sum: i64,
    days_sum: i64,
    time_micros_sum: i64,
    count: u64,
) -> Result<<IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native> {
    if count == 0 {
        return Err(datafusion_common::DataFusionError::Execution(
            "avg interval division by zero".to_owned(),
        ));
    }
    validate_pg_finite_interval(months_sum, days_sum, time_micros_sum, "final input")?;

    let factor = count as f64;
    let orig_month = months_sum as f64;
    let orig_day = days_sum as f64;

    let result_month_double = orig_month / factor;
    if !float_fits_i32(result_month_double) {
        return interval_out_of_range();
    }
    let result_month = result_month_double as i32;

    let result_day_double = orig_day / factor;
    if !float_fits_i32(result_day_double) {
        return interval_out_of_range();
    }
    let mut result_day = result_day_double as i32;

    let month_remainder_days =
        tsround((orig_month / factor - f64::from(result_month)) * DAYS_PER_MONTH);
    let mut sec_remainder = tsround(
        (orig_day / factor - f64::from(result_day) + month_remainder_days
            - f64::from(month_remainder_days as i32))
            * SECS_PER_DAY,
    );
    if sec_remainder.abs() >= SECS_PER_DAY {
        let whole_days = (sec_remainder / SECS_PER_DAY) as i32;
        result_day = result_day
            .checked_add(whole_days)
            .ok_or_else(interval_range_error)?;
        sec_remainder -= f64::from(whole_days) * SECS_PER_DAY;
    }

    result_day = result_day
        .checked_add(month_remainder_days as i32)
        .ok_or_else(interval_range_error)?;

    let time_double =
        (time_micros_sum as f64 / factor + sec_remainder * USECS_PER_SEC).round_ties_even();
    if !float_fits_i64(time_double) {
        return interval_out_of_range();
    }
    let result_time_micros = time_double as i64;
    validate_pg_finite_interval(
        i64::from(result_month),
        i64::from(result_day),
        result_time_micros,
        "final result",
    )?;
    let result_time_nanos = result_time_micros
        .checked_mul(NANOS_PER_MICRO)
        .ok_or_else(interval_range_error)?;
    Ok(IntervalMonthDayNanoType::make_value(
        result_month,
        result_day,
        result_time_nanos,
    ))
}

fn tsround(value: f64) -> f64 {
    (value * TS_PREC_INV).round_ties_even() / TS_PREC_INV
}

fn float_fits_i32(value: f64) -> bool {
    value.is_finite() && value >= f64::from(i32::MIN) && value <= f64::from(i32::MAX)
}

fn float_fits_i64(value: f64) -> bool {
    value.is_finite() && value >= i64::MIN as f64 && value < -(i64::MIN as f64)
}

fn interval_range_error() -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Execution("avg interval result out of range".to_owned())
}

fn interval_out_of_range<T>() -> Result<T> {
    Err(interval_range_error())
}

#[derive(Debug, Default)]
struct FloatAvgAccumulator {
    finite_sum: f64,
    finite_sxx: f64,
    count: u64,
    nan_count: u64,
    pos_inf_count: u64,
    neg_inf_count: u64,
}

impl FloatAvgAccumulator {
    fn add_value(&mut self, value: f64) -> Result<()> {
        let next_count = self.count.checked_add(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg float transition count overflowed u64".to_owned(),
            )
        })?;

        if value.is_nan() {
            self.nan_count = self.nan_count.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float transition NaN count overflowed u64".to_owned(),
                )
            })?;
        } else if value == f64::INFINITY {
            self.pos_inf_count = self.pos_inf_count.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float transition +Infinity count overflowed u64".to_owned(),
                )
            })?;
        } else if value == f64::NEG_INFINITY {
            self.neg_inf_count = self.neg_inf_count.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float transition -Infinity count overflowed u64".to_owned(),
                )
            })?;
        } else {
            let finite_count = self.finite_count()?;
            let next_finite_sum = checked_float_sum(self.finite_sum, value, "transition")?;
            let next_finite_sxx = checked_float_transition_sxx(
                finite_count,
                self.finite_sum,
                self.finite_sxx,
                value,
                next_finite_sum,
            )?;
            self.finite_sum = next_finite_sum;
            self.finite_sxx = next_finite_sxx;
        }

        self.count = next_count;
        Ok(())
    }

    fn retract_value(&mut self, value: f64) -> Result<()> {
        let next_count = self.count.checked_sub(1).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg float retraction count underflowed u64".to_owned(),
            )
        })?;

        if value.is_nan() {
            self.nan_count = self.nan_count.checked_sub(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float retraction NaN count underflowed u64".to_owned(),
                )
            })?;
        } else if value == f64::INFINITY {
            self.pos_inf_count = self.pos_inf_count.checked_sub(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float retraction +Infinity count underflowed u64".to_owned(),
                )
            })?;
        } else if value == f64::NEG_INFINITY {
            self.neg_inf_count = self.neg_inf_count.checked_sub(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "avg float retraction -Infinity count underflowed u64".to_owned(),
                )
            })?;
        } else {
            let finite_count = self.finite_count()?;
            let next_finite_sum = checked_float_sum(self.finite_sum, -value, "retraction")?;
            let next_finite_sxx =
                checked_float_retract_sxx(finite_count, self.finite_sum, self.finite_sxx, value)?;
            self.finite_sum = next_finite_sum;
            self.finite_sxx = next_finite_sxx;
        }

        self.count = next_count;
        Ok(())
    }

    fn merge_state(
        &mut self,
        count: u64,
        finite_sum: f64,
        finite_sxx: f64,
        nan_count: u64,
        pos_inf_count: u64,
        neg_inf_count: u64,
    ) -> Result<()> {
        let next_count = self.count.checked_add(count).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg float merged count overflowed u64".to_owned(),
            )
        })?;
        let next_nan_count = self.nan_count.checked_add(nan_count).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg float merged NaN count overflowed u64".to_owned(),
            )
        })?;
        let next_pos_inf_count =
            self.pos_inf_count
                .checked_add(pos_inf_count)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "avg float merged +Infinity count overflowed u64".to_owned(),
                    )
                })?;
        let next_neg_inf_count =
            self.neg_inf_count
                .checked_add(neg_inf_count)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "avg float merged -Infinity count overflowed u64".to_owned(),
                    )
                })?;
        let finite_count = self.finite_count()?;
        let other_finite_count =
            float_finite_count(count, nan_count, pos_inf_count, neg_inf_count)?;
        let (next_finite_sum, next_finite_sxx) = checked_float_combine_state(
            finite_count,
            self.finite_sum,
            self.finite_sxx,
            other_finite_count,
            finite_sum,
            finite_sxx,
        )?;

        self.finite_sum = next_finite_sum;
        self.finite_sxx = next_finite_sxx;
        self.count = next_count;
        self.nan_count = next_nan_count;
        self.pos_inf_count = next_pos_inf_count;
        self.neg_inf_count = next_neg_inf_count;
        Ok(())
    }

    fn finite_count(&self) -> Result<u64> {
        float_finite_count(
            self.count,
            self.nan_count,
            self.pos_inf_count,
            self.neg_inf_count,
        )
    }
}

fn checked_float_sum(current: f64, delta: f64, context: &str) -> Result<f64> {
    let next = current + delta;
    if next.is_infinite() && current.is_finite() && delta.is_finite() {
        return Err(datafusion_common::DataFusionError::Execution(format!(
            "avg float {context} sum overflowed float8"
        )));
    }
    Ok(next)
}

fn float_finite_count(
    count: u64,
    nan_count: u64,
    pos_inf_count: u64,
    neg_inf_count: u64,
) -> Result<u64> {
    let special_count = nan_count
        .checked_add(pos_inf_count)
        .and_then(|count| count.checked_add(neg_inf_count))
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "avg float special count overflowed u64".to_owned(),
            )
        })?;
    count.checked_sub(special_count).ok_or_else(|| {
        datafusion_common::DataFusionError::Execution(
            "avg float state has more special values than rows".to_owned(),
        )
    })
}

fn checked_float_transition_sxx(
    old_count: u64,
    old_sum: f64,
    old_sxx: f64,
    value: f64,
    new_sum: f64,
) -> Result<f64> {
    if old_count == 0 {
        return Ok(0.0);
    }

    let old_count_f = old_count as f64;
    let new_count_f = old_count_f + 1.0;
    let tmp = value * new_count_f - new_sum;
    let next_sxx = old_sxx + tmp * tmp / (new_count_f * old_count_f);
    if next_sxx.is_infinite() && old_sxx.is_finite() && old_sum.is_finite() && value.is_finite() {
        return Err(datafusion_common::DataFusionError::Execution(
            "avg float transition sxx overflowed float8".to_owned(),
        ));
    }
    Ok(next_sxx)
}

fn checked_float_retract_sxx(
    old_count: u64,
    old_sum: f64,
    old_sxx: f64,
    value: f64,
) -> Result<f64> {
    let Some(new_count) = old_count.checked_sub(1) else {
        return Err(datafusion_common::DataFusionError::Execution(
            "avg float retraction finite count underflowed u64".to_owned(),
        ));
    };
    if new_count == 0 {
        return Ok(0.0);
    }

    let old_count_f = old_count as f64;
    let new_count_f = new_count as f64;
    let tmp = value * old_count_f - old_sum;
    let next_sxx = old_sxx - tmp * tmp / (old_count_f * new_count_f);
    if next_sxx.is_infinite() && old_sxx.is_finite() && old_sum.is_finite() && value.is_finite() {
        return Err(datafusion_common::DataFusionError::Execution(
            "avg float retraction sxx overflowed float8".to_owned(),
        ));
    }
    Ok(next_sxx)
}

fn checked_float_combine_state(
    left_count: u64,
    left_sum: f64,
    left_sxx: f64,
    right_count: u64,
    right_sum: f64,
    right_sxx: f64,
) -> Result<(f64, f64)> {
    if left_count == 0 {
        return Ok((right_sum, right_sxx));
    }
    if right_count == 0 {
        return Ok((left_sum, left_sxx));
    }

    let sum = checked_float_sum(left_sum, right_sum, "merged")?;
    let left_count_f = left_count as f64;
    let right_count_f = right_count as f64;
    let count_f = left_count_f + right_count_f;
    let tmp = left_sum / left_count_f - right_sum / right_count_f;
    let sxx = left_sxx + right_sxx + left_count_f * right_count_f * tmp * tmp / count_f;
    if sxx.is_infinite() && left_sxx.is_finite() && right_sxx.is_finite() {
        return Err(datafusion_common::DataFusionError::Execution(
            "avg float merged sxx overflowed float8".to_owned(),
        ));
    }

    Ok((sum, sxx))
}

impl Accumulator for FloatAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        let values = values.as_primitive::<Float64Type>();
        for value in values.iter().flatten() {
            self.add_value(value)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("avg expects one input array");
        };
        let values = values.as_primitive::<Float64Type>();
        for value in values.iter().flatten() {
            self.retract_value(value)?;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.count == 0 {
            None
        } else if self.nan_count > 0 || (self.pos_inf_count > 0 && self.neg_inf_count > 0) {
            Some(f64::NAN)
        } else if self.pos_inf_count > 0 {
            Some(f64::INFINITY)
        } else if self.neg_inf_count > 0 {
            Some(f64::NEG_INFINITY)
        } else {
            Some(self.finite_sum / self.count as f64)
        };
        Ok(ScalarValue::Float64(value))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.count)),
            ScalarValue::Float64(Some(self.finite_sum)),
            ScalarValue::Float64(Some(self.finite_sxx)),
            ScalarValue::UInt64(Some(self.nan_count)),
            ScalarValue::UInt64(Some(self.pos_inf_count)),
            ScalarValue::UInt64(Some(self.neg_inf_count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 6 {
            return exec_err!(
                "avg float merge expects count, finite_sum, finite_sxx, NaN count, +Infinity count, and -Infinity count states"
            );
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let finite_sums = states[1].as_primitive::<Float64Type>();
        let finite_sxxs = states[2].as_primitive::<Float64Type>();
        let nan_counts = states[3].as_primitive::<UInt64Type>();
        let pos_inf_counts = states[4].as_primitive::<UInt64Type>();
        let neg_inf_counts = states[5].as_primitive::<UInt64Type>();
        if counts.len() != finite_sums.len()
            || counts.len() != finite_sxxs.len()
            || counts.len() != nan_counts.len()
            || counts.len() != pos_inf_counts.len()
            || counts.len() != neg_inf_counts.len()
        {
            return exec_err!("avg float merge state arrays have different lengths");
        }

        for row in 0..counts.len() {
            if counts.is_null(row)
                || finite_sums.is_null(row)
                || finite_sxxs.is_null(row)
                || nan_counts.is_null(row)
                || pos_inf_counts.is_null(row)
                || neg_inf_counts.is_null(row)
            {
                continue;
            }
            self.merge_state(
                counts.value(row),
                finite_sums.value(row),
                finite_sxxs.value(row),
                nan_counts.value(row),
                pos_inf_counts.value(row),
                neg_inf_counts.value(row),
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Decimal128Array, Float64Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
        UInt64Array,
    };

    fn decimal128_value(value: ScalarValue) -> i128 {
        match value {
            ScalarValue::Decimal128(Some(value), NUMERIC_AVG_PRECISION, NUMERIC_AVG_SCALE) => value,
            other => panic!("unexpected scalar value {other:?}"),
        }
    }

    fn float64_value(value: ScalarValue) -> Option<f64> {
        match value {
            ScalarValue::Float64(value) => value,
            other => panic!("unexpected scalar value {other:?}"),
        }
    }

    fn interval_value(
        value: ScalarValue,
    ) -> Option<<IntervalMonthDayNanoType as arrow_array::types::ArrowPrimitiveType>::Native> {
        match value {
            ScalarValue::IntervalMonthDayNano(value) => value,
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
    fn distinct_integer_avg_deduplicates_and_merges_state() {
        let mut left = DistinctAvgAccumulator::try_new(&DataType::Int32).unwrap();
        left.update_batch(&[Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(2),
            None,
        ]))])
        .unwrap();
        assert_eq!(
            decimal128_value(left.evaluate().unwrap()),
            INT_AVG_SCALE_FACTOR + INT_AVG_SCALE_FACTOR / 2
        );

        let state = left.state().unwrap();
        let mut merged = DistinctAvgAccumulator::try_new(&DataType::Int32).unwrap();
        merged
            .update_batch(&[Arc::new(Int32Array::from(vec![Some(2), Some(4)]))])
            .unwrap();
        merged.merge_batch(&[state[0].to_array().unwrap()]).unwrap();
        assert_eq!(
            decimal128_value(merged.evaluate().unwrap()),
            23_333_333_333_333_333
        );
    }

    #[test]
    fn distinct_decimal_avg_deduplicates_scaled_values() {
        let mut acc = DistinctAvgAccumulator::try_new(&DataType::Decimal128(10, 1)).unwrap();
        acc.update_batch(&[decimal128_array(vec![Some(15), Some(15), Some(25)], 10, 1)])
            .unwrap();

        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            2 * INT_AVG_SCALE_FACTOR
        );
    }

    #[test]
    fn distinct_float_avg_uses_pg_like_distinct_keys() {
        let mut zero = DistinctAvgAccumulator::try_new(&DataType::Float64).unwrap();
        zero.update_batch(&[Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(2.0),
        ]))])
        .unwrap();
        assert_eq!(float64_value(zero.evaluate().unwrap()), Some(1.0));

        let mut nan = DistinctAvgAccumulator::try_new(&DataType::Float64).unwrap();
        nan.update_batch(&[Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(1.0),
        ]))])
        .unwrap();
        assert!(float64_value(nan.evaluate().unwrap()).unwrap().is_nan());

        let mut infinity = DistinctAvgAccumulator::try_new(&DataType::Float64).unwrap();
        infinity
            .update_batch(&[Arc::new(Float64Array::from(vec![
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(1.0),
            ]))])
            .unwrap();
        assert_eq!(
            float64_value(infinity.evaluate().unwrap()),
            Some(f64::INFINITY)
        );
    }

    #[test]
    fn distinct_interval_avg_deduplicates_finite_values() {
        let mut acc =
            DistinctAvgAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        acc.update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 0, 1_000_000_000)),
            Some(IntervalMonthDayNanoType::make_value(0, 0, 1_000_000_000)),
            Some(IntervalMonthDayNanoType::make_value(0, 0, 3_000_000_000)),
        ]))])
        .unwrap();

        assert_eq!(
            interval_value(acc.evaluate().unwrap()),
            Some(IntervalMonthDayNanoType::make_value(0, 0, 2_000_000_000))
        );
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
    fn integer_avg_retracts_rows_for_sliding_windows() {
        let mut acc = IntegerAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Int64Array::from(vec![Some(1), Some(2), None]))])
            .unwrap();
        acc.retract_batch(&[Arc::new(Int64Array::from(vec![Some(1), None]))])
            .unwrap();

        assert!(acc.supports_retract_batch());
        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            2 * INT_AVG_SCALE_FACTOR
        );

        acc.retract_batch(&[Arc::new(Int64Array::from(vec![Some(2)]))])
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
    fn float_avg_retracts_rows_for_sliding_windows() {
        let mut acc = FloatAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
        ]))])
        .unwrap();
        acc.retract_batch(&[Arc::new(Float64Array::from(vec![Some(1.0), None]))])
            .unwrap();

        assert!(acc.supports_retract_batch());
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(2.0)));

        acc.retract_batch(&[Arc::new(Float64Array::from(vec![Some(2.0)]))])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(None));
    }

    #[test]
    fn float_avg_preserves_nan_and_infinity() {
        let cases = [
            (vec![Some(f64::INFINITY), Some(1.0)], f64::INFINITY),
            (vec![Some(f64::NEG_INFINITY), Some(1.0)], f64::NEG_INFINITY),
            (vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY)], f64::NAN),
            (vec![Some(f64::NAN), Some(1.0)], f64::NAN),
        ];

        for (input, expected) in cases {
            let mut acc = FloatAvgAccumulator::default();
            acc.update_batch(&[Arc::new(Float64Array::from(input))])
                .unwrap();
            let actual = float64_value(acc.evaluate().unwrap()).unwrap();
            if expected.is_nan() {
                assert!(actual.is_nan());
            } else {
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn float_avg_retracts_special_values_for_sliding_windows() {
        let mut acc = FloatAvgAccumulator::default();
        acc.update_batch(&[Arc::new(Float64Array::from(vec![
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(1.0),
        ]))])
        .unwrap();
        assert!(float64_value(acc.evaluate().unwrap()).unwrap().is_nan());

        acc.retract_batch(&[Arc::new(Float64Array::from(vec![Some(f64::NEG_INFINITY)]))])
            .unwrap();
        assert_eq!(float64_value(acc.evaluate().unwrap()), Some(f64::INFINITY));

        acc.retract_batch(&[Arc::new(Float64Array::from(vec![Some(f64::INFINITY)]))])
            .unwrap();
        assert_eq!(float64_value(acc.evaluate().unwrap()), Some(1.0));
    }

    #[test]
    fn float_avg_errors_when_finite_sum_overflows() {
        let mut acc = FloatAvgAccumulator::default();
        let err = acc
            .update_batch(&[Arc::new(Float64Array::from(vec![
                Some(f64::MAX),
                Some(f64::MAX),
            ]))])
            .unwrap_err();
        assert!(err.to_string().contains("transition sum overflowed float8"));

        let mut left = FloatAvgAccumulator::default();
        left.update_batch(&[Arc::new(Float64Array::from(vec![Some(f64::MAX)]))])
            .unwrap();
        let state = left.state().unwrap();
        let mut merged = FloatAvgAccumulator::default();
        merged
            .update_batch(&[Arc::new(Float64Array::from(vec![Some(f64::MAX)]))])
            .unwrap();
        let err = merged
            .merge_batch(&[
                state[0].to_array().unwrap(),
                state[1].to_array().unwrap(),
                state[2].to_array().unwrap(),
                state[3].to_array().unwrap(),
                state[4].to_array().unwrap(),
                state[5].to_array().unwrap(),
            ])
            .unwrap_err();
        assert!(err.to_string().contains("merged sum overflowed float8"));

        let mut retract = FloatAvgAccumulator {
            finite_sum: -f64::MAX,
            finite_sxx: 0.0,
            count: 2,
            nan_count: 0,
            pos_inf_count: 0,
            neg_inf_count: 0,
        };
        let err = retract
            .retract_batch(&[Arc::new(Float64Array::from(vec![Some(f64::MAX)]))])
            .unwrap_err();
        assert!(err.to_string().contains("retraction sum overflowed float8"));
    }

    #[test]
    fn float_avg_errors_when_finite_sxx_overflows() {
        let mut acc = FloatAvgAccumulator::default();
        let err = acc
            .update_batch(&[Arc::new(Float64Array::from(vec![
                Some(f64::MAX),
                Some(-f64::MAX),
            ]))])
            .unwrap_err();
        assert!(err.to_string().contains("transition sxx overflowed float8"));

        let mut left = FloatAvgAccumulator::default();
        left.update_batch(&[Arc::new(Float64Array::from(vec![Some(f64::MAX)]))])
            .unwrap();
        let state = left.state().unwrap();
        let mut merged = FloatAvgAccumulator::default();
        merged
            .update_batch(&[Arc::new(Float64Array::from(vec![Some(-f64::MAX)]))])
            .unwrap();
        let err = merged
            .merge_batch(&[
                state[0].to_array().unwrap(),
                state[1].to_array().unwrap(),
                state[2].to_array().unwrap(),
                state[3].to_array().unwrap(),
                state[4].to_array().unwrap(),
                state[5].to_array().unwrap(),
            ])
            .unwrap_err();
        assert!(err.to_string().contains("merged sxx overflowed float8"));
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
    fn decimal_avg_retracts_rows_for_sliding_windows() {
        let mut acc = DecimalAvgAccumulator::try_new(10, 1).unwrap();
        acc.update_batch(&[decimal128_array(vec![Some(15), Some(25), None], 10, 1)])
            .unwrap();
        acc.retract_batch(&[decimal128_array(vec![Some(15), None], 10, 1)])
            .unwrap();

        assert!(acc.supports_retract_batch());
        assert_eq!(
            decimal128_value(acc.evaluate().unwrap()),
            25 * (INT_AVG_SCALE_FACTOR / 10)
        );

        acc.retract_batch(&[decimal128_array(vec![Some(25)], 10, 1)])
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
    fn interval_avg_cascades_fractional_months_like_postgres() {
        let mut acc = IntervalAvgAccumulator::default();
        acc.update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(1, 0, 0)),
            Some(IntervalMonthDayNanoType::make_value(2, 0, 0)),
        ]))])
        .unwrap();

        assert_eq!(
            interval_value(acc.evaluate().unwrap()),
            Some(IntervalMonthDayNanoType::make_value(1, 15, 0))
        );
    }

    #[test]
    fn interval_avg_cascades_fractional_days_to_time_like_postgres() {
        let mut acc = IntervalAvgAccumulator::default();
        acc.update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(0, 1, 0)),
            Some(IntervalMonthDayNanoType::make_value(0, 2, 0)),
        ]))])
        .unwrap();

        assert_eq!(
            interval_value(acc.evaluate().unwrap()),
            Some(IntervalMonthDayNanoType::make_value(
                0,
                1,
                43_200_000_000_000
            ))
        );
    }

    #[test]
    fn interval_avg_retracts_rows_for_sliding_windows() {
        let mut acc = IntervalAvgAccumulator::default();
        acc.update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(1, 0, 0)),
            Some(IntervalMonthDayNanoType::make_value(3, 0, 0)),
            None,
        ]))])
        .unwrap();
        acc.retract_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(1, 0, 0)),
            None,
        ]))])
        .unwrap();

        assert!(acc.supports_retract_batch());
        assert_eq!(
            interval_value(acc.evaluate().unwrap()),
            Some(IntervalMonthDayNanoType::make_value(3, 0, 0))
        );

        acc.retract_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
            IntervalMonthDayNanoType::make_value(3, 0, 0),
        )]))])
        .unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::IntervalMonthDayNano(None)
        );
    }

    #[test]
    fn interval_avg_merge_combines_partial_states() {
        let mut left = IntervalAvgAccumulator::default();
        left.update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(1, 0, 0)),
            Some(IntervalMonthDayNanoType::make_value(3, 0, 0)),
        ]))])
        .unwrap();
        let state = left.state().unwrap();

        let mut merged = IntervalAvgAccumulator::default();
        merged
            .merge_batch(&[
                state[0].to_array().unwrap(),
                state[1].to_array().unwrap(),
                state[2].to_array().unwrap(),
                state[3].to_array().unwrap(),
            ])
            .unwrap();

        assert_eq!(
            interval_value(merged.evaluate().unwrap()),
            Some(IntervalMonthDayNanoType::make_value(2, 0, 0))
        );
    }

    #[test]
    fn interval_avg_rejects_nanosecond_precision() {
        let mut acc = IntervalAvgAccumulator::default();
        let err = acc
            .update_batch(&[Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
                IntervalMonthDayNanoType::make_value(0, 0, 1),
            )]))])
            .unwrap_err();
        assert!(err.to_string().contains("nanosecond precision"));
    }

    #[test]
    fn interval_avg_rejects_postgres_interval_infinity_state() {
        let mut acc = IntervalAvgAccumulator::default();
        let err = acc
            .merge_batch(&[
                Arc::new(UInt64Array::from(vec![Some(1)])),
                Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX))])),
                Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX))])),
                Arc::new(Int64Array::from(vec![Some(i64::MAX)])),
            ])
            .unwrap_err();
        assert!(err.to_string().contains("PostgreSQL interval infinity"));
    }

    #[test]
    fn pg_avg_type_rules_match_pg_integer_float_decimal_and_interval_surface() {
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
        assert_eq!(
            avg.return_type(&[DataType::Interval(IntervalUnit::MonthDayNano)])
                .unwrap(),
            DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            avg.coerce_types(&[DataType::Interval(IntervalUnit::MonthDayNano)])
                .unwrap(),
            vec![DataType::Interval(IntervalUnit::MonthDayNano)]
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
