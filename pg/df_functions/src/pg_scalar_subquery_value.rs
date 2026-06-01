use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};

const CARDINALITY_ERROR: &str = "more than one row returned by a subquery used as an expression";

/// PostgreSQL scalar-subquery value aggregate.
///
/// A scalar subquery is an aggregate over its full input: no rows produce NULL,
/// one row produces that row's value, and more than one row is a cardinality
/// error.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgScalarSubqueryValue {
    signature: Signature,
}

impl PgScalarSubqueryValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for PgScalarSubqueryValue {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_scalar_subquery_value_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(PgScalarSubqueryValue::new()))
}

impl AggregateUDFImpl for PgScalarSubqueryValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pg_scalar_subquery_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(single_arg_type(self.name(), arg_types)?.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(expr) = acc_args.exprs.first() else {
            return exec_err!("{} expects one input expression", self.name());
        };
        Ok(Box::new(PgScalarSubqueryValueAccumulator {
            value_type: expr.data_type(acc_args.schema)?,
            count: 0,
            first_value: None,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = single_arg_field_type(self.name(), args.input_fields)?;
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                false,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "first_value"),
                input_type.clone(),
                true,
            )),
        ])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(vec![single_arg_type(self.name(), arg_types)?.clone()])
    }
}

#[derive(Debug)]
struct PgScalarSubqueryValueAccumulator {
    value_type: DataType,
    count: u64,
    first_value: Option<ScalarValue>,
}

impl PgScalarSubqueryValueAccumulator {
    fn cardinality_error<T>() -> Result<T> {
        Err(DataFusionError::Execution(CARDINALITY_ERROR.to_owned()))
    }

    fn checked_add_count(left: u64, right: u64) -> Result<u64> {
        left.checked_add(right)
            .ok_or_else(|| DataFusionError::Execution(CARDINALITY_ERROR.to_owned()))
    }

    fn typed_null(&self) -> Result<ScalarValue> {
        ScalarValue::try_new_null(&self.value_type)
    }
}

impl Accumulator for PgScalarSubqueryValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return exec_err!("pg_scalar_subquery_value expects one input array");
        };
        if values.is_empty() {
            return Ok(());
        }
        let input_rows = u64::try_from(values.len())
            .map_err(|_| DataFusionError::Execution(CARDINALITY_ERROR.to_owned()))?;
        let new_count = Self::checked_add_count(self.count, input_rows)?;
        if new_count > 1 {
            return Self::cardinality_error();
        }

        self.first_value = Some(ScalarValue::try_from_array(values.as_ref(), 0)?);
        self.count = new_count;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.count {
            0 => self.typed_null(),
            1 => match self.first_value.clone() {
                Some(value) => Ok(value),
                None => self.typed_null(),
            },
            _ => Self::cardinality_error(),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let first_value = match self.first_value.clone() {
            Some(value) => value,
            None => self.typed_null()?,
        };
        Ok(vec![ScalarValue::UInt64(Some(self.count)), first_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return exec_err!(
                "pg_scalar_subquery_value merge expects count and first value states"
            );
        }
        if states[0].len() != states[1].len() {
            return exec_err!("pg_scalar_subquery_value merge state arrays have different lengths");
        }

        let counts = states[0].as_primitive::<UInt64Type>();
        let values = states[1].as_ref();
        for index in 0..counts.len() {
            let state_count = if counts.is_null(index) {
                0
            } else {
                counts.value(index)
            };
            if state_count == 0 {
                continue;
            }
            if state_count > 1 {
                return Self::cardinality_error();
            }
            let new_count = Self::checked_add_count(self.count, state_count)?;
            if new_count > 1 {
                return Self::cardinality_error();
            }
            self.first_value = Some(ScalarValue::try_from_array(values, index)?);
            self.count = new_count;
        }
        Ok(())
    }
}

fn single_arg_type<'a>(name: &str, arg_types: &'a [DataType]) -> Result<&'a DataType> {
    match arg_types {
        [arg_type] => Ok(arg_type),
        _ => exec_err!("{name} expects exactly one argument"),
    }
}

fn single_arg_field_type<'a>(name: &str, fields: &'a [FieldRef]) -> Result<&'a DataType> {
    match fields {
        [field] => Ok(field.data_type()),
        _ => exec_err!("{name} expects exactly one argument"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringViewArray, UInt64Array};

    fn accumulator(data_type: DataType) -> PgScalarSubqueryValueAccumulator {
        PgScalarSubqueryValueAccumulator {
            value_type: data_type,
            count: 0,
            first_value: None,
        }
    }

    fn state_arrays(state: Vec<ScalarValue>) -> Vec<ArrayRef> {
        state
            .into_iter()
            .map(|value| value.to_array_of_size(1).expect("state array"))
            .collect()
    }

    #[test]
    fn empty_input_returns_typed_null() {
        let mut acc = accumulator(DataType::Int32);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(None));
    }

    #[test]
    fn one_non_null_row_returns_value() {
        let mut acc = accumulator(DataType::Int32);
        acc.update_batch(&[Arc::new(Int32Array::from(vec![Some(42)]))])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(Some(42)));
    }

    #[test]
    fn one_null_row_counts_as_one_row() {
        let mut acc = accumulator(DataType::Int32);
        acc.update_batch(&[Arc::new(Int32Array::from(vec![None]))])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(None));

        let err = acc
            .update_batch(&[Arc::new(Int32Array::from(vec![Some(1)]))])
            .expect_err("second row must violate scalar subquery cardinality");
        assert!(
            err.to_string().contains(CARDINALITY_ERROR),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn two_rows_in_one_batch_error() {
        let mut acc = accumulator(DataType::Int32);
        let err = acc
            .update_batch(&[Arc::new(Int32Array::from(vec![Some(1), Some(2)]))])
            .expect_err("two rows must violate scalar subquery cardinality");
        assert!(
            err.to_string().contains(CARDINALITY_ERROR),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merging_two_single_row_states_errors() {
        let mut left = accumulator(DataType::Int32);
        left.update_batch(&[Arc::new(Int32Array::from(vec![Some(1)]))])
            .unwrap();
        let left_state = left.state().unwrap();

        let mut right = accumulator(DataType::Int32);
        right
            .update_batch(&[Arc::new(Int32Array::from(vec![Some(2)]))])
            .unwrap();
        let right_state = right.state().unwrap();

        assert_eq!(left_state[0], ScalarValue::UInt64(Some(1)));
        assert_eq!(right_state[0], ScalarValue::UInt64(Some(1)));
        let counts = Arc::new(UInt64Array::from(vec![Some(1), Some(1)]));
        let values = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let mut merged = accumulator(DataType::Int32);
        let err = merged
            .merge_batch(&[counts, values])
            .expect_err("two merged rows must violate scalar subquery cardinality");
        assert!(
            err.to_string().contains(CARDINALITY_ERROR),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merges_empty_and_single_string_view_states() {
        let mut empty = accumulator(DataType::Utf8View);
        let empty_state = empty.state().unwrap();

        let mut single = accumulator(DataType::Utf8View);
        single
            .update_batch(&[Arc::new(StringViewArray::from(vec![Some("value")]))])
            .unwrap();
        let single_state = single.state().unwrap();

        let mut merged = accumulator(DataType::Utf8View);
        merged.merge_batch(&state_arrays(empty_state)).unwrap();
        merged.merge_batch(&state_arrays(single_state)).unwrap();
        assert_eq!(
            merged.evaluate().unwrap(),
            ScalarValue::Utf8View(Some("value".into()))
        );
    }
}
