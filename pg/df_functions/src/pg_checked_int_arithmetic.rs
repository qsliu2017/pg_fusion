use std::any::Any;
use std::sync::Arc;

use arrow_arith::numeric;
use arrow_array::Datum;
use arrow_schema::{ArrowError, DataType};
use datafusion_common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum CheckedIntOp {
    Add,
    Sub,
    Mul,
}

/// PostgreSQL-compatible checked integer arithmetic for `int2`/`int4`/`int8`.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgCheckedIntArithmetic {
    name: &'static str,
    op: CheckedIntOp,
    signature: Signature,
}

impl PgCheckedIntArithmetic {
    fn new(name: &'static str, op: CheckedIntOp) -> Self {
        Self {
            name,
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

pub fn pg_int_add_checked_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgCheckedIntArithmetic::new(
        "pg_fusion_int_add_checked",
        CheckedIntOp::Add,
    )))
}

pub fn pg_int_sub_checked_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgCheckedIntArithmetic::new(
        "pg_fusion_int_sub_checked",
        CheckedIntOp::Sub,
    )))
}

pub fn pg_int_mul_checked_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgCheckedIntArithmetic::new(
        "pg_fusion_int_mul_checked",
        CheckedIntOp::Mul,
    )))
}

impl ScalarUDFImpl for PgCheckedIntArithmetic {
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
        checked_int_arg_type(self.name, arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let data_type = checked_int_arg_type(self.name, arg_types)?;
        Ok(vec![data_type.clone(), data_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("{} expects exactly two arguments", self.name);
        }

        if let [ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)] = &args.args[..] {
            return scalar_checked_int(self.op, left, right);
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let data_type = checked_int_arg_type(
            self.name,
            &[arrays[0].data_type().clone(), arrays[1].data_type().clone()],
        )?;
        let left = arrays[0].as_ref();
        let right = arrays[1].as_ref();
        let result = match self.op {
            CheckedIntOp::Add => numeric::add(&left as &dyn Datum, &right as &dyn Datum),
            CheckedIntOp::Sub => numeric::sub(&left as &dyn Datum, &right as &dyn Datum),
            CheckedIntOp::Mul => numeric::mul(&left as &dyn Datum, &right as &dyn Datum),
        }
        .map_err(|err| map_arithmetic_error(err, &data_type))?;
        Ok(ColumnarValue::Array(result))
    }
}

fn checked_int_arg_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    if arg_types.len() != 2 {
        return plan_err!("{name} expects exactly two arguments");
    }
    let (left, right) = (&arg_types[0], &arg_types[1]);
    if left != right {
        return plan_err!("{name} expects two arguments of the same integer type");
    }
    if is_checked_int_type(left) {
        Ok(left.clone())
    } else {
        plan_err!("{name} expects int2, int4, or int8 arguments")
    }
}

fn is_checked_int_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn scalar_checked_int(
    op: CheckedIntOp,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<ColumnarValue> {
    let scalar = match (left, right) {
        (ScalarValue::Int16(left), ScalarValue::Int16(right)) => {
            ScalarValue::Int16(checked_option_i16(op, *left, *right)?)
        }
        (ScalarValue::Int32(left), ScalarValue::Int32(right)) => {
            ScalarValue::Int32(checked_option_i32(op, *left, *right)?)
        }
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => {
            ScalarValue::Int64(checked_option_i64(op, *left, *right)?)
        }
        _ => return exec_err!("checked integer arithmetic expected matching integer scalars"),
    };
    Ok(ColumnarValue::Scalar(scalar))
}

fn checked_option_i16(
    op: CheckedIntOp,
    left: Option<i16>,
    right: Option<i16>,
) -> Result<Option<i16>> {
    checked_option(left, right, |left, right| match op {
        CheckedIntOp::Add => left.checked_add(right),
        CheckedIntOp::Sub => left.checked_sub(right),
        CheckedIntOp::Mul => left.checked_mul(right),
    })
    .map_err(|_| out_of_range_error(&DataType::Int16))
}

fn checked_option_i32(
    op: CheckedIntOp,
    left: Option<i32>,
    right: Option<i32>,
) -> Result<Option<i32>> {
    checked_option(left, right, |left, right| match op {
        CheckedIntOp::Add => left.checked_add(right),
        CheckedIntOp::Sub => left.checked_sub(right),
        CheckedIntOp::Mul => left.checked_mul(right),
    })
    .map_err(|_| out_of_range_error(&DataType::Int32))
}

fn checked_option_i64(
    op: CheckedIntOp,
    left: Option<i64>,
    right: Option<i64>,
) -> Result<Option<i64>> {
    checked_option(left, right, |left, right| match op {
        CheckedIntOp::Add => left.checked_add(right),
        CheckedIntOp::Sub => left.checked_sub(right),
        CheckedIntOp::Mul => left.checked_mul(right),
    })
    .map_err(|_| out_of_range_error(&DataType::Int64))
}

fn checked_option<T>(
    left: Option<T>,
    right: Option<T>,
    op: impl FnOnce(T, T) -> Option<T>,
) -> std::result::Result<Option<T>, ()> {
    match (left, right) {
        (Some(left), Some(right)) => op(left, right).map(Some).ok_or(()),
        _ => Ok(None),
    }
}

fn map_arithmetic_error(error: ArrowError, data_type: &DataType) -> DataFusionError {
    match error {
        ArrowError::ArithmeticOverflow(_) => out_of_range_error(data_type),
        other => DataFusionError::ArrowError(Box::new(other), None),
    }
}

fn out_of_range_error(data_type: &DataType) -> DataFusionError {
    DataFusionError::Execution(
        match data_type {
            DataType::Int16 => "smallint out of range",
            DataType::Int32 => "integer out of range",
            DataType::Int64 => "bigint out of range",
            _ => "integer out of range",
        }
        .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ArrayRef, Int16Array, Int32Array, Int64Array};
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    fn scalar_args(
        left: ScalarValue,
        right: ScalarValue,
        data_type: DataType,
    ) -> ScalarFunctionArgs {
        let arg_field = Arc::new(Field::new("arg", data_type.clone(), true));
        let return_field = Arc::new(Field::new("result", data_type, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)],
            arg_fields: vec![Arc::clone(&arg_field), arg_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn array_args(left: ArrayRef, right: ArrayRef, data_type: DataType) -> ScalarFunctionArgs {
        let arg_field = Arc::new(Field::new("arg", data_type.clone(), true));
        let return_field = Arc::new(Field::new("result", data_type, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
            arg_fields: vec![Arc::clone(&arg_field), arg_field],
            number_rows: 2,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn scalar_arithmetic_returns_checked_integer_results() {
        let add = PgCheckedIntArithmetic::new("pg_fusion_int_add_checked", CheckedIntOp::Add);
        let sub = PgCheckedIntArithmetic::new("pg_fusion_int_sub_checked", CheckedIntOp::Sub);
        let mul = PgCheckedIntArithmetic::new("pg_fusion_int_mul_checked", CheckedIntOp::Mul);

        let result = add
            .invoke_with_args(scalar_args(
                ScalarValue::Int16(Some(10)),
                ScalarValue::Int16(Some(5)),
                DataType::Int16,
            ))
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int16(Some(15)))
        ));

        let result = sub
            .invoke_with_args(scalar_args(
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(5)),
                DataType::Int32,
            ))
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5)))
        ));

        let result = mul
            .invoke_with_args(scalar_args(
                ScalarValue::Int64(Some(10)),
                ScalarValue::Int64(Some(5)),
                DataType::Int64,
            ))
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(50)))
        ));
    }

    #[test]
    fn scalar_arithmetic_preserves_nulls() {
        let add = PgCheckedIntArithmetic::new("pg_fusion_int_add_checked", CheckedIntOp::Add);
        let result = add
            .invoke_with_args(scalar_args(
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(i32::MAX)),
                DataType::Int32,
            ))
            .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Int32(None))
        ));
    }

    #[test]
    fn scalar_arithmetic_errors_on_overflow() {
        let cases = [
            (
                PgCheckedIntArithmetic::new("pg_fusion_int_add_checked", CheckedIntOp::Add),
                ScalarValue::Int16(Some(i16::MAX)),
                ScalarValue::Int16(Some(1)),
                DataType::Int16,
                "smallint out of range",
            ),
            (
                PgCheckedIntArithmetic::new("pg_fusion_int_sub_checked", CheckedIntOp::Sub),
                ScalarValue::Int32(Some(i32::MIN)),
                ScalarValue::Int32(Some(1)),
                DataType::Int32,
                "integer out of range",
            ),
            (
                PgCheckedIntArithmetic::new("pg_fusion_int_mul_checked", CheckedIntOp::Mul),
                ScalarValue::Int64(Some(i64::MAX)),
                ScalarValue::Int64(Some(2)),
                DataType::Int64,
                "bigint out of range",
            ),
        ];

        for (udf, left, right, data_type, expected) in cases {
            let error = udf
                .invoke_with_args(scalar_args(left, right, data_type))
                .expect_err("integer overflow must fail");
            assert!(
                error.to_string().contains(expected),
                "unexpected overflow error: {error}"
            );
        }
    }

    #[test]
    fn array_arithmetic_uses_checked_arrow_kernels() {
        let add = PgCheckedIntArithmetic::new("pg_fusion_int_add_checked", CheckedIntOp::Add);
        let left = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let right = Arc::new(Int32Array::from(vec![Some(2), Some(i32::MAX)])) as ArrayRef;
        let result = add
            .invoke_with_args(array_args(left, right, DataType::Int32))
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("array inputs should produce array output");
        };
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), 3);
        assert!(result.is_null(1));

        let sub = PgCheckedIntArithmetic::new("pg_fusion_int_sub_checked", CheckedIntOp::Sub);
        let left = Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(5)])) as ArrayRef;
        let right = Arc::new(Int64Array::from(vec![Some(1), Some(1)])) as ArrayRef;
        let error = sub
            .invoke_with_args(array_args(left, right, DataType::Int64))
            .expect_err("array integer overflow must fail");
        assert!(error.to_string().contains("bigint out of range"));

        let mul = PgCheckedIntArithmetic::new("pg_fusion_int_mul_checked", CheckedIntOp::Mul);
        let left = Arc::new(Int16Array::from(vec![Some(100), Some(2)])) as ArrayRef;
        let right = Arc::new(Int16Array::from(vec![Some(3), Some(4)])) as ArrayRef;
        let result = mul
            .invoke_with_args(array_args(left, right, DataType::Int16))
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("array inputs should produce array output");
        };
        let result = result.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(result.value(0), 300);
        assert_eq!(result.value(1), 8);
    }
}
