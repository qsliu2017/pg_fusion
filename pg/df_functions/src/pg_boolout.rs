use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// PostgreSQL-compatible `boolout` textification for internal frontend lowering.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgBoolOut {
    signature: Signature,
}

impl PgBoolOut {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Boolean], Volatility::Stable),
        }
    }
}

impl Default for PgBoolOut {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_boolout_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgBoolOut::new()))
}

impl ScalarUDFImpl for PgBoolOut {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "pg_fusion_boolout"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Boolean] => Ok(DataType::Utf8),
            _ => plan_err!("pg_fusion_boolout expects one boolean argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("pg_fusion_boolout expects one boolean argument");
        };
        match arg {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                scalar_boolout(value)?,
            ))),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(boolout_array(array)?)),
        }
    }
}

fn scalar_boolout(value: &ScalarValue) -> Result<Option<String>> {
    match value {
        ScalarValue::Boolean(Some(true)) => Ok(Some("t".to_owned())),
        ScalarValue::Boolean(Some(false)) => Ok(Some("f".to_owned())),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        other => exec_err!("pg_fusion_boolout expected boolean argument, got {other:?}"),
    }
}

fn boolout_array(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(
                "pg_fusion_boolout expected Boolean array".into(),
            )
        })?;
    let mut builder = StringBuilder::new();
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(if values.value(row) { "t" } else { "f" });
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn formats_scalar_booleans_with_postgresql_boolout() {
        assert_eq!(
            boolout_scalar(ScalarValue::Boolean(Some(true))).unwrap(),
            Some("t".into())
        );
        assert_eq!(
            boolout_scalar(ScalarValue::Boolean(Some(false))).unwrap(),
            Some("f".into())
        );
        assert_eq!(boolout_scalar(ScalarValue::Boolean(None)).unwrap(), None);
    }

    #[test]
    fn formats_boolean_arrays_with_postgresql_boolout() {
        let arg_field = Arc::new(Field::new("arg", DataType::Boolean, true));
        let return_field = Arc::new(Field::new("result", DataType::Utf8, true));
        let result = PgBoolOut::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                ])))],
                arg_fields: vec![arg_field],
                number_rows: 3,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("boolout array input should return array");
        };
        let array = array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("boolout result should be StringArray");
        assert_eq!(array.value(0), "t");
        assert_eq!(array.value(1), "f");
        assert!(array.is_null(2));
    }

    fn boolout_scalar(value: ScalarValue) -> Result<Option<String>> {
        let arg_field = Arc::new(Field::new("arg", DataType::Boolean, true));
        let return_field = Arc::new(Field::new("result", DataType::Utf8, true));
        match PgBoolOut::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })? {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(value),
            other => exec_err!("unexpected boolout result: {other:?}"),
        }
    }
}
