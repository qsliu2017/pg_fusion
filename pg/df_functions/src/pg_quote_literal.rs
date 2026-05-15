use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::pg_format::quote_literal;

/// PostgreSQL-compatible `quote_literal(text)` scalar function.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct PgQuoteLiteral {
    signature: Signature,
}

impl PgQuoteLiteral {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Stable),
        }
    }
}

impl Default for PgQuoteLiteral {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_quote_literal_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgQuoteLiteral::new()))
}

impl ScalarUDFImpl for PgQuoteLiteral {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "quote_literal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("quote_literal requires exactly one argument");
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!("quote_literal requires exactly one argument");
        }
        Ok(vec![DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("quote_literal requires exactly one argument");
        }

        match &args.args[0] {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                scalar_text(value)?.map(|value| quote_literal(&value)),
            ))),
            ColumnarValue::Array(array) => quote_literal_array(array, args.number_rows),
        }
    }
}

fn scalar_text(value: &ScalarValue) -> Result<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value.clone())
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("quote_literal expected text argument after coercion, got {other:?}"),
    }
}

fn quote_literal_array(array: &ArrayRef, rows: usize) -> Result<ColumnarValue> {
    let mut builder = StringBuilder::new();
    for row in 0..rows {
        match array_text(array, row)? {
            Some(value) => builder.append_value(quote_literal(&value)),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn array_text(array: &ArrayRef, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "quote_literal expected Utf8 array".into(),
                    )
                })?;
            Ok(Some(array.value(row).to_owned()))
        }
        DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "quote_literal expected Utf8View array".into(),
                    )
                })?;
            Ok(Some(array.value(row).to_owned()))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "quote_literal expected LargeUtf8 array".into(),
                    )
                })?;
            Ok(Some(array.value(row).to_owned()))
        }
        other => exec_err!("quote_literal expected text array after coercion, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;

    fn quote_scalar(value: Option<&str>) -> Result<Option<String>> {
        let arg_field = Arc::new(Field::new("arg", DataType::Utf8, true));
        let return_field = Arc::new(Field::new("quote_literal", DataType::Utf8, true));
        match PgQuoteLiteral::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(
                value.map(str::to_owned),
            ))],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })? {
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => Ok(value),
            other => exec_err!("unexpected quote_literal result: {other:?}"),
        }
    }

    #[test]
    fn quotes_literals() {
        assert_eq!(quote_scalar(None).unwrap(), None);
        assert_eq!(quote_scalar(Some("")).unwrap(), Some("''".into()));
        assert_eq!(quote_scalar(Some("abc'")).unwrap(), Some("'abc'''".into()));
        assert_eq!(quote_scalar(Some(r"a\b")).unwrap(), Some(r"E'a\\b'".into()));
    }
}
