use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// PostgreSQL-compatible `format(text, ...)` scalar function for the supported
/// pg_fusion text surface.
#[derive(Debug)]
pub struct PgFormat {
    signature: Signature,
}

impl PgFormat {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Stable),
        }
    }
}

impl Default for PgFormat {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pg_format_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(PgFormat::new()))
}

impl ScalarUDFImpl for PgFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("format requires at least one argument");
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return plan_err!("format requires at least one argument");
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("format requires at least one argument");
        }

        let has_arrays = args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)));
        if !has_arrays {
            let values = args
                .args
                .iter()
                .map(scalar_text)
                .collect::<Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(format_row(
                &ScalarTextArgs(values),
                0,
            )?)));
        }

        let values = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let array_args = ArrayTextArgs(values);
        let mut builder = StringBuilder::new();
        for row in 0..args.number_rows {
            match format_row(&array_args, row)? {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

trait TextArgs {
    fn len(&self) -> usize;
    fn value(&self, index: usize, row: usize) -> Result<Option<String>>;
}

struct ScalarTextArgs(Vec<Option<String>>);

impl TextArgs for ScalarTextArgs {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn value(&self, index: usize, _row: usize) -> Result<Option<String>> {
        Ok(self.0[index].clone())
    }
}

struct ArrayTextArgs(Vec<ArrayRef>);

impl TextArgs for ArrayTextArgs {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn value(&self, index: usize, row: usize) -> Result<Option<String>> {
        array_text(&self.0[index], row)
    }
}

fn scalar_text(arg: &ColumnarValue) -> Result<Option<String>> {
    match arg {
        ColumnarValue::Scalar(value) => scalar_value_text(value),
        ColumnarValue::Array(_) => exec_err!("format expected scalar argument"),
    }
}

fn scalar_value_text(value: &ScalarValue) -> Result<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value.clone())
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("format expected text argument after coercion, got {other:?}"),
    }
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
                        "format expected Utf8 array".into(),
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
                        "format expected Utf8View array".into(),
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
                        "format expected LargeUtf8 array".into(),
                    )
                })?;
            Ok(Some(array.value(row).to_owned()))
        }
        other => exec_err!("format expected text array after coercion, got {other:?}"),
    }
}

fn format_row(args: &dyn TextArgs, row: usize) -> Result<Option<String>> {
    let Some(format) = args.value(0, row)? else {
        return Ok(None);
    };

    let mut output = String::new();
    let mut next_arg = 0usize;
    let chars = format.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if ch != '%' {
            output.push(ch);
            index += 1;
            continue;
        }

        index += 1;
        if index >= chars.len() {
            return unterminated_format();
        }
        if chars[index] == '%' {
            output.push('%');
            index += 1;
            continue;
        }

        let spec = parse_spec(&chars, index)?;
        index = spec.next_index;

        let mut left_align = spec.left_align;
        let mut width = 0i32;
        if let Some(width_arg) = spec.width {
            width = match width_arg {
                Width::Literal(width) => width,
                Width::Argument(position) => {
                    let arg_index = match position {
                        Some(position) => {
                            next_arg = position;
                            position - 1
                        }
                        None => {
                            let arg_index = next_arg;
                            next_arg += 1;
                            arg_index
                        }
                    };
                    parse_width_arg(args, row, arg_index)?
                }
            };
            if width < 0 {
                if width == i32::MIN {
                    return exec_err!("number is out of range");
                }
                left_align = true;
                width = -width;
            }
        }

        let value_index = match spec.arg_position {
            Some(position) => {
                next_arg = position;
                position - 1
            }
            None => {
                let arg_index = next_arg;
                next_arg += 1;
                arg_index
            }
        };

        if value_index + 1 >= args.len() {
            return exec_err!("too few arguments for format()");
        }
        let value = args.value(value_index + 1, row)?;
        let converted = convert_value(spec.conversion, value)?;
        append_with_width(&mut output, &converted, width as usize, left_align);
    }

    Ok(Some(output))
}

#[derive(Clone, Copy, Debug)]
struct FormatSpec {
    arg_position: Option<usize>,
    width: Option<Width>,
    left_align: bool,
    conversion: char,
    next_index: usize,
}

#[derive(Clone, Copy, Debug)]
enum Width {
    Literal(i32),
    Argument(Option<usize>),
}

fn parse_spec(chars: &[char], mut index: usize) -> Result<FormatSpec> {
    let mut arg_position = None;
    let mut width = None;
    let mut left_align = false;

    let (digits, next) = parse_digits(chars, index)?;
    if next > index {
        if next < chars.len() && chars[next] == '$' {
            arg_position = Some(validate_position(digits)?);
            index = next + 1;
        } else {
            width = Some(Width::Literal(digits));
            index = next;
        }
    }

    while index < chars.len() && chars[index] == '-' {
        left_align = true;
        index += 1;
    }

    if width.is_none() {
        if index < chars.len() && chars[index] == '*' {
            index += 1;
            let (digits, next) = parse_digits(chars, index)?;
            if next > index {
                if next >= chars.len() || chars[next] != '$' {
                    return exec_err!("width argument position must be ended by \"$\"");
                }
                width = Some(Width::Argument(Some(validate_position(digits)?)));
                index = next + 1;
            } else {
                width = Some(Width::Argument(None));
            }
        } else {
            let (digits, next) = parse_digits(chars, index)?;
            if next > index {
                width = Some(Width::Literal(digits));
                index = next;
            }
        }
    }

    if index >= chars.len() {
        return unterminated_format();
    }

    let conversion = chars[index];
    match conversion {
        's' | 'I' | 'L' => Ok(FormatSpec {
            arg_position,
            width,
            left_align,
            conversion,
            next_index: index + 1,
        }),
        other => exec_err!("unrecognized format() type specifier \"{other}\""),
    }
}

fn parse_digits(chars: &[char], mut index: usize) -> Result<(i32, usize)> {
    let mut value: i64 = 0;
    let start = index;
    while index < chars.len() {
        let Some(digit) = chars[index].to_digit(10) else {
            break;
        };
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i64::from(digit)))
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("number is out of range".into())
            })?;
        if value > i64::from(i32::MAX) {
            return exec_err!("number is out of range");
        }
        index += 1;
    }
    if index == start {
        return Ok((0, index));
    }
    Ok((value as i32, index))
}

fn validate_position(position: i32) -> Result<usize> {
    if position == 0 {
        return exec_err!("format specifies argument 0, but arguments are numbered from 1");
    }
    Ok(position as usize)
}

fn parse_width_arg(args: &dyn TextArgs, row: usize, arg_index: usize) -> Result<i32> {
    if arg_index + 1 >= args.len() {
        return exec_err!("too few arguments for format()");
    }
    match args.value(arg_index + 1, row)? {
        Some(value) => value.parse::<i32>().map_err(|_| {
            datafusion_common::DataFusionError::Execution(format!(
                "invalid input syntax for type integer: \"{value}\""
            ))
        }),
        None => Ok(0),
    }
}

fn convert_value(conversion: char, value: Option<String>) -> Result<String> {
    match (conversion, value) {
        ('s', None) => Ok(String::new()),
        ('L', None) => Ok("NULL".to_owned()),
        ('I', None) => exec_err!("null values cannot be formatted as an SQL identifier"),
        ('s', Some(value)) => Ok(value),
        ('I', Some(value)) => Ok(quote_identifier(&value)),
        ('L', Some(value)) => Ok(quote_literal(&value)),
        _ => exec_err!("unrecognized format() type specifier \"{conversion}\""),
    }
}

fn append_with_width(output: &mut String, value: &str, width: usize, left_align: bool) {
    let len = value.chars().count();
    if width <= len {
        output.push_str(value);
        return;
    }

    let padding = " ".repeat(width - len);
    if left_align {
        output.push_str(value);
        output.push_str(&padding);
    } else {
        output.push_str(&padding);
        output.push_str(value);
    }
}

fn quote_identifier(value: &str) -> String {
    if is_safe_identifier(value) {
        return value.to_owned();
    }

    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn quote_literal(value: &str) -> String {
    let needs_escape = value.contains('\\');
    let mut quoted = String::with_capacity(value.len() + 3);
    if needs_escape {
        quoted.push('E');
    }
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' || ch == '\\' {
            quoted.push(ch);
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

fn unterminated_format<T>() -> Result<T> {
    exec_err!("unterminated format() type specifier")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format_scalar(format: Option<&str>, args: &[Option<&str>]) -> Result<Option<String>> {
        let mut values = vec![format.map(str::to_owned)];
        values.extend(args.iter().map(|value| value.map(str::to_owned)));
        format_row(&ScalarTextArgs(values), 0)
    }

    #[test]
    fn formats_basic_values() {
        assert_eq!(format_scalar(None, &[]).unwrap(), None);
        assert_eq!(
            format_scalar(Some("Hello"), &[]).unwrap(),
            Some("Hello".into())
        );
        assert_eq!(
            format_scalar(Some("Hello %s"), &[Some("World")]).unwrap(),
            Some("Hello World".into())
        );
        assert_eq!(
            format_scalar(Some("Hello %%"), &[]).unwrap(),
            Some("Hello %".into())
        );
        assert_eq!(
            format_scalar(Some("%s%s%s"), &[Some("Hello"), None, Some("World")]).unwrap(),
            Some("HelloWorld".into())
        );
    }

    #[test]
    fn formats_identifiers_and_literals() {
        assert_eq!(
            format_scalar(
                Some("INSERT INTO %I VALUES(%L,%L)"),
                &[Some("mytab"), Some("10"), Some("Hello")]
            )
            .unwrap(),
            Some("INSERT INTO mytab VALUES('10','Hello')".into())
        );
        assert_eq!(
            format_scalar(
                Some("INSERT INTO %I VALUES(%L,%L)"),
                &[Some("my\"tab"), Some("it's"), None]
            )
            .unwrap(),
            Some("INSERT INTO \"my\"\"tab\" VALUES('it''s',NULL)".into())
        );
    }

    #[test]
    fn formats_positions_and_widths() {
        assert_eq!(
            format_scalar(Some("%1$s %3$s"), &[Some("1"), Some("2"), Some("3")]).unwrap(),
            Some("1 3".into())
        );
        assert_eq!(
            format_scalar(
                Some("Hello %s %1$s %s"),
                &[Some("World"), Some("Hello again")]
            )
            .unwrap(),
            Some("Hello World World Hello again".into())
        );
        assert_eq!(
            format_scalar(Some(">>%10s<<"), &[Some("Hello")]).unwrap(),
            Some(">>     Hello<<".into())
        );
        assert_eq!(
            format_scalar(Some(">>%-10s<<"), &[Some("Hello")]).unwrap(),
            Some(">>Hello     <<".into())
        );
        assert_eq!(
            format_scalar(Some(">>%2$*1$L<<"), &[Some("10"), Some("Hello")]).unwrap(),
            Some(">>   'Hello'<<".into())
        );
        assert_eq!(
            format_scalar(Some(">>%2$*1$L<<"), &[Some("-10"), None]).unwrap(),
            Some(">>NULL      <<".into())
        );
        assert_eq!(
            format_scalar(Some(">>%*1$s<<"), &[Some("10"), Some("Hello")]).unwrap(),
            Some(">>     Hello<<".into())
        );
        assert_eq!(
            format_scalar(Some(">>%2$*1$L<<"), &[None, Some("Hello")]).unwrap(),
            Some(">>'Hello'<<".into())
        );
    }

    #[test]
    fn rejects_postgresql_error_cases() {
        assert!(format_scalar(Some("Hello %s"), &[]).is_err());
        assert!(format_scalar(Some("Hello %x"), &[Some("20")]).is_err());
        assert!(format_scalar(Some("INSERT INTO %I"), &[None]).is_err());
        assert!(format_scalar(Some("%0$s"), &[Some("Hello")]).is_err());
        assert!(format_scalar(Some("%*0$s"), &[Some("Hello")]).is_err());
        assert!(format_scalar(Some("%1$"), &[Some("1")]).is_err());
        assert!(format_scalar(Some("%1$1"), &[Some("1")]).is_err());
    }
}
