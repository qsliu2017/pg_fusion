#![allow(clippy::module_inception)]

use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_name_data_until_nul() {
        let mut name = zeroed_name();
        write_name_bytes(&mut name, b"foo\0ignored");

        assert_eq!(decode_name_data(&name).unwrap(), "foo");
    }

    #[test]
    fn decodes_full_name_data_without_nul() {
        let mut name = zeroed_name();
        for byte in &mut name.data {
            *byte = b'a' as c_char;
        }

        assert_eq!(
            decode_name_data(&name).unwrap(),
            "a".repeat(name.data.len())
        );
    }

    #[test]
    fn rejects_invalid_utf8_name_data() {
        let mut name = zeroed_name();
        name.data[0] = 0xff_u8 as c_char;

        assert!(decode_name_data(&name).is_err());
    }

    #[test]
    fn target_output_type_uses_typed_aggregate_type() {
        let pg_type = PgTypeRef::new(pg_type::oid::NUMERICOID, -1, 0);
        let expr = QueryExpr::AggregateCall {
            func: AggregateFunction::Avg,
            args: Vec::new(),
            distinct: false,
            filter: None,
            pg_type,
        };

        assert_eq!(target_output_type(&expr), Some(pg_type));
    }

    #[test]
    fn target_output_type_defers_relabel_typmod_to_pg_tree() {
        let expr = QueryExpr::RelabelType(Box::new(QueryExpr::Const(Const {
            pg_type: PgTypeRef::new(pg_type::oid::TEXTOID, -1, 0),
            value: None,
        })));

        assert_eq!(target_output_type(&expr), None);
    }

    #[test]
    fn accepts_plain_limit_option() {
        validate_limit_option(pg_sys::LimitOption::LIMIT_OPTION_COUNT)
            .expect("ordinary LIMIT/FETCH ONLY option must be accepted");
    }

    #[test]
    fn rejects_fetch_with_ties_limit_option() {
        let err = validate_limit_option(pg_sys::LimitOption::LIMIT_OPTION_WITH_TIES)
            .expect_err("FETCH WITH TIES must fail closed");

        assert!(
            err.to_string().contains("FETCH WITH TIES"),
            "error should mention FETCH WITH TIES: {err}"
        );
    }

    fn zeroed_name() -> pg_sys::NameData {
        unsafe { std::mem::zeroed() }
    }

    fn write_name_bytes(name: &mut pg_sys::NameData, bytes: &[u8]) {
        for (slot, byte) in name.data.iter_mut().zip(bytes.iter().copied()) {
            *slot = byte as c_char;
        }
    }
}
