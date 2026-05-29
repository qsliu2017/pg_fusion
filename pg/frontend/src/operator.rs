use crate::ir::PgOperator;

/// Return the DataFusion operator for pg_catalog comparison operators that v1 can lower.
///
/// PostgreSQL operator names are user-extensible, so matching by spelling would
/// silently turn a user-defined `=` or `+` into DataFusion's builtin operator.
/// These OIDs are the stable builtin pg_catalog operator OIDs from
/// `pg_operator.dat`; unsupported OIDs fail closed.
pub(crate) fn supported_operator(opno: u32) -> Option<PgOperator> {
    match opno {
        // bool, int2, int4, int8, float4, float8, text, name, bpchar,
        // bytea, date, time, timestamp, timestamptz, numeric.
        91 | 94 | 96 | 410 | 620 | 670 | 98 | 93 | 1054 | 1955 | 1093 | 1108 | 2060 | 1320
        | 1752 => Some(PgOperator::Eq),
        85 | 519 | 518 | 411 | 621 | 671 | 531 | 643 | 1057 | 1956 | 1094 | 1109 | 2061 | 1321
        | 1753 => Some(PgOperator::NotEq),
        58 | 95 | 97 | 412 | 622 | 672 | 664 | 660 | 1058 | 1957 | 1095 | 1110 | 2062 | 1322
        | 1754 => Some(PgOperator::Lt),
        1694 | 522 | 523 | 414 | 624 | 673 | 665 | 661 | 1059 | 1958 | 1096 | 1111 | 2063
        | 1323 | 1755 => Some(PgOperator::LtEq),
        59 | 520 | 521 | 413 | 623 | 674 | 666 | 662 | 1060 | 1959 | 1097 | 1112 | 2064 | 1324
        | 1756 => Some(PgOperator::Gt),
        1695 | 524 | 525 | 415 | 625 | 675 | 667 | 663 | 1061 | 1960 | 1098 | 1113 | 2065
        | 1325 | 1757 => Some(PgOperator::GtEq),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_builtin_operator_oids() {
        assert_eq!(supported_operator(96), Some(PgOperator::Eq));
        assert_eq!(supported_operator(518), Some(PgOperator::NotEq));
        assert_eq!(supported_operator(664), Some(PgOperator::Lt));
        assert_eq!(supported_operator(1325), Some(PgOperator::GtEq));
    }

    #[test]
    fn rejects_unknown_operator_oids() {
        assert_eq!(supported_operator(0), None);
        assert_eq!(supported_operator(999_999), None);
    }

    #[test]
    fn rejects_arithmetic_operator_oids() {
        assert_eq!(supported_operator(551), None);
        assert_eq!(supported_operator(593), None);
        assert_eq!(supported_operator(1758), None);
    }
}
