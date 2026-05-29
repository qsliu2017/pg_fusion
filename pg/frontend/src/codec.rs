use thiserror::Error;

use crate::ir::PgQuery;

#[derive(Debug, Error)]
pub enum PgFrontendCodecError {
    #[error("failed to encode PostgreSQL query IR: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("failed to decode PostgreSQL query IR: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

pub fn encode_query_ir(query: &PgQuery) -> Result<Vec<u8>, PgFrontendCodecError> {
    rmp_serde::to_vec_named(query).map_err(PgFrontendCodecError::Encode)
}

pub fn decode_query_ir(bytes: &[u8]) -> Result<PgQuery, PgFrontendCodecError> {
    rmp_serde::from_slice(bytes).map_err(PgFrontendCodecError::Decode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        PgColumnRef, PgCommand, PgConst, PgConstValue, PgExpr, PgFromItem, PgRelationRef, PgTarget,
        PgTypeRef,
    };

    #[test]
    fn query_ir_roundtrips() {
        let query = PgQuery {
            command: PgCommand::Select,
            relations: vec![PgRelationRef {
                rtindex: 1,
                relid: 42,
                schema: "public".into(),
                name: "t".into(),
                alias: Some("alias".into()),
                columns: vec![PgColumnRef {
                    attnum: 1,
                    name: "a".into(),
                    pg_type: int4_type(),
                    nullable: false,
                }],
            }],
            from: PgFromItem::Relation { rtindex: 1 },
            selection: Some(PgExpr::Const(PgConst {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            targets: vec![PgTarget {
                expr: PgExpr::Const(PgConst {
                    pg_type: int4_type(),
                    value: Some(PgConstValue::Int32(1)),
                }),
                name: Some("one".into()),
                pg_type: int4_type(),
                resno: 1,
                resjunk: false,
            }],
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            has_distinct: false,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
            has_limit: false,
            has_sort: false,
            has_row_marks: false,
        };

        let encoded = encode_query_ir(&query).unwrap();
        let decoded = decode_query_ir(&encoded).unwrap();
        assert_eq!(decoded, query);
    }

    fn int4_type() -> PgTypeRef {
        PgTypeRef {
            oid: pgrx::pg_sys::INT4OID.into(),
            typmod: -1,
            collation: 0,
        }
    }
}
