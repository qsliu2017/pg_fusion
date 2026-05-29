use std::ffi::{c_char, CStr};
use std::slice;
use std::str;

use pgrx::datum::FromDatum;
use pgrx::pg_sys;

use crate::error::PgFrontendError;
use crate::shippability::{supported_non_null_const_type, supported_value_type};
use crate::typed_query::{
    Const, FromItem, Param, PgConstValue, QueryCommand, QueryExpr, RelationRef, Target, TypedQuery,
    Var,
};

use super::common::{
    bool_op, cstr_from_pg, expr_type_ref, finite_float32_const, finite_float64_const, list_len,
    list_ptr_at, param_kind, read_operator, time_const, type_ref, unsupported_temporal_const,
};

pub(crate) unsafe fn read_query(query: *mut pg_sys::Query) -> Result<TypedQuery, PgFrontendError> {
    if query.is_null() {
        return Err(PgFrontendError::NullQuery);
    }
    let query_ref = unsafe { &*query };
    if query_ref.commandType != pg_sys::CmdType::CMD_SELECT {
        return Err(PgFrontendError::unsupported(
            "only SELECT queries are supported",
        ));
    }
    if query_ref.hasModifyingCTE {
        return Err(PgFrontendError::unsupported(
            "data-modifying CTEs are not supported",
        ));
    }

    let relations = unsafe { read_rtable(query_ref.rtable) }?;
    let from = unsafe { read_from_item(query_ref.jointree) }?;
    let selection =
        if query_ref.jointree.is_null() || unsafe { (*query_ref.jointree).quals }.is_null() {
            None
        } else {
            Some(unsafe { read_expr((*query_ref.jointree).quals) }?)
        };
    let targets = unsafe { read_target_list(query_ref.targetList) }?;

    Ok(TypedQuery {
        command: QueryCommand::Select,
        relations,
        from,
        selection,
        targets,
        has_aggregates: query_ref.hasAggs,
        has_windows: query_ref.hasWindowFuncs,
        has_sublinks: query_ref.hasSubLinks,
        has_distinct: !query_ref.distinctClause.is_null() || query_ref.hasDistinctOn,
        has_group_by: !query_ref.groupClause.is_null(),
        has_having: !query_ref.havingQual.is_null(),
        has_grouping_sets: !query_ref.groupingSets.is_null(),
        has_set_operations: !query_ref.setOperations.is_null(),
        has_limit: !query_ref.limitCount.is_null() || !query_ref.limitOffset.is_null(),
        has_sort: !query_ref.sortClause.is_null(),
        has_row_marks: !query_ref.rowMarks.is_null(),
    })
}

unsafe fn read_rtable(rtable: *mut pg_sys::List) -> Result<Vec<RelationRef>, PgFrontendError> {
    let mut relations = Vec::new();
    for index in 0..unsafe { list_len(rtable) } {
        let rte = unsafe { list_ptr_at(rtable, index) as *mut pg_sys::RangeTblEntry };
        if rte.is_null() {
            return Err(PgFrontendError::unsupported("null rtable entry"));
        }
        let rte_ref = unsafe { &*rte };
        if rte_ref.rtekind != pg_sys::RTEKind::RTE_RELATION {
            return Err(PgFrontendError::unsupported(format!(
                "range table entry kind {} is not supported",
                rte_ref.rtekind
            )));
        }
        if !rte_ref.tablesample.is_null() {
            return Err(PgFrontendError::unsupported(
                "TABLESAMPLE range table entries are not supported",
            ));
        }
        if !rte_ref.securityQuals.is_null() {
            return Err(PgFrontendError::unsupported(
                "range table security quals are not supported",
            ));
        }
        if !rte_ref.inh {
            return Err(PgFrontendError::unsupported(
                "ONLY relation scans are not supported by pg_frontend v1",
            ));
        }

        relations.push(unsafe { read_relation_ref((index + 1) as usize, rte_ref) }?);
    }
    Ok(relations)
}

unsafe fn read_relation_ref(
    rtindex: usize,
    rte: &pg_sys::RangeTblEntry,
) -> Result<RelationRef, PgFrontendError> {
    let schema_oid = unsafe { pg_sys::get_rel_namespace(rte.relid) };
    let schema = unsafe { cstr_from_pg(pg_sys::get_namespace_name(schema_oid)) }?;
    let name = unsafe { cstr_from_pg(pg_sys::get_rel_name(rte.relid)) }?;
    let alias = unsafe { read_alias_name(rte.alias) };

    Ok(RelationRef {
        rtindex,
        relid: u32::from(rte.relid),
        schema,
        name,
        alias,
        columns: Vec::new(),
        catalog_resolved: false,
    })
}

unsafe fn read_alias_name(alias: *mut pg_sys::Alias) -> Option<String> {
    if alias.is_null() || unsafe { (*alias).aliasname }.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr((*alias).aliasname) }
            .to_string_lossy()
            .into_owned(),
    )
}

unsafe fn read_from_item(jointree: *mut pg_sys::FromExpr) -> Result<FromItem, PgFrontendError> {
    if jointree.is_null() {
        return Err(PgFrontendError::unsupported("query has no jointree"));
    }
    let fromlist = unsafe { (*jointree).fromlist };
    if unsafe { list_len(fromlist) } != 1 {
        return Err(PgFrontendError::unsupported(
            "pg_frontend v1 supports exactly one base relation",
        ));
    }

    let node = unsafe { list_ptr_at(fromlist, 0) as *mut pg_sys::Node };
    if node.is_null() {
        return Err(PgFrontendError::unsupported("null fromlist node"));
    }
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_RangeTblRef => {
            let range_ref = node.cast::<pg_sys::RangeTblRef>();
            Ok(FromItem::Relation {
                rtindex: unsafe { (*range_ref).rtindex as usize },
            })
        }
        tag => Err(PgFrontendError::unsupported(format!(
            "fromlist node {:?} is not supported by pg_frontend v1",
            tag
        ))),
    }
}

unsafe fn read_target_list(target_list: *mut pg_sys::List) -> Result<Vec<Target>, PgFrontendError> {
    let mut targets = Vec::new();
    for index in 0..unsafe { list_len(target_list) } {
        let entry = unsafe { list_ptr_at(target_list, index) as *mut pg_sys::TargetEntry };
        if entry.is_null() {
            return Err(PgFrontendError::unsupported("null target entry"));
        }
        let entry_ref = unsafe { &*entry };
        let expr = unsafe { read_expr(entry_ref.expr.cast()) }?;
        let pg_type = unsafe { expr_type_ref(entry_ref.expr.cast()) };
        supported_value_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
        let name = if entry_ref.resname.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(entry_ref.resname) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        targets.push(Target {
            expr,
            name,
            pg_type,
            resno: entry_ref.resno,
            resjunk: entry_ref.resjunk,
        });
    }
    Ok(targets)
}

unsafe fn read_expr(node: *mut pg_sys::Node) -> Result<QueryExpr, PgFrontendError> {
    if node.is_null() {
        return Err(PgFrontendError::unsupported("null expression node"));
    }

    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            let var = unsafe { &*node.cast::<pg_sys::Var>() };
            if var.varlevelsup != 0 {
                return Err(PgFrontendError::unsupported(
                    "outer-reference Vars are not supported",
                ));
            }
            if var.varattno <= 0 {
                return Err(PgFrontendError::unsupported(
                    "whole-row and system-column Vars are not supported",
                ));
            }
            Ok(QueryExpr::Var(Var {
                rtindex: var.varno as usize,
                attnum: var.varattno,
                pg_type: type_ref(var.vartype, var.vartypmod, var.varcollid),
            }))
        }
        pg_sys::NodeTag::T_Const => {
            let constant = unsafe { &*node.cast::<pg_sys::Const>() };
            Ok(QueryExpr::Const(unsafe { read_const(constant) }?))
        }
        pg_sys::NodeTag::T_Param => {
            let param = unsafe { &*node.cast::<pg_sys::Param>() };
            Ok(QueryExpr::Param(Param {
                kind: param_kind(param.paramkind),
                id: param.paramid,
                pg_type: type_ref(param.paramtype, param.paramtypmod, param.paramcollid),
            }))
        }
        pg_sys::NodeTag::T_RelabelType => {
            let relabel = unsafe { &*node.cast::<pg_sys::RelabelType>() };
            Ok(QueryExpr::RelabelType(Box::new(unsafe {
                read_expr(relabel.arg.cast())
            }?)))
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let bool_expr = unsafe { &*node.cast::<pg_sys::BoolExpr>() };
            let mut args = Vec::new();
            for index in 0..unsafe { list_len(bool_expr.args) } {
                args.push(unsafe { read_expr(list_ptr_at(bool_expr.args, index).cast()) }?);
            }
            Ok(QueryExpr::Bool {
                op: bool_op(bool_expr.boolop)?,
                args,
            })
        }
        pg_sys::NodeTag::T_OpExpr => {
            let op_expr = unsafe { &*node.cast::<pg_sys::OpExpr>() };
            if unsafe { list_len(op_expr.args) } != 2 {
                return Err(PgFrontendError::unsupported(
                    "only binary operator expressions are supported",
                ));
            }
            let left = unsafe { read_expr(list_ptr_at(op_expr.args, 0).cast()) }?;
            let right = unsafe { read_expr(list_ptr_at(op_expr.args, 1).cast()) }?;
            Ok(QueryExpr::BinaryOp {
                op: read_operator(op_expr.opno)?,
                left: Box::new(left),
                right: Box::new(right),
                pg_type: type_ref(op_expr.opresulttype, -1, op_expr.opcollid),
            })
        }
        pg_sys::NodeTag::T_NullTest => {
            let null_test = unsafe { &*node.cast::<pg_sys::NullTest>() };
            if null_test.argisrow {
                return Err(PgFrontendError::unsupported(
                    "row-valued NULL tests are not supported",
                ));
            }
            Ok(QueryExpr::NullTest {
                arg: Box::new(unsafe { read_expr(null_test.arg.cast()) }?),
                is_null: null_test.nulltesttype == pg_sys::NullTestType::IS_NULL,
            })
        }
        tag => Err(PgFrontendError::unsupported(format!(
            "expression node {:?} is not supported by pg_frontend v1",
            tag
        ))),
    }
}

unsafe fn read_const(constant: &pg_sys::Const) -> Result<Const, PgFrontendError> {
    let pg_type = type_ref(
        constant.consttype,
        constant.consttypmod,
        constant.constcollid,
    );
    supported_value_type(pg_type).map_err(|reason| PgFrontendError::unsupported(reason.message))?;
    if constant.constisnull {
        return Ok(Const {
            pg_type,
            value: None,
        });
    }
    supported_non_null_const_type(pg_type)
        .map_err(|reason| PgFrontendError::unsupported(reason.message))?;

    let value = match constant.consttype {
        oid if oid == pg_sys::BOOLOID => {
            PgConstValue::Bool(unsafe { bool::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT2OID => {
            PgConstValue::Int16(unsafe { i16::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT4OID => {
            PgConstValue::Int32(unsafe { i32::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT8OID => {
            PgConstValue::Int64(unsafe { i64::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::FLOAT4OID => PgConstValue::Float32(finite_float32_const(
            unsafe { f32::from_datum(constant.constvalue, false) }.unwrap(),
        )?),
        oid if oid == pg_sys::FLOAT8OID => PgConstValue::Float64(finite_float64_const(
            unsafe { f64::from_datum(constant.constvalue, false) }.unwrap(),
        )?),
        oid if oid == pg_sys::TEXTOID || oid == pg_sys::VARCHAROID || oid == pg_sys::BPCHAROID => {
            PgConstValue::Text(
                unsafe {
                    String::from_polymorphic_datum(constant.constvalue, false, constant.consttype)
                }
                .unwrap(),
            )
        }
        oid if oid == pg_sys::NAMEOID => {
            PgConstValue::Text(unsafe { read_name_const(constant.constvalue) }?)
        }
        oid if oid == pg_sys::BYTEAOID => PgConstValue::Binary(
            unsafe {
                Vec::<u8>::from_polymorphic_datum(constant.constvalue, false, constant.consttype)
            }
            .unwrap(),
        ),
        oid if oid == pg_sys::DATEOID => {
            return Err(unsupported_temporal_const("date"));
        }
        oid if oid == pg_sys::TIMEOID => PgConstValue::Time64Microsecond(time_const(
            unsafe { i64::from_datum(constant.constvalue, false) }.unwrap(),
        )?),
        oid if oid == pg_sys::TIMESTAMPOID => {
            return Err(unsupported_temporal_const("timestamp"));
        }
        oid if oid == pg_sys::TIMESTAMPTZOID => {
            return Err(unsupported_temporal_const("timestamptz"));
        }
        oid => {
            return Err(PgFrontendError::unsupported(format!(
                "constant type oid {} is not supported by pg_frontend v1",
                u32::from(oid)
            )))
        }
    };

    Ok(Const {
        pg_type,
        value: Some(value),
    })
}

unsafe fn read_name_const(datum: pg_sys::Datum) -> Result<String, PgFrontendError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
    if ptr.is_null() {
        return Err(PgFrontendError::unsupported("null name datum pointer"));
    }
    decode_name_data(unsafe { &*ptr })
}

fn decode_name_data(name: &pg_sys::NameData) -> Result<String, PgFrontendError> {
    decode_name_bytes(&name.data)
}

fn decode_name_bytes(bytes: &[c_char]) -> Result<String, PgFrontendError> {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    let raw = unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end) };
    str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|_| PgFrontendError::unsupported("name constants must contain valid UTF-8"))
}

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

    fn zeroed_name() -> pg_sys::NameData {
        unsafe { std::mem::zeroed() }
    }

    fn write_name_bytes(name: &mut pg_sys::NameData, bytes: &[u8]) {
        for (slot, byte) in name.data.iter_mut().zip(bytes.iter().copied()) {
            *slot = byte as c_char;
        }
    }
}
