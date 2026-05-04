use std::ffi::{c_char, c_int, c_void, CStr};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ptr::null_mut;

use arrow_schema::DataType;
use datafusion::logical_expr::LogicalPlan;
use pgrx::pg_sys::SysCacheIdentifier::TYPEOID;
use pgrx::pg_sys::{
    list_append_unique_ptr, list_make1_impl, palloc0, planner_hook, planner_hook_type,
    standard_planner, CommonTableExpr, Const, CustomScan, List, ListCell, Node, NodeTag, Oid,
    ParamListInfo, Plan, PlannedStmt, Query, RangeTblEntry,
};
use pgrx::prelude::*;

use crate::custom_scan::scan_methods;
use crate::guc::ENABLE;
use crate::utility_hook::skip_planner;

static mut PREV_PLANNER_HOOK: planner_hook_type = None;

const SPECIAL_NUMERIC_ERROR: &str =
    "pg_fusion does not support PostgreSQL numeric NaN/Infinity values because Arrow Decimal128 cannot represent them";

pub fn register_hooks() {
    unsafe {
        PREV_PLANNER_HOOK = planner_hook;
        planner_hook = Some(pg_fusion_planner_hook);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_fusion_planner_hook(
    parse: *mut Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
    if ENABLE.get() && !skip_planner() {
        if !parse.is_null()
            && (*parse).commandType == pgrx::pg_sys::CmdType::CMD_SELECT
            && !(*parse).hasModifyingCTE
            && !is_pg_fusion_management_sql(query_string)
            && !should_bypass_pg_fusion_planner(parse, bound_params)
        {
            return build_planned_custom_scan(parse, query_string, bound_params);
        }
    }

    if let Some(prev) = PREV_PLANNER_HOOK {
        prev(parse, query_string, cursor_options, bound_params)
    } else {
        standard_planner(parse, query_string, cursor_options, bound_params)
    }
}

unsafe fn is_pg_fusion_management_sql(query_string: *const c_char) -> bool {
    if query_string.is_null() {
        return false;
    }
    let Ok(sql) = CStr::from_ptr(query_string).to_str() else {
        return false;
    };
    let sql = sql.to_ascii_lowercase();
    sql.contains("pg_fusion_metrics(") || sql.contains("pg_fusion_metrics_reset(")
}

unsafe fn should_bypass_pg_fusion_planner(parse: *mut Query, bound_params: ParamListInfo) -> bool {
    has_bound_params(bound_params) || query_requires_vanilla_planner(parse)
}

unsafe fn has_bound_params(bound_params: ParamListInfo) -> bool {
    !bound_params.is_null() && (*bound_params).numParams > 0
}

unsafe fn query_requires_vanilla_planner(parse: *mut Query) -> bool {
    if parse.is_null() {
        return false;
    }

    rtable_requires_vanilla_planner((*parse).rtable)
        || cte_list_requires_vanilla_planner((*parse).cteList)
}

unsafe fn rtable_requires_vanilla_planner(rtable: *mut List) -> bool {
    for index in 0..list_len(rtable) {
        let rte = list_ptr_at(rtable, index) as *mut RangeTblEntry;
        if rte.is_null() {
            continue;
        }
        match (*rte).rtekind {
            pgrx::pg_sys::RTEKind::RTE_RELATION => {
                if relation_is_catalog_or_toast((*rte).relid) {
                    return true;
                }
            }
            pgrx::pg_sys::RTEKind::RTE_SUBQUERY => {
                if query_requires_vanilla_planner((*rte).subquery) {
                    return true;
                }
            }
            pgrx::pg_sys::RTEKind::RTE_FUNCTION | pgrx::pg_sys::RTEKind::RTE_TABLEFUNC => {
                return true;
            }
            _ => {}
        }
    }
    false
}

unsafe fn cte_list_requires_vanilla_planner(cte_list: *mut List) -> bool {
    for index in 0..list_len(cte_list) {
        let cte = list_ptr_at(cte_list, index) as *mut CommonTableExpr;
        if cte.is_null() || (*cte).ctequery.is_null() {
            continue;
        }
        if (*(*cte).ctequery).type_ != NodeTag::T_Query {
            continue;
        }
        if query_requires_vanilla_planner((*cte).ctequery as *mut Query) {
            return true;
        }
    }
    false
}

unsafe fn relation_is_catalog_or_toast(relid: Oid) -> bool {
    let namespace = pgrx::pg_sys::get_rel_namespace(relid);
    pgrx::pg_sys::IsCatalogNamespace(namespace) || pgrx::pg_sys::IsToastNamespace(namespace)
}

unsafe fn list_len(list: *mut List) -> i32 {
    if list.is_null() {
        0
    } else {
        (*list).length
    }
}

unsafe fn list_ptr_at(list: *mut List, index: i32) -> *mut c_void {
    if list.is_null() || index < 0 || index >= (*list).length {
        return null_mut();
    }
    (*(*list).elements.offset(index as isize)).ptr_value
}

#[pg_guard]
unsafe extern "C-unwind" fn build_planned_custom_scan(
    parse: *mut Query,
    query_string: *const c_char,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
    if !bound_params.is_null() && (*bound_params).numParams > 0 {
        error!(
            "pg_fusion v1 does not support bind parameters yet; see planner.rs TODO for ParamListInfo -> ScalarValue bridging"
        );
    }
    if query_contains_special_numeric_const(parse) {
        error!("{SPECIAL_NUMERIC_ERROR}");
    }

    let sql = select_sql_from_query(parse, query_string);
    let config = crate::host_config().unwrap_or_else(|err| error!("pg_fusion config error: {err}"));
    let built = plan_builder::PlanBuilder::new()
        .with_config(config.plan_builder_config())
        .build(plan_builder::PlanBuildInput {
            sql: &sql,
            params: Vec::new(),
        })
        .unwrap_or_else(|err| error!("pg_fusion planner build failed: {err}"));

    let target_lists = build_custom_scan_target_lists(&built.logical_plan)
        .unwrap_or_else(|err| error!("pg_fusion targetlist build failed: {err}"));
    let custom_scan = pack_custom_scan(&sql, target_lists);

    let stmt_ptr = palloc0(size_of::<PlannedStmt>()) as *mut PlannedStmt;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    let statement = PlannedStmt {
        type_: NodeTag::T_PlannedStmt,
        commandType: pgrx::pg_sys::CmdType::CMD_SELECT,
        queryId: hasher.finish(),
        hasReturning: false,
        hasModifyingCTE: false,
        canSetTag: false,
        transientPlan: false,
        dependsOnRole: false,
        parallelModeNeeded: false,
        planTree: custom_scan as *mut Plan,
        rtable: null_mut(),
        permInfos: null_mut(),
        resultRelations: null_mut(),
        subplans: null_mut(),
        rewindPlanIDs: null_mut(),
        rowMarks: null_mut(),
        relationOids: null_mut(),
        invalItems: null_mut(),
        paramExecTypes: null_mut(),
        utilityStmt: null_mut(),
        stmt_location: -1,
        stmt_len: 0,
        ..Default::default()
    };
    std::ptr::write(stmt_ptr, statement);
    stmt_ptr
}

unsafe fn query_contains_special_numeric_const(parse: *mut Query) -> bool {
    if parse.is_null() {
        return false;
    }

    let mut found = false;
    pgrx::pg_sys::query_tree_walker(
        parse,
        Some(special_numeric_const_walker),
        (&mut found as *mut bool).cast::<c_void>(),
        0,
    );
    found
}

unsafe extern "C-unwind" fn special_numeric_const_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    match (*node).type_ {
        NodeTag::T_Query => pgrx::pg_sys::query_tree_walker(
            node.cast::<Query>(),
            Some(special_numeric_const_walker),
            context,
            0,
        ),
        NodeTag::T_Const => {
            let constant = node.cast::<Const>();
            if !(*constant).constisnull
                && (*constant).consttype == pgrx::pg_sys::NUMERICOID
                && numeric_datum_is_special((*constant).constvalue)
            {
                *context.cast::<bool>() = true;
                return true;
            }
            pgrx::pg_sys::expression_tree_walker(node, Some(special_numeric_const_walker), context)
        }
        _ => {
            pgrx::pg_sys::expression_tree_walker(node, Some(special_numeric_const_walker), context)
        }
    }
}

unsafe fn numeric_datum_is_special(datum: pgrx::pg_sys::Datum) -> bool {
    let original = datum.cast_mut_ptr::<pgrx::pg_sys::varlena>();
    let detoasted = pgrx::pg_sys::pg_detoast_datum(original);
    let is_copy = !std::ptr::eq(detoasted, original);
    let numeric = detoasted.cast::<pgrx::pg_sys::NumericData>();
    let is_special = pgrx::pg_sys::numeric_is_nan(numeric) || pgrx::pg_sys::numeric_is_inf(numeric);
    if is_copy {
        pgrx::pg_sys::pfree(detoasted.cast());
    }
    is_special
}

unsafe fn select_sql_from_query(parse: *mut Query, query_string: *const c_char) -> String {
    let sql = CStr::from_ptr(query_string)
        .to_str()
        .expect("planner query string must be valid UTF-8");
    if parse.is_null() {
        return sql.to_owned();
    }

    if should_deparse_planner_query(sql) {
        let deparsed = pgrx::pg_sys::pg_get_querydef(parse, false);
        if !deparsed.is_null() {
            return CStr::from_ptr(deparsed)
                .to_str()
                .expect("deparsed query text must be valid UTF-8")
                .to_owned();
        }
    }

    sql.to_owned()
}

fn should_deparse_planner_query(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    sql.starts_with("EXPLAIN") || sql.starts_with("COPY")
}

struct CustomScanTargetLists {
    plan_target_list: *mut List,
    scan_target_list: *mut List,
}

unsafe fn pack_custom_scan(sql: &str, target_lists: CustomScanTargetLists) -> *mut CustomScan {
    let sql_copy = palloc0(sql.len() + 1) as *mut u8;
    std::ptr::copy_nonoverlapping(sql.as_ptr(), sql_copy, sql.len());
    let query = ListCell {
        ptr_value: pgrx::pg_sys::makeString(sql_copy.cast()) as *mut c_void,
    };

    let mut custom_scan = CustomScan::default();
    custom_scan.scan.plan.type_ = NodeTag::T_CustomScan;
    custom_scan.custom_private = list_make1_impl(NodeTag::T_List, query);
    custom_scan.custom_scan_tlist = target_lists.scan_target_list;
    custom_scan.scan.plan.targetlist = target_lists.plan_target_list;
    custom_scan.methods = scan_methods();

    let ptr = palloc0(size_of::<CustomScan>()) as *mut CustomScan;
    std::ptr::write(ptr, custom_scan);
    ptr
}

fn build_custom_scan_target_lists(
    logical_plan: &LogicalPlan,
) -> Result<CustomScanTargetLists, String> {
    let fields = logical_plan.schema().fields();
    let mut plan_target_list: *mut List = std::ptr::null_mut();
    let mut scan_target_list: *mut List = std::ptr::null_mut();
    for (index, field) in fields.iter().enumerate() {
        let oid = type_to_oid(field.data_type())
            .ok_or_else(|| format!("unsupported output type {}", field.data_type()))?;
        unsafe {
            let tuple =
                pgrx::pg_sys::SearchSysCache1(TYPEOID as i32, pgrx::pg_sys::ObjectIdGetDatum(oid));
            if tuple.is_null() {
                return Err(format!("type cache lookup failed for oid {}", oid.to_u32()));
            }
            let typtup = pgrx::pg_sys::GETSTRUCT(tuple) as pgrx::pg_sys::Form_pg_type;
            let attr_number = i16::try_from(index + 1)
                .map_err(|_| "custom scan output has too many columns".to_string())?;
            let typmod = (*typtup).typtypmod;
            let collation = (*typtup).typcollation;
            let plan_expr = pgrx::pg_sys::makeVar(
                pgrx::pg_sys::INDEX_VAR,
                attr_number,
                oid,
                typmod,
                collation,
                0,
            );
            let scan_expr = pgrx::pg_sys::makeNullConst(oid, typmod, collation);
            let name = field.name();
            let plan_entry = pgrx::pg_sys::makeTargetEntry(
                plan_expr as *mut pgrx::pg_sys::Expr,
                attr_number as _,
                pstrdup(name),
                false,
            );
            let scan_entry = pgrx::pg_sys::makeTargetEntry(
                scan_expr as *mut pgrx::pg_sys::Expr,
                attr_number as _,
                pstrdup(name),
                false,
            );
            plan_target_list = list_append_unique_ptr(plan_target_list, plan_entry as *mut c_void);
            scan_target_list = list_append_unique_ptr(scan_target_list, scan_entry as *mut c_void);
            pgrx::pg_sys::ReleaseSysCache(tuple);
        }
    }
    Ok(CustomScanTargetLists {
        plan_target_list,
        scan_target_list,
    })
}

unsafe fn pstrdup(value: &str) -> *mut i8 {
    let ptr = palloc0(value.len() + 1) as *mut u8;
    std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, value.len());
    ptr.cast()
}

fn type_to_oid(data_type: &DataType) -> Option<Oid> {
    match data_type {
        DataType::Boolean => Some(pgrx::pg_sys::BOOLOID),
        DataType::Int16 => Some(pgrx::pg_sys::INT2OID),
        DataType::Int32 => Some(pgrx::pg_sys::INT4OID),
        DataType::Int64 => Some(pgrx::pg_sys::INT8OID),
        DataType::Float32 => Some(pgrx::pg_sys::FLOAT4OID),
        DataType::Float64 => Some(pgrx::pg_sys::FLOAT8OID),
        DataType::Decimal128(_, _) => Some(pgrx::pg_sys::NUMERICOID),
        DataType::Utf8 | DataType::Utf8View => Some(pgrx::pg_sys::TEXTOID),
        DataType::Binary | DataType::BinaryView => Some(pgrx::pg_sys::BYTEAOID),
        DataType::FixedSizeBinary(16) => Some(pgrx::pg_sys::UUIDOID),
        _ => None,
    }
}

// TODO(darthunix): add ParamListInfo -> ScalarValue bridging for bind params in
// the new thin-host planner/custom-scan path. The first cutover intentionally
// matches current effective behavior and does not support backend bind params.
