use std::ffi::{c_char, c_int, c_void, CStr};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ptr::null_mut;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::logical_expr::{
    lit,
    logical_plan::{EmptyRelation, Filter, Projection},
    Expr, LogicalPlan,
};
use datafusion_common::{DFSchema, ScalarValue};
use pg_frontend::{PgFrontend, PgFrontendConfig, Target};
use pgrx::pg_sys::{
    list_append_unique_oid, list_append_unique_ptr, list_make1_impl, palloc0, planner_hook,
    planner_hook_type, standard_planner, Const, CustomScan, List, ListCell, Node, NodeTag, Oid,
    ParamListInfo, Plan, PlannedStmt, Query,
};
use pgrx::prelude::*;

use crate::custom_scan::scan_methods;
use crate::guc::{HostConfig, ENABLE};
use crate::plan_payload::encode_frontend_plan;
use crate::utility_hook::skip_planner;

static mut PREV_PLANNER_HOOK: planner_hook_type = None;

const SPECIAL_NUMERIC_ERROR: &str =
    "pg_fusion Decimal128 avg cannot represent PostgreSQL numeric NaN/Infinity values";

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
    if ENABLE.get()
        && !skip_planner()
        && !parse.is_null()
        && (*parse).commandType == pgrx::pg_sys::CmdType::CMD_SELECT
        && !is_pg_fusion_management_sql(query_string)
    {
        let config =
            crate::host_config().unwrap_or_else(|err| error!("pg_fusion config error: {err}"));
        match build_planned_custom_scan(parse, &config) {
            Ok(planned) => return planned.into_planned_stmt(),
            Err(err) => {
                error!("pg_fusion query-tree frontend planning failed: {err}");
            }
        }
    }

    call_next_planner(parse, query_string, cursor_options, bound_params)
}

unsafe fn call_next_planner(
    parse: *mut Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
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

unsafe fn build_planned_custom_scan(
    parse: *mut Query,
    config: &HostConfig,
) -> Result<PlannedCustomScan, String> {
    if query_contains_special_numeric_const(parse) {
        error!("{SPECIAL_NUMERIC_ERROR}");
    }

    build_frontend_plan(parse, config)
}

struct PlannedCustomScan {
    custom_scan: *mut CustomScan,
    query_id_seed: String,
    relation_oids: Vec<u32>,
}

impl PlannedCustomScan {
    unsafe fn into_planned_stmt(self) -> *mut PlannedStmt {
        let stmt_ptr = palloc0(size_of::<PlannedStmt>()) as *mut PlannedStmt;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.query_id_seed.hash(&mut hasher);
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
            planTree: self.custom_scan as *mut Plan,
            rtable: null_mut(),
            permInfos: null_mut(),
            resultRelations: null_mut(),
            subplans: null_mut(),
            rewindPlanIDs: null_mut(),
            rowMarks: null_mut(),
            relationOids: relation_oid_list(&self.relation_oids),
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
}

unsafe fn build_frontend_plan(
    parse: *mut Query,
    config: &HostConfig,
) -> Result<PlannedCustomScan, String> {
    let frontend = PgFrontend::new().with_config(PgFrontendConfig {
        identifier_max_bytes: config.plan_builder_config().identifier_max_bytes,
    });
    let typed_query = frontend.read_query(parse).map_err(|err| err.to_string())?;
    let output = frontend
        .build_query(typed_query)
        .map_err(|err| err.to_string())?;
    let built = plan_builder::build_frontend_logical_plan(
        output.logical_plan,
        config.plan_builder_config(),
    )
    .map_err(|err| err.to_string())?;
    let logical_plan =
        restore_frontend_empty_schema(built.logical_plan, output.result_schema.clone())?;
    let target_lists = build_custom_scan_target_lists_from_pg_targets(&output.result_targets)
        .map_err(|err| format!("pg_fusion frontend targetlist build failed: {err}"))?;
    let payload = encode_frontend_plan(&logical_plan).map_err(|err| err.to_string())?;
    let relation_oids = relation_oids_from_scans(built.scan_plan.scans());
    Ok(PlannedCustomScan {
        custom_scan: pack_custom_scan_payload(&payload, target_lists),
        query_id_seed: payload,
        relation_oids,
    })
}

fn restore_frontend_empty_schema(
    plan: LogicalPlan,
    result_schema: SchemaRef,
) -> Result<LogicalPlan, String> {
    if result_schema.fields().is_empty() {
        return Ok(plan);
    }

    match plan {
        LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row => {
            let input = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(DFSchema::empty()),
            });
            let filter = LogicalPlan::Filter(
                Filter::try_new(lit(false), Arc::new(input))
                    .map_err(|err| format!("pg_fusion frontend empty filter failed: {err}"))?,
            );
            let projection = result_schema
                .fields()
                .iter()
                .map(|field| {
                    let value = ScalarValue::try_from(field.data_type()).map_err(|err| {
                        format!("pg_fusion frontend empty result literal failed: {err}")
                    })?;
                    Ok(Expr::Literal(value, None).alias(field.name()))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Projection::try_new(projection, Arc::new(filter))
                .map(LogicalPlan::Projection)
                .map_err(|err| format!("pg_fusion frontend empty projection failed: {err}"))
        }
        other => Ok(other),
    }
}

fn relation_oids_from_scans(scans: &[std::sync::Arc<scan_node::PgScanSpec>]) -> Vec<u32> {
    scans.iter().map(|scan| scan.table_oid).collect()
}

unsafe fn relation_oid_list(relation_oids: &[u32]) -> *mut List {
    let mut list: *mut List = null_mut();
    for oid in relation_oids {
        list = list_append_unique_oid(list, Oid::from_u32(*oid));
    }
    list
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

struct CustomScanTargetLists {
    plan_target_list: *mut List,
    scan_target_list: *mut List,
}

unsafe fn pack_custom_scan_payload(
    payload: &str,
    target_lists: CustomScanTargetLists,
) -> *mut CustomScan {
    let payload_copy = palloc0(payload.len() + 1) as *mut u8;
    std::ptr::copy_nonoverlapping(payload.as_ptr(), payload_copy, payload.len());
    let query = ListCell {
        ptr_value: pgrx::pg_sys::makeString(payload_copy.cast()) as *mut c_void,
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

fn build_custom_scan_target_lists_from_pg_targets(
    targets: &[Target],
) -> Result<CustomScanTargetLists, String> {
    let mut plan_target_list: *mut List = std::ptr::null_mut();
    let mut scan_target_list: *mut List = std::ptr::null_mut();
    for (index, target) in targets.iter().enumerate() {
        unsafe {
            let attr_number = i16::try_from(index + 1)
                .map_err(|_| "custom scan output has too many columns".to_string())?;
            let oid = Oid::from_u32(target.pg_type.oid);
            let typmod = target.pg_type.typmod;
            let collation = Oid::from_u32(target.pg_type.collation);
            let plan_expr = pgrx::pg_sys::makeVar(
                pgrx::pg_sys::INDEX_VAR,
                attr_number,
                oid,
                typmod,
                collation,
                0,
            );
            let scan_expr = pgrx::pg_sys::makeNullConst(oid, typmod, collation);
            let name = target.name.as_deref().unwrap_or("?column?");
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

// TODO(darthunix): add ParamListInfo -> ScalarValue bridging for bind params in
// the new thin-host planner/custom-scan path. The first cutover intentionally
// matches current effective behavior and does not support backend bind params.
