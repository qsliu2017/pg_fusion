use super::*;

pub(super) unsafe fn read_target_list(
    target_list: *mut pg_sys::List,
    scope: &CteScope,
) -> Result<Vec<Target>, PgFrontendError> {
    let mut targets = Vec::new();
    for index in 0..unsafe { list_len(target_list) } {
        let entry = unsafe { list_ptr_at(target_list, index) as *mut pg_sys::TargetEntry };
        if entry.is_null() {
            return Err(PgFrontendError::unsupported("null target entry"));
        }
        let entry_ref = unsafe { &*entry };
        let fallback_pg_type = unsafe { expr_type_ref(entry_ref.expr.cast()) };
        let expr = unsafe { read_expr(entry_ref.expr.cast(), scope) }?;
        let pg_type = target_output_type(&expr).unwrap_or(fallback_pg_type);
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
            ressortgroupref: entry_ref.ressortgroupref,
            resjunk: entry_ref.resjunk,
        });
    }
    Ok(targets)
}

pub(super) fn target_output_type(expr: &QueryExpr) -> Option<PgTypeRef> {
    match expr {
        QueryExpr::Var(var) => Some(var.pg_type),
        QueryExpr::OuterVar(var) => Some(var.pg_type),
        QueryExpr::Const(constant) => Some(constant.pg_type),
        QueryExpr::Param(param) => Some(param.pg_type),
        QueryExpr::Cast { pg_type, .. }
        | QueryExpr::Array { pg_type, .. }
        | QueryExpr::ArraySubscript { pg_type, .. }
        | QueryExpr::FunctionCall { pg_type, .. }
        | QueryExpr::BinaryOp { pg_type, .. }
        | QueryExpr::UnaryOp { pg_type, .. }
        | QueryExpr::AggregateCall { pg_type, .. }
        | QueryExpr::WindowCall { pg_type, .. }
        | QueryExpr::Coalesce { pg_type, .. }
        | QueryExpr::Case { pg_type, .. }
        | QueryExpr::ExistsSubquery { pg_type, .. }
        | QueryExpr::InSubquery { pg_type, .. } => Some(*pg_type),
        QueryExpr::RelabelType(_)
        | QueryExpr::Bool { .. }
        | QueryExpr::ScalarSubquery(_)
        | QueryExpr::NullTest { .. }
        | QueryExpr::BooleanTest { .. } => None,
    }
}

pub(super) unsafe fn read_sort_clause(
    sort_clause: *mut pg_sys::List,
) -> Result<Vec<SortKey>, PgFrontendError> {
    let mut sort = Vec::new();
    for index in 0..unsafe { list_len(sort_clause) } {
        let clause = unsafe { list_ptr_at(sort_clause, index) as *mut pg_sys::SortGroupClause };
        if clause.is_null() {
            return Err(PgFrontendError::unsupported("null sort clause"));
        }
        let clause_ref = unsafe { &*clause };
        let op = read_operator(clause_ref.sortop)?;
        let asc = match op {
            QueryOperator::Lt => true,
            QueryOperator::Gt => false,
            _ => {
                return Err(PgFrontendError::unsupported(
                    "ORDER BY sort operator is not supported by pg_frontend v1",
                ))
            }
        };
        sort.push(SortKey {
            target_ref: clause_ref.tleSortGroupRef,
            asc,
            nulls_first: clause_ref.nulls_first,
        });
    }
    Ok(sort)
}

pub(super) unsafe fn read_sort_group_refs(
    group_clause: *mut pg_sys::List,
) -> Result<Vec<u32>, PgFrontendError> {
    let mut refs = Vec::new();
    for index in 0..unsafe { list_len(group_clause) } {
        let clause = unsafe { list_ptr_at(group_clause, index) as *mut pg_sys::SortGroupClause };
        if clause.is_null() {
            return Err(PgFrontendError::unsupported("null group clause"));
        }
        refs.push(unsafe { (*clause).tleSortGroupRef });
    }
    Ok(refs)
}

pub(super) unsafe fn read_distinct_spec(
    query_ref: &pg_sys::Query,
) -> Result<DistinctSpec, PgFrontendError> {
    if query_ref.hasDistinctOn {
        let target_refs = unsafe { read_sort_group_refs(query_ref.distinctClause) }?;
        if target_refs.is_empty() {
            return Err(PgFrontendError::unsupported(
                "DISTINCT ON has no target expressions",
            ));
        }
        return Ok(DistinctSpec::On { target_refs });
    }

    if !query_ref.distinctClause.is_null() {
        return Ok(DistinctSpec::FullRow);
    }

    Ok(DistinctSpec::None)
}

pub(super) unsafe fn read_grouping_sets(
    grouping_sets: *mut pg_sys::List,
) -> Result<Vec<GroupingSetSpec>, PgFrontendError> {
    let mut sets = Vec::new();
    for index in 0..unsafe { list_len(grouping_sets) } {
        let set = unsafe { list_ptr_at(grouping_sets, index).cast::<pg_sys::GroupingSet>() };
        sets.push(unsafe { read_grouping_set(set) }?);
    }
    Ok(sets)
}

pub(super) unsafe fn read_grouping_set(
    set: *mut pg_sys::GroupingSet,
) -> Result<GroupingSetSpec, PgFrontendError> {
    if set.is_null() {
        return Err(PgFrontendError::unsupported("null grouping set"));
    }
    let set_ref = unsafe { &*set };
    match set_ref.kind {
        pg_sys::GroupingSetKind::GROUPING_SET_EMPTY => Ok(GroupingSetSpec::Empty),
        pg_sys::GroupingSetKind::GROUPING_SET_SIMPLE => Ok(GroupingSetSpec::Simple(unsafe {
            read_grouping_ref_list(set_ref.content)
        }?)),
        pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP => Ok(GroupingSetSpec::Rollup(unsafe {
            read_grouping_atom_list(set_ref.content)
        }?)),
        pg_sys::GroupingSetKind::GROUPING_SET_CUBE => Ok(GroupingSetSpec::Cube(unsafe {
            read_grouping_atom_list(set_ref.content)
        }?)),
        pg_sys::GroupingSetKind::GROUPING_SET_SETS => {
            let mut nested = Vec::new();
            for index in 0..unsafe { list_len(set_ref.content) } {
                let child =
                    unsafe { list_ptr_at(set_ref.content, index).cast::<pg_sys::GroupingSet>() };
                nested.push(unsafe { read_grouping_set(child) }?);
            }
            Ok(GroupingSetSpec::Sets(nested))
        }
        kind => Err(PgFrontendError::unsupported(format!(
            "grouping set kind {kind} is not supported by pg_frontend v1"
        ))),
    }
}

pub(super) unsafe fn read_grouping_ref_list(
    refs: *mut pg_sys::List,
) -> Result<Vec<u32>, PgFrontendError> {
    let mut out = Vec::new();
    for index in 0..unsafe { list_len(refs) } {
        out.push(unsafe { list_int_at(refs, index) as u32 });
    }
    Ok(out)
}

pub(super) unsafe fn read_grouping_atom_list(
    atoms: *mut pg_sys::List,
) -> Result<Vec<Vec<u32>>, PgFrontendError> {
    let mut out = Vec::new();
    for index in 0..unsafe { list_len(atoms) } {
        let atom = unsafe { list_ptr_at(atoms, index).cast::<pg_sys::GroupingSet>() };
        if atom.is_null() {
            return Err(PgFrontendError::unsupported("null grouping set atom"));
        }
        let atom_ref = unsafe { &*atom };
        if atom_ref.kind != pg_sys::GroupingSetKind::GROUPING_SET_SIMPLE {
            return Err(PgFrontendError::unsupported(
                "ROLLUP/CUBE grouping set atoms must be SIMPLE nodes",
            ));
        }
        out.push(unsafe { read_grouping_ref_list(atom_ref.content) }?);
    }
    Ok(out)
}
