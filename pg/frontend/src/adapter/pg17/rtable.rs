use super::*;

pub(super) struct RTableRefs {
    pub(super) relations: Vec<RelationRef>,
    pub(super) values: Vec<ValuesRef>,
    pub(super) ctes: Vec<CteRangeRef>,
    pub(super) subqueries: Vec<SubqueryRef>,
    pub(super) outer_columns: OuterScope,
    pub(super) join_aliases: HashMap<(usize, i16), QueryExpr>,
}

impl RTableRefs {
    pub(super) fn values_rtindexes(&self) -> HashSet<usize> {
        self.values.iter().map(|values| values.rtindex).collect()
    }

    pub(super) fn cte_rtindexes(&self) -> HashSet<usize> {
        self.ctes.iter().map(|cte| cte.rtindex).collect()
    }

    pub(super) fn subquery_rtindexes(&self) -> HashSet<usize> {
        self.subqueries
            .iter()
            .map(|subquery| subquery.rtindex)
            .collect()
    }
}

pub(super) unsafe fn read_rtable(
    rtable: *mut pg_sys::List,
    scope: &CteScope,
) -> Result<RTableRefs, PgFrontendError> {
    let mut relations = Vec::new();
    let mut values = Vec::new();
    let mut ctes = Vec::new();
    let mut subqueries = Vec::new();
    let mut outer_columns = OuterScope::default();
    let mut join_aliases = HashMap::new();
    for index in 0..unsafe { list_len(rtable) } {
        let rte = unsafe { list_ptr_at(rtable, index) as *mut pg_sys::RangeTblEntry };
        if rte.is_null() {
            return Err(PgFrontendError::unsupported("null rtable entry"));
        }
        let rte_ref = unsafe { &*rte };
        let rtindex = (index + 1) as usize;
        if rte_ref.rtekind == pg_sys::RTEKind::RTE_JOIN {
            unsafe {
                read_join_aliases(
                    rtindex,
                    rte_ref,
                    scope,
                    &mut outer_columns,
                    &mut join_aliases,
                )
            }?;
            continue;
        }
        if rte_ref.rtekind == pg_sys::RTEKind::RTE_VALUES {
            let values_ref = unsafe { read_values_ref(rtindex, rte_ref, scope) }?;
            add_outer_columns(
                &mut outer_columns,
                rtindex,
                table_ref_for_values(&values_ref),
                &values_ref.columns,
            );
            values.push(values_ref);
            continue;
        }
        if rte_ref.rtekind == pg_sys::RTEKind::RTE_CTE {
            let cte = unsafe { read_cte_range_ref(rtindex, rte_ref, &scope.ids) }?;
            add_outer_columns(
                &mut outer_columns,
                rtindex,
                table_ref_for_cte(&cte),
                &cte.columns,
            );
            ctes.push(cte);
            continue;
        }
        if rte_ref.rtekind == pg_sys::RTEKind::RTE_SUBQUERY {
            let subquery = unsafe { read_subquery_ref(rtindex, rte_ref, scope) }?;
            add_outer_columns(
                &mut outer_columns,
                rtindex,
                table_ref_for_subquery(&subquery),
                &subquery.columns,
            );
            subqueries.push(subquery);
            continue;
        }
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

        let relation = unsafe { read_relation_ref(rtindex, rte_ref) }?;
        outer_columns.relations.insert(
            rtindex,
            OuterRelation {
                relid: relation.relid,
                relation: Some(table_ref_for_relation(&relation)),
            },
        );
        relations.push(relation);
    }
    Ok(RTableRefs {
        relations,
        values,
        ctes,
        subqueries,
        outer_columns,
        join_aliases,
    })
}

pub(super) unsafe fn read_join_aliases(
    rtindex: usize,
    rte: &pg_sys::RangeTblEntry,
    scope: &CteScope,
    outer_columns: &mut OuterScope,
    join_aliases: &mut HashMap<(usize, i16), QueryExpr>,
) -> Result<(), PgFrontendError> {
    let alias_scope = scope.with_join_aliases(join_aliases.clone());
    let mut columns = Vec::new();
    for index in 0..unsafe { list_len(rte.joinaliasvars) } {
        let attnum = index as i16 + 1;
        let node = unsafe { list_ptr_at(rte.joinaliasvars, index) as *mut pg_sys::Node };
        if node.is_null() {
            continue;
        }
        let pg_type = unsafe { expr_type_ref(node) };
        supported_value_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
        let expr = unsafe { read_expr(node, &alias_scope) }?;
        join_aliases.insert((rtindex, attnum), expr);
        columns.push(ColumnRef {
            attnum,
            name: unsafe { read_alias_colname(rte.eref, index) }
                .unwrap_or_else(|| format!("column{}", index + 1)),
            pg_type,
            nullable: true,
        });
    }

    let relation =
        unsafe { read_alias_name(rte.eref) }.or_else(|| unsafe { read_alias_name(rte.alias) });
    add_outer_columns(outer_columns, rtindex, relation, &columns);
    Ok(())
}

pub(super) fn add_outer_columns(
    scope: &mut OuterScope,
    rtindex: usize,
    relation: Option<String>,
    columns: &[ColumnRef],
) {
    for column in columns {
        scope.columns.insert(
            (rtindex, column.attnum),
            OuterColumn {
                relation: relation.clone(),
                name: column.name.clone(),
            },
        );
    }
}

pub(super) fn table_ref_for_relation(relation: &RelationRef) -> String {
    relation
        .alias
        .clone()
        .unwrap_or_else(|| format!("{}.{}", relation.schema, relation.name))
}

pub(super) fn table_ref_for_values(values: &ValuesRef) -> Option<String> {
    Some(
        values
            .alias
            .clone()
            .unwrap_or_else(|| format!("values_{}", values.rtindex)),
    )
}

pub(super) fn table_ref_for_cte(cte: &CteRangeRef) -> Option<String> {
    Some(cte.alias.clone().unwrap_or_else(|| cte.name.clone()))
}

pub(super) fn table_ref_for_subquery(subquery: &SubqueryRef) -> Option<String> {
    Some(
        subquery
            .alias
            .clone()
            .unwrap_or_else(|| format!("subquery_{}", subquery.rtindex)),
    )
}

pub(super) unsafe fn relation_attname(relid: u32, attnum: i16) -> Result<String, PgFrontendError> {
    let name = unsafe { pg_sys::get_attname(relid.into(), attnum, true) };
    if name.is_null() {
        return Err(PgFrontendError::unsupported(format!(
            "outer-reference relation attribute {attnum} was not found"
        )));
    }
    unsafe { cstr_from_pg(name) }
}

pub(super) unsafe fn read_ctes(
    cte_list: *mut pg_sys::List,
    visible_scope: &mut CteScope,
) -> Result<(), PgFrontendError> {
    for index in 0..unsafe { list_len(cte_list) } {
        let cte = unsafe { list_ptr_at(cte_list, index) as *mut pg_sys::CommonTableExpr };
        if cte.is_null() {
            return Err(PgFrontendError::unsupported("null CTE entry"));
        }
        let cte_ref = unsafe { &*cte };
        if cte_ref.cterecursive {
            return Err(PgFrontendError::unsupported(
                "recursive CTEs are not supported by pg_frontend v1",
            ));
        }
        if !cte_ref.search_clause.is_null() || !cte_ref.cycle_clause.is_null() {
            return Err(PgFrontendError::unsupported(
                "CTE SEARCH/CYCLE clauses are not supported by pg_frontend v1",
            ));
        }
        if cte_ref.ctequery.is_null()
            || unsafe { (*cte_ref.ctequery).type_ } != pg_sys::NodeTag::T_Query
        {
            return Err(PgFrontendError::unsupported(
                "CTE query is not a SELECT query tree",
            ));
        }
        let id = visible_scope.allocate_cte_id();
        let name = unsafe { cstr_from_pg(cte_ref.ctename) }?;
        let cte_def = CteDef {
            id,
            name: name.clone(),
            query: Box::new(unsafe {
                read_query_with_scope(cte_ref.ctequery.cast(), visible_scope)
            }?),
        };
        visible_scope.ids.insert(name, id);
        visible_scope.defs.push(cte_def);
    }
    Ok(())
}

pub(super) unsafe fn read_relation_ref(
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

pub(super) unsafe fn read_values_ref(
    rtindex: usize,
    rte: &pg_sys::RangeTblEntry,
    scope: &CteScope,
) -> Result<ValuesRef, PgFrontendError> {
    let alias =
        unsafe { read_alias_name(rte.eref) }.or_else(|| unsafe { read_alias_name(rte.alias) });
    let mut columns = Vec::new();
    for index in 0..unsafe { list_len(rte.coltypes) } {
        let pg_type = type_ref(
            unsafe { list_oid_at(rte.coltypes, index) },
            unsafe { list_int_at(rte.coltypmods, index) },
            unsafe { list_oid_at(rte.colcollations, index) },
        );
        supported_value_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
        columns.push(ColumnRef {
            attnum: index as i16 + 1,
            name: unsafe { read_alias_colname(rte.eref, index) }
                .unwrap_or_else(|| format!("column{}", index + 1)),
            pg_type,
            nullable: true,
        });
    }

    let mut rows = Vec::new();
    for row_index in 0..unsafe { list_len(rte.values_lists) } {
        let row_list = unsafe { list_ptr_at(rte.values_lists, row_index) as *mut pg_sys::List };
        let mut row = Vec::new();
        for col_index in 0..unsafe { list_len(row_list) } {
            row.push(unsafe { read_expr(list_ptr_at(row_list, col_index).cast(), scope) }?);
        }
        rows.push(row);
    }

    Ok(ValuesRef {
        rtindex,
        alias,
        columns,
        rows,
    })
}

pub(super) unsafe fn read_cte_range_ref(
    rtindex: usize,
    rte: &pg_sys::RangeTblEntry,
    cte_ids: &HashMap<String, u64>,
) -> Result<CteRangeRef, PgFrontendError> {
    if rte.self_reference {
        return Err(PgFrontendError::unsupported(
            "recursive CTE references are not supported by pg_frontend v1",
        ));
    }
    let name = unsafe { cstr_from_pg(rte.ctename) }?;
    let cte_id = *cte_ids.get(&name).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "CTE reference {name} has no matching CTE definition"
        ))
    })?;
    let alias =
        unsafe { read_alias_name(rte.eref) }.or_else(|| unsafe { read_alias_name(rte.alias) });
    let mut columns = Vec::new();
    for index in 0..unsafe { list_len(rte.coltypes) } {
        let pg_type = type_ref(
            unsafe { list_oid_at(rte.coltypes, index) },
            unsafe { list_int_at(rte.coltypmods, index) },
            unsafe { list_oid_at(rte.colcollations, index) },
        );
        supported_value_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
        columns.push(ColumnRef {
            attnum: index as i16 + 1,
            name: unsafe { read_alias_colname(rte.eref, index) }
                .unwrap_or_else(|| format!("column{}", index + 1)),
            pg_type,
            nullable: true,
        });
    }
    Ok(CteRangeRef {
        rtindex,
        cte_id,
        name,
        alias,
        columns,
    })
}

pub(super) unsafe fn read_subquery_ref(
    rtindex: usize,
    rte: &pg_sys::RangeTblEntry,
    scope: &CteScope,
) -> Result<SubqueryRef, PgFrontendError> {
    if rte.lateral {
        return Err(PgFrontendError::unsupported(
            "LATERAL subqueries are not supported by pg_frontend v1",
        ));
    }
    if rte.subquery.is_null() {
        return Err(PgFrontendError::unsupported(
            "subquery range table entry has no query tree",
        ));
    }
    let alias =
        unsafe { read_alias_name(rte.eref) }.or_else(|| unsafe { read_alias_name(rte.alias) });
    let mut columns = Vec::new();
    let mut visible_index = 0;
    for index in 0..unsafe { list_len((*rte.subquery).targetList) } {
        let entry =
            unsafe { list_ptr_at((*rte.subquery).targetList, index) as *mut pg_sys::TargetEntry };
        if entry.is_null() {
            return Err(PgFrontendError::unsupported("null subquery target entry"));
        }
        let entry_ref = unsafe { &*entry };
        if entry_ref.resjunk {
            continue;
        }
        let pg_type = unsafe { expr_type_ref(entry_ref.expr.cast()) };
        supported_value_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
        columns.push(ColumnRef {
            attnum: entry_ref.resno,
            name: unsafe { read_alias_colname(rte.eref, visible_index) }
                .unwrap_or_else(|| format!("column{}", visible_index + 1)),
            pg_type,
            nullable: true,
        });
        visible_index += 1;
    }
    Ok(SubqueryRef {
        rtindex,
        alias,
        columns,
        query: Box::new(unsafe { read_query_with_scope(rte.subquery, scope) }?),
    })
}

pub(super) unsafe fn read_alias_colname(alias: *mut pg_sys::Alias, index: i32) -> Option<String> {
    if alias.is_null() {
        return None;
    }
    let colnames = unsafe { (*alias).colnames };
    let node = unsafe { list_ptr_at(colnames, index) as *mut pg_sys::String };
    if node.is_null() || unsafe { (*node).sval }.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr((*node).sval) }
            .to_string_lossy()
            .into_owned(),
    )
}

pub(super) unsafe fn read_alias_name(alias: *mut pg_sys::Alias) -> Option<String> {
    if alias.is_null() || unsafe { (*alias).aliasname }.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr((*alias).aliasname) }
            .to_string_lossy()
            .into_owned(),
    )
}

pub(super) unsafe fn read_from_item(
    jointree: *mut pg_sys::FromExpr,
    values_rtindexes: &HashSet<usize>,
    cte_rtindexes: &HashSet<usize>,
    subquery_rtindexes: &HashSet<usize>,
    scope: &CteScope,
) -> Result<FromItem, PgFrontendError> {
    if jointree.is_null() {
        return Err(PgFrontendError::unsupported("query has no jointree"));
    }
    let fromlist = unsafe { (*jointree).fromlist };
    if unsafe { list_len(fromlist) } == 0 {
        return Ok(FromItem::Empty);
    }
    let mut items = Vec::new();
    for index in 0..unsafe { list_len(fromlist) } {
        let node = unsafe { list_ptr_at(fromlist, index) as *mut pg_sys::Node };
        items.push(unsafe {
            read_from_node(
                node,
                values_rtindexes,
                cte_rtindexes,
                subquery_rtindexes,
                scope,
            )
        }?);
    }
    let mut iter = items.into_iter();
    let first = iter.next().expect("checked non-empty fromlist");
    Ok(iter.fold(first, |left, right| FromItem::Join {
        kind: JoinKind::Inner,
        left: Box::new(left),
        right: Box::new(right),
        quals: None,
    }))
}

pub(super) unsafe fn read_from_node(
    node: *mut pg_sys::Node,
    values_rtindexes: &HashSet<usize>,
    cte_rtindexes: &HashSet<usize>,
    subquery_rtindexes: &HashSet<usize>,
    scope: &CteScope,
) -> Result<FromItem, PgFrontendError> {
    if node.is_null() {
        return Err(PgFrontendError::unsupported("null fromlist node"));
    }
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_RangeTblRef => {
            let range_ref = node.cast::<pg_sys::RangeTblRef>();
            let rtindex = unsafe { (*range_ref).rtindex as usize };
            if values_rtindexes.contains(&rtindex) {
                Ok(FromItem::Values { rtindex })
            } else if cte_rtindexes.contains(&rtindex) {
                Ok(FromItem::Cte { rtindex })
            } else if subquery_rtindexes.contains(&rtindex) {
                Ok(FromItem::Subquery { rtindex })
            } else {
                Ok(FromItem::Relation { rtindex })
            }
        }
        pg_sys::NodeTag::T_JoinExpr => {
            let join = unsafe { &*node.cast::<pg_sys::JoinExpr>() };
            Ok(FromItem::Join {
                kind: join_kind(join.jointype)?,
                left: Box::new(unsafe {
                    read_from_node(
                        join.larg,
                        values_rtindexes,
                        cte_rtindexes,
                        subquery_rtindexes,
                        scope,
                    )
                }?),
                right: Box::new(unsafe {
                    read_from_node(
                        join.rarg,
                        values_rtindexes,
                        cte_rtindexes,
                        subquery_rtindexes,
                        scope,
                    )
                }?),
                quals: if join.quals.is_null() {
                    None
                } else {
                    Some(unsafe { read_expr(join.quals, scope) }?)
                },
            })
        }
        tag => Err(PgFrontendError::unsupported(format!(
            "fromlist node {:?} is not supported by pg_frontend v1",
            tag
        ))),
    }
}
