use super::*;

#[derive(Debug, Clone)]
pub(super) struct CteScope {
    pub(super) defs: Vec<CteDef>,
    pub(super) ids: HashMap<String, u64>,
    pub(super) next_cte_id: Rc<Cell<u64>>,
    pub(super) outer_columns: Vec<OuterScope>,
    pub(super) current_columns: OuterScope,
    pub(super) join_aliases: HashMap<(usize, i16), QueryExpr>,
    pub(super) case_operand: Option<QueryExpr>,
}

impl Default for CteScope {
    fn default() -> Self {
        Self {
            defs: Vec::new(),
            ids: HashMap::new(),
            next_cte_id: Rc::new(Cell::new(1)),
            outer_columns: Vec::new(),
            current_columns: OuterScope::default(),
            join_aliases: HashMap::new(),
            case_operand: None,
        }
    }
}

impl CteScope {
    pub(super) fn allocate_cte_id(&self) -> u64 {
        let id = self.next_cte_id.get();
        self.next_cte_id.set(id + 1);
        id
    }

    pub(super) fn with_current_columns(&self, current_columns: OuterScope) -> Self {
        let mut scope = self.clone();
        scope.current_columns = current_columns;
        scope
    }

    pub(super) fn with_join_aliases(&self, join_aliases: HashMap<(usize, i16), QueryExpr>) -> Self {
        let mut scope = self.clone();
        scope.join_aliases = join_aliases;
        scope
    }

    pub(super) fn for_child_query(&self) -> Self {
        let mut scope = self.clone();
        scope.outer_columns.push(self.current_columns.clone());
        scope.current_columns = OuterScope::default();
        scope.case_operand = None;
        scope
    }

    pub(super) fn with_case_operand(&self, operand: QueryExpr) -> Self {
        let mut scope = self.clone();
        scope.case_operand = Some(operand);
        scope
    }

    pub(super) unsafe fn outer_var(&self, var: &pg_sys::Var) -> Result<OuterVar, PgFrontendError> {
        let level = usize::try_from(var.varlevelsup).map_err(|_| {
            PgFrontendError::unsupported("outer-reference Var level is out of range")
        })?;
        if level == 0 || level > self.outer_columns.len() {
            return Err(PgFrontendError::unsupported(format!(
                "outer-reference Var level {} has no matching query scope",
                var.varlevelsup
            )));
        }
        let scope = &self.outer_columns[self.outer_columns.len() - level];
        let key = (var.varno as usize, var.varattno);
        let pg_type = type_ref(var.vartype, var.vartypmod, var.varcollid);
        if let Some(column) = scope.columns.get(&key) {
            return Ok(OuterVar {
                relation: column.relation.clone(),
                name: column.name.clone(),
                pg_type,
            });
        }
        if let Some(relation) = scope.relations.get(&(var.varno as usize)) {
            let name = unsafe { relation_attname(relation.relid, var.varattno) }?;
            return Ok(OuterVar {
                relation: relation.relation.clone(),
                name,
                pg_type,
            });
        }
        Err(PgFrontendError::unsupported(format!(
            "outer-reference Var rtindex {} attnum {} was not found",
            var.varno, var.varattno
        )))
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct OuterScope {
    pub(super) columns: HashMap<(usize, i16), OuterColumn>,
    pub(super) relations: HashMap<usize, OuterRelation>,
}

#[derive(Debug, Clone)]
pub(super) struct OuterColumn {
    pub(super) relation: Option<String>,
    pub(super) name: String,
}

#[derive(Debug, Clone)]
pub(super) struct OuterRelation {
    pub(super) relid: u32,
    pub(super) relation: Option<String>,
}
