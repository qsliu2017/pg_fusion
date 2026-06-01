use super::*;

pub(super) fn compile_sort(
    query: &TypedQuery,
    input: &LogicalPlan,
) -> Result<Vec<datafusion_expr::expr::Sort>, PgFrontendError> {
    query
        .sort
        .iter()
        .map(|key| compile_sort_key(key, query, input))
        .collect()
}

pub(super) fn compile_sort_key(
    key: &SortKey,
    query: &TypedQuery,
    input: &LogicalPlan,
) -> Result<datafusion_expr::expr::Sort, PgFrontendError> {
    let index = target_index_by_sort_group_ref(query, key.target_ref)?;
    let (qualifier, field) = input.schema().qualified_field(index);
    Ok(Expr::Column(Column::from((qualifier, field))).sort(key.asc, key.nulls_first))
}

pub(super) fn target_by_sort_group_ref(
    query: &TypedQuery,
    target_ref: u32,
) -> Result<&Target, PgFrontendError> {
    let index = target_index_by_sort_group_ref(query, target_ref)?;
    Ok(&query.targets[index])
}

pub(super) fn target_index_by_sort_group_ref(
    query: &TypedQuery,
    target_ref: u32,
) -> Result<usize, PgFrontendError> {
    query
        .targets
        .iter()
        .position(|target| target.ressortgroupref == target_ref)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "sort/group target ref {target_ref} was not found in target list"
            ))
        })
}
