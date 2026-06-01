use super::*;

pub(super) fn table_reference_for_resolved_relation(
    relation: &scan_sql::PgRelation,
) -> TableReference {
    match relation.schema.as_deref() {
        Some(schema) => TableReference::partial(schema, relation.table.as_str()),
        None => TableReference::bare(relation.table.as_str()),
    }
}

pub(super) fn table_reference_for_query_relation(
    relation: &RelationRef,
    resolved: &ResolvedTable,
) -> TableReference {
    match relation.alias.as_deref() {
        Some(alias) => TableReference::bare(alias),
        None => table_reference_for_resolved_relation(&resolved.relation),
    }
}

pub(super) fn table_reference_for_values(values: &ValuesRef) -> TableReference {
    match values.alias.as_deref() {
        Some(alias) => TableReference::bare(alias),
        None => TableReference::bare(format!("values_{}", values.rtindex)),
    }
}

pub(super) fn table_reference_for_cte(cte: &CteRangeRef) -> TableReference {
    match cte.alias.as_deref() {
        Some(alias) => TableReference::bare(alias),
        None => TableReference::bare(cte.name.as_str()),
    }
}

pub(super) fn table_reference_for_subquery(subquery: &SubqueryRef) -> TableReference {
    match subquery.alias.as_deref() {
        Some(alias) => TableReference::bare(alias),
        None => TableReference::bare(format!("subquery_{}", subquery.rtindex)),
    }
}

pub(super) fn relation_by_rtindex(
    query: &TypedQuery,
    rtindex: usize,
) -> Result<&RelationRef, PgFrontendError> {
    query
        .relations
        .iter()
        .find(|relation| relation.rtindex == rtindex)
        .ok_or_else(|| PgFrontendError::unsupported(format!("missing rtable index {rtindex}")))
}

pub(super) fn validate_identifier_len(
    identifier: &str,
    max_bytes: usize,
    kind: &'static str,
) -> Result<(), PgFrontendError> {
    if identifier.len() > max_bytes {
        return Err(PgFrontendError::unsupported(format!(
            "{kind} identifier `{identifier}` exceeds PostgreSQL limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

pub(super) fn resolved_table_for_relation(
    relation: &RelationRef,
) -> Result<ResolvedTable, PgFrontendError> {
    if !relation.catalog_resolved {
        return Err(PgFrontendError::unsupported(format!(
            "relation rtindex {} was not resolved before compilation",
            relation.rtindex
        )));
    }

    let mut fields = Vec::with_capacity(relation.columns.len());
    let mut column_attnums = Vec::with_capacity(relation.columns.len());
    let mut columns = Vec::with_capacity(relation.columns.len());
    for column in &relation.columns {
        let data_type = arrow_type_for_pg_type(column.pg_type).ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "resolved column {} has unsupported PostgreSQL type oid {}",
                column.name, column.pg_type.oid
            ))
        })?;
        fields.push(pg_output_field(
            &column.name,
            data_type,
            column.nullable,
            column.pg_type,
        ));
        column_attnums.push(column.attnum);
        columns.push(ResolvedColumn {
            attnum: column.attnum,
            name: column.name.clone(),
            pg_type: column.pg_type,
            nullable: column.nullable,
        });
    }

    let schema = (!relation.schema.is_empty()).then_some(relation.schema.as_str());
    Ok(ResolvedTable {
        table_oid: relation.relid,
        relation: scan_sql::PgRelation::new(schema, relation.name.as_str()),
        column_attnums,
        schema: Arc::new(Schema::new(fields)),
        columns,
    })
}

pub(super) fn pg_output_field(
    name: impl Into<String>,
    data_type: DataType,
    nullable: bool,
    pg_type: pg_type::PgTypeRef,
) -> Field {
    let field = Field::new(name, data_type, nullable);
    if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) && pg_type.typmod < 0 {
        return field.with_metadata(HashMap::from([(
            PG_NUMERIC_TRIM_TRAILING_ZEROS_METADATA_KEY.to_owned(),
            "true".to_owned(),
        )]));
    }
    field
}
