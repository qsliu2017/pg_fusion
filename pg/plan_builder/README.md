# plan_builder

`plan_builder` is pg_fusion's post-frontend planning bridge. It accepts an
already typed DataFusion `LogicalPlan`, validates the supported shape, applies
pg_fusion planning passes, and replaces PostgreSQL table-source leaves with
`scan_node::PgScanNode` leaves backed by `scan_node::PgScanSpec`.

The crate does not parse SQL. SQL text planning was removed; user queries enter
pg_fusion through PostgreSQL's analyzed `Query` tree and `pg_frontend`.

## Role

- input is a DataFusion logical plan whose PostgreSQL relation leaves use
  `df_catalog::PgPlanningTableSource`
- pushdown SQL for each leaf is compiled by `scan_sql`
- scan leaves are represented by `scan_node::PgScanSpec`
- eligible inner/cross join components are reordered with PostgreSQL
  statistics through `pg_statistics` and the standalone `join_order` optimizer
- snapshot ownership, plan serialization, backend scan serving, and page
  transport are left to later layers
- subquery expressions are accepted only when the incoming plan or DataFusion
  optimization rewrites them into ordinary relational operators above
  PostgreSQL scan leaves

The output `HybridPlan` contains both the DataFusion logical plan and the
PostgreSQL scan plan referenced by its custom scan leaves. It is self-contained
enough for `plan_codec` to serialize the compiled scan SQL, relation identity,
`scan_id`, `table_oid`, and fetch hints, but it does not contain snapshot ids.

## Execution Contract

`build_preplanned_logical_plan` performs DataFusion logical optimization with
`target_partitions = 1` in v1. This avoids inventing DataFusion-level
multi-partition semantics for one PostgreSQL scan id. It does not disable
PostgreSQL-side parallel planning: `slot_scan` can still prepare and run a
PostgreSQL plan that contains `Gather` or other PostgreSQL parallel scan nodes.

The frontend-specific entry point, `build_frontend_logical_plan`, builds scan
leaves before generic DataFusion optimization so PostgreSQL type metadata on
scan predicates reaches `scan_sql` intact. It rejects residual scan filters,
because frontend WHERE predicates are expected to execute inside PostgreSQL
scan SQL until DataFusion/PostgreSQL semantic differences are modeled more
widely.

Filter pushdown is deliberately two-stage. Planning table sources report
filters as exactly pushable so DataFusion attaches them to `TableScan`. During
scan building, `scan_sql` recompiles those filters and returns unsupported
predicates as residual filters. `plan_builder` restores those residual
predicates above `PgScanNode` and projects away residual-only columns if needed.

## Join Ordering

`PlanBuilderConfig::join_reordering_enabled` is on by default. After normal
DataFusion logical optimization and subquery validation, `plan_builder`
searches for maximal reorderable join components and replaces them with a
costed tree from `join_order`.

A component is reorderable only when all of these are true:

- joins are `INNER` joins with `ON` predicates or cross joins
- there is no residual join filter and `null_equals_null` is false
- leaves are PostgreSQL planning table scans, optionally wrapped in
  `SubqueryAlias`
- transparent column-only projections may sit above the join component
- join predicates are simple equi-column pairs that can be mapped back to
  PostgreSQL attnums

For each leaf, the reordering pass recompiles the same pushed-down scan SQL that
will later become `PgScanSpec`, asks `pg_statistics::estimate_scan_sql` for
filtered rows/bytes, loads column NDV/null stats for join columns, and loads
relation-wide unique keys.

## Example

```rust,ignore
use plan_builder::{build_frontend_logical_plan, PlanBuilderConfig};

let hybrid = build_frontend_logical_plan(logical_plan, PlanBuilderConfig::default())?;

for scan in hybrid.scan_plan.scans() {
    println!(
        "scan {} table_oid={} sql={}",
        scan.scan_id.get(),
        scan.table_oid,
        scan.compiled_scan.sql,
    );
}
```

Worker-side physical planning later installs `scan_node::PgScanExtensionPlanner`
and provides a runtime-specific execution factory for the scan specs.
