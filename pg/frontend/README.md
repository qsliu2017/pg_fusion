# pg_frontend

`pg_frontend` is the experimental PostgreSQL typed-tree frontend for
`pg_fusion`.

The crate reads PostgreSQL's analyzed `Query` tree, copies the PostgreSQL type
metadata that matters at the engine boundary, and compiles the supported subset
into a DataFusion logical plan with `PgScanNode` leaves.

The first version is intentionally fail-closed. The production planner can try
this frontend for its supported subset and fall back to the existing SQL-text
`plan_builder` path for broader query coverage.

The frontend payload stored in `CustomScan` is a versioned serialized `PgQuery`
IR, not a borrowed PostgreSQL `Query*` pointer and not a raw Rust
`LogicalPlan`. Execution recompiles that typed IR so supported frontend queries
avoid SQL re-parsing while keeping PostgreSQL plan nodes copy-safe.
