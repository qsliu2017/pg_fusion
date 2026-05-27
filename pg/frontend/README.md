# pg_frontend

`pg_frontend` is the experimental PostgreSQL typed-tree frontend for
`pg_fusion`.

The crate reads PostgreSQL's analyzed `Query` tree, copies the PostgreSQL type
metadata that matters at the engine boundary, and compiles the supported subset
into a DataFusion logical plan with `PgScanNode` leaves.

The first version is intentionally fail-closed and is not wired into the
production planner hook by default. The existing SQL-text `plan_builder` path
continues to serve production queries while this crate grows enough coverage for
prepared statements and PostgreSQL-specific type semantics.
