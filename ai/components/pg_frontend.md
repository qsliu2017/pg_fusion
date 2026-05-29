---
id: component-pg-frontend-0001
type: fact
scope: component
tags: ["postgres", "datafusion", "planning", "query-tree"]
updated_at: "2026-05-29"
importance: 0.7
---

# pg_frontend

`pg/frontend` is the experimental PostgreSQL typed-tree frontend. It reads a
live analyzed `pg_sys::Query` into a neutral `TypedQuery`, copies PostgreSQL
OID/typmod/collation metadata, resolves catalog metadata in place by
`RTE_RELATION.relid`, and compiles the supported subset into an ordinary
DataFusion `LogicalPlan` with PostgreSQL table-source leaves. The host-side
planning pipeline then builds those leaves into PostgreSQL scan nodes and
normalizes root output types. This keeps `pg_frontend` focused on PostgreSQL
query-tree semantics instead of duplicating scan-building policy.

PostgreSQL major-version layout differences terminate at `adapter/`. Each build
targets exactly one PostgreSQL major selected by Cargo feature; today only
`pg17` is wired. The stable `TypedQuery` model, catalog resolution, and
compiler code must stay free of PostgreSQL-version `cfg`.

The frontend phases are explicit: PostgreSQL query tree -> `TypedQuery` ->
in-place catalog resolution -> DataFusion logical plan -> `plan_builder`
`HybridPlan`. Catalog resolution mutates relation metadata on the existing
`TypedQuery` by filling column names, attnums, PostgreSQL type refs, nullability,
and normalized scan relation identity. Compilation takes a `ResolvedQuery` view
over the same tree, so catalog lookup is not mixed into expression lowering.

v1 is intentionally fail-closed and is now available on the production planner
path behind `pg_fusion.frontend_mode`. In `try` mode the planner attempts
`pg_frontend` first and falls back to SQL-text `plan_builder` when the typed
frontend rejects a query. In `require` mode rejection becomes a controlled
frontend planning error. The supported execution shape is one base relation
with simple projection/filter expressions; joins, aggregates, windows, set
operations, GROUP BY, HAVING, sort, limit, CTEs, row-locking clauses, `ONLY`
scans, parameters, and subqueries return structured unsupported errors. The
production planner also bypasses the frontend and SQL-text custom scan paths
when the analyzed tree still contains `Param` nodes or an `RTE.inh = false`
relation, so prepared/generic parameter values and `ONLY` semantics remain
PostgreSQL-owned until the typed model and scan SQL can preserve them
explicitly.

Frontend `CustomScan` nodes store a versioned text-safe wrapper around
`plan_codec` bytes in `custom_private`. The active encoded payload contains the
already built DataFusion logical plan plus PostgreSQL scan specs from the
`HybridPlan`; it is not a raw PostgreSQL `Query*` pointer or Rust
`Arc<LogicalPlan>` because PostgreSQL plan nodes can be copied and serialized by
core code. Serialized `TypedQuery` payloads are rejected; the typed model is a
planning-time structure only. `BeginCustomScan` decodes the built plan once,
uses its output schema for result transport, and starts execution with the same
decoded plan. Supported current frontend queries avoid SQL re-parsing, frontend
recompilation, catalog re-resolution, and scan SQL rebuilding during execution
while staying PostgreSQL-plan-node safe.

The design rule is that PostgreSQL analyzed metadata is the boundary source of
truth. DataFusion schema is a transport/execution representation, not authority
for PostgreSQL result OIDs, typmods, collations, or temporal/text/numeric
semantics. The shared PostgreSQL/Arrow type policy lives in `pg/type`;
`pg_frontend` should call that crate for supported type checks, Arrow type
mapping, typed literal metadata, and typed NULL/constant scalar construction.

Shippability distinguishes value types from non-null constant types. Value
types can appear as columns, typed NULLs, and external parameter metadata;
non-null constants are limited to the current `PgConstValue` carriers.
`name` constants decode through fixed-size `NameData`, not varlena conversion,
and `name` values accept PostgreSQL's built-in C collation. Text-like constants
carry PostgreSQL OID/typmod/collation metadata into `scan_sql` so `text`,
`varchar`, `bpchar`, and `name` predicates are rendered with PostgreSQL type
semantics instead of untyped string literals.
Non-null `date`, `timestamp`, and `timestamptz` constants fail closed until
temporal representation is lossless across scan input, DataFusion execution,
and PostgreSQL result import. Non-finite float constants also fail closed
because PostgreSQL and Arrow/DataFusion disagree on `NaN` comparison semantics.
`TIME '24:00:00'` constants fail closed because scan SQL renders time literals
through interval arithmetic that PostgreSQL normalizes modulo one day.
Operator compilation reads the resolved `OpExpr.opno` from PostgreSQL's syscache
and accepts only binary `pg_catalog` comparison operators over identical
supported scalar operand types with `bool` results. This keeps user-defined
operators with builtin spellings, mixed-type comparison operators, arithmetic,
and PostgreSQL-specific operator semantics fail-closed until scan SQL can
preserve them explicitly.
Frontend `WHERE` filters must reach `scan_sql` before generic DataFusion
optimization can fold or rewrite them. After scan building, those filters must
fully compile into PostgreSQL scan SQL; residual DataFusion filters are
rejected before execution. `SELECT` targets do not compile PostgreSQL operator
expressions in v1 because scan SQL cannot yet project expressions with
PostgreSQL semantics.
