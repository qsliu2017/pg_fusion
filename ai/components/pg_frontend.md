---
id: component-pg-frontend-0001
type: fact
scope: component
tags: ["postgres", "datafusion", "planning", "query-tree"]
updated_at: "2026-05-22"
importance: 0.7
---

# pg_frontend

`pg/frontend` is the experimental PostgreSQL typed-tree frontend. It reads a
live analyzed `pg_sys::Query`, copies PostgreSQL OID/typmod/collation metadata
into a stable Rust IR, and lowers the supported subset into a DataFusion
`LogicalPlan` with `PgScanNode` leaves.

PostgreSQL major-version layout differences terminate at `adapter/`. Each build
targets exactly one PostgreSQL major selected by Cargo feature; today only
`pg17` is wired. The stable `PgQuery` IR and lowering code must stay free of
PostgreSQL-version `cfg`.

v1 is intentionally fail-closed and is not wired into the production planner
hook. The supported execution shape is one base relation with simple
projection/filter expressions; joins, aggregates, windows, set operations,
GROUP BY, HAVING, sort, limit, CTEs, row-locking clauses, `ONLY` scans, and
subqueries return structured unsupported errors.

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
Operator lowering reads the resolved `OpExpr.opno` from PostgreSQL's syscache
and accepts only binary `pg_catalog` comparison operators over identical
supported scalar operand types with `bool` results. This keeps user-defined
operators with builtin spellings, mixed-type comparison operators, arithmetic,
and PostgreSQL-specific operator semantics fail-closed until scan SQL can
preserve them explicitly.
`WHERE` filters must fully compile into PostgreSQL scan SQL; residual
DataFusion filters are rejected. `SELECT` targets do not lower PostgreSQL
operator expressions in v1 because scan SQL cannot yet project expressions
with PostgreSQL semantics.
