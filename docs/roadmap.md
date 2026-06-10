# Roadmap

[Documentation home](index.md)

This page lists the main technical directions for `pg_fusion`. It does not
promise dates or release scope.

## Typed PostgreSQL Planning

The current runtime path uses PostgreSQL analyzed query-tree planning:

1. PostgreSQL receives and analyzes SQL.
2. pg_fusion copies the analyzed query tree into a typed frontend model.
3. pg_fusion lowers the typed model into a DataFusion logical plan.
4. pg_fusion maps PostgreSQL table scans into scan streams and maps results
   back into PostgreSQL slots.

Earlier SQL-text planning validated the execution path, shared-memory
transport, and worker runtime, but it is no longer the SQL semantics boundary
for user SELECTs.

The hard part is the PostgreSQL -> DataFusion -> PostgreSQL scan sandwich.
PostgreSQL has already analyzed the query and resolved types,
typmods, collations, casts, operators, functions, `unknown` literals, and
parameters. pg_fusion must preserve that PostgreSQL identity before predicates
are rendered back into PostgreSQL scan SQL.

That round trip creates non-obvious correctness risks. A value can have a valid
DataFusion type in the middle of the plan, but still need its original
PostgreSQL type identity when it is pushed back into a PostgreSQL scan or
projected into a PostgreSQL result slot. Temporal values, UUIDs, text typmods,
`bpchar` semantics, numeric edge cases, collations, and prepared parameters are
examples where losing the analyzed PostgreSQL type can produce wrong scan SQL,
wrong result metadata, or incorrect results instead of a clean unsupported-case
failure.

The target direction is:

- PostgreSQL analyzed `Query` trees are the source of truth for SQL semantics.
- PostgreSQL OID, typmod, and collation are the source of truth for expression
  and output types.
- PostgreSQL relation OIDs and attribute numbers are the source of truth for
  table and column identity.
- PostgreSQL operator and function identity are preserved when planning into
  DataFusion.
- PostgreSQL scan leaves remain PostgreSQL-owned scan streams.

This should reduce compatibility code around temporal values, numeric values,
UUIDs, typmods, parameters, and pushed PostgreSQL scan SQL.

The first production steps are in place: supported query shapes are planned
through `pg_frontend`, and the resulting typed DataFusion plan flows through the
post-planning pipeline for PostgreSQL scan building and output normalization.
The frontend path builds scans before generic DataFusion optimization can
rewrite PostgreSQL-semantic predicates. There is no SQL-text or native
PostgreSQL planner fallback for user SELECTs while `pg_fusion.enable` is on.
The remaining work is to expand typed-query coverage for common analytical
queries.

Expanding typed frontend coverage requires at least:

- typed planning for joins, grouped aggregates, CTEs, and the broader
  expression/function subset previously accepted by the SQL-text path;
- parameter value propagation for prepared and extended-protocol queries;
- compatibility tests that compare supported typed-frontend results with
  vanilla PostgreSQL.

## PostgreSQL Version Support

The current public setup and test commands are PostgreSQL 17 focused.

PostgreSQL 18 support is a roadmap item. It should include:

- pgrx feature wiring for PG18;
- extension build and pgrx test commands for PG18;
- PG18 compatibility corpus coverage;
- checks for planner, executor, type, and catalog differences that affect
  pg_fusion planning or scan execution.

Until that work is done, treat PG17 as the documented development target.

## Hash Join Scalability

Large TPC-H-style join chains can exhaust the finite DataFusion worker memory
pool because ordinary `HashJoinExec` does not spill today. The near-term
benchmark workaround is to run SF10 with an unbounded worker pool, but the
engine direction is to make large hash joins bounded and more selective.

Important directions include:

- add spilling and multi-pass execution for large hash joins;
- investigate parallel hash-table build for large build sides;
- derive and publish filters across adjacent joins in join chains so later
  hash-table builds receive fewer rows.

## Compatibility

Compatibility work is broader than type conversion.

Important areas include:

- casts, typmods, and domains;
- collations and text semantics;
- operator and function resolution;
- parameters and prepared statements;
- SQL invocation contexts such as SPI and PL/pgSQL;
- EXPLAIN output and error messages;
- unsupported-shape fail-closed behavior.

The goal is not to claim arbitrary PostgreSQL SQL support. The goal is to make
the supported subset explicit, tested, and PostgreSQL-compatible.

## Testing

Testing should grow alongside compatibility.

Important directions include:

- expand the PostgreSQL compatibility corpus;
- compare supported query results against vanilla PostgreSQL;
- add runtime tests for joins, aggregates, scan pushdown, runtime filters,
  spill, metrics, and cancellation;
- add negative tests for unsupported shapes that must fail closed or bypass;
- run the same relevant coverage across supported PostgreSQL versions.

Benchmarks remain diagnostic. Correctness and clear unsupported-case behavior
come first.
