# Roadmap

[Documentation home](index.md)

This page lists the main technical directions for `pg_fusion`. It does not
promise dates or release scope.

## Typed PostgreSQL Planning

The current runtime path uses DataFusion SQL planning as a bootstrap:

1. PostgreSQL receives and analyzes SQL.
2. pg_fusion uses SQL text to build a DataFusion logical plan.
3. DataFusion infers its own expression and output types.
4. pg_fusion maps PostgreSQL table scans into scan streams and maps results
   back into PostgreSQL slots.

This validated the execution path, shared-memory transport, and worker runtime,
but it is not the intended long-term SQL semantics boundary.

The hard part is the current PostgreSQL -> DataFusion -> PostgreSQL scan
sandwich. PostgreSQL has already analyzed the query and resolved types,
typmods, collations, casts, operators, functions, `unknown` literals, and
parameters. If pg_fusion then lets DataFusion infer a similar-but-not-identical
typed plan from SQL text, that PostgreSQL identity can be lost before
predicates are rendered back into PostgreSQL scan SQL.

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

The first production steps are in place as a migration path: supported
single-relation projection/filter queries can be planned through `pg_frontend`,
and the resulting typed DataFusion plan now flows through the same
post-planning pipeline used by the SQL-text planner for PostgreSQL scan
building and output normalization. The frontend path builds scans before
generic DataFusion optimization can rewrite PostgreSQL-semantic predicates.
Unsupported shapes still fall back to the existing SQL-text planner unless the
frontend is explicitly required. The remaining work is to expand typed-query
coverage until the fallback is no longer needed for common analytical queries.

Removing the SQL-text fallback requires at least:

- typed planning for joins, grouped aggregates, sorting, limits, CTEs, and the
  expression/function subset already accepted by the SQL-text path;
- parameter value propagation for prepared and extended-protocol queries;
- PostgreSQL relid/attnum-based scan identity instead of relying on deparsed
  relation names at the frontend boundary;
- compatibility tests that compare supported typed-frontend results with
  vanilla PostgreSQL before making the frontend mandatory.

## PostgreSQL Version Support

The current public setup and test commands are PostgreSQL 17 focused.

PostgreSQL 18 support is a roadmap item. It should include:

- pgrx feature wiring for PG18;
- extension build and pgrx test commands for PG18;
- PG18 compatibility corpus coverage;
- checks for planner, executor, type, and catalog differences that affect
  pg_fusion planning or scan execution.

Until that work is done, treat PG17 as the documented development target.

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
