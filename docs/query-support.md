# Query Support

[Documentation home](index.md)

`pg_fusion` supports a conservative subset of analytical PostgreSQL `SELECT`
queries. With `pg_fusion.enable = on`, unsupported user SELECT shapes are
rejected with controlled pg_fusion planning errors instead of silently falling
back to PostgreSQL's native planner.

If the terms are new, start with [Glossary](glossary.md). The data path and
page lifetime rules are described in [Execution Model](execution-model.md) and
[Memory And Pages](memory-and-pages.md).

This page is about eligibility and supported shapes. For performance fit,
including good and poor workload candidates, see [Workloads](workloads.md).
For detailed PostgreSQL to DataFusion type, expression, function, aggregate,
and window mappings, see [Compatibility Matrix](compatibility-matrix.md).

## Current Entry Point

Queries should run as top-level SQL through the normal PostgreSQL client
protocol.

The following invocation paths are not supported yet:

- SPI-owned execution contexts;
- PL/pgSQL-internal SQL execution;
- function-internal SQL wrappers around a pg_fusion query;
- bound or prepared-statement parameters.

## Planner Bypasses

The planner hook bypasses pg_fusion for:

- non-`SELECT` statements;
- pg_fusion management SQL such as metrics functions.

## COPY

`COPY (SELECT ...) TO STDOUT` can use pg_fusion when the nested `SELECT` is
supported by the pg_fusion planner path. The `SELECT` body follows the same
support and fail-closed rules as an ordinary top-level `SELECT`.

This does not mean pg_fusion accelerates data loading. `COPY FROM`, table
loads, and other non-`SELECT` utility paths remain PostgreSQL-owned execution
paths.

## Relational Operators

The current strict query-tree frontend supports:

- filters;
- projections;
- no-FROM SELECTs;
- ORDER BY and LIMIT/OFFSET over supported plans;
- simple `count`/`sum`/`avg`/`min`/`max` aggregate calls without GROUP BY;
- selected scalar expressions and operators with PostgreSQL-compatible
  mappings in pg_fusion.

The intended supported direction is:

- grouped aggregates;
- selected joins;
- selected CTE shapes after DataFusion optimization;
- DataFusion-supported expressions and functions that have PostgreSQL-compatible
  mappings in pg_fusion.

Subqueries are not supported by the current strict frontend. Longer term, they
should be accepted only when they can be decorrelated or rewritten into
ordinary relational operators before PostgreSQL scan building.

## Scan Pushdown And Parallel Scan Producers

PostgreSQL table access remains PostgreSQL-owned. pg_fusion can still reduce
the amount of data crossing into Arrow by lowering scan filters and projections
into PostgreSQL scan SQL:

- pushdown filters run before slot-to-Arrow encoding;
- projections avoid encoding unused columns;
- unsupported scan expressions stay above the scan or make the shape
  ineligible, depending on where they appear.

For eligible heap scans, pg_fusion can also split scan production by CTID block
ranges. This is PostgreSQL-side scan parallelism, separate from DataFusion
worker tasks. The detailed lifecycle is described in
[Execution Model](execution-model.md#scan-production).

## Joins And Runtime Filters

Statistics-based join reordering is enabled for eligible inner or cross join
components whose leaves are PostgreSQL table scans and whose join predicates
are simple equi-column pairs. Outer joins, residual join filters, unsupported
expressions, and unsupported subquery shapes keep their DataFusion-planned
order.

The join-order search uses PostgreSQL statistics and the DPHyp algorithm. It is
restricted to join components where pg_fusion can reason about the relation
leaves and equi-column predicates safely.

Runtime Bloom filters can be attached to eligible inner hash joins. The first
implementation is intentionally narrow: simple `Column = Column` join keys,
single-partition build side, supported scalar key types, and a PostgreSQL scan
on the probe side.

Runtime filters help reduce the expensive boundary crossing: rows rejected by a
ready filter can be skipped before slot-to-Arrow encoding.

Runtime filters are not the same thing as PostgreSQL pushdown filters. Pushdown
filters come from query predicates during planning. Runtime Bloom filters are
built while a hash join is already executing and can only reject values that are
definitely absent from the build side.

## Type Support

Supported PostgreSQL types currently map to Arrow/DataFusion types such as:

- `boolean`;
- `int2`, `int4`, `int8`;
- `float4`, `float8`;
- finite `numeric` within the Decimal128 subset;
- `text`, `varchar`, `bpchar`, `name`;
- `uuid`;
- `bytea`;
- `date`;
- `time`;
- `timestamp`, `timestamptz`;
- finite `interval`.

Known unsupported or restricted cases include:

- `timetz`;
- PostgreSQL `numeric` `NaN` and `Infinity`;
- finite `numeric` values outside the selected Decimal128 shape;
- interval infinities.

The detailed compatibility matrix is maintained in
[Compatibility Matrix](compatibility-matrix.md).

## Validate With PostgreSQL

When testing a workload, compare results and plans with pg_fusion off and on:

```sql
SET pg_fusion.enable = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT ...;

SET pg_fusion.enable = on;
EXPLAIN ANALYZE
SELECT ...;
```

If pg_fusion is slower, inspect whether scan encoding and transport dominate
the query, whether filters were pushed down, and whether retaining operators
forced materialization. [Metrics](metrics.md) has diagnostic queries for that.
