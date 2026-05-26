# Query Support

[Documentation home](index.md)

`pg_fusion` supports a conservative subset of analytical PostgreSQL `SELECT`
queries. Unsupported shapes are either bypassed before planning or rejected
with controlled errors.

The important practical rule is:

> The more rows and columns pg_fusion must move from PostgreSQL heap tuples into
> Arrow pages, the more work the query must do in DataFusion to be worthwhile.

## Good First Candidates

Start with queries that have:

- ordinary PostgreSQL heap tables;
- join-heavy analytical work that creates large intermediate batches inside the
  DataFusion worker;
- joins where runtime filters can reject probe-side rows before scan encoding;
- grouped aggregates after selective PostgreSQL-side filters;
- sort or window work above a reduced scan stream;
- filters that can be pushed into PostgreSQL scan SQL;
- projections that use only a subset of table columns.

These shapes are more likely to benefit from moving analytical work to the
worker because pg_fusion can avoid encoding unused rows and columns, and
worker-local intermediate batches stay in Arrow form.

## Shapes To Be Careful With

pg_fusion can be a poor fit when:

- the query mostly returns raw table rows;
- the projection is very wide;
- filters cannot be pushed into PostgreSQL scans;
- most of the runtime is PostgreSQL heap scan and tuple-to-Arrow encoding;
- the SQL shape is outside the current supported subset.

In those cases, the trip from PostgreSQL heap tuples to Arrow pages and back to
PostgreSQL result slots can dominate the query.

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
- modifying CTEs;
- PostgreSQL catalog or TOAST relations;
- PostgreSQL function or table-function range entries;
- pg_fusion management SQL such as metrics functions.

## COPY

`COPY (SELECT ...) TO STDOUT` can use pg_fusion when the nested `SELECT` is
eligible for the pg_fusion planner path. The `SELECT` body follows the same
support, bypass, and fail-closed rules as an ordinary top-level `SELECT`.

This does not mean pg_fusion accelerates data loading. `COPY FROM`, table
loads, and other non-`SELECT` utility paths remain PostgreSQL-owned execution
paths.

## Relational Operators

The intended supported direction is:

- filters;
- projections;
- grouped aggregates;
- selected joins;
- selected CTE shapes after DataFusion optimization;
- DataFusion-supported expressions and functions that have PostgreSQL-compatible
  mappings in pg_fusion.

Subqueries are accepted only when DataFusion can decorrelate or rewrite them
into ordinary relational operators before PostgreSQL scan lowering. Surviving
`EXISTS`, `IN (SELECT ...)`, scalar subqueries, correlated subqueries, and
logical subquery plan nodes are rejected.

## Joins And Runtime Filters

Statistics-based join reordering is enabled for eligible inner or cross join
components whose leaves are PostgreSQL table scans and whose join predicates
are simple equi-column pairs. Outer joins, residual join filters, unsupported
expressions, and unsupported subquery shapes keep their DataFusion-planned
order.

Runtime Bloom filters can be attached to eligible inner hash joins. The first
implementation is intentionally narrow: simple `Column = Column` join keys,
single-partition build side, supported scalar key types, and a PostgreSQL scan
on the probe side.

Runtime filters help reduce the expensive boundary crossing: rows rejected by a
ready filter can be skipped before slot-to-Arrow encoding.

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
the query. [Metrics](metrics.md) has diagnostic queries for that.
