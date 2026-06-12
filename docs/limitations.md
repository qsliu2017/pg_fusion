# Limitations

[Documentation home](index.md)

`pg_fusion` is experimental. Unsupported query shapes should be treated as not
implemented, not as silently equivalent to PostgreSQL execution.

## Overhead Cases

pg_fusion has a real boundary cost:

1. PostgreSQL reads heap tuples.
2. Backends decode tuple slots.
3. Rows are encoded into Arrow pages.
4. DataFusion runs worker-side operators.
5. Results are encoded into pages and imported back into PostgreSQL slots.

This can be slower than PostgreSQL when the query returns many raw rows, uses
wide projections, cannot push selective filters into PostgreSQL scans, or does
little analytical work above the scan.

See [Workloads](workloads.md) for workload fit and
[Memory And Pages](memory-and-pages.md) for page transport costs.

## SQL Coverage

The current planner path does not support every PostgreSQL SQL shape.

Known limitations include:

- non-`SELECT` statements;
- modifying CTEs;
- bound or prepared-statement parameters;
- SPI-owned execution contexts;
- PL/pgSQL-internal invocation paths;
- PostgreSQL function or table-function range entries;
- unsupported surviving subquery expressions;
- unsupported PostgreSQL types.

## Type Coverage

Some PostgreSQL values do not have a lossless Arrow/DataFusion representation in
the current transport.

The current supported and restricted type list lives in
[Query Support](query-support.md#type-support). Detailed type, cast, operator,
function, aggregate, and window mappings are listed in
[Compatibility Matrix](compatibility-matrix.md).

## Bare Numeric Display Scale

pg_fusion preserves finite `numeric` values within its Decimal128 subset, but it
does not preserve PostgreSQL's per-value display scale for bare `numeric`.
Typmodless numeric results are returned in a canonical form with trailing
fractional zeros removed.

Examples such as `1.20::numeric`, `1.20::numeric + 3.00::numeric`, or mixed-scale
`VALUES (1.2::numeric), (1.20::numeric)` may therefore display fewer fractional
zeros under pg_fusion than under PostgreSQL native execution. The same applies
to typmodless numeric aggregate outputs such as `avg(int)`. Cast to
`numeric(p,s)` when the output scale itself is part of the expected result.

## Spill

Worker spill is owned by the pg_fusion worker runtime and uses OS temporary
storage, not PostgreSQL temporary-file infrastructure. Configuration details
and diagnostics are covered in [Configuration](configuration.md#worker-spill).

## Planning Boundary

The current runtime path uses PostgreSQL analyzed query-tree planning. There is
no SQL-text planner fallback for user SELECTs while `pg_fusion.enable` is on.

See the [Roadmap](roadmap.md) for why that matters for PostgreSQL types, casts,
collations, operators, and parameters. The current compatibility matrix is
documented in [Compatibility Matrix](compatibility-matrix.md).

## Validation

When in doubt, compare:

```sql
SET pg_fusion.enable = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT ...;

SET pg_fusion.enable = on;
EXPLAIN ANALYZE
SELECT ...;
```

If results differ outside the documented limitations above, treat it as a bug
or unsupported case.
