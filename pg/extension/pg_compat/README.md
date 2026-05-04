# `pg_compat`

This directory contains a committed PostgreSQL 17 compatibility corpus for
SELECT shapes that are expected to execute through `pg_fusion`.

The corpus is intentionally data-local and runner-local. It does not compare
against upstream `expected/*.out` files. Instead, the pgrx test loads
`fixtures.sql` and runs each passing query twice in one PostgreSQL cluster:

- `pg_fusion.enable = off` for the vanilla result
- `pg_fusion.enable = on` for the extension result

Each case also requires `EXPLAIN` to show `Custom Scan (PgFusionScan)`, so
bypassed catalog/service queries do not count as compatibility coverage.

Files:

- `fixtures.sql`: local temp-table fixtures adapted from PostgreSQL regression
  tests, restricted to currently supported column types.
- `passed.sql`: 536 allowlisted queries that must match vanilla PostgreSQL.
- `failing.sql`: 1516 known failing or unsupported repros kept for iterative
  fixes; the current pgrx runner does not execute this file.

Each query in `passed.sql` and `failing.sql` carries lightweight metadata:

- `id`: stable case name used in assertion messages.
- `origin`: upstream PostgreSQL source location or local origin note.
- `compare`: `ordered` for exact row order or `multiset` for unordered result
  comparison.

The current corpus was extracted from PostgreSQL `REL_17_STABLE`
`src/test/regress/sql/*.sql`. It includes the original core files used by this
runner plus additional candidates from the wider regression tree when their
visible relations are available in `fixtures.sql`.
