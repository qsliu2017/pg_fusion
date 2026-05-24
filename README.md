# pg_fusion

`pg_fusion` is an experimental PostgreSQL extension for running selected
analytical `SELECT` queries through Apache DataFusion while PostgreSQL still
owns table access.

The core boundary is:

> PostgreSQL owns table access; DataFusion owns selected analytical execution
> above PostgreSQL scan streams.

Users connect to PostgreSQL normally. Eligible PostgreSQL backends stream scan
rows into page-backed Arrow batches in shared memory, send the work to a
separate `pg_fusion` background worker that runs DataFusion, and import result
pages back into PostgreSQL tuple slots.

DataFusion is not copied into every backend process.

## Try A Query

```sql
CREATE TABLE t AS
SELECT i AS id, i % 100 AS a, i % 10 AS group_id, i::double precision AS value
FROM generate_series(1, 1000000) AS i;

ANALYZE t;

SET pg_fusion.enable = on;

SELECT group_id, count(*), avg(value)
FROM t
GROUP BY group_id
ORDER BY group_id;

SELECT count(*)
FROM t o
JOIN t i USING (a)
WHERE i.a = 42;
```

See [Quick start](docs/quickstart.md) for the local pgrx setup. The rendered
documentation site is published at
[darthunix.github.io/pg_fusion](https://darthunix.github.io/pg_fusion/).

## How It Works

`pg_fusion` installs PostgreSQL hooks and a custom scan path. When a query is
eligible, PostgreSQL still resolves catalogs, owns the snapshot, reads heap
tuples, checks MVCC visibility, handles TOAST, and returns final tuple slots to
the client.

The expensive part is the boundary crossing: PostgreSQL heap tuples are row
oriented, while DataFusion runs on Arrow batches. `pg_fusion` therefore tries to
push filters and narrow projections into PostgreSQL scans before rows are
encoded into Arrow pages. Sending unused rows or columns to the worker can cost
more than the DataFusion execution saves.

The DataFusion worker is a shared resource box:

- backend processes keep PostgreSQL session and table-access work;
- one background worker owns DataFusion CPU scheduling, memory, and spill;
- shared memory owns fixed page, control, filter, wakeup, and metrics capacity.

## When It Can Help

`pg_fusion` is intended for analytical reads where the work above PostgreSQL
scans is large enough to justify tuple-to-Arrow conversion and transport:

- join-heavy plans that create large intermediate batches inside the DataFusion
  worker;
- runtime-filter-friendly joins that reduce probe-side scan encoding;
- grouped aggregation after selective PostgreSQL-side filters;
- sort and window-like analytical execution;
- queries where pushed filters and projections greatly reduce scan output.

It is less likely to help when the query mostly returns raw rows, needs many
wide columns, cannot push selective filters into PostgreSQL, or uses SQL shapes
that are not supported yet.

## Status

`pg_fusion` is experimental infrastructure for engineering evaluation,
benchmarks, and workload feedback. Review [Query support](docs/query-support.md)
and [Limitations](docs/limitations.md) before using it beyond controlled
experiments.

## Documentation

| Topic | Description |
| --- | --- |
| [Quick start](docs/quickstart.md) | Build, configure, and run a first local query |
| [Architecture](docs/architecture.md) | Runtime model, data movement, and resource boundary |
| [Query support](docs/query-support.md) | What query shapes are currently eligible |
| [Configuration](docs/configuration.md) | GUCs for the worker, shared memory, scans, filters, and spill |
| [Metrics](docs/metrics.md) | Diagnostic workflows for scan, worker, result, filter, and spill metrics |
| [Benchmarks](docs/benchmarks.md) | Local diagnostic benchmark workflow |
| [Workloads](docs/workloads.md) | Good and poor workload candidates |
| [Limitations](docs/limitations.md) | Practical restrictions and overhead cases |
| [Development](docs/development.md) | Contributor environment and workspace map |
| [Testing](docs/testing.md) | Standalone Rust and pgrx test commands |
| [Roadmap](docs/roadmap.md) | Typed planning, PG18 support, compatibility, and testing direction |

## Development

Contributor setup starts in [Development](docs/development.md). Internal
maintainer and agent notes live under [`ai/`](ai/); they are not a replacement
for public documentation.
