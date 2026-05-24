# Workloads

This page describes workload shapes that are useful to evaluate with
`pg_fusion`, and what information to collect when a query is faster, slower, or
unsupported.

The main question is whether the DataFusion worker can do enough useful work
after scan ingress to justify the PostgreSQL heap-tuple to Arrow conversion
cost.

## Good Candidates

The best early candidates are join-heavy analytical queries that create large
intermediate results inside the DataFusion worker.

Those intermediate batches are already columnar Arrow data. They do not pay the
PostgreSQL heap-row decoding, Arrow encoding, or backend-to-worker transport
cost again while DataFusion joins, filters, aggregates, sorts, or repartitions
them inside the worker.

Other useful candidates include:

- joins where runtime filters can reject probe-side rows before scan encoding;
- grouped aggregation after selective PostgreSQL-side filters;
- sort or window work over a reduced scan stream;
- queries where pushed filters remove many heap rows before Arrow encoding;
- queries where projection pushdown removes most table columns.

## Why These Workloads Fit

pg_fusion pays boundary costs at scan ingress and result egress:

- PostgreSQL heap rows are decoded into slots;
- selected rows and columns are encoded into Arrow pages;
- pages move through shared memory to the worker;
- result pages move back to PostgreSQL tuple slots.

Once data is inside the worker, intermediate DataFusion batches stay in Arrow
form. A query is a better candidate when it does substantial worker-local
relational work and returns less data than it scanned.

## Poor Candidates

pg_fusion is usually a poor fit when:

- the query returns raw rows without much analytical work;
- the projection needs most columns from a wide table;
- filters are weak or cannot be pushed into PostgreSQL scans;
- the query mostly measures PostgreSQL heap scan and tuple-to-Arrow encoding;
- the workload depends on SQL features listed in [Limitations](limitations.md);
- you need support commitments rather than architecture and benchmark feedback.

These cases are still useful to report when they expose a specific bottleneck.

## What To Collect

Please include:

- PostgreSQL version;
- approximate table sizes;
- anonymized query text;
- `EXPLAIN (ANALYZE, BUFFERS)` with `pg_fusion` disabled;
- `EXPLAIN ANALYZE` with `pg_fusion` enabled, if it runs;
- relevant non-zero rows from `pg_fusion_metrics()`;
- the bottleneck you want to understand.

Use GitHub Issues or Discussions for now. Avoid sharing sensitive schema or
customer data.
