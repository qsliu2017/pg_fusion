# Architecture

`pg_fusion` adds a DataFusion execution path to PostgreSQL without taking table
access away from PostgreSQL.

The core boundary is:

> PostgreSQL owns table access; DataFusion owns selected analytical execution
> above PostgreSQL scan streams.

This page is written for administrators and users who need to understand what
the extension adds to a PostgreSQL instance, where resources are spent, and why
some queries are a better fit than others.

## Runtime Shape

A running `pg_fusion` installation has three important pieces.

**PostgreSQL backends** are the ordinary per-connection PostgreSQL processes.
They receive SQL, run PostgreSQL hooks, own snapshots, and execute PostgreSQL
table scans.

**The pg_fusion worker** is one PostgreSQL background worker process that hosts
the DataFusion runtime. Eligible backends send plans and scan pages to this
worker. DataFusion is not embedded as a separate runtime in every backend.

**Shared memory** is the preallocated communication area between backends and
the worker. It contains page storage, execution channels, scan channels,
runtime filters, wakeups, and metrics.

## What Happens To A Query

When a backend receives a top-level `SELECT`, the pg_fusion planner hook checks
whether the query is eligible. Unsupported queries stay on PostgreSQL or fail
closed with a controlled error, depending on the point where the unsupported
shape is discovered.

For an eligible query:

1. PostgreSQL resolves relations, types, and the execution snapshot.
2. pg_fusion builds a DataFusion plan with PostgreSQL table leaves represented
   as PostgreSQL scan streams.
3. The backend starts an execution session with the shared worker.
4. PostgreSQL scan producers read heap tuples through PostgreSQL executor
   paths.
5. Scan rows are encoded into page-backed Arrow batches in shared memory.
6. The worker imports those Arrow batches and runs DataFusion operators.
7. Result batches are written back into shared-memory pages.
8. The backend imports result pages and returns PostgreSQL tuple slots to the
   client.

The worker never scans PostgreSQL heap storage directly. PostgreSQL remains the
owner of MVCC visibility, TOAST, tuple decoding, and final tuple materialization.

## The Expensive Boundary

PostgreSQL heap tuples are row oriented. DataFusion runs on Arrow batches. Every
row that crosses into the worker must be decoded from PostgreSQL slots and
encoded into Arrow page layout.

That conversion is useful only when the work moved to DataFusion pays for the
boundary cost. For that reason, pg_fusion tries to keep scan output small:

- push filters into PostgreSQL scan SQL when possible;
- project only the columns needed above the scan;
- apply eligible runtime filters before encoding probe-side scan rows;
- stream rows page by page instead of building large backend-local buffers.

If a query sends many wide rows to the worker and does little analytical work
after the scan, the round trip through Arrow pages can be a net cost.

## Resource Model

The intended operational model is a resource box.

- PostgreSQL backends own sessions, snapshots, and table access.
- Backend scan producers write output page by page into the shared page pool.
- The DataFusion worker owns analytical CPU scheduling, its memory pool, and
  worker spill files.
- Shared memory owns fixed transport capacity: pages, execution channels, scan
  channels, filters, wakeups, and metrics.

This is different from putting a separate DataFusion runtime in each backend.
Many backends can feed work into the same cooperative DataFusion/Tokio runtime,
and the worker can reuse its configured threads across submitted executions. At
the same time, memory, spill, and shared-memory capacity are easier to reason
about because they are configured as one worker plus one preallocated transport
area.

The page pool bounds scan memory behavior. If scan pages or scan channels are
exhausted, execution applies backpressure instead of allocating an unbounded
amount of memory inside each backend.

## Shared Memory In Plain Terms

Shared memory is not a cache of table data. It is a transport area.

- Primary execution channels carry session lifecycle messages such as start,
  cancellation, completion, and errors.
- Scan channels let the worker ask PostgreSQL scan producers for table data.
- The page pool carries Arrow scan pages to the worker and result pages back to
  the backend.
- Runtime filter slots let the worker publish compact Bloom filters that
  PostgreSQL scan producers can use before encoding rows.
- Metrics record where time and data movement are spent.

Shared-memory size is fixed at PostgreSQL startup. See
[Configuration](configuration.md) for the settings that size the worker and
transport area, and [Metrics](metrics.md) for runtime diagnostics.

## What This Means In Practice

`pg_fusion` is most interesting when the query has enough analytical work above
the scan to justify the conversion cost. Join-heavy plans are especially useful
to evaluate when they create large intermediate results inside the DataFusion
worker: those intermediate columnar batches stay in Arrow form and do not pay
the PostgreSQL heap-row to Arrow conversion cost again.

Other useful candidates include grouped aggregation after selective filters,
sort-heavy plans, and queries where PostgreSQL-side filters and projections
remove much of the table before Arrow encoding.

It is less interesting for raw table export, very wide projections, unsupported
SQL shapes, or queries where PostgreSQL can already finish the work without
moving many rows into another execution engine.
