# Execution Model

[Documentation home](index.md)

This page follows one eligible query from PostgreSQL planning to result rows.
For the higher-level process and resource model, see [Architecture](architecture.md).

## Startup

`pg_fusion` must be loaded at PostgreSQL startup because it registers:

- PostgreSQL planner and custom scan hooks;
- shared-memory regions;
- the DataFusion background worker.

The setup command is documented in [Configuration](configuration.md#required-preload).

## Planning

`pg_fusion` currently targets top-level `SELECT` statements. The planner hook
first decides whether the query should stay on PostgreSQL. Common bypasses
include disabled `pg_fusion.enable`, non-`SELECT` statements, modifying CTEs,
catalog or TOAST relations, function range entries, and bound parameters.

Eligible queries are planned for DataFusion, but PostgreSQL table access is
kept as PostgreSQL scan streams. pg_fusion tries to push filters and projections
into those PostgreSQL scans before rows cross into Arrow pages.

For eligible inner and cross equi-join components, pg_fusion also uses
PostgreSQL statistics with the DPHyp join-order optimizer to choose a better
join order before execution. Join shapes outside that supported subset keep the
order produced by the normal planning path.

This pushdown matters. Tuple decoding and slot-to-Arrow encoding happen in
PostgreSQL backends, so every unnecessary row or column sent to the worker adds
conversion and shared-memory transport cost.

## Execution

At execution time PostgreSQL runs a `PgFusionScan` custom scan root.

The backend opens an execution session with the shared worker and starts scan
producers for PostgreSQL table leaves. Each producer reads rows through
PostgreSQL executor paths, using the query snapshot owned by the backend.

Scan producers do not build an unbounded backend-local result set. They fill
shared-memory pages with Arrow batches and hand those pages to the worker. If
the shared page pool or scan channels are full, scan production waits for the
worker to catch up.

The worker imports scan pages, runs DataFusion physical operators, and writes
result pages back to shared memory. The backend imports those result pages and
stores rows into the PostgreSQL tuple slot returned to the client.

## Runtime Filters

For selected inner hash joins, the worker can build a runtime Bloom filter from
one side of the join and publish it through shared memory. PostgreSQL scan
producers can then test probe-side rows before encoding them into Arrow pages.

Runtime filters are an optimization, not a semantic requirement. If a filter is
not available or no shared filter slot can be acquired, execution continues
without it.

## Cancellation And Cleanup

The backend owns the PostgreSQL execution lifecycle. When a pg_fusion query
finishes, errors, or is canceled, the backend tears down active scan producers,
releases transport leases, and releases result ingress state. Worker-side
execution directories for spill are cleaned up by the worker runtime.

## EXPLAIN

`EXPLAIN` shows the DataFusion plan and the PostgreSQL scan SQL used by scan
leaves. For CTID-range scan producers, verbose output shows representative scan
SQL with range placeholders rather than exposing one concrete worker range as
if it were the only scan.

`EXPLAIN ANALYZE` can also report actual scan producer counts installed during
execution. Use [Metrics](metrics.md) when you need timing and transport
diagnostics beyond plan shape.
