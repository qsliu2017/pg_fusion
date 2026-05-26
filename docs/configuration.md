# Configuration

[Documentation home](index.md)

`pg_fusion` uses PostgreSQL GUCs to configure the runtime path, the DataFusion
worker, shared-memory transport, scan streaming, runtime filters, spill, and
diagnostics.

The [architecture](architecture.md) explains why the runtime is shaped as one
background worker plus preallocated shared memory. This page lists the knobs
that configure that shape.

## Required Preload

```conf
shared_preload_libraries = 'pg_fusion'
```

`pg_fusion` must be loaded at postmaster start because it registers PostgreSQL
hooks, shared memory, and a background worker.

## Enable The Runtime Path

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.enable` | `off` | User/session | Enables the pg_fusion path for eligible queries in the session. |
| `pg_fusion.backend_log_level` | `0` | User/session | Backend diagnostics: `0=off`, `1=basic`, `2=trace`. |

Use `pg_fusion.enable` when comparing a query against vanilla PostgreSQL:

```sql
SET pg_fusion.enable = off;
-- run PostgreSQL plan

SET pg_fusion.enable = on;
-- run pg_fusion plan if eligible
```

## Size The Worker

The worker is the DataFusion resource box. These settings are postmaster-level.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.worker_threads` | `0` | DataFusion worker thread count. `0` lets the worker choose automatically. |
| `pg_fusion.worker_memory_limit_mb` | `0` | DataFusion worker memory limit. `0` uses the default unbounded runtime and disables worker spill. |
| `pg_fusion.worker_spill_directory` | `''` | Base directory for worker-owned spill files. Empty uses OS temporary storage. |
| `pg_fusion.worker_log_filter` | `warn` | Worker tracing filter. |
| `pg_fusion.log_path` | `/tmp/pg_fusion.log` | Worker diagnostic log path. |

Set `pg_fusion.worker_memory_limit_mb` above `0` only when you want a finite
DataFusion memory pool and worker-owned spill. Spill files are not PostgreSQL
temporary files.

## Size Shared Memory

Shared-memory settings are postmaster-level because they define fixed transport
layout at PostgreSQL startup.

Primary execution channels carry session lifecycle messages such as start,
cancel, completion, and errors.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.control_slot_count` | `64` | Number of primary backend/worker control slots. |
| `pg_fusion.control_backend_to_worker_capacity` | `8192` | Per-slot primary control ring capacity from backend to worker. |
| `pg_fusion.control_worker_to_backend_capacity` | `8192` | Per-slot primary control ring capacity from worker to backend. |

Scan channels are separate because scan requests and responses can be frequent.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.scan_slot_count` | `64` | Number of dedicated scan control slots. |
| `pg_fusion.scan_backend_to_worker_capacity` | `256` | Dedicated scan ring capacity from backend to worker. |
| `pg_fusion.scan_worker_to_backend_capacity` | `256` | Dedicated scan ring capacity from worker to backend. |

The page pool carries Arrow scan pages to the worker and result pages back to
the backend.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.page_size` | `65536` | Shared page size in bytes. |
| `pg_fusion.page_count` | `256` | Number of shared pages. Also sizes the issued-page permit pool. |

More pages can reduce backpressure but increase fixed shared-memory footprint.

## Tune Scan Streaming

Scan tuning controls how PostgreSQL scan producers feed Arrow pages to the
worker.

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.scan_fetch_batch_rows` | `1024` | Postmaster | Rows requested per PostgreSQL portal drain in backend scan streaming. |
| `pg_fusion.scan_batch_channel_capacity` | `32` | User/session | Bounded worker scan batch channel capacity per PostgreSQL scan stream. |
| `pg_fusion.scan_idle_poll_interval_us` | `50` | User/session | Worker scan idle poll interval in microseconds. |
| `pg_fusion.estimator_initial_tail_bytes_per_row` | `64` | Postmaster | Initial variable-width Arrow page tail estimate. |

If scan metrics show high backend page fill time, the bottleneck may be
PostgreSQL scanning, tuple decoding, detoast, or slot-to-Arrow encoding rather
than worker execution.

## Configure Planning Optimizations

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.join_reordering` | `on` | User/session | Enables statistics-based join reordering for eligible joins. |

PostgreSQL scan planning still matters because scan leaves execute trusted
PostgreSQL scan SQL through PostgreSQL executor portals.

Useful PostgreSQL planner experiment settings include:

```conf
max_parallel_workers_per_gather = 2
min_parallel_table_scan_size = '8MB'
parallel_setup_cost = 1000
parallel_tuple_cost = 0.1
```

These settings affect PostgreSQL-side scan planning. They do not configure
DataFusion worker memory.

## Configure Runtime Filters

Runtime filters can reduce rows before slot-to-Arrow encoding on eligible hash
join probe scans.

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.runtime_filter_enable` | `on` | User/session | Enables runtime Bloom filters for eligible hash joins. |
| `pg_fusion.runtime_filter_count` | `64` | Postmaster | Number of shared-memory runtime filter slots. |
| `pg_fusion.runtime_filter_bits` | `1048576` | Postmaster | Bloom filter bit count per slot. |
| `pg_fusion.runtime_filter_hashes` | `4` | Postmaster | Number of Bloom hash probes per slot. |

If the pool is exhausted, execution continues without the missing filter and
records a diagnostic counter.

## Worker Spill

`pg_fusion.worker_memory_limit_mb = 0` keeps DataFusion on the default
unbounded runtime and disables worker spill.

Setting it above `0` enables a finite DataFusion memory pool and worker-owned
OS temporary spill files. `pg_fusion.worker_spill_directory` may point at an
absolute spill root; empty uses OS temporary storage under `pg_fusion/spill`.

This v1 spill path is owned by the pg_fusion worker. It does not use PostgreSQL
`temp_tablespaces`, `temp_file_limit`, or `ResourceOwner` cleanup.

See [Metrics](metrics.md#did-spill-happen) for spill diagnostics.
