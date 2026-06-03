---
title: pg_fusion Documentation
---

# pg_fusion Documentation

`pg_fusion` runs selected analytical PostgreSQL `SELECT` queries through a
shared DataFusion background worker. PostgreSQL still owns heap access,
snapshots, MVCC visibility, TOAST, tuple decoding, and final result slots.

DataFusion is a Rust analytical execution engine over Apache Arrow columnar
batches. pg_fusion uses it for selected analytical execution above PostgreSQL
scan streams; it does not replace PostgreSQL storage or MVCC.

Start with the pages that answer operational questions first.

## Start Here

| Topic | Use It For |
| --- | --- |
| [Quick start](quickstart.md) | Build the extension, configure a local pgrx cluster, and run a first query |
| [Glossary](glossary.md) | Learn the terms: DataFusion, Arrow, slots, page pool, filters, DPHyp, CTID scans |
| [Architecture](architecture.md) | Understand the backend/worker/shared-memory model and why rows cross into Arrow |
| [Memory and pages](memory-and-pages.md) | Understand shared blocks, zero-copy imports, materialization, and page reuse |
| [Execution model](execution-model.md) | Follow one eligible query from planning to result slots |
| [Query support](query-support.md) | Check which query shapes and types are currently eligible |
| [Compatibility matrix](compatibility-matrix.md) | Inspect PostgreSQL to DataFusion type, expression, function, aggregate, and window mappings |
| [Workloads](workloads.md) | Evaluate good and poor workload candidates |
| [Limitations](limitations.md) | Understand overhead cases, semantic boundaries, and unsupported features |

## Operate

| Topic | Use It For |
| --- | --- |
| [Configuration](configuration.md) | Size the worker, shared memory, scan streaming, runtime filters, and spill |
| [Metrics](metrics.md) | Diagnose scan encoding, worker backpressure, result transfer, filters, and spill |
| [Benchmarks](benchmarks.md) | Run local comparison benchmarks and interpret the results |

## Build And Contribute

| Topic | Use It For |
| --- | --- |
| [Development](development.md) | Set up Rust, pgrx, and the contributor workflow |
| [Testing](testing.md) | Run standalone Rust tests and PostgreSQL-backed pgrx tests |
| [Roadmap](roadmap.md) | Follow typed planning, PG18 support, compatibility, and testing direction |

## Status

`pg_fusion` is experimental. Treat unsupported query shapes as not implemented,
not as implicitly equivalent to PostgreSQL execution.
