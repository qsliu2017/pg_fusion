---
title: pg_fusion Documentation
---

# pg_fusion Documentation

`pg_fusion` runs selected analytical PostgreSQL `SELECT` queries through a
shared DataFusion background worker. PostgreSQL still owns heap access,
snapshots, MVCC visibility, TOAST, tuple decoding, and final result slots.

Start with the pages that answer operational questions first.

## Start Here

| Topic | Use It For |
| --- | --- |
| [Quick start](quickstart.md) | Build the extension, configure a local pgrx cluster, and run a first query |
| [Architecture](architecture.md) | Understand the backend/worker/shared-memory model and why rows cross into Arrow |
| [Query support](query-support.md) | Check which query shapes and types are currently eligible |
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
| [Workloads](workloads.md) | Evaluate good and poor workload candidates |

## Status

`pg_fusion` is experimental. Treat unsupported query shapes as not implemented,
not as implicitly equivalent to PostgreSQL execution.
