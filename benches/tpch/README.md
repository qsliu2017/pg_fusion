# Native TPC-H Diagnostic Benchmark

This directory contains the native TPC-H harness for comparing vanilla
PostgreSQL with `pg_fusion`. It is intended for local engineering diagnosis,
not for audited TPC-H publication.

The supported runner is the Rust binary crate `pg_fusion_tpch`. It embeds the
Apache-2.0 `tpchgen` crate, streams generated rows directly into PostgreSQL with
`COPY FROM STDIN`, and loads a native schema:

- TPC-H decimal fields use `numeric(15,2)`;
- TPC-H date fields use PostgreSQL `date`;
- query outputs are compared between `pg_fusion.enable = off` and
  `pg_fusion.enable = on`.

## Build And Install pg_fusion

Use release builds for benchmark runs; debug builds are too slow for meaningful
timings. No external TPC-H generator is required.

For a local pgrx-managed PostgreSQL 17 cluster:

```sh
cargo pgrx init --pg17 /path/to/pg_config
cargo pgrx install --release -p pg_fusion --pg-config /path/to/pg_config
```

For an external PostgreSQL 17 installation, build and install against that
server's `pg_config`:

```sh
cargo pgrx install --release -p pg_fusion --pg-config /path/to/pg_config
```

`pg_fusion` must be preloaded before PostgreSQL starts:

```conf
shared_preload_libraries = 'pg_fusion'
```

## Runtime Settings For Benchmarks

`shared_preload_libraries` is only the required loading hook. For stable TPC-H
runs, configure the worker, shared-memory transport, page pool, scan streaming,
and runtime filter pool before starting PostgreSQL. A practical profile for a
16 GiB development machine is:

```conf
shared_preload_libraries = 'pg_fusion'

pg_fusion.worker_threads = 0
pg_fusion.worker_memory_limit_mb = 2048
pg_fusion.worker_spill_directory = '/tmp/pg_fusion_spill'

pg_fusion.control_slot_count = 128
pg_fusion.control_backend_to_worker_capacity = 65536
pg_fusion.control_worker_to_backend_capacity = 65536

pg_fusion.scan_slot_count = 256
pg_fusion.scan_backend_to_worker_capacity = 4096
pg_fusion.scan_worker_to_backend_capacity = 4096

pg_fusion.page_size = 262144
pg_fusion.page_count = 1024

pg_fusion.scan_fetch_batch_rows = 4096
pg_fusion.scan_batch_channel_capacity = 128
pg_fusion.scan_idle_poll_interval_us = 50

pg_fusion.runtime_filter_enable = on
pg_fusion.runtime_filter_count = 128
pg_fusion.runtime_filter_bits = 4194304
pg_fusion.runtime_filter_hashes = 4
```

Most of these settings are postmaster-level and require a PostgreSQL restart.
`pg_fusion.page_size * pg_fusion.page_count` is the fixed shared page pool; the
profile above reserves about 256 MiB for pages. Runtime filters reserve roughly
`pg_fusion.runtime_filter_count * pg_fusion.runtime_filter_bits / 8`, about
64 MiB in this profile, plus overhead. `pg_fusion.worker_memory_limit_mb` caps
the DataFusion worker memory pool; it is separate from PostgreSQL
`shared_buffers`.

For smaller machines, reduce `pg_fusion.page_count` first, then
`pg_fusion.runtime_filter_count` or `pg_fusion.runtime_filter_bits`, then
`pg_fusion.worker_memory_limit_mb`. Defaults may be enough for SF0.01 smoke
runs, but use this profile or
[Quick Start](../../docs/quickstart.md#configure-postgresql) for SF1 and larger
comparisons. The full GUC reference is in
[Configuration](../../docs/configuration.md).

Restart PostgreSQL after changing preload or postmaster-level `pg_fusion`
settings. For a pgrx-managed cluster, start PostgreSQL and open `psql` with:

```sh
cargo pgrx start pg17 -p pg_fusion
cargo pgrx run --release pg17 -p pg_fusion
```

Then install the extension in the benchmark database:

```sql
CREATE EXTENSION IF NOT EXISTS pg_fusion;

SHOW shared_preload_libraries;
SELECT extversion FROM pg_extension WHERE extname = 'pg_fusion';
```

The runner toggles `pg_fusion.enable` for vanilla and Fusion measurements. It
also sets `max_parallel_workers_per_gather` from `--parallel-workers` for each
session.

## PostgreSQL Connection

The runner accepts standard connection options:

- `--dbname` / `-d` for the database name;
- `--host` for TCP host or Unix socket directory;
- `--port` for PostgreSQL port;
- `--user` / `-U` for the PostgreSQL user.

If these flags are omitted, it reads `PGDATABASE`, `PGHOST`, `PGPORT`,
`PGUSER`, and `PGPASSWORD`. Passwords should be passed through `PGPASSWORD`;
there is no `--password` flag.

For a TCP connection:

```sh
PGPASSWORD=secret cargo run --release -p pg_fusion_tpch -- \
  --host localhost \
  --port 5432 \
  --user postgres \
  --dbname pg_fusion \
  --scale-factor 0.01
```

For a Unix socket connection:

```sh
cargo run --release -p pg_fusion_tpch -- \
  --host /tmp \
  --port 5432 \
  --user postgres \
  --dbname pg_fusion
```

When `--host`, `--port`, `PGHOST`, and `PGPORT` are absent, the runner tries to
auto-detect a single pgrx Unix socket under `~/.pgrx`.

## Quick Run

From the repository root:

```sh
cargo run --release -p pg_fusion_tpch -- \
  --dbname pg_fusion \
  --scale-factor 0.01 \
  --runs 3 \
  --warmup 1
```

By default the runner:

1. recreates schema `tpch`;
2. streams generated TPC-H rows into PostgreSQL;
3. analyzes all benchmark tables;
4. runs each selected query with `pg_fusion.enable = off` and `on`;
5. alternates measured modes to reduce hot-cache ordering bias;
6. prints a console comparison report;
7. writes CSV and JSON summaries under `benches/tpch/results/`.

To reuse an existing loaded schema:

```sh
cargo run --release -p pg_fusion_tpch -- \
  --dbname pg_fusion \
  --no-prepare \
  --queries q01,q06,q14
```

To only generate and load data:

```sh
cargo run --release -p pg_fusion_tpch -- \
  --dbname pg_fusion \
  --scale-factor 1 \
  --only-prepare
```

## Useful Options

- `--scale-factor 0.01` controls embedded `tpchgen` generation.
- `--schema tpch` selects the PostgreSQL schema to drop/recreate.
- `--queries all` or `--queries q01,q06,q14` selects query ids.
- `--parallel-workers 2` sets PostgreSQL `max_parallel_workers_per_gather` for
  both vanilla and `pg_fusion` runs.
- `--timeout 120` sets PostgreSQL `statement_timeout` for each query/mode run.
- `--host`, `--port`, `--user`, and `--dbname` configure the PostgreSQL
  connection. If a single pgrx Unix socket exists under `~/.pgrx`, the runner
  auto-detects it when host/port are not set.
- `--no-color` disables ANSI status colors in the console report.

## Result Comparison

The runner compares vanilla PostgreSQL and `pg_fusion` output exactly. A query
is `ok` only when both modes succeed and the `COPY ... TO STDOUT WITH (FORMAT
csv)` bytes are identical. Numeric formatting, row order, dates, text values,
and NULL representation must match. Any numeric drift is a correctness failure
for this benchmark.

Statuses are:

- `ok`: vanilla and `pg_fusion` both succeeded and returned matching rows;
- `mismatch`: both succeeded but byte-identical output differed;
- `fusion_fail`: vanilla preflight succeeded, but `pg_fusion` failed or did
  not plan through `PgFusionScan`;
- `pg_fail`: vanilla PostgreSQL failed, so the comparison is invalid.
