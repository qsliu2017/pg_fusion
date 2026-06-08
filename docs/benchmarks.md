# Benchmarks

[Documentation home](index.md)

`pg_fusion` includes local diagnostic benchmarks. They are for engineering
evaluation, not audited TPC-H publication.

Benchmark results should be read together with query plans and metrics. A
pg_fusion query can be slower if scan, tuple decoding, Arrow encoding, or page
transport dominates the DataFusion-side work.

## Native TPC-H Harness

The supported TPC-H harness is the Rust binary crate:

```text
benches/tpch/runner
```

It embeds the Apache-2.0 `tpchgen` crate, streams generated rows into
PostgreSQL with `COPY FROM STDIN`, and uses native PostgreSQL `numeric(15,2)`
and `date` columns.

## Prerequisites

Use release builds for benchmark runs; debug builds are too slow for meaningful
timings.

For a local pgrx-managed PostgreSQL 17 cluster:

```sh
cargo pgrx init --pg17 $(which pg_config)
cargo build --release -p pg_fusion
```

For an external PostgreSQL 17 installation:

```sh
cargo pgrx install --release -p pg_fusion --pg-config /path/to/pg_config
```

Set `shared_preload_libraries = 'pg_fusion'` before PostgreSQL starts. For
stable TPC-H runs, also size the page pool, scan slots, control rings,
DataFusion worker memory, and runtime filter pool. One useful 16 GiB
development profile is in [Quick Start](quickstart.md#configure-postgresql);
all GUCs and sizing tradeoffs are listed in [Configuration](configuration.md).
After changing preload or postmaster-level `pg_fusion` settings, restart
PostgreSQL, then run `cargo pgrx start pg17` and
`cargo pgrx run --release pg17 -p pg_fusion` for a pgrx-managed cluster. Run
`CREATE EXTENSION IF NOT EXISTS pg_fusion;` in the benchmark database. The TPC-H
runner toggles `pg_fusion.enable` itself and sets
`max_parallel_workers_per_gather` from `--parallel-workers`.

## PostgreSQL Connection

Use `--dbname` / `-d`, `--host`, `--port`, and `--user` / `-U` for explicit
connections. The runner also reads `PGDATABASE`, `PGHOST`, `PGPORT`, `PGUSER`,
and `PGPASSWORD`. Passwords should use `PGPASSWORD`; there is no `--password`
flag.

```sh
PGPASSWORD=secret cargo run --release -p pg_fusion_tpch -- \
  --host localhost \
  --port 5432 \
  --user postgres \
  --dbname pg_fusion \
  --scale-factor 0.01
```

## Quick Run

From the repository root:

```sh
cargo run --release -p pg_fusion_tpch -- \
  --dbname pg_fusion \
  --scale-factor 0.01 \
  --runs 3 \
  --warmup 1
```

By default, the runner:

1. recreates the benchmark schema;
2. streams generated TPC-H data into PostgreSQL;
3. analyzes the tables;
4. runs each query with `pg_fusion.enable = off`;
5. runs each query with `pg_fusion.enable = on`;
6. alternates measured modes to reduce hot-cache ordering bias;
7. writes CSV and JSON summaries.

## Reuse An Existing Schema

```sh
cargo run --release -p pg_fusion_tpch -- \
  --dbname pg_fusion \
  --no-prepare \
  --queries q01,q03,q06
```

## Result Statuses

- `ok`: PostgreSQL and pg_fusion both succeeded and returned matching rows.
- `mismatch`: both succeeded but byte-identical output differed.
- `fusion_fail`: vanilla preflight succeeded, but pg_fusion failed or did not
  plan through `PgFusionScan`.
- `pg_fail`: PostgreSQL failed, so the comparison is invalid.

## Report Output

The console report shows median latency for measured runs. `speedup` is
`pg_median_ms / fusion_median_ms`, so values above `1` mean `pg_fusion` was
faster. The JSON artifact includes raw timings, row counts, result hashes,
PostgreSQL metadata, and pg_fusion extension version when available. For runtime
diagnostics, use [Metrics](metrics.md).

## Useful Query Groups

For scan and encoding experiments, start with:

- `q01`;
- `q06`;
- `q14`;
- `q19`.

For joins and grouped aggregation, inspect:

- `q03`;
- `q05`;
- `q10`;
- `q12`.

The remaining canonical-derived queries are useful for planner coverage and
failure reporting.

## Row Encoder Microbenchmark

To isolate PostgreSQL-free Rust page encoding work, run the Criterion
benchmark:

```sh
cargo bench -p row_encoder --bench q05_encode
```

This is not a vanilla-vs-Fusion TPC-H run. The benchmark name reflects its
q05-shaped input fixture. If `PG_FUSION_TPCH_DIR` points to compatible CSV
files, the benchmark uses them; otherwise it falls back to a deterministic
synthetic fixture.
