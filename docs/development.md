# Development

[Documentation home](index.md)

This page is for contributors working on the repository. User-facing runtime
terms are covered in [Glossary](glossary.md), [Architecture](architecture.md),
[Execution Model](execution-model.md), and
[Memory And Pages](memory-and-pages.md).

## Development Environment

`pg_fusion` development requires:

- Rust 1.89 or newer;
- PostgreSQL 17 development headers;
- a PostgreSQL 17 `pg_config`;
- `cargo-pgrx`.

Install Rust with `rustup`, then add formatting and linting components:

```sh
rustup component add rustfmt clippy
```

Install and initialize pgrx:

```sh
cargo install cargo-pgrx
cargo pgrx init --pg17 $(which pg_config)
```

If multiple PostgreSQL versions are installed, pass the full path to the
PostgreSQL 17 `pg_config`.

### Build PostgreSQL 17 From Source

When working from a local PostgreSQL checkout, keep the upstream checkout and
the PostgreSQL 17 worktree separate. The examples below use `WORK_ROOT` as the
parent directory for both PostgreSQL and `pg_fusion` checkouts:

- `postgres`: upstream PostgreSQL repository;
- `postgres-17`: PostgreSQL 17 worktree;
- `pg_fusion`: this repository;
- `postgres-17/build`: out-of-tree build directory and install prefix.

Create or refresh the PostgreSQL 17 worktree:

```sh
WORK_ROOT=/path/to/workspace

cd "$WORK_ROOT/postgres"
git fetch origin REL_17_STABLE
git worktree add "$WORK_ROOT/postgres-17" origin/REL_17_STABLE
```

If `postgres-17` already exists, update it instead:

```sh
WORK_ROOT=/path/to/workspace

cd "$WORK_ROOT/postgres-17"
git fetch origin REL_17_STABLE
git checkout REL_17_STABLE
git pull --ff-only origin REL_17_STABLE
```

Build and install PostgreSQL from inside `postgres-17/build`:

```sh
WORK_ROOT=/path/to/workspace

cd "$WORK_ROOT/postgres-17"
mkdir -p build
cd build
../configure --with-llvm --prefix "$(pwd)"
make -j
make -j install
```

Then initialize pgrx against that exact PostgreSQL 17 installation and build
`pg_fusion` in release mode:

```sh
WORK_ROOT=/path/to/workspace

cd "$WORK_ROOT/pg_fusion"
cargo pgrx init --pg17 "$WORK_ROOT/postgres-17/build/bin/pg_config"
cargo build --release -p pg_fusion
```

## Common Commands

Check formatting:

```sh
cargo fmt --all -- --check
```

Check the workspace:

```sh
cargo check --workspace
```

Build the extension:

```sh
cargo build -p pg_fusion
```

Run clippy for the PostgreSQL 17 feature set:

```sh
cargo clippy --all-targets --features "pg17, pg_test" --no-default-features
```

Tests are documented in [Testing](testing.md).

## Workspace Map

The workspace is split by boundary.

### PostgreSQL Integration

| Path | Purpose |
| --- | --- |
| `pg/extension/` | pgrx extension crate, GUCs, hooks, background worker, custom scan |
| `pg/backend_service/` | Backend-side execution orchestration and scan production |
| `pg/df_catalog/` | PostgreSQL catalog resolver for DataFusion planning |
| `pg/plan_builder/` | SQL-to-DataFusion logical plan builder |
| `pg/scan_node/` | Custom DataFusion scan nodes for PostgreSQL scans |
| `pg/scan_sql/` | Trusted PostgreSQL scan SQL rendering |
| `pg/slot_scan/` | PostgreSQL executor portal scan path |
| `pg/slot_encoder/` | PostgreSQL slot to Arrow page encoding |
| `pg/slot_import/` | Arrow result page to PostgreSQL tuple-slot projection |
| `pg/statistics/` | PostgreSQL statistics bridge for join planning |
| `pg/type/` | Shared PostgreSQL type policy used by catalog, planning, scan, and slot crates |

### Worker Runtime

| Path | Purpose |
| --- | --- |
| `runtime/worker/` | DataFusion worker runtime |
| `runtime/protocol/` | Typed backend/worker protocol |
| `runtime/control_transport/` | Shared-memory control rings |
| `runtime/filter/` | Runtime Bloom filter pool |
| `runtime/metrics/` | Shared-memory runtime metrics |

### Page Transport

| Path | Purpose |
| --- | --- |
| `page/pool/` | Shared page pool |
| `page/transfer/` | Page transfer primitives |
| `page/issuance/` | Issued-frame lifecycle |
| `page/arrow_layout/` | Arrow-compatible page layout |
| `page/batch_encoder/` | Arrow batch to shared-page block encoding |
| `page/import/` | Worker-side page import |
| `page/plan_codec/` | Shared-memory physical-plan payload codec |
| `page/plan_flow/` | Plan payload transfer flow |
| `page/row_encoder/` | PostgreSQL-free row encoder helpers |
| `page/row_estimator/` | Page row estimator |
| `page/scan_flow/` | Scan stream page flow between producers and worker |

### Planning

| Path | Purpose |
| --- | --- |
| `join_order/` | Standalone compact join-order optimizer |
| `pg/statistics/` | PostgreSQL estimates and relation statistics |

## Transport And Materialization Boundaries

Keep ownership boundaries explicit when changing scan, page, or DataFusion
execution code:

- `pg/slot_encoder` is where PostgreSQL `TupleTableSlot` values become
  Arrow-compatible page blocks.
- `page/import` creates Arrow arrays over shared page bytes and keeps the page
  lease alive for zero-copy scan import.
- `pg/scan_node` owns DataFusion scan nodes and inserts materialization before
  operators that can retain page-backed batches.
- `page/pool`, `page/issuance`, `page/scan_flow`, and `page/transfer` define
  page ownership, reuse, and handoff.
- `pg/slot_import` projects worker result pages back into PostgreSQL result
  slots, where PostgreSQL-owned datums may require copies.

## Development Rules

- Avoid panics in PostgreSQL extension paths.
- Return controlled PostgreSQL errors for backend-facing failures.
- Keep PostgreSQL responsible for physical table access.
- Do not borrow from shared-memory pages after release.
- Keep Arrow page schemas and PostgreSQL slot projection schemas in sync.
- Keep `ai/` updated when behavior or architecture changes.
