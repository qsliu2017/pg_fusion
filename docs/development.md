# Development

[Documentation home](index.md)

This page is for contributors working on the repository. User-facing runtime
concepts are covered in [Architecture](architecture.md).

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
| `page/import/` | Worker-side page import |
| `page/row_encoder/` | PostgreSQL-free row encoder helpers |
| `page/row_estimator/` | Page row estimator |

### Planning

| Path | Purpose |
| --- | --- |
| `join_order/` | Standalone compact join-order optimizer |
| `pg/statistics/` | PostgreSQL estimates and relation statistics |

## Development Rules

- Avoid panics in PostgreSQL extension paths.
- Return controlled PostgreSQL errors for backend-facing failures.
- Keep PostgreSQL responsible for physical table access.
- Do not borrow from shared-memory pages after release.
- Keep Arrow page schemas and PostgreSQL slot projection schemas in sync.
- Keep `ai/` updated when behavior or architecture changes.
