# pg_frontend

`pg_frontend` is pg_fusion's typed frontend for PostgreSQL analyzed `Query`
trees. It lets the planner use metadata that PostgreSQL has already resolved,
instead of deparsing back to SQL and asking DataFusion to infer PostgreSQL
semantics from text.

## Role

The crate owns the planning-time path from a live PostgreSQL `pg_sys::Query` to
a DataFusion `LogicalPlan` with PostgreSQL table-source leaves:

1. `adapter/` copies the supported PostgreSQL `Query` shape into a neutral
   `TypedQuery`.
2. Catalog resolution mutates that `TypedQuery` in place, filling relation
   columns, attribute numbers, PostgreSQL type refs, nullability, and the scan
   relation identity.
3. The compiler consumes a `ResolvedQuery` view and lowers expressions to a
   DataFusion logical plan without performing catalog lookup itself.
4. The extension/backend host passes that logical plan to `plan_builder`, which
   returns a `HybridPlan`: the DataFusion plan plus the PostgreSQL scan plan
   referenced by its custom scan leaves.

`TypedQuery` is not serialized and is not stored in PostgreSQL plan nodes. The
active frontend `CustomScan` payload is a `plan_codec` `frontend_plan` payload
containing the already built logical plan and PostgreSQL scan specs.

## Scope

The v1 frontend is intentionally fail-closed. It targets one base relation with
simple projection/filter expressions and PostgreSQL type metadata preserved for
scan SQL compilation. Unsupported query shapes return structured frontend
errors so the production planner can fall back to the SQL-text `plan_builder`
path in `try` mode.

PostgreSQL major-version layout details must stop at `adapter/`. The stable
typed model, catalog resolution, and compiler code should stay independent of
PostgreSQL-version `cfg`s.

## Documentation

Build crate documentation with:

```bash
cargo doc -p pg_frontend --no-deps
```

Use `--open` locally when you want to inspect the rendered API docs in a
browser.
