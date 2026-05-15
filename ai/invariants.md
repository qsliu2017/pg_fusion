---
id: inv-project-0001
type: invariant
scope: project
tags: ["safety", "pgrx", "ipc", "datafusion", "shared-memory", "arrow"]
updated_at: "2026-05-04"
importance: 0.95
---

# Project Invariants

1. No panics in PostgreSQL extension paths.

- In `pg/extension` and backend-facing pgrx code, prefer structured
  errors and controlled PostgreSQL error reporting.

2. SHM slices and page payloads must stay within bounds.

- Any slice from shared memory must be clamped to the advertised layout
  capacity.
- Readers must follow transfer/issuance ownership and must not borrow from pages
  after release.

3. Arrow page layout must match the external schema.

- `slot_encoder`, `page/import`, and `slot_import` must agree on
  `arrow_layout` block shape, type tags, validity layout, and view payload
  ownership.
- Decimal result pages carry Arrow precision/scale in the schema. The raw
  `TypeTag::Decimal128` only records the 16-byte fixed-width layout, so
  import/projection code must preserve precision/scale from the Arrow schema
  rather than reconstructing it from the page tag.
- Scan-side PostgreSQL `numeric` support is a finite Decimal128 subset:
  `numeric(p,s)` maps to `Decimal128(p,s)` for `p <= 38` and non-negative
  scale, while bare `numeric` maps to the fixed fallback `Decimal128(38,16)`.
  Values outside the selected shape and PostgreSQL `numeric` `NaN`/`Infinity`
  must fail with controlled scan errors. Dynamic filters for `numeric` must
  hash a canonical decimal value, not the raw scaled Decimal128 integer, so
  equal finite values match across different non-negative Decimal128 scales.
- Result schema normalization must preserve finite PostgreSQL interval outputs
  as Arrow `Interval(MonthDayNano)` so worker result pages and `slot_import`
  agree on the `INTERVALOID` projection path.
- `pg/df_functions` `avg` has two compatibility tiers. `avg(float4/float8)`
  returns Arrow `Float64` and preserves PostgreSQL-facing `NaN`/`Infinity`
  behavior. Its finite transition and merge paths track PostgreSQL-style
  Youngs-Cramer `Sxx` state so finite `Sx` or `Sxx` overflow fails with a
  controlled error; the DataFusion window inverse path is still pg_fusion-owned
  because PostgreSQL does not declare a moving inverse for float avg. Integer
  and finite `numeric` averages use the fast Arrow `Decimal128(38,16)` result
  path; this intentionally does not model PostgreSQL numeric's value-dependent
  display scale or arbitrary
  precision. PostgreSQL `numeric` `NaN`/`Infinity` cannot be represented in
  Arrow decimal arrays; known special numeric constants and literal numeric
  casts must fail with a controlled pg_fusion error before worker-side
  Decimal128 aggregation. Finite `avg(interval)` uses Arrow
  `Interval(MonthDayNano)` with PostgreSQL-compatible finite division cascade;
  PostgreSQL interval infinities must fail with controlled errors because Arrow
  has no interval special values. `avg(DISTINCT)` uses mergeable `AHashSet`
  accumulators whose partial state is a list of distinct finite input values;
  float distinct keys collapse all `NaN` values and treat `+0.0`/`-0.0` as one
  key. Document accepted
  `Decimal128` differences in
  `pg/extension/pg_compat/limitations.sql`, not in the passing corpus.
- pg_fusion planning disables DataFusion's
  `single_distinct_aggregation_to_group_by` rewrite. That rule delegates
  distinctness to DataFusion grouping keys and can bypass PostgreSQL-compatible
  aggregate distinct semantics, notably `avg(DISTINCT float8)` handling of
  signed zero.
- `pg/df_functions` aggregate overrides that can appear in DataFusion window
  frames must keep `update_batch` and `retract_batch` symmetric. Sliding frames
  such as `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` move their start
  bound and DataFusion will call the inverse transition.

4. PostgreSQL owns physical table access.

- The active runtime path scans through PostgreSQL `slot_scan`; worker code must
  not reimplement heap visibility, tuple decoding, or TOAST semantics.

5. Lock-free ring buffers require aligned allocation.

- Construct shared-memory rings through `control_transport`, `page/pool`, or
  the approved `lockfree` layout helpers so atomic head/tail words are aligned.
