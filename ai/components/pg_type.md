---
id: component-pg-type-0001
type: fact
scope: component
tags: ["postgres", "arrow", "types", "transport"]
updated_at: "2026-05-28"
importance: 0.8
---

# pg_type

`pg/type` is the shared PostgreSQL type-policy crate for pg_fusion. It is the
source of truth for the supported PostgreSQL OID surface, typmod/collation
checks, PostgreSQL-to-Arrow transport mapping, page-layout `TypeTag` mapping,
and Arrow transport schema normalization.

The crate intentionally does not read or write PostgreSQL `Datum` values.
PostgreSQL-bound crates such as `slot_encoder`, `slot_import`, and
`df_catalog` keep ownership of memory contexts, TOAST/detoast, varlena layout,
numeric/interval struct access, and tuple-slot projection.

`timestamp` and `timestamptz` currently share the same Arrow transport type
(`Timestamp(Microsecond, None)`), so callers that render SQL or expose
PostgreSQL result metadata must keep original PostgreSQL type identity when it
matters.
