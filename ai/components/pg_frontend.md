---
id: component-pg-frontend-0001
type: fact
scope: component
tags: ["postgres", "datafusion", "planning", "query-tree"]
updated_at: "2026-05-31"
importance: 0.7
---

# pg_frontend

`pg/frontend` is the experimental PostgreSQL typed-tree frontend. It reads a
live analyzed `pg_sys::Query` into a neutral `TypedQuery`, copies PostgreSQL
OID/typmod/collation metadata, resolves catalog metadata in place by
`RTE_RELATION.relid`, and compiles the supported subset into an ordinary
DataFusion `LogicalPlan` with PostgreSQL table-source leaves. The host-side
planning pipeline then builds those leaves into PostgreSQL scan nodes and
normalizes root output types. This keeps `pg_frontend` focused on PostgreSQL
query-tree semantics instead of duplicating scan-building policy.

PostgreSQL major-version layout differences terminate at `adapter/`. Each build
targets exactly one PostgreSQL major selected by Cargo feature; today only
`pg17` is wired. The stable `TypedQuery` model, catalog resolution, and
compiler code must stay free of PostgreSQL-version `cfg`.
The `pg17` adapter is split by PostgreSQL tree concerns (`scope`, `rtable`,
`expr`, `clauses`, `window`, `functions`, `consts`). The compiler is split by
logical-planning concerns (`from`, `subquery`, `aggregate`, `window`,
`projection`, `expr`, `sort`, `join`, and reference/schema helpers); these
submodules are internal organization only and do not define separate public
frontend phases.

The frontend phases are explicit: PostgreSQL query tree -> `TypedQuery` ->
in-place catalog resolution -> DataFusion logical plan -> `plan_builder`
`HybridPlan`. Catalog resolution mutates relation metadata on the existing
`TypedQuery` by filling column names, attnums, PostgreSQL type refs, nullability,
and normalized scan relation identity. Compilation takes a `ResolvedQuery` view
over the same tree, so catalog lookup is not mixed into expression lowering.
`HybridPlan` is the boundary object after frontend compilation: it carries the
DataFusion logical plan plus the PostgreSQL scan plan that the custom scan
leaves reference.

v1 is intentionally fail-closed and is now the production planner path whenever
`pg_fusion.enable = on`. The frontend no longer has a `try` mode and no longer
falls back to PostgreSQL's native planner for user SELECTs. Rejection becomes a
controlled frontend planning error. The supported execution shape includes
no-FROM SELECTs, base relation/VALUES/CTE/subquery range table leaves,
projection/filter expressions, ORDER BY/LIMIT, inner/left/right joins, GROUP BY,
HAVING, grouping sets, full-row `DISTINCT`, PostgreSQL `DISTINCT ON`,
`UNION`/`UNION ALL`, scalar expression subqueries,
predicate `EXISTS`, top-level correlated `IN` lowered to a semi-join,
`count`/`sum`/`avg`/`min`/`max`/statistical aggregates, `string_agg`,
aggregate FILTER clauses, aggregate and rank-like window functions with typed
frame offsets, and scalar functions such as `format`, `quote_literal`,
`concat`, and `concat_ws`. CTE references compile to `PgCteRefNode` so
multi-use CTEs share one materialization identity through the backend plan.
`GROUPING()` remains a DataFusion grouping aggregate until frontend analysis so
DataFusion rewrites it from its hidden grouping-set id; pg_frontend must not
infer grouping bits from nullable grouped output values because real data NULLs
and rollup NULLs are semantically distinct.
Scalar subqueries are wrapped in the internal
`pg_scalar_subquery_value` aggregate before DataFusion sees them as scalar
values or joined bindings, preserving PostgreSQL cardinality semantics:
zero rows become NULL, one row becomes the value, and more than one row raises
the PostgreSQL scalar-subquery error during execution.
Row marks are accepted as read-only query-tree markers; pg_fusion does not
implement PostgreSQL row-lock semantics in custom scans. `ONLY` scans and
parameters still return structured unsupported errors instead of bypassing
pg_fusion. `FETCH ... WITH TIES` also fails closed because a plain DataFusion
`Limit` cannot preserve PostgreSQL peer-row semantics.

Frontend `CustomScan` nodes store a versioned text-safe wrapper around
`plan_codec` bytes in `custom_private`. The active encoded payload contains the
already built DataFusion logical plan plus PostgreSQL scan specs from the
`HybridPlan`; it is not a raw PostgreSQL `Query*` pointer or Rust
`Arc<LogicalPlan>` because PostgreSQL plan nodes can be copied and serialized by
core code. Serialized `TypedQuery` payloads are rejected; the typed model is a
planning-time structure only. `BeginCustomScan` decodes the built plan once,
uses its output schema for result transport, and starts execution with the same
decoded plan. Supported current frontend queries avoid SQL re-parsing, frontend
recompilation, catalog re-resolution, and scan SQL rebuilding during execution
while staying PostgreSQL-plan-node safe.

The design rule is that PostgreSQL analyzed metadata is the boundary source of
truth. DataFusion schema is a transport/execution representation, not authority
for PostgreSQL result OIDs, typmods, collations, or temporal/text/numeric
semantics. The shared PostgreSQL/Arrow type policy lives in `pg/type`;
`pg_frontend` should call that crate for supported type checks, Arrow type
mapping, typed literal metadata, and typed NULL/constant scalar construction.

Shippability distinguishes value types from non-null constant types. Value
types can appear as columns, typed NULLs, and external parameter metadata;
non-null constants are limited to the current `PgConstValue` carriers.
`name` constants decode through fixed-size `NameData`, not varlena conversion,
and `name` values accept PostgreSQL's built-in C collation. Text-like constants
carry PostgreSQL OID/typmod/collation metadata into `scan_sql` so `text`,
`varchar`, `bpchar`, and `name` predicates are rendered with PostgreSQL type
semantics instead of untyped string literals.
Non-null `date`, `timestamp`, and `timestamptz` constants fail closed until
temporal representation is lossless across scan input, DataFusion execution,
and PostgreSQL result import. Non-finite `float4`/`float8` constants are allowed
so PostgreSQL float aggregate semantics can be preserved through DataFusion.
PostgreSQL `numeric` `NaN`/`Infinity` cannot enter Arrow Decimal128 execution:
typed numeric constants and known text/VALUES sources cast to `numeric` fail in
frontend planning before worker execution. Finite `numeric` comparisons across
different typmods are cast to a common Decimal128 shape before DataFusion sees
the predicate.
`TIME '24:00:00'` constants fail closed because scan SQL renders time literals
through interval arithmetic that PostgreSQL normalizes modulo one day.
Operator compilation reads the resolved `OpExpr.opno` from PostgreSQL's syscache
and accepts binary `pg_catalog` comparison, arithmetic, and text-concatenation
operators over supported scalar operand/result types. This keeps user-defined
operators with builtin spellings, mixed-type operators, and PostgreSQL-specific
operator semantics fail-closed until scan SQL can preserve them explicitly.
`int2`/`int4`/`int8` `+`, `-`, and `*` lower to internal checked DataFusion
UDFs instead of DataFusion binary arithmetic so PostgreSQL integer overflow
raises `smallint`/`integer`/`bigint out of range` instead of wrapping.
`varchar(n)` and `bpchar(n)` casts lower to internal text-typmod DataFusion
UDFs so intermediate expressions apply PostgreSQL truncation/padding before
DataFusion compares, filters, sorts, or projects the value. The pg17 adapter
must preserve `exprTypmod` for cast nodes; `TypedQuery` is the authority for
the target PostgreSQL OID/typmod/collation, while Arrow `Utf8View` is only the
transport type.
`bpchar` equality/distinct comparisons lower their operands through an internal
comparison-key UDF that trims PostgreSQL padding spaces before DataFusion
evaluates boolean/null semantics. `length(bpchar)` lowers to an internal UDF
that ignores trailing padding spaces instead of using DataFusion
`character_length` directly.
Scalar function lowering validates PostgreSQL's resolved `FuncExpr.funcid`,
argument OIDs, and result OID before recording a neutral `ScalarFunction`; a
supported spelling with an unsupported overload, such as `length(bytea)`, must
fail closed instead of lowering to a same-named DataFusion function.
`round(numeric, int4)` and `trunc(numeric, int4)` lower to internal Decimal128
UDFs so frontend execution preserves PostgreSQL rounding/truncation direction;
scan pushdown renders the same calls back to PostgreSQL SQL.
Frontend `WHERE` filters are split by top-level `AND` before logical planning.
Relation-local filters are pushed into PostgreSQL scans whenever the current
join tree preserves that relation (`INNER` preserves both sides; `LEFT`/`RIGHT`
only the preserved side; `FULL` neither). Filters that reach `scan_sql` must
fully compile into PostgreSQL scan SQL; scan residuals are rejected before
execution. Residual filters above joins may execute in DataFusion only when
their typed expression is known not to depend on PostgreSQL-specific text-like
semantics; `bpchar` equality/distinct and `length(bpchar)` use PG-aware UDFs,
while `bpchar` ordering, text ordering, regex, unsupported collation residuals,
and uncovered text-sensitive function shapes fail closed.
Target expressions compile in the DataFusion logical plan after PostgreSQL
query-tree analysis has supplied function/operator OIDs and PostgreSQL type
metadata.
