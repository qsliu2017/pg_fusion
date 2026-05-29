---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc", "slot_scan", "statistics"]
updated_at: "2026-05-07"
importance: 0.8
---

# pg_fusion Architecture Overview

The active PostgreSQL extension is `pg/extension` (`pg_fusion`). It
hooks PostgreSQL planning/execution and delegates selected query work to an
in-process DataFusion worker runtime through shared-memory control rings and
page-backed Arrow batches.

## Top-Level Runtime Path

- `pg/extension/`: pgrx host extension, GUCs, planner/custom-scan hooks,
  shared-memory bootstrap, and background worker entrypoint.
- `pg/backend_service/`: backend execution state, scan coordination, and
  PostgreSQL slot-scan page production.
- `runtime/worker`: worker-side DataFusion runtime, physical scan integration,
  and result page production.
- `runtime/protocol`: typed backend/worker control-plane messages.
- `runtime/control_transport`: shared-memory control rings and backend/worker
  leases.
- `runtime/filter`: shared-memory friendly runtime filters. The Bloom bitset
  layer is separate from the shared-memory pool/lifecycle layer: builders
  acquire an exclusive generation before clearing/inserting, publish `Ready`
  with a CAS, and probes only reject rows for a matching ready generation.
  Stale or in-progress generations pass rows unfiltered. Pool slots use
  reference counts so ready storage is reused only after the owner and all
  probes have exited, preserving the no-false-negative property.
- `page/pool`, `page/transfer`, `page/issuance`: fixed-page ownership,
  transfer, and issued-frame flow.
- `runtime/metrics`: shared-memory runtime counters and page-slot handoff
  stamps exposed through SQL.
- `page/arrow_layout`, `page/row_encoder`, `page/import`,
  `pg/slot_encoder`, `pg/slot_import`: page-backed Arrow layout,
  PostgreSQL-free row-to-page encoding, PostgreSQL slot adaptation, and result
  projection.
- `pg/type`, `pg/df_catalog`, `pg/frontend`, `pg/plan_builder`, `pg/scan_node`,
  `pg/scan_sql`, `pg/slot_scan`: backend-side DataFusion planning and trusted
  PostgreSQL scan SQL execution. `pg/type` owns the shared PostgreSQL type
  policy for supported OIDs, typmods, collations, Arrow transport types,
  page-layout type tags, typed literal metadata, and typed NULL/constant scalar
  conversion. PostgreSQL-bound crates still own raw `Datum`, TOAST, varlena,
  memory-context, and tuple-slot mechanics. `pg/frontend` is the typed
  PostgreSQL `Query` tree frontend; it copies analyzed PostgreSQL type metadata
  into a stable IR and plans the supported fail-closed subset as DataFusion
  logical plans with resolved PostgreSQL table-source leaves. The host-side
  frontend pipeline builds those leaves into PostgreSQL scan nodes before
  generic DataFusion optimization can rewrite PostgreSQL-semantic scan
  predicates. The production planner hook can try this frontend first behind
  `pg_fusion.frontend_mode`; unsupported frontend shapes fall back to the
  SQL-text `plan_builder` path unless the mode is `require`.
- `pg/df_functions`: PostgreSQL-compatible DataFusion function overrides used
  by both backend planning and worker/codec decoding. Its `format(text, ...)`
  scalar UDF supports PostgreSQL `%s`/`%I`/`%L`, argument positions, and width
  handling for ordinary non-`VARIADIC ARRAY` calls. Its `avg` UDAF returns
  `Float64` for `float4`/`float8` inputs with PostgreSQL-facing
  `NaN`/`Infinity` and finite-overflow behavior, while integer and finite
  Decimal128 averages use the fast Arrow `Decimal128(38,16)` result path.
  Finite `interval` averages use Arrow `Interval(MonthDayNano)` and mirror
  PostgreSQL's month/day/time division cascade; PostgreSQL interval infinities
  are rejected because Arrow intervals have no special values. The decimal path
  is intentionally a pragmatic `numeric` subset with documented
  precision/display-scale limitations, and all `avg` accumulators implement
  inverse transitions so bounded/sliding window frames can retract rows during
  DataFusion window execution. `quote_literal(text)` shares the same text
  quoting helper as `format(... %L ...)`.
- `pg/statistics`: PostgreSQL planner/catalog statistics bridge. It is
  PostgreSQL-specific but independent of DataFusion and `join_order`;
  `plan_builder` uses it to turn pushed-down scan SQL, `pg_class`,
  `pg_statistic`, and unique indexes into compact relation/join estimates. It
  reports only relation-wide unique keys; partial unique indexes are skipped
  until predicate implication is modeled explicitly.
- `join_order`: standalone compact join-order optimizer core. It has no
  DataFusion or PostgreSQL dependency; `plan_builder` provides filtered
  relation statistics, join edges, and opaque predicate handles.

## Data Path

1. Backend planning first keeps PostgreSQL-owned service paths out of
   `pg_fusion`: the planner hook bypasses DataFusion planning for queries with
   bind parameters, relations in `pg_catalog`/TOAST namespaces, or
   function/table-function range entries produced by PostgreSQL rewrite, then
   deparses wrapper query strings such as `EXPLAIN` and `COPY (SELECT ...)` back
   to the inner `Query` text so PostgreSQL can keep native wrapper execution
   around a pg_fusion custom scan. The typed `pg_frontend` path can compile a
   supported analyzed `Query` tree into a DataFusion logical plan without SQL
   re-parsing, then hands that plan to the frontend scan-building pipeline so
   PostgreSQL-typed predicates reach `scan_sql` before generic DataFusion
   rewrites. The SQL-text fallback path parses eligible user query text with
   sqlparser's PostgreSQL dialect before DataFusion planning; deparsed casts to
   PostgreSQL's `unknown` pseudo-type are stripped so untyped NULL/string
   literals can still be coerced by DataFusion function planning. This accepts
   more PostgreSQL surface syntax but does not make unsupported PostgreSQL
   semantics executable. Both paths resolve PostgreSQL catalog metadata for
   eligible user queries. The SQL-text path runs DataFusion logical
   optimization, then uses
   `pg_statistics` plus `join_order` to reorder eligible inner/cross join
   components before scan building. The reorder pass
   estimates each PostgreSQL leaf from the same pushed-down scan SQL that will
   later become a scan descriptor, maps join columns back to PostgreSQL attnums,
   and uses NDV/null fractions plus relation-wide unique keys for equi-join
   selectivity. The `join_order` solution also chooses the hash build side for
   each binary join; `plan_builder` emits that side as the DataFusion left
   child because `CollectLeft` hash joins build the left input, then restores
   the original visible output order with a projection when needed. Ineligible
   join shapes keep their DataFusion order. PostgreSQL-compatible function
   overrides are registered before SQL planning, logical optimization, plan
   codec decoding, worker physical planning, and EXPLAIN physical planning; in
   particular ordinary `format(text, ...)` and `quote_literal(text)` calls
   execute through pg_fusion, PostgreSQL aliases such as `ceiling` and
   `variance` resolve to DataFusion's existing `ceil` and `var_samp`
   implementations, `float4`/`float8` `avg` keeps float semantics, integer
   and finite Decimal128 `avg` are planned as `Decimal128(38,16)`, and finite
   `interval` `avg` stays as `Interval(MonthDayNano)` end to end. PostgreSQL `numeric`
   `NaN`/`Infinity` constants and literal numeric casts are rejected before
   they can enter Arrow Decimal128 execution, interval infinities are rejected
   before/while converting through Arrow interval pages, and accepted decimal
   formatting/precision differences live in
   `pg/extension/pg_compat/limitations.sql`. Root `UInt64` and `LargeUtf8`
   outputs are cast to PostgreSQL-facing `bigint`/`text` Arrow types before
   result transport. Scan leaves are then built as
   `PgScanNode`/`scan_sql` descriptors. For the subset supported by
   `pg_frontend`, the planner stores a serialized typed `PgQuery` IR in
   `CustomScan.custom_private`; `BeginCustomScan` recompiles that IR and builds
   scan leaves without SQL re-parsing. The SQL-text
   `plan_builder` path remains the fallback for broader query coverage.
   Non-recursive CTEs
   referenced more than once are planned as `PgCteRefNode` reads over a single
   built CTE producer so worker execution materializes the CTE once and
   reuses the owned batches. PostgreSQL text-like columns are represented as
   Arrow `Utf8View` in the DataFusion logical schema so scan pages can stay
   zero-copy for string payloads.
2. Worker physical planning attaches runtime Bloom filters to eligible
   `HashJoinExec` nodes unless `pg_fusion.runtime_filter_enable` is disabled.
   The v1 path is intentionally narrow: one `Inner` hash join equi-key, `Column =
   Column`, single-partition build side, supported key type (`bool`,
   `int2`/`int4`/`int8`, `float4`/`float8`, `date`, `time`, `timestamp`,
   `timestamptz`, finite `interval` as `Interval(MonthDayNano)`, `uuid`,
   finite `numeric` as `Decimal128`, `bytea` as `BinaryView`, or text-like `Utf8View` from
   `text`/`varchar`/`bpchar`/`name`), and a `WorkerPgScanExec` on the
   probe side, possibly below DataFusion's
   schema-preserving `CooperativeExec` wrapper. The worker registers the target
   by `(session_epoch,
   scan_id, output_column)` in shared memory, fills the filter while consuming
   the build side, and publishes it when that stream reaches EOF. If the pool
   is full the join runs unchanged and increments a diagnostic counter.
3. Worker DataFusion execution opens scans through the runtime protocol.
4. Backend executes trusted scan SQL through `slot_scan`, drains PostgreSQL
   executor slots with a custom `DestReceiver` and explicit fetch row budgets,
   optionally applies attached runtime filters before slot-to-Arrow encoding,
   encodes surviving `TupleTableSlot` rows into initialized `arrow_layout`
   pages with `slot_encoder`, and sends issued pages to the worker. The filter
   key is deformed once and the same deformed slot is then reused by
   `slot_encoder`. Each scan always has a
   leader backend producer. PostgreSQL `max_parallel_workers_per_gather` is a
   query-wide budget for additional dynamic background-worker producers across
   eligible heap scans, capped at `32` and bounded by `max_worker_processes`;
   each producer owns a dedicated scan control slot and writes its own Arrow
   pages into shared memory.
5. Worker imports scan pages as Arrow `RecordBatch` values, runs DataFusion
   operators under its current-thread Tokio runtime, writes Arrow result pages,
   and sends issued frames back. Zero-row plans whose DataFusion stream has an
   empty schema, such as `EmptyExec`, use a close-only result path with no Arrow
   pages. Row-count-only PostgreSQL scans use dummy SQL projection in
   PostgreSQL but transfer non-empty empty-schema Arrow pages; these scans stay
   leader-only because dynamic scan workers require projected base-relation
   columns. Scan production remains on PostgreSQL backend/scan-worker threads;
   Tokio only drives DataFusion planning, multi-partition root collection, and
   result-stream polling.
   If `pg_fusion.worker_memory_limit_mb` is positive, execution uses a
   worker-owned DataFusion `RuntimeEnv` with a finite memory pool and
   per-execution OS spill directory below a cluster-scoped worker spill root.
   The worker garbage-collects pg_fusion-marked stale spill directories in that
   cluster namespace at generation startup; disabled spill does not create
   directories or run garbage collection. v1 spill is independent of PostgreSQL
   temp-file accounting; on DataFusion 53 it applies to sort and row-hash
   aggregate spill paths, while ordinary `HashJoinExec` remains non-spilling.
6. Backend imports result pages with `slot_import` and projects rows into
   PostgreSQL tuple slots. Result transport supports Decimal128 fixed-width
   pages for PostgreSQL `numeric` outputs produced by worker-side expressions
   and `Interval(MonthDayNano)` pages for finite PostgreSQL `interval` values.
   Backend heap scans encode the finite PostgreSQL `numeric` subset as
   Decimal128: `numeric(p,s)` uses `Decimal128(p,s)` when `p <= 38` and
   `0 <= s <= p`, while bare `numeric` uses the fixed
   `Decimal128(38,16)` fallback. PostgreSQL `numeric` `NaN`/`Infinity` and
   values outside the selected Decimal128 shape fail during scan encoding.

Page-backed scan batches stay zero-copy through streaming DataFusion operators.
After physical planning, `scan_node` inserts `PageMaterializeExec` only before
operators that can retain input batches beyond immediate streaming, such as
sort/window operators and join build sides. The wrapper copies Arrow arrays into
ordinary allocations at that boundary so shared-memory pages and permits can be
released while preserving zero-copy for simple scans, filters, projections,
limits, and plain aggregates. Multi-use CTE materialization is another owned
boundary: `CteScanExec` deep-copies the producer output once before replaying it
to multiple consumers.

Runtime metrics live in a separate shared-memory region. The runtime does not
wrap control rings for v1 metrics; scan/result page senders stamp page
descriptors, and receivers use those stamps to measure backend-to-worker and
worker-to-backend page handoff latency. Worker scan threads also split
backend-to-worker latency into idle sleeps, page import, and DataFusion scan
channel send/delivery time. Backend scan page timing is always-on and coarse:
`scan_page_fill_ns` covers successful emitted page fills, with cheap
prepare/finish buckets and a retry counter. Slot-to-Arrow serialization
internals are left to external profilers. Runtime filter counters track
allocated/ready filters, pool exhaustion, build rows, probe rows, rejected rows,
and rows that passed unfiltered because the filter was not ready for that probe.

Dynamic scan workers use CTID block-range chunking as the first parallel scan
strategy. The leader backend scans one heap block range, additional dynamic
background workers scan disjoint ranges, and `runtime/worker` fans all producer
streams into one logical `PgScanExec`. The query-wide worker budget is assigned
before execution starts; if PostgreSQL cannot launch more dynamic workers at
runtime, pg_fusion cancels any partially launched producers for that scan and
continues leader-only for that and later scans. Each producer has its own
ordered issued-page receive stream because producer-local page transfer ids
start at `1`. Relations with dropped attributes or unsupported scan shapes stay
on leader-only portal streaming. Dynamic scan worker jobs carry a resolved
standalone scan descriptor for one PostgreSQL leaf scan rather than the original
user SQL, so worker startup does not depend on backend-local `search_path` or
repeat full DataFusion planning. `EXPLAIN` uses the same eligibility and budget
logic to show planned producer counts for every PostgreSQL scan leaf. For CTID
range scans, verbose `EXPLAIN` renders a representative producer SQL and nested
PostgreSQL plan rather than an unchunked leaf plan, but masks the representative
CTID bounds as `$1`/`$2` placeholders in the rendered text. `EXPLAIN ANALYZE`
also reports the producer channels installed for the real execution.

## Retired Legacy Stack

The old raw-heap-page stack has been retired from the workspace: `executor`,
`scan`, `storage`, `protocol`, and `common` are no longer active crates. The
active extension crate now lives at `pg/extension`. `lockfree` remains active
because it underpins the new transport/page stack.
