# Memory And Pages

[Documentation home](index.md)

This page describes the shared page pool, the format of one Arrow block, and
where pg_fusion can and cannot stay zero-copy.

## Page Pool

The shared page pool is a fixed-size pool of equal-size pages allocated in
PostgreSQL shared memory at startup. It is a transport area, not a cache of
PostgreSQL table data.

A page moves through a simple ownership cycle:

```text
Free -> producer acquires -> producer writes block
     -> descriptor is sent -> consumer imports block
     -> last owner releases -> Free
```

The same page can later be reused for another scan page or result page. The page
generation and pool identity checks prevent stale descriptors from becoming
valid again.

Page count and page size are configured up front. When producers run out of free
pages or scan channels fill, they wait instead of allocating unbounded memory.
This backpressure is part of the resource model.

## One Block

Each page payload contains one `arrow_layout` block. The block is designed so it
can become Arrow arrays without first building a separate staging batch.

The layout is front-and-tail:

```text
+----------------------------+
| BlockHeader                |
| ColumnDesc[0..n]           |
| fixed values / validity    |
| ByteView slots             |
| ... free space ...         |
| long string/binary payload |
+----------------------------+
```

- `BlockHeader` records row count, column count, and layout offsets.
- `ColumnDesc[]` describes each column's type tag, nullability, validity buffer,
  and value buffer.
- Fixed-width columns store values in the front region.
- Validity bitmaps store null information.
- Text and binary columns use Arrow view types: `Utf8View` and `BinaryView`.
- Short view values can be inline in a `ByteView` slot.
- Long view values live in a shared tail arena that grows backward from the end
  of the page.

The format is same-host and native-endian. It is a shared-memory execution
format, not a portable file or network format.

## Writing Scan Pages

PostgreSQL scan producers read rows through PostgreSQL executor paths. Each
producer owns a PostgreSQL `TupleTableSlot` stream and writes selected columns
into an initialized block.

For eligible parallel heap scans, each CTID-range producer writes its own pages
into the shared page pool. The scan-production lifecycle is described in
[Execution Model](execution-model.md#scan-production).

## Zero-Copy Import

When the worker receives a page descriptor, `page/import` validates the block
against the expected Arrow schema and creates Arrow buffers over the page bytes.
The imported `RecordBatch` keeps the page lease alive through Arrow buffer
ownership.

As long as some Arrow array still references the page, that page cannot return
to the pool. When the last Arrow owner drops, the page is released and can be
reused.

This is the zero-copy fast path for scan-adjacent streaming execution: scan,
filter, projection, limit, coalescing, repartition, and plain aggregate paths
can consume page-backed batches without copying the underlying buffers.

## Why Some Operators Copy

Some DataFusion operators retain input batches after the immediate poll. Those
operators cannot safely hold shared page leases forever, because that would
starve the page pool and block unrelated producers.

pg_fusion inserts materialization before retaining boundaries such as:

- hash join build sides;
- sort and window inputs;
- multi-use CTE materialization.

At those boundaries, page-backed Arrow arrays are copied into ordinary owned
Arrow buffers. For `Utf8View` and `BinaryView`, this must be a deep copy of long
payload bytes, not just a clone of view slots. After the copy, the original page
can return to the pool when streaming consumers drop it.

The point is not "copy never". The point is "copy only where retention requires
owned memory".

## Result Pages

Worker result batches are encoded back into the same block format and sent
through the shared page pool. The backend imports result pages and projects
values into PostgreSQL tuple slots.

Some result projection still copies into PostgreSQL memory contexts because
PostgreSQL result slots need PostgreSQL-owned datums. For example, text-like
values and `bytea` are rebuilt in PostgreSQL memory. The page lifetime still
follows the same rule: page-backed arrays keep the page leased until released.

## Progress, Not Fairness

The runtime divides resources by purpose: primary control rings, scan rings,
page pool, issued-page permits, runtime filter slots, worker memory, and spill.
This separation is meant to keep the system bounded and able to make progress
under backpressure.

It is not a strict fairness model. Exact fairness is not realistic because query
shapes, PostgreSQL scheduling, CTID range sizes, DataFusion operator behavior,
runtime filter timing, and spill behavior are all different. pg_fusion aims for
bounded resource ownership and forward progress, not equal service for every
query or producer.
