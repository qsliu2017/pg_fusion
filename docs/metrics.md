# Metrics

`pg_fusion` exposes cumulative runtime counters from shared memory through SQL.
Use them to answer practical questions: did the query spend time scanning,
encoding, waiting on the worker, spilling, or sending results back?

## Reset, Run, Read

```sql
SELECT pg_fusion_metrics_reset();

SET pg_fusion.enable = on;
SELECT ...;

SELECT component, metric, kind, unit, value, reset_epoch
FROM pg_fusion_metrics()
WHERE value <> 0
ORDER BY component, metric;
```

`pg_fusion_metrics()` returns rows, not JSON, so metrics can be filtered with
ordinary SQL.

Timer totals can overlap. For example, backend scan time and worker execution
time can run concurrently, so do not expect:

```text
backend_total_ns + worker_total_ns == query_total_ns
```

## Is Scan Encoding Expensive?

Start with scan page counters:

```sql
SELECT metric, value
FROM pg_fusion_metrics()
WHERE component = 'scan'
  AND metric IN (
    'scan_page_fill_ns',
    'scan_fetch_calls_total',
    'scan_rows_encoded_total',
    'scan_pages_sent_total',
    'scan_bytes_sent_total',
    'scan_full_pages_total',
    'scan_eof_pages_total'
  )
ORDER BY metric;
```

`scan_page_fill_ns` is the coarse backend timer for successful scan pages. It
includes PostgreSQL scan output, slot work, tuple-to-Arrow encoding, page
layout, and row-estimator work.

If this dominates, the query may be paying mostly for PostgreSQL scanning and
Arrow conversion. This is common when many rows or wide columns are sent to the
worker.

## Is The Worker Backpressured?

```sql
SELECT metric, value
FROM pg_fusion_metrics()
WHERE metric IN (
  'scan_b2w_wait_ns',
  'scan_page_read_ns',
  'scan_batch_send_ns',
  'scan_batch_delivery_ns',
  'scan_idle_sleep_ns',
  'worker_total_ns'
)
ORDER BY metric;
```

High `scan_batch_send_ns` means the worker scan path spent time handing batches
to DataFusion. This often means downstream DataFusion operators were not
polling the scan stream quickly.

High `scan_b2w_wait_ns` with low `scan_batch_send_ns` points more toward page
handoff or scan wakeup scheduling.

## Are Results Expensive?

```sql
SELECT metric, value
FROM pg_fusion_metrics()
WHERE metric IN (
  'worker_result_page_fill_ns',
  'worker_result_pages_total',
  'worker_result_bytes_sent_total',
  'result_w2b_wait_ns',
  'result_page_read_ns',
  'result_pages_read_total',
  'backend_rows_returned_total'
)
ORDER BY metric;
```

Large result-page counts or bytes mean the query returns a lot of data to
PostgreSQL. pg_fusion is usually more interesting when DataFusion reduces data
before it returns to the backend.

## Are Runtime Filters Helping?

```sql
SELECT metric, value
FROM pg_fusion_metrics()
WHERE component = 'runtime_filter'
ORDER BY metric;
```

Useful fields include:

- `runtime_filter_allocated_total`;
- `runtime_filter_ready_total`;
- `runtime_filter_pool_exhausted_total`;
- `runtime_filter_build_rows_total`;
- `runtime_filter_probe_rows_total`;
- `runtime_filter_probe_rows_rejected_total`;
- `runtime_filter_probe_pass_unfiltered_total`.

Rejected probe rows were skipped before scan encoding. Pass-unfiltered rows
were encoded because no ready filter was available for that probe.

## Did Spill Happen?

Worker spill is disabled when:

```conf
pg_fusion.worker_memory_limit_mb = 0
```

When spill is enabled, inspect:

```sql
SELECT metric, value
FROM pg_fusion_metrics()
WHERE metric LIKE 'worker_spill%'
ORDER BY metric;
```

Important spill metrics:

- `worker_spill_count_total`;
- `worker_spilled_rows_total`;
- `worker_spilled_bytes_total`;
- `worker_spill_dirs_created_total`;
- `worker_spill_dirs_removed_total`;
- `worker_spill_cleanup_errors_total`;
- `worker_spill_leaked_files_total`;
- `worker_spill_leaked_bytes_total`.

Leak counters should stay zero after execution. Cleanup errors should also stay
zero.

## Useful One-Page Diagnostic Query

```sql
SELECT component, metric, value, unit
FROM pg_fusion_metrics()
WHERE value <> 0
  AND metric IN (
    'query_total_ns',
    'backend_total_ns',
    'worker_total_ns',
    'scan_page_fill_ns',
    'scan_rows_encoded_total',
    'scan_pages_sent_total',
    'scan_bytes_sent_total',
    'scan_batch_send_ns',
    'worker_result_page_fill_ns',
    'result_page_read_ns',
    'worker_spill_count_total',
    'runtime_filter_probe_rows_rejected_total'
  )
ORDER BY component, metric;
```
