# Benchmarks

[Documentation home](index.md)

`pg_fusion` includes local diagnostic benchmarks. They are for engineering
evaluation, not audited TPC-H publication.

Benchmark results should be read together with query plans and metrics. A
pg_fusion query can be slower if scan, tuple decoding, Arrow encoding, or page
transport dominates the DataFusion-side work.

## TPC-H Diagnostic Harness

The harness lives in:

```text
benches/tpch/
```

It compares vanilla PostgreSQL with `pg_fusion` for selected query shapes.

## Prerequisites

```sh
cargo install tpchgen-cli
cargo build -p pg_fusion
cargo pgrx start pg17
```

If needed, install the extension into the pgrx cluster:

```sh
cargo pgrx run pg17 -p pg_fusion
```

## Quick Run

From the repository root:

```sh
python3 benches/tpch/scripts/tpch_bench.py \
  --dbname pg_fusion \
  --scale-factor 0.01 \
  --runs 3 \
  --warmup 1
```

By default, the script:

1. generates CSV data;
2. recreates the benchmark schema;
3. loads TPC-H tables;
4. runs each query with `pg_fusion.enable = off`;
5. runs each query with `pg_fusion.enable = on`;
6. writes CSV and JSON summaries.

## Reuse An Existing Schema

```sh
python3 benches/tpch/scripts/tpch_bench.py \
  --dbname pg_fusion \
  --no-prepare \
  --queries q01,q03,q06
```

## Result Statuses

- `ok`: PostgreSQL and pg_fusion both succeeded and returned matching rows.
- `mismatch`: both succeeded but output rows differed beyond tolerance.
- `fusion_fail`: PostgreSQL succeeded but pg_fusion failed.
- `pg_fail`: PostgreSQL failed, so the comparison is invalid.

## Latest Checked-In SF1 Snapshot

The latest checked-in SF1 summary is
`benches/tpch/results/tpch_sf_1_20260518T132909Z.csv`. It contains 19 `ok`
queries with matching PostgreSQL and pg_fusion results.

The ratio column is `fusion_median_ms / pg_median_ms`, so lower is better for
pg_fusion. This snapshot uses a +/-10% bucket for "about the same". The faster
numbers below are speedups; the slower numbers are slowdowns.

| Bucket | Queries |
| --- | --- |
| Faster | `q01` 1.19x, `q02` 1.78x, `q04` 1.25x, `q09` 1.85x, `q13` 3.03x, `q16` 2.28x, `q18` 2.93x |
| About the same | `q06` 1.01x |
| Slower | `q03` 1.23x, `q05` 1.81x, `q07` 1.42x, `q08` 1.52x, `q10` 1.23x, `q11` 1.14x, `q12` 2.68x, `q14` 1.57x, `q15` 1.53x, `q19` 1.33x, `q22` 1.44x |

Recent SF1 observations also show `q20` and `q21` as orders-of-magnitude
pg_fusion wins. They are not included in the table above because this checked-in
SF1 summary does not contain `q20` or `q21` rows, so exact timings are omitted
here.

## Interpreting Results

Do not look only at absolute timings. PostgreSQL scan timing can vary with data
cache state and background system activity. Compare ratios across repeated runs
and inspect plans.

If pg_fusion is unexpectedly slow, rerun the query manually with metrics:

```sql
SELECT pg_fusion_metrics_reset();
SET pg_fusion.enable = on;
SELECT ...;

SELECT component, metric, value, unit
FROM pg_fusion_metrics()
WHERE value <> 0
ORDER BY component, metric;
```

Common explanations:

- `scan_page_fill_ns` dominates: PostgreSQL scan and Arrow encoding are the
  main cost.
- `scan_rows_encoded_total` is high: filters or runtime filters did not reduce
  enough rows before encoding.
- `scan_bytes_sent_total` is high: projection may be too wide.
- `scan_batch_send_ns` is high: DataFusion is applying backpressure to scan
  streams.
- result page metrics are high: the query returns a large result set to
  PostgreSQL.

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

Queries with subqueries or CTEs are useful for exposing current planner
limitations.

## Q05 Encoder Microbenchmark

To isolate PostgreSQL-free Rust page encoding work, run the Criterion
benchmark:

```sh
PG_FUSION_TPCH_DIR=benches/tpch/data/sf_0_01 \
  cargo bench -p row_encoder --bench q05_encode
```
