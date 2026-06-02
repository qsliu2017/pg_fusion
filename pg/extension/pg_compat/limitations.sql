-- Known semantic differences that are intentionally not part of the passing
-- corpus. These queries are executable, but pg_fusion does not promise exact
-- byte-for-byte PostgreSQL output for them yet.

-- id: window_35_select_avg_four_over_partition_by_four_order_by_thousand_100_from_tenk1__c0d80dfa
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:104
-- compare: ordered
-- reason: pg_fusion computes integer avg through Decimal128(38,16), while PostgreSQL numeric keeps value-dependent display scale.
SELECT avg(four) OVER (PARTITION BY four ORDER BY thousand / 100) FROM tenk1 WHERE unique2 < 10;

-- id: local_avg_integer_repeating_decimal_precision
-- origin: local pg_fusion avg compatibility limitation
-- compare: ordered
-- reason: pg_fusion rounds integer avg to Decimal128(38,16); PostgreSQL numeric can display more fractional digits for repeating decimals.
SELECT avg(v) FROM (VALUES (1), (0), (0)) AS input(v);

-- id: local_float_avg_special_cast_to_text_format
-- origin: local pg_fusion avg compatibility limitation
-- compare: ordered
-- reason: DataFusion formats Float64 Infinity as inf when the cast to text is planned inside DataFusion; PostgreSQL float8 output uses Infinity.
SELECT avg('Infinity'::float8)::text;

-- id: local_pg_scalar_subquery_zero_rows_is_null
-- origin: local pg_fusion scalar subquery compatibility limitation
-- compare: multiset
-- reason: zero-row scalar subqueries preserve execution semantics, but this query shape currently exposes a plan-codec decode issue during EXPLAIN.
SELECT unique1
FROM (VALUES (0), (1), (2)) AS t(unique1)
WHERE unique1 < 3
  AND (SELECT v FROM (SELECT 1 WHERE false) AS s(v)) IS NULL;
