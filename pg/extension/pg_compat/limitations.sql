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

-- id: aggregates_5_select_avg_four_as_avg_1_from_onek_a38cdb1e
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:20
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT avg(four) AS avg_1 FROM onek;

-- id: aggregates_6_select_avg_a_as_avg_32_from_aggtest_where_a_100_6adf2685
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:23
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- id: memoize_5_select_count_avg_t1_unique1_from_tenk1_t1_inner_join_tenk1_t2_on_t1_uniq_0538405e
-- origin: postgres REL_17_STABLE src/test/regress/sql/memoize.sql:40
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT COUNT(*),AVG(t1.unique1) FROM tenk1 t1
INNER JOIN tenk1 t2 ON t1.unique1 = t2.twenty
WHERE t2.unique1 < 1000;

-- id: select_parallel_147_select_avg_unique1_int8_from_tenk1_e045371e
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:327
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
select avg(unique1::int8) from tenk1;

-- id: window_337_select_i_avg_v_bigint_over_order_by_i_rows_between_current_row_and_unbou_158cce69
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1754
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric window avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT i,AVG(v::bigint) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_338_select_i_avg_v_int_over_order_by_i_rows_between_current_row_and_unbounde_32a66afa
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1762
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric window avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT i,AVG(v::int) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_339_select_i_avg_v_smallint_over_order_by_i_rows_between_current_row_and_unb_3da4b12c
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1765
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric window avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT i,AVG(v::smallint) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_340_select_i_avg_v_numeric_over_order_by_i_rows_between_current_row_and_unbo_e2759460
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1768
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric window avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT i,AVG(v::numeric) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1.5),(2,2.5),(3,NULL),(4,NULL)) t(i,v);

-- id: local_pg_avg_distinct_int
-- origin: local pg_fusion avg distinct compatibility
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT avg(DISTINCT v::int) FROM (VALUES (1),(2),(2),(NULL)) t(v);

-- id: local_pg_avg_distinct_numeric
-- origin: local pg_fusion avg distinct compatibility
-- compare: multiset
-- reason: pg_fusion canonicalizes bare numeric avg output by trimming trailing fractional zeros; PostgreSQL preserves the numeric result dscale.
SELECT avg(DISTINCT v::numeric) FROM (VALUES (1.5),(1.5),(2.5),(NULL)) t(v);

-- id: local_bare_numeric_literal_display_scale
-- origin: local pg_fusion numeric display-scale limitation
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric output by trimming trailing fractional zeros; PostgreSQL preserves per-value numeric dscale.
SELECT 1.20::numeric;

-- id: local_bare_numeric_arithmetic_display_scale
-- origin: local pg_fusion numeric display-scale limitation
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric output by trimming trailing fractional zeros; PostgreSQL preserves per-value numeric dscale.
SELECT 1.20::numeric + 3.00::numeric;

-- id: local_bare_numeric_values_mixed_display_scale
-- origin: local pg_fusion numeric display-scale limitation
-- compare: ordered
-- reason: pg_fusion canonicalizes bare numeric output by trimming trailing fractional zeros; PostgreSQL preserves per-value numeric dscale.
SELECT * FROM (VALUES (1.2::numeric), (1.20::numeric)) AS v(x);

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
