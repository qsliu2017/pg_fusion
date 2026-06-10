SELECT
    all_nations.o_year,
    sum(CASE WHEN all_nations.nation = 'BRAZIL' THEN all_nations.volume ELSE 0 END)
        / sum(all_nations.volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o.o_orderdate) AS o_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume,
        n2.n_name AS nation
    FROM part p
    JOIN lineitem l ON p.p_partkey = l.l_partkey
    JOIN supplier s ON s.s_suppkey = l.l_suppkey
    JOIN orders o ON o.o_orderkey = l.l_orderkey
    JOIN customer c ON c.c_custkey = o.o_custkey
    JOIN nation n1 ON c.c_nationkey = n1.n_nationkey
    JOIN region r ON n1.n_regionkey = r.r_regionkey
    JOIN nation n2 ON s.s_nationkey = n2.n_nationkey
    WHERE r.r_name = 'AMERICA'
      AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p.p_type = 'ECONOMY ANODIZED STEEL'
) all_nations
GROUP BY all_nations.o_year
ORDER BY all_nations.o_year;
