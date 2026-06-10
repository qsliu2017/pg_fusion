SELECT
    profit.nation,
    profit.o_year,
    sum(profit.amount) AS sum_profit
FROM (
    SELECT
        n.n_name AS nation,
        extract(year FROM o.o_orderdate) AS o_year,
        l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity AS amount
    FROM part p
    JOIN lineitem l ON p.p_partkey = l.l_partkey
    JOIN partsupp ps ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey
    JOIN orders o ON o.o_orderkey = l.l_orderkey
    JOIN supplier s ON s.s_suppkey = l.l_suppkey
    JOIN nation n ON s.s_nationkey = n.n_nationkey
    WHERE p.p_name LIKE '%green%'
) profit
GROUP BY profit.nation, profit.o_year
ORDER BY profit.nation, profit.o_year DESC;
