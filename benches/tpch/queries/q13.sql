SELECT
    c_orders.c_count,
    count(*) AS custdist
FROM (
    SELECT
        c.c_custkey,
        count(o.o_orderkey) AS c_count
    FROM customer c
    LEFT JOIN orders o
      ON c.c_custkey = o.o_custkey
     AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) c_orders
GROUP BY c_orders.c_count
ORDER BY custdist DESC, c_orders.c_count DESC;
