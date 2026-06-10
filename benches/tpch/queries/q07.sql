SELECT
    shipping.supp_nation,
    shipping.cust_nation,
    shipping.l_year,
    sum(shipping.volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l.l_shipdate) AS l_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume
    FROM supplier s
    JOIN lineitem l ON s.s_suppkey = l.l_suppkey
    JOIN orders o ON o.o_orderkey = l.l_orderkey
    JOIN customer c ON c.c_custkey = o.o_custkey
    JOIN nation n1 ON s.s_nationkey = n1.n_nationkey
    JOIN nation n2 ON c.c_nationkey = n2.n_nationkey
    WHERE (
          (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
       OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    )
      AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) shipping
GROUP BY shipping.supp_nation, shipping.cust_nation, shipping.l_year
ORDER BY shipping.supp_nation, shipping.cust_nation, shipping.l_year;
