SELECT
    custsale.cntrycode,
    count(*) AS numcust,
    sum(custsale.c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c.c_phone FROM 1 FOR 2) AS cntrycode,
        c.c_acctbal
    FROM customer c
    WHERE substring(c.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c.c_acctbal > (
          SELECT avg(c2.c_acctbal)
          FROM customer c2
          WHERE c2.c_acctbal > 0.00
            AND substring(c2.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM orders o
          WHERE o.o_custkey = c.c_custkey
      )
) custsale
GROUP BY custsale.cntrycode
ORDER BY custsale.cntrycode;
