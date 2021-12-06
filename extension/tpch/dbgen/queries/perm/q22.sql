with select1 as (
    SELECT customer.rowid as customer_rowid, c_acctbal
    FROM
        customer
    WHERE
        c_acctbal > 0.00
        AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
), not_exists as (
  select o_custkey from orders group by o_custkey
), joins1 as (
    SELECT
        customer.rowid as customer_rowid2,
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
                SELECT avg(c_acctbal)
                FROM select1
              )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    not_exists
                WHERE
                    o_custkey = c_custkey)
), groups as (
  SELECT
      cntrycode,
      count(*) AS numcust,
      sum(c_acctbal) AS totacctbal
  FROM joins1
  GROUP BY
      cntrycode
  ORDER BY
      cntrycode
)

select cntrycode, customer_rowid2 from groups join  joins1 using (cntrycode)
