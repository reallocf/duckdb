with select1 as (
    SELECT customer.rowid as customer_rowid_0, c_acctbal
    FROM customer
    WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
), avg_c_acctbal as (
    select avg(c_acctbal) from select1
), joins1 as (
    SELECT customer.rowid as customer_rowid_1, substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
    FROM customer
    WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
          AND c_acctbal > (SELECT * from avg_c_acctbal)
          AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
), groups as (
  SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
  FROM joins1
  GROUP BY cntrycode
  ORDER BY cntrycode
), end_to_end as (
  select groups.*, customer_rowid_1, customer_rowid_0
  from groups join joins1 using (cntrycode),
    (select group_concat(customer_rowid_0) as customer_rowid_0
      from select1, joins1 where joins1.c_acctbal > (SELECT * from avg_c_acctbal))
)

select * from end_to_end
