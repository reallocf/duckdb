with exists_q as (
    SELECT
      l_orderkey, l_suppkey
    FROM
      lineitem l2
    group by l_orderkey, l_suppkey
), not_exists as (
    SELECT
      l_orderkey, l_suppkey
    FROM
        lineitem l3
    WHERE l3.l_receiptdate > l3.l_commitdate
    GROUP BY l_orderkey, l_suppkey
), joins as (
  SELECT
      supplier.rowid as supplier_rowid,
      l1.rowid as l1_rowid,
      orders.rowid as orders_rowid,
      nation.rowid as nation_rowid,
      s_name,
      s_suppkey,
      o_orderkey
  FROM
      supplier,
      lineitem l1,
      orders,
      nation
  WHERE
      s_suppkey = l1.l_suppkey
      AND o_orderkey = l1.l_orderkey
      AND o_orderstatus = 'F'
      AND l1.l_receiptdate > l1.l_commitdate
      AND EXISTS (
        SELECT
            *
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
      AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
      AND s_nationkey = n_nationkey
      AND n_name = 'SAUDI ARABIA'
), groups as (
  SELECT
      s_name,
      count(*) AS numwait
  FROM joins
  GROUP BY
      s_name
  ORDER BY
      numwait DESC,
      s_name
  LIMIT 100
), out_count as (
select 2706
), end_to_end_no_exists as (
  select * from groups join joins using (s_name)
), end_to_end_exists as (
  select * from groups join joins using (s_name), exists_q
  where exists_q.l_orderkey = o_orderkey
        AND exists_q.l_suppkey <> s_suppkey
)

