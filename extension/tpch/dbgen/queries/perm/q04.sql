with exists_st as (
  SELECT
      lineitem.rowid as lineitem_rowid,
      l_orderkey
  FROM
      lineitem
  WHERE l_commitdate < l_receiptdate
), select_st as (
  SELECT
      orders.rowid as orders_rowid,
      o_orderpriority,
      o_orderkey
  FROM
      orders
  WHERE
      o_orderdate >= CAST('1993-07-01' AS date)
      AND o_orderdate < CAST('1993-10-01' AS date)
      AND EXISTS (select * from exists_st where l_orderkey=o_orderkey)
), groups as (
  SELECT
      o_orderpriority,
      count(*) AS order_count
  FROM
      select_st
  GROUP BY
      o_orderpriority
  ORDER BY
      o_orderpriority
), final_count as (
select 144868
)

SELECT  orders_rowid, lineitem_rowid
FROM groups join select_st USING (o_orderpriority) join exists_st on (o_orderkey=l_orderkey)
