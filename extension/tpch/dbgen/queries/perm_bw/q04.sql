with lineage as (
  SELECT orders_rowid, lineitem_rowid, o_orderpriority
  FROM (
    SELECT o_orderpriority, count(*) AS order_count
    FROM (
      SELECT o_orderpriority
      FROM orders
      WHERE o_orderdate >= CAST('1993-07-01' AS date)
          AND o_orderdate < CAST('1993-10-01' AS date)
          AND EXISTS (SELECT * FROM lineitem
                       WHERE l_commitdate < l_receiptdate
                         and l_orderkey=o_orderkey
                      )
    )
    GROUP BY o_orderpriority ORDER BY o_orderpriority
  ) as groups join (
    SELECT orders.rowid as orders_rowid, o_orderpriority, o_orderkey
    FROM orders
    WHERE o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (SELECT * FROM lineitem
                    WHERE l_commitdate < l_receiptdate
                      and l_orderkey=o_orderkey
                    )
  ) as select_st USING (o_orderpriority) join (
    SELECT lineitem.rowid as lineitem_rowid, l_orderkey
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
  ) as exists_st on ( select_st.o_orderkey=exists_st.l_orderkey)
)
select count(*) as c from lineage
