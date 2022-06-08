with lineage as (
  select groups.*, joins.*, l2.rowid as lineitem_rowid_0
  from (
      SELECT s_name, count(*) AS numwait
      FROM (
          SELECT s_name, s_suppkey, o_orderkey
          FROM supplier, lineitem l1, orders, nation
          WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
              AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
      )
      GROUP BY s_name
      ORDER BY numwait DESC, s_name
      LIMIT 100
    ) as groups join (
    SELECT supplier.rowid as supplier_rowid, l1.rowid as lineitem_rowid,
           orders.rowid as orders_rowid, nation.rowid as nation_rowid, s_name, l_orderkey, l_suppkey, l_commitdate
    FROM supplier, lineitem l1, orders, nation
    WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
        AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
        AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        AND s_nationkey = n_nationkey
        AND n_name = 'SAUDI ARABIA'
  ) as joins using (s_name) join
  lineitem as l2 on (l2.l_orderkey = joins.l_orderkey AND l2.l_suppkey <> joins.l_suppkey)
)
select count(*) as c,
    max(lineitem_rowid_0),
    max(supplier_rowid),
    max(lineitem_rowid),
    max(orders_rowid),
    max(nation_rowid) from lineage
