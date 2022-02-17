CREATE TABLE lineage as (
  select groups.*,
      customer_rowid,
      orders_rowid,
      lineitem_rowid,
      l2.rowid as lineitem_rowid_2
  from (
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
        FROM (
            SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity
            FROM customer, orders, lineitem
            WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
                  AND c_custkey = o_custkey
                  AND o_orderkey = l_orderkey
        )
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
  ) as groups join (
        SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid, c_name,
               c_custkey, o_orderkey, o_orderdate, o_totalprice
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
  ) as joins using (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) join
  (
    SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
  ) as in1 on (in1.l_orderkey=joins.o_orderkey) join lineitem as l2 on (l2.l_orderkey=in1.l_orderkey)
)
