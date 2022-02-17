with lineage as (
  SELECT groups.l_orderkey, groups.o_orderdate, groups.o_shippriority, customer_rowid, orders_rowid, lineitem_rowid
  FROM (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid,
        l_orderkey, o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
        AND l_shipdate > CAST('1995-03-15' AS date)
  ) as joins join (
    SELECT
        l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
    FROM (
      SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid,
          l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
      FROM customer, orders, lineitem
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
          AND l_shipdate > CAST('1995-03-15' AS date)
    )
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    LIMIT 10
  ) as groups using (l_orderkey, o_orderdate, o_shippriority)
), bw as (
select * from lineage
)

select count(*) as c from bw
