with lineage as (
  select groups.n_name, joins.*
  from (
    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM (
      SELECT n_name, l_extendedprice, l_discount
      FROM customer, orders, lineitem, supplier, nation, region
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
         AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
         AND o_orderdate < CAST('1995-01-01' AS date)
    )
    GROUP BY n_name
    ORDER BY revenue DESC
  ) as groups join (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid,
           lineitem.rowid as lineitem_rowid, supplier.rowid as supplier_rowid,
           nation.rowid as nation_rowid, region.rowid as region_rowid,
           n_name
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
       AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
       AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
       AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
       AND o_orderdate < CAST('1995-01-01' AS date)
  ) as joins using (n_name)
)

select count(*) as c,
    max(customer_rowid), max(orders_rowid), max(lineitem_rowid), max(supplier_rowid),
    max(nation_rowid), max(region_rowid) from lineage

