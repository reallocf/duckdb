with joins as (
  SELECT
      customer.rowid as customer_rowid,
      orders.rowid as orders_rowid,
      lineitem.rowid as lineitem_rowid,
      supplier.rowid as supplier_rowid,
      nation.rowid as nation_rowid,
      region.rowid as region_rowid,
      n_name,
      l_extendedprice,
      l_discount
  FROM
      customer,
      orders,
      lineitem,
      supplier,
      nation,
      region
  WHERE
      c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'ASIA'
      AND o_orderdate >= CAST('1994-01-01' AS date)
      AND o_orderdate < CAST('1995-01-01' AS date)
), groups as (
  SELECT
      n_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue
  FROM joins
  GROUP BY
      n_name
  ORDER BY
      revenue DESC
), final_count as (
select 7242
)

select customer_rowid, orders_rowid, lineitem_rowid, supplier_rowid, nation_rowid, region_rowid
from groups join joins using (n_name)
