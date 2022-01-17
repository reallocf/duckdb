with in1 as (
    SELECT
        l_orderkey
    FROM
        lineitem
    GROUP BY
        l_orderkey
    HAVING sum(l_quantity) > 300
), joins as (
  SELECT
      customer.rowid as customer_rowid,
      orders.rowid as orders_rowid,
      lineitem.rowid as lineitem_rowid,
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      l_quantity
  FROM
      customer,
      orders,
      lineitem
  WHERE
      o_orderkey IN (select l_orderkey from in1)
      AND c_custkey = o_custkey
      AND o_orderkey = l_orderkey
), groups as (
  SELECT
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      sum(l_quantity)
  FROM joins
  GROUP BY
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice
  ORDER BY
      o_totalprice DESC,
      o_orderdate
  LIMIT 100
), final_count as (
select 2792
), end_to_end as (
  select  
      customer_rowid,
      orders_rowid,
      lineitem_rowid,
      l2.rowid as lineitem_rowid_2
  from groups join joins using (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice),
      in1, lineitem as l2
  where  o_orderkey=in1.l_orderkey
      and l2.l_orderkey=in1.l_orderkey
), original as (
  select l0.l_orderkey, o_orderkey, o_custkey, c_custkey, l5.l_orderkey, c_name
  from end_to_end, lineitem as l0, lineitem as l5, customer, orders
  where
  l0.rowid=lineitem_rowid and
  l5.rowid=lineitem_rowid_2 and
  customer.rowid=customer_rowid and
  orders.rowid=orders_rowid
)
select * from end_to_end;
