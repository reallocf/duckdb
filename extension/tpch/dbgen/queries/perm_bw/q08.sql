with lineage as (
  select joins.*, groups.o_year
  from (
    SELECT part.rowid as part_rowid, supplier.rowid as supplier_rowid,
           lineitem.rowid as lineitem_rowid, orders.rowid as orders_rowid,
           customer.rowid as customer_rowid, n1.rowid as n1_rowid,
           n2.rowid as n2_rowid, region.rowid as region_rowid,
           extract(year FROM o_orderdate) AS o_year
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL'
  ) as joins join (
    SELECT o_year, sum(
              CASE WHEN nation = 'BRAZIL' THEN
                  volume
              ELSE
                  0
              END) / sum(volume) AS mkt_share
    FROM (
          SELECT extract(year FROM o_orderdate) AS o_year, n2.n_name as nation, l_extendedprice * (1 - l_discount) AS volume
          FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
              AND CAST('1996-12-31' AS date)
              AND p_type = 'ECONOMY ANODIZED STEEL'
    )
    GROUP BY o_year
    ORDER BY o_year
  ) as groups using (o_year)
)

select count(*) as c,
    max(part_rowid),
    max(supplier_rowid),
    max(lineitem_rowid), 
    max(orders_rowid),
    max(customer_rowid), 
    max(n1_rowid),
    max(n2_rowid), 
    max(region_rowid) from lineage
