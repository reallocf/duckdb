with joins as (
    SELECT
        supplier.rowid as supplier_rowid,
        lineitem.rowid as lineitem_rowid,
        orders.rowid as orders_rowid,
        customer.rowid as customer_rowid,
        n1.rowid as n2_rowid,
        n2.rowid as n2_rowid,
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
), groups as (
  SELECT
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) AS revenue
  FROM joins AS shipping
  GROUP BY
      supp_nation,
      cust_nation,
      l_year
  ORDER BY
      supp_nation,
      cust_nation,
      l_year
), final_count as (
select 5923
)

select 
  supplier_rowid,
  lineitem_rowid,
  orders_rowid,
  customer_rowid,
  n2_rowid,
  n2_rowid
from joins join groups using (supp_nation, cust_nation, l_year)
