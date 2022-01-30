CREATE TABLE lineage as (
  select joins2.*, joins1.*, groups1.*
  from 
      (
          SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
          FROM (
                SELECT l_suppkey, l_extendedprice, l_discount FROM lineitem
                WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
          )
          GROUP BY supplier_no
      ) as groups1 join
      ( 
        SELECT lineitem.rowid as lineitem_rowid, l_suppkey, l_extendedprice, l_discount
          FROM lineitem
          WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
      ) as joins1 on (groups1.supplier_no=joins1.l_suppkey) join
      (
          SELECT supplier.rowid as supplier_rowid, s_suppkey
          FROM supplier
          ORDER BY s_suppkey
      ) as joins2 on (groups1.supplier_no=joins2.s_suppkey)
      join ( SELECT max(total_revenue) as max_r FROM (
          SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
          FROM (
                SELECT l_suppkey, l_extendedprice, l_discount FROM lineitem
                WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
          )
          GROUP BY supplier_no
      ) as groups1) as groups2 on (groups2.max_r=groups1.total_revenue)
)
