CREATE TABLE lineage as (
  select main_join.*, from_join.lineitem_rowid, where_clause.lineitem_rowid_2
  from 
      (
          SELECT supplier.rowid as supplier_rowid,
                 s_suppkey, total_revenue, s_name, s_address, s_phone
          FROM supplier,
            (
                SELECT
                    l_suppkey AS supplier_no,
                    sum(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM
                    lineitem
                WHERE
                    l_shipdate >= CAST('1996-01-01' AS date)
                    AND l_shipdate < CAST('1996-04-01' AS date)
                GROUP BY
                    supplier_no) revenue0
          WHERE s_suppkey = supplier_no AND total_revenue = (
              SELECT
                  max(total_revenue)
              FROM (
                  SELECT
                      l_suppkey AS supplier_no,
                      sum(l_extendedprice * (1 - l_discount)) AS total_revenue
                  FROM
                      lineitem
                  WHERE
                      l_shipdate >= CAST('1996-01-01' AS date)
                      AND l_shipdate < CAST('1996-04-01' AS date)
                  GROUP BY
                      supplier_no) revenue1)
          ORDER BY s_suppkey
      )  as main_join join (
        SELECT lineitem.rowid as lineitem_rowid, l_suppkey, l_extendedprice, l_discount
          FROM lineitem
          WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
      ) as from_join on (main_join.s_suppkey=from_join.l_suppkey) join (
          select lineitem_rowid as lineitem_rowid_2, total_revenue from
          (
            SELECT lineitem.rowid as lineitem_rowid, l_suppkey, l_extendedprice, l_discount
              FROM lineitem
              WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
          ) t1 join (
              SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
              FROM lineitem
              WHERE l_shipdate >= CAST('1996-01-01' AS date)
                  AND l_shipdate < CAST('1996-04-01' AS date)
              GROUP BY
                  supplier_no
          ) t2 on (t1.l_suppkey=t2.supplier_no)
      ) as where_clause using (total_revenue)
)
