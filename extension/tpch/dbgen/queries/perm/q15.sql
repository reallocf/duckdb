CREATE TABLE lineage as (
  select main.*,  where_clause.lineitem_rowid_2 from (
    select main_join.*, from_join.lineitem_rowid
    from  ( SELECT supplier.rowid as supplier_rowid, s_suppkey, total_revenue, s_name, s_address, s_phone
            FROM supplier, (
              select
                l_suppkey as supplier_no,
                sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from
                lineitem
              where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-01-01' + interval '3' month
              group by
                l_suppkey
            ) as revenue0
            WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM (
              select
                l_suppkey as supplier_no,
                sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from
                lineitem
              where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-01-01' + interval '3' month
              group by
                l_suppkey
            ) as revenue1)
            ORDER BY s_suppkey
        )  as main_join join (
          SELECT lineitem.rowid as lineitem_rowid, l_suppkey, l_extendedprice, l_discount
            FROM lineitem
            WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
        ) as from_join on (main_join.s_suppkey=from_join.l_suppkey)
    ) as main, (
            SELECT lineitem.rowid as lineitem_rowid_2
              FROM lineitem
              WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
      ) as where_clause
)
      
