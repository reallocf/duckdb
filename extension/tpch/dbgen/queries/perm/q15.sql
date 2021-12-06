with joins1 as (
        SELECT
            lineitem.rowid as lineitem_rowid,
            l_suppkey,
            l_extendedprice,
            l_discount
        FROM
            lineitem
        WHERE
            l_shipdate >= CAST('1996-01-01' AS date)
            AND l_shipdate < CAST('1996-04-01' AS date)
), groups1 as (
        SELECT
            l_suppkey AS supplier_no,
            sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM joins1
        GROUP BY supplier_no
), groups2 as (
      SELECT max(total_revenue) as max_r
      FROM groups1
), joins2 as (
  SELECT
      supplier.rowid as supplier_rowid,
      s_suppkey,
      s_name,
      s_address,
      s_phone,
      total_revenue
  FROM
      supplier, groups1
  WHERE
      s_suppkey = supplier_no
      AND total_revenue = (select max_r from groups2)
  ORDER BY
      s_suppkey
), final_count as (
select 20030
)

select joins2.*,lineitem_rowid, lineitem.rowid as lineitem_rowid_2
from joins2,
    (select lineitem_rowid, supplier_no, total_revenue from groups1, joins1 where groups1.supplier_no=joins1.l_suppkey) as t1,
    groups2, lineitem
where s_suppkey=supplier_no and groups2.max_r=joins2.total_revenue
    and t1.total_revenue=groups2.max_r
    and t1.supplier_no=l_suppkey
