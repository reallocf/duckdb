with in2 as (
    SELECT
        part.rowid as part_rowid,
        p_partkey
    FROM
        part
    WHERE
        p_name LIKE 'forest%'
), in3_select as (
    SELECT
        lineitem.rowid as lineitem_rowid,
        l_partkey, l_suppkey,
        l_quantity
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
), in3 as (
    SELECT
        l_partkey, l_suppkey,
        0.5 * sum(l_quantity) as sum_q
    FROM
        in3_select
    GROUP BY l_partkey, l_suppkey
), in1 as (
  SELECT
      partsupp.rowid as partsupp_rowid,
      ps_suppkey,
      ps_partkey
  FROM
      partsupp
  WHERE
      ps_partkey IN ( select p_partkey from in2 )
          AND ps_availqty > (
                select sum_q
                from in3
                where l_partkey=ps_partkey and l_suppkey=ps_suppkey
              )
), final as (
  SELECT
      supplier.rowid as supplier_rowid,
      nation.rowid as nation_rowid,
      s_name,
      s_address,
      s_suppkey
  FROM
      supplier,
      nation
  WHERE
      s_suppkey IN ( select ps_suppkey from in1 )
              AND s_nationkey = n_nationkey
              AND n_name = 'CANADA'
          ORDER BY
              s_name
), final_count as (
select 432
)

select final.*, partsupp_rowid, part_rowid, lineitem_rowid
from final, in1, in2, in3_select
where final.s_suppkey=in1.ps_suppkey
and in1.ps_partkey=in2.p_partkey
and in3_select.l_partkey=in1.ps_partkey
and in3_select.l_suppkey=in1.ps_suppkey
