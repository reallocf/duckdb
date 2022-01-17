with joins as (
  SELECT
      lineitem.rowid as lineitem_rowid,
      part.rowid as part_rowid,
      p_type, l_extendedprice, l_discount
  FROM
      lineitem,
      part
  WHERE
      l_partkey = p_partkey
      AND l_shipdate >= date '1995-09-01'
      AND l_shipdate < CAST('1995-10-01' AS date)
), groups as (
  SELECT
      100.00 * sum(
          CASE WHEN p_type LIKE 'PROMO%' THEN
              l_extendedprice * (1 - l_discount)
          ELSE
              0
          END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
  FROM joins
), final_count as (
select 75982
)

select groups.*, lineitem_rowid, part_rowid from joins, groups
