with sub as (
  SELECT
      l_partkey, avg(l_quantity) as avg_quantity
  FROM
      lineitem
  GROUP BY l_partkey
), joins as (
  SELECT
      lineitem.rowid as lineitem_rowid,
      part.rowid as part_rowid,
      l_extendedprice,
      l_partkey
  FROM
      lineitem,
      part
  WHERE
      p_partkey = l_partkey
      AND p_brand = 'Brand#23'
      AND p_container = 'MED BOX'
      AND l_quantity < (
          SELECT
              0.2 * avg_quantity
          FROM
              sub
          WHERE
              l_partkey = p_partkey)
), groups as (
  SELECT
      sum(l_extendedprice) / 7.0 AS avg_yearly
  FROM joins
), end_to_end as (
  select lineitem_rowid, part_rowid, lineitem.rowid as lineitem_rowid_2
  from  groups, joins, lineitem
  where lineitem.l_partkey=joins.l_partkey
), original as (
    SELECT p_partkey, l1.l_partkey, l2.l_partkey, p_brand, p_container
    FROM end_to_end, lineitem l1, part, lineitem l2
    WHERE l1.rowid=lineitem_rowid
          and l2.rowid=lineitem_rowid_2
          and part.rowid=part_rowid
), final_count as (
select 18056
)
select * from end_to_end
