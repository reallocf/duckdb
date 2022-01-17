WITH joins AS (
SELECT
    orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid,
    l_shipmode, o_orderpriority
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS date)
    AND l_receiptdate < CAST('1995-01-01' AS date)
), groups as (
  SELECT
      l_shipmode,
      sum(
          CASE WHEN o_orderpriority = '1-URGENT'
              OR o_orderpriority = '2-HIGH' THEN
              1
          ELSE
              0
          END) AS high_line_count,
      sum(
          CASE WHEN o_orderpriority <> '1-URGENT'
              AND o_orderpriority <> '2-HIGH' THEN
              1
          ELSE
              0
          END) AS low_line_count
  FROM
    joins
  GROUP BY
      l_shipmode
  ORDER BY
      l_shipmode
), final_count as (
select 30987
)

SELECT groups.*, orders_rowid, lineitem_rowid
FROM groups join joins using (l_shipmode)
