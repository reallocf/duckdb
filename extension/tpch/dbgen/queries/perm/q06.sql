with select_st as (
  SELECT
      lineitem.rowid as lineitem_rowid,
      l_extendedprice,
      l_discount
  FROM
      lineitem
  WHERE
      l_shipdate >= CAST('1994-01-01' AS date)
      AND l_shipdate < CAST('1995-01-01' AS date)
      AND l_discount BETWEEN 0.05
      AND 0.07
      AND l_quantity < 24
), groups as (
  SELECT
      sum(l_extendedprice * l_discount) AS revenue
  FROM
      select_st
)

select lineitem_rowid from select_st
