with select_st as (
  SELECT
      lineitem.rowid as lineitem_rowid,
      l_returnflag,
      l_linestatus,
      l_quantity,
      l_extendedprice,
      l_discount,
      l_tax
  FROM
      lineitem
  WHERE
      l_shipdate <= CAST('1998-09-02' AS date)
), groups as (
  SELECT
      l_returnflag,
      l_linestatus,
      sum(l_quantity) AS sum_qty,
      sum(l_extendedprice) AS sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
      avg(l_quantity) AS avg_qty,
      avg(l_extendedprice) AS avg_price,
      avg(l_discount) AS avg_disc,
      count(*) AS count_order
  FROM select_st
  GROUP BY
      l_returnflag,
      l_linestatus
  ORDER BY
      l_returnflag,
      l_linestatus
)

select groups.*, lineitem_rowid from groups join select_st using (l_returnflag, l_linestatus)
