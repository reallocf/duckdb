with joins1 as (
    SELECT
        customer.rowid as customer_rowid,
        orders.rowid as orders_rowid,
        c_custkey,
        o_orderkey
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
), groups1 as (
    SELECT
        c_custkey,
        count(o_orderkey) as c_count
    FROM joins1
    GROUP BY c_custkey
), groups2 as (
  SELECT
      c_count,
      count(*) AS custdist
  FROM groups1
  GROUP BY
      c_count
  ORDER BY
      custdist DESC,
      c_count DESC
), final_count as (
select 1533922
)

select groups2.*, customer_rowid, orders_rowid from joins1 left outer join groups1 using (c_custkey) join groups2 using (c_count)
