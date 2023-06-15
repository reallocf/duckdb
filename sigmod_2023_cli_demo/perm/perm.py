import duckdb
from flask import Flask, request
app = Flask(__name__)

con = duckdb.connect(database=':memory:', read_only=False)
sf = 0.1
print(f"Loading TPC-H data with sf {sf}")
con.execute(f"CALL dbgen(sf={sf});")

@app.route('/')
def hello_world():
    return 'Hello, Docker!'

@app.post("/sql")
def execute_sql():
    body = request.json
    query = body['query']
    if query in valid_perm_queries:
        perm_query = valid_perm_queries[query]
        return {"perm_res": True, "res": con.execute(perm_query).fetchall()}
    else:
        return {"perm_res": False}

valid_perm_queries = {
    # Test query to confirm things are working
    "select * from lineitem limit 10;": "select * from lineitem limit 10",
    # Q1
    """SELECT
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
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;""": """select groups.*, lineitem_rowid
  from (
    SELECT
        l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order
    FROM (
      SELECT
          lineitem.rowid as lineitem_rowid,
          l_returnflag,l_linestatus,l_quantity,l_extendedprice,l_discount,l_tax
      FROM lineitem
      WHERE l_shipdate <= CAST('1998-09-02' AS date)
    )
    GROUP BY l_returnflag, l_linestatus
  ) as groups join (
    SELECT
        lineitem.rowid as lineitem_rowid,l_returnflag, l_linestatus
    FROM lineitem
    WHERE l_shipdate <= CAST('1998-09-02' AS date)
  ) using (l_returnflag, l_linestatus)""",
    # Q2
    """SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;""": """select joins2.*, partsupp_rowid2, supplier_rowid2, nation_rowid2, region_rowid2
  from (
    SELECT partsupp.rowid as partsupp_rowid2, supplier.rowid as supplier_rowid2,
           nation.rowid as nation_rowid2, region.rowid as region_rowid2, ps_partkey, ps_supplycost
    FROM partsupp, supplier, nation, region
    WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
  ) as joins1 join (
    SELECT ps_partkey, min(ps_supplycost) as min_ps_supplycost
    FROM (
      SELECT ps_partkey, ps_supplycost
      FROM partsupp, supplier, nation, region
      WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
    )
    GROUP BY ps_partkey
  ) as group1 using ( ps_partkey ) join (
    SELECT part.rowid as part_rowid, supplier.rowid as supplier_rowid, partsupp.rowid as partsupp_rowid, nation.rowid as nation_rowid, region.rowid as region_rowid,
        ps_supplycost,s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = partsupp.ps_partkey
        AND s_suppkey = partsupp.ps_suppkey AND p_size = 15
        AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
      AND ps_supplycost = (
          SELECT min(ps_supplycost)
          FROM partsupp, supplier, nation, region
          WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
    LIMIT 100
  ) as joins2 on (group1.min_ps_supplycost=joins2.ps_supplycost)""",
    # Q3
    """SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;""": """SELECT groups.*, customer_rowid, orders_rowid, lineitem_rowid
  FROM (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid,
        l_orderkey, o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
        AND l_shipdate > CAST('1995-03-15' AS date)
  ) as joins join (
    SELECT
        l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
    FROM (
      SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid,
          l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
      FROM customer, orders, lineitem
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
          AND l_shipdate > CAST('1995-03-15' AS date)
    )
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    LIMIT 10
  ) as groups using (l_orderkey, o_orderdate, o_shippriority)""",
    # Q4
    """SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;""": """SELECT groups.*, orders_rowid, lineitem_rowid
  FROM (
    SELECT o_orderpriority, count(*) AS order_count
    FROM (
      SELECT o_orderpriority
      FROM orders
      WHERE o_orderdate >= CAST('1993-07-01' AS date)
          AND o_orderdate < CAST('1993-10-01' AS date)
          AND EXISTS (SELECT * FROM lineitem
                       WHERE l_commitdate < l_receiptdate
                         and l_orderkey=o_orderkey
                      )
    )
    GROUP BY o_orderpriority ORDER BY o_orderpriority
  ) as groups join (
    SELECT orders.rowid as orders_rowid, o_orderpriority, o_orderkey
    FROM orders
    WHERE o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (SELECT * FROM lineitem
                    WHERE l_commitdate < l_receiptdate
                      and l_orderkey=o_orderkey
                    )
  ) as select_st USING (o_orderpriority) join (
    SELECT lineitem.rowid as lineitem_rowid, l_orderkey
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
  ) as exists_st on ( select_st.o_orderkey=exists_st.l_orderkey)""",
    # Q5
    """SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;""": """select groups.*, customer_rowid, orders_rowid, lineitem_rowid, supplier_rowid,
         nation_rowid, region_rowid
  from (
    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM (
      SELECT n_name, l_extendedprice, l_discount
      FROM customer, orders, lineitem, supplier, nation, region
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
         AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
         AND o_orderdate < CAST('1995-01-01' AS date)
    )
    GROUP BY n_name
    ORDER BY revenue DESC
  ) as groups join (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid,
           lineitem.rowid as lineitem_rowid, supplier.rowid as supplier_rowid,
           nation.rowid as nation_rowid, region.rowid as region_rowid,
           n_name
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
       AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
       AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
       AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
       AND o_orderdate < CAST('1995-01-01' AS date)
  ) as joins using (n_name)""",
    # Q6
    """SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;""": """select lineitem_rowid, revenue
  from (
    SELECT lineitem.rowid as lineitem_rowid
    FROM lineitem
    WHERE l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05 AND 0.07
        AND l_quantity < 24
  ), (
    SELECT  sum(l_extendedprice * l_discount) AS revenue
    FROM (
      SELECT l_extendedprice, l_discount
      FROM lineitem
      WHERE l_shipdate >= CAST('1994-01-01' AS date)
          AND l_shipdate < CAST('1995-01-01' AS date)
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    )
  )""",
    # Q7
    """SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;""": """select groups.*, supplier_rowid, lineitem_rowid, orders_rowid, customer_rowid, n2_rowid,  n2_rowid
  from (
    SELECT supplier.rowid as supplier_rowid, lineitem.rowid as lineitem_rowid, orders.rowid as orders_rowid,
        customer.rowid as customer_rowid, n1.rowid as n2_rowid, n2.rowid as n2_rowid,
        n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year
    FROM supplier, lineitem, orders, customer, nation n1, nation n2
    WHERE s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
  ) as joins join (
    SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue
    FROM (
      SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year,
             l_extendedprice * (1 - l_discount) AS volume
      FROM supplier, lineitem, orders, customer, nation n1, nation n2
      WHERE s_suppkey = l_suppkey
          AND o_orderkey = l_orderkey AND c_custkey = o_custkey
          AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
          AND ((n1.n_name = 'FRANCE'
                  AND n2.n_name = 'GERMANY')
              OR (n1.n_name = 'GERMANY'
                  AND n2.n_name = 'FRANCE'))
          AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
          AND CAST('1996-12-31' AS date)
    )
    GROUP BY supp_nation, cust_nation, l_year
    ORDER BY supp_nation, cust_nation, l_year
  ) as groups using (supp_nation, cust_nation, l_year)""",
    # Q8
    """SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;""": """select groups.*, part_rowid, supplier_rowid, lineitem_rowid, orders_rowid,
         customer_rowid, n1_rowid, n2_rowid, region_rowid
  from (
    SELECT part.rowid as part_rowid, supplier.rowid as supplier_rowid,
           lineitem.rowid as lineitem_rowid, orders.rowid as orders_rowid,
           customer.rowid as customer_rowid, n1.rowid as n1_rowid,
           n2.rowid as n2_rowid, region.rowid as region_rowid,
           extract(year FROM o_orderdate) AS o_year
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL'
  ) as joins join (
    SELECT o_year, sum(
              CASE WHEN nation = 'BRAZIL' THEN
                  volume
              ELSE
                  0
              END) / sum(volume) AS mkt_share
    FROM (
          SELECT extract(year FROM o_orderdate) AS o_year, n2.n_name as nation, l_extendedprice * (1 - l_discount) AS volume
          FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
              AND CAST('1996-12-31' AS date)
              AND p_type = 'ECONOMY ANODIZED STEEL'
    )
    GROUP BY o_year
    ORDER BY o_year
  ) as groups using (o_year)""",
    # Q9
    """SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;""": """select groups.*, part_rowid,  supplier_rowid, lineitem_rowid,  partsupp_rowid,
         orders_rowid, nation_rowid
  from (
    SELECT part.rowid as part_rowid, supplier.rowid as supplier_rowid,
           lineitem.rowid as lineitem_rowid, partsupp.rowid as partsupp_rowid,
           orders.rowid as orders_rowid, nation.rowid as nation_rowid,
           n_name AS nation, extract(year FROM o_orderdate) AS o_year
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
  ) as joins join (
    SELECT nation, o_year, sum(amount) AS sum_profit
    FROM (
      SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year,
             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
      FROM part, supplier, lineitem, partsupp, orders, nation
      WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
          AND ps_partkey = l_partkey AND p_partkey = l_partkey
          AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
          AND p_name LIKE '%green%'
    )
    GROUP BY nation, o_year
    ORDER BY nation, o_year DESC
  ) as groups using (nation, o_year)""",
    # Q10
    """SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;""": """SELECT groups.*, customer_rowid, orders_rowid, lineitem_rowid,  nation_rowid
  FROM (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid,
           lineitem.rowid as lineitem_rowid, nation.rowid as nation_rowid,
           c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, orders, lineitem, nation
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate >= CAST('1993-10-01' AS date)
        AND o_orderdate < CAST('1994-01-01' AS date)
        AND l_returnflag = 'R' AND c_nationkey = n_nationkey
  ) as joins join (
    SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue,
           c_acctbal, n_name, c_address, c_phone, c_comment
    FROM (
        SELECT l_extendedprice, l_discount, c_custkey, c_name, c_acctbal, n_name,
               c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate >= CAST('1993-10-01' AS date)
            AND o_orderdate < CAST('1994-01-01' AS date)
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey
    )
    GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
    ORDER BY revenue DESC
    LIMIT 20
  ) as groups using (c_custkey, c_name, c_acctbal, c_phone, c_name, c_address, c_comment)""",
    # Q11
    """SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    sum(ps_supplycost * ps_availqty) > (
        SELECT
            sum(ps_supplycost * ps_availqty) * 0.0001000000
        FROM
            partsupp,
            supplier,
            nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY')
ORDER BY
    value DESC;""": """select main.*, subq.* from (
      select groups.*,
      j1.partsupp_rowid as join_partsupp_rowid, j1.supplier_rowid as join_supplier_rowid, j1.nation_rowid as join_nation_rowid
      from (
        SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
        FROM (
          SELECT ps_partkey, ps_supplycost, ps_availqty
          FROM partsupp, supplier, nation
          WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
        )
        GROUP BY ps_partkey
        HAVING sum(ps_supplycost * ps_availqty) > (SELECT value FROM (
          SELECT sum(ps_supplycost * ps_availqty) * 0.0000100000 as value
          FROM (SELECT ps_supplycost, ps_availqty FROM partsupp, supplier, nation
                WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
                                                ) as simple_agg)
        ORDER BY value DESC
      ) as groups join (
        SELECT partsupp.rowid as partsupp_rowid, supplier.rowid as supplier_rowid, nation.rowid as nation_rowid,
               ps_partkey, ps_supplycost, ps_availqty
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
      ) as j1 using (ps_partkey)
    ) as main, (
        SELECT partsupp.rowid as partsupp_rowid_2, supplier.rowid as supplier_rowid_2, nation.rowid as nation_rowid_2
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
    ) as subq""",
    # Q12
    """SELECT
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
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS date)
    AND l_receiptdate < CAST('1995-01-01' AS date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;""": """SELECT groups.*, orders_rowid, lineitem_rowid
  FROM (
      SELECT l_shipmode,
          sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
          sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
      FROM (
        SELECT l_shipmode, o_orderpriority
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
            AND l_receiptdate >= CAST('1994-01-01' AS date)
            AND l_receiptdate < CAST('1995-01-01' AS date)
      )
      GROUP BY l_shipmode
      ORDER BY l_shipmode
  ) as groups join (
    SELECT orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid, l_shipmode
    FROM orders, lineitem
    WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
        AND l_receiptdate >= CAST('1994-01-01' AS date)
        AND l_receiptdate < CAST('1995-01-01' AS date)
  ) as joins using (l_shipmode)""",
    # Q13
    """SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey)
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;""": """select groups2.*,  customer_rowid, orders_rowid
  from (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid,
           c_custkey, o_orderkey
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  ) as joins1 left outer join (
    SELECT c_custkey, count(o_orderkey) as c_count
    FROM (
      SELECT c_custkey, o_orderkey
      FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    )
    GROUP BY c_custkey
  ) as groups1 using (c_custkey) join (
    SELECT c_count, count(*) AS custdist
    FROM (
        SELECT c_custkey, count(o_orderkey) as c_count
        FROM (
          SELECT c_custkey, o_orderkey
          FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
        )
        GROUP BY c_custkey
    )
    GROUP BY c_count
    ORDER BY custdist DESC, c_count DESC
  ) as groups2 using (c_count)""",
    # Q14
    """SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);""": """select groups.*,  lineitem_rowid, part_rowid
  from (
    SELECT lineitem.rowid as lineitem_rowid, part.rowid as part_rowid
    FROM  lineitem, part
    WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < CAST('1995-10-01' AS date)
  ) as joins, (
    SELECT
        100.00 * sum(
            CASE WHEN p_type LIKE 'PROMO%' THEN
                l_extendedprice * (1 - l_discount)
            ELSE
                0
            END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
    FROM (
      SELECT p_type, l_extendedprice, l_discount
      FROM lineitem, part
      WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
          AND l_shipdate < CAST('1995-10-01' AS date)
    )
  ) as groups""",
    # Q15
    """SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
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
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
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
ORDER BY
    s_suppkey;""": """select main.*,  where_clause.lineitem_rowid_2 from (
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
      ) as where_clause""",
    # Q16
    """SELECT
    p_brand,
    p_type,
    p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%')
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;""": """select groups.*, partsupp_rowid, part_rowid, subq.*
  from (
        SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt
        FROM (
            SELECT p_brand, p_type, p_size, ps_suppkey
            FROM partsupp, part
            WHERE p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT
                        s_suppkey
                    FROM
                        supplier
                    WHERE
                        s_comment LIKE '%Customer%Complaints%')
        )
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
    ) as groups join (
      SELECT partsupp.rowid as partsupp_rowid, part.rowid as part_rowid, p_brand, p_type, p_size
      FROM partsupp, part
      WHERE p_partkey = ps_partkey
          AND p_brand <> 'Brand#45'
          AND p_type NOT LIKE 'MEDIUM POLISHED%'
          AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND ps_suppkey NOT IN (
              SELECT
                  s_suppkey
              FROM
                  supplier
              WHERE
                  s_comment LIKE '%Customer%Complaints%')
  ) as joins using (p_brand, p_type, p_size), (
          SELECT
              supplier.rowid as supplier_rowid
          FROM
              supplier
          WHERE
              s_comment LIKE '%Customer%Complaints%'
  ) as subq""",
    # Q17
    """SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey);""": """select groups.*, lineitem.rowid as lineitem_rowid_2,
         lineitem_rowid, part_rowid
  from  (
        SELECT
            sum(l_extendedprice) / 7.0 AS avg_yearly
        FROM (
            SELECT l_extendedprice, l_partkey
            FROM lineitem, part
            WHERE p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT
                        0.2 * avg(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = p_partkey)
            )
    ) as groups, (
        SELECT lineitem.rowid as lineitem_rowid, part.rowid as part_rowid, l_extendedprice, l_partkey
        FROM lineitem, part
        WHERE p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container = 'MED BOX'
            AND l_quantity < (
                SELECT
                    0.2 * avg(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = p_partkey)
  ) as joins, lineitem
  where lineitem.l_partkey=joins.l_partkey""",
    # Q18
    """SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    customer,
    orders,
    lineitem
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;""": """select groups.*,
      customer_rowid,
      orders_rowid,
      lineitem_rowid,
      l2.rowid as lineitem_rowid_2
  from (
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
        FROM (
            SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity
            FROM customer, orders, lineitem
            WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
                  AND c_custkey = o_custkey
                  AND o_orderkey = l_orderkey
        )
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate
        LIMIT 100
  ) as groups join (
        SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid, lineitem.rowid as lineitem_rowid, c_name,
               c_custkey, o_orderkey, o_orderdate, o_totalprice
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
  ) as joins using (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) join
  (
    SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
  ) as in1 on (in1.l_orderkey=joins.o_orderkey) join lineitem as l2 on (l2.l_orderkey=in1.l_orderkey)""",
    # Q19
    """SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE (p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');""": """select * from (
        SELECT
            sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM (
              SELECT
                  l_extendedprice, l_discount
              FROM
                  lineitem,
                  part
              WHERE (p_partkey = l_partkey
                  AND p_brand = 'Brand#12'
                  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                  AND l_quantity >= 1
                  AND l_quantity <= 1 + 10
                  AND p_size BETWEEN 1 AND 5
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON')
                  OR (p_partkey = l_partkey
                      AND p_brand = 'Brand#23'
                      AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                      AND l_quantity >= 10
                      AND l_quantity <= 10 + 10
                      AND p_size BETWEEN 1 AND 10
                      AND l_shipmode IN ('AIR', 'AIR REG')
                      AND l_shipinstruct = 'DELIVER IN PERSON')
                  OR (p_partkey = l_partkey
                      AND p_brand = 'Brand#34'
                      AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                      AND l_quantity >= 20
                      AND l_quantity <= 20 + 10
                      AND p_size BETWEEN 1 AND 15
                      AND l_shipmode IN ('AIR', 'AIR REG')
                      AND l_shipinstruct = 'DELIVER IN PERSON')
          )
    ) as groups, (
      SELECT
          lineitem.rowid as lineitem_rowid,
          part.rowid as part_rowid
      FROM
          lineitem,
          part
      WHERE (p_partkey = l_partkey
          AND p_brand = 'Brand#12'
          AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND l_quantity >= 1
          AND l_quantity <= 1 + 10
          AND p_size BETWEEN 1 AND 5
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
              AND l_quantity >= 10
              AND l_quantity <= 10 + 10
              AND p_size BETWEEN 1 AND 10
              AND l_shipmode IN ('AIR', 'AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey
              AND p_brand = 'Brand#34'
              AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
              AND l_quantity >= 20
              AND l_quantity <= 20 + 10
              AND p_size BETWEEN 1 AND 15
              AND l_shipmode IN ('AIR', 'AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
  ) as joins""",
    # Q20
    """SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'forest%')
                AND ps_availqty > (
                    SELECT
                        0.5 * sum(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date)))
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
        ORDER BY
            s_name;""": """select final.*, partsupp_rowid, part_rowid, lineitem_rowid
  from (
    SELECT supplier.rowid as supplier_rowid, nation.rowid as nation_rowid,
           s_name, s_address, s_suppkey
    FROM supplier, nation
    WHERE s_suppkey IN ( 
          SELECT ps_suppkey FROM partsupp
          WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
            AND ps_availqty > (
                          SELECT 0.5 * sum(l_quantity)
                          FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                              AND l_shipdate >= CAST('1994-01-01' AS date)
                              AND l_shipdate < CAST('1995-01-01' AS date)))
      AND s_nationkey = n_nationkey
      AND n_name = 'CANADA'
    ORDER BY s_name
  ) as final, (
    SELECT partsupp.rowid as partsupp_rowid, ps_suppkey, ps_partkey
    FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
      AND ps_availqty > (
                    SELECT 0.5 * sum(l_quantity)
                    FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date))
  ) as in1, (
    SELECT part.rowid as part_rowid, p_partkey
    FROM part
    WHERE p_name LIKE 'forest%'
  ) as in2, (
    SELECT lineitem.rowid as lineitem_rowid, l_partkey, l_suppkey, l_quantity
    FROM lineitem
    WHERE l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
  ) as in3_select
  where final.s_suppkey=in1.ps_suppkey
  and in1.ps_partkey=in2.p_partkey
  and in3_select.l_partkey=in1.ps_partkey
  and in3_select.l_suppkey=in1.ps_suppkey""",
    # Q21
    """SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;""": """select groups.*, joins.*, l2.rowid as lineitem_rowid_0
  from (
      SELECT s_name, count(*) AS numwait
      FROM (
          SELECT s_name, s_suppkey, o_orderkey
          FROM supplier, lineitem l1, orders, nation
          WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
              AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
      )
      GROUP BY s_name
      ORDER BY numwait DESC, s_name
      LIMIT 100
    ) as groups join (
    SELECT supplier.rowid as supplier_rowid, l1.rowid as lineitem_rowid,
           orders.rowid as orders_rowid, nation.rowid as nation_rowid, s_name, l_orderkey, l_suppkey, l_commitdate
    FROM supplier, lineitem l1, orders, nation
    WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
        AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
        AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        AND s_nationkey = n_nationkey
        AND n_name = 'SAUDI ARABIA'
  ) as joins using (s_name) join
  lineitem as l2 on (l2.l_orderkey = joins.l_orderkey AND l2.l_suppkey <> joins.l_suppkey)""",
    # Q22
    """SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM
        customer
    WHERE
        substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT
                avg(c_acctbal)
            FROM
                customer
            WHERE
                c_acctbal > 0.00
                AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    o_custkey = c_custkey)) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;""": """select main.*, subq.* from (
      select groups.*, customer_rowid_1
      from (
        SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
        FROM (
          SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
          FROM customer
          WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
                AND c_acctbal >  (select avg(c_acctbal) from (
                  SELECT c_acctbal FROM customer
                  WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
                ))
                AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
        )
        GROUP BY cntrycode
        ORDER BY cntrycode
      ) as groups join (
        SELECT customer.rowid as customer_rowid_1, substring(c_phone FROM 1 FOR 2) AS cntrycode, c_custkey
        FROM customer
        WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal >  (select avg(c_acctbal) from (
                SELECT c_acctbal FROM customer
                WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              ))
              AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
      ) as joins1 using (cntrycode)
    ) as main, (select customer.rowid as customer_rowid_0
      FROM customer
        WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    ) as subq""",
}
