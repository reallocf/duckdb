create table lineage as (
  select joins2.*, partsupp_rowid2, supplier_rowid2, nation_rowid2, region_rowid2
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
  ) as joins2 on (group1.min_ps_supplycost=joins2.ps_supplycost)
)
