create table lineage as (
  select main.*, subq.* from (
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
    ) as subq
)
