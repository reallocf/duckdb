create table lineage as (
  select groups.*, partsupp_rowid, part_rowid
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
              s_suppkey, supplier.rowid
          FROM
              supplier
          WHERE
              s_comment LIKE '%Customer%Complaints%'
  )
)
