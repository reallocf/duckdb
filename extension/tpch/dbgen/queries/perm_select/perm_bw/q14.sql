with lineage as (
    SELECT lineitem.rowid as lineitem_rowid, part.rowid as part_rowid
    FROM  lineitem, part
    WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < CAST('1995-10-01' AS date)
)
select count(*) as c,
    max(lineitem_rowid), 
    max(part_rowid) from lineage
