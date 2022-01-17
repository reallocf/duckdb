with end_to_end as (
  SELECT temp_opio5.*, 0 as out_index
  FROM (SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0,
              LINEAGE_{0}_HASH_JOIN_2_0.in_index as part_rowid_1, 0 as out_index
        from LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0,
             LINEAGE_{0}_SEQ_SCAN_0_0
        where LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address
          and LINEAGE_{0}_HASH_JOIN_2_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_0_0.out_index) as temp_opio5
), original as (
    SELECT l_partkey, p_partkey
    FROM end_to_end, lineitem, part
    WHERE lineitem.rowid=lineitem_rowid_0
      and part.rowid=part_rowid_1
), final_count as (
  select 75982
)

SELECT * FROM original;

