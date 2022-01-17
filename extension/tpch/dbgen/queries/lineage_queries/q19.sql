with end_to_end as (
  SELECT temp_opio8.*, 0 as out_index FROM (SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0, LINEAGE_{0}_HASH_JOIN_4_0.in_index as part_rowid_3, 0 as out_index from LINEAGE_{0}_FILTER_5_0, LINEAGE_{0}_HASH_JOIN_4_1, LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_FILTER_1_0, LINEAGE_{0}_SEQ_SCAN_0_0 where LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_FILTER_5_0.in_index and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_FILTER_1_0.out_index and LINEAGE_{0}_SEQ_SCAN_0_0.out_index=LINEAGE_{0}_FILTER_1_0.in_index) as temp_opio8
), original as (
    SELECT l_partkey, p_partkey
    FROM end_to_end, lineitem, part
    WHERE lineitem.rowid=lineitem_rowid_0
      and part.rowid=part_rowid_3
), final_count as (
select 120
)

SELECT * FROM end_to_end;
