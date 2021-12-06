with end_to_end as (
  select temp_opio5.*, 0 as out_index
  FROM (SELECT  LINEAGE_0_SEQ_SCAN_0_0.in_index as lineitem_rowid_opid_0,
                LINEAGE_0_HASH_JOIN_2_0.in_index as part_rowid_opid_1,
                0 as out_index
        from LINEAGE_0_HASH_JOIN_2_1,
             LINEAGE_0_HASH_JOIN_2_0,
             LINEAGE_0_SEQ_SCAN_0_0
        where LINEAGE_0_HASH_JOIN_2_0.out_address=LINEAGE_0_HASH_JOIN_2_1.lhs_address
          and LINEAGE_0_HASH_JOIN_2_1.rhs_index=LINEAGE_0_SEQ_SCAN_0_0.out_index) as temp_opio5
), original as (
    SELECT l_partkey, p_partkey
    FROM end_to_end, lineitem, part
    WHERE lineitem.rowid=lineitem_rowid_opid_0
      and part.rowid=part_rowid_opid_1
  ), final_count as (
  select 75982
)

SELECT * FROM end_to_end;

