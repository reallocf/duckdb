with end_to_end as (
SELECT LINEAGE_{0}_HASH_JOIN_4_1.rhs_index as partsupp_rowid_0,
       LINEAGE_{0}_HASH_JOIN_3_1.rhs_index as supplier_rowid_1,
       LINEAGE_{0}_SEQ_SCAN_2_0.in_index as nation_rowid_2, temp_opio17.*,
       LINEAGE_{0}_ORDER_BY_20_0.out_index
FROM LINEAGE_{0}_ORDER_BY_20_0, LINEAGE_{0}_PIECEWISE_MERGE_JOIN_18_1, 
    LINEAGE_{0}_HASH_GROUP_BY_6_1, LINEAGE_{0}_HASH_GROUP_BY_6_0,
    LINEAGE_{0}_HASH_JOIN_4_1, LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_HASH_JOIN_3_1,
    LINEAGE_{0}_HASH_JOIN_3_0, LINEAGE_{0}_SEQ_SCAN_2_0,
    (SELECT temp_opio13.*, 0 as out_index from LINEAGE_{0}_LIMIT_15_0,
        (SELECT LINEAGE_{0}_HASH_JOIN_11_1.rhs_index as partsupp_rowid_7, 
            LINEAGE_{0}_HASH_JOIN_10_1.rhs_index as supplier_rowid_8,
            LINEAGE_{0}_SEQ_SCAN_9_0.in_index as nation_rowid_9, 0 as out_index
          from LINEAGE_{0}_HASH_JOIN_11_1, LINEAGE_{0}_HASH_JOIN_11_0,
          LINEAGE_{0}_HASH_JOIN_10_1, LINEAGE_{0}_HASH_JOIN_10_0,
          LINEAGE_{0}_SEQ_SCAN_9_0
        where LINEAGE_{0}_HASH_JOIN_11_0.out_address=LINEAGE_{0}_HASH_JOIN_11_1.lhs_address
          and LINEAGE_{0}_HASH_JOIN_11_0.in_index=LINEAGE_{0}_HASH_JOIN_10_1.out_index
          and LINEAGE_{0}_HASH_JOIN_10_0.out_address=LINEAGE_{0}_HASH_JOIN_10_1.lhs_address
          and LINEAGE_{0}_HASH_JOIN_10_0.in_index=LINEAGE_{0}_SEQ_SCAN_9_0.out_index) as temp_opio13
      where temp_opio13.out_index=LINEAGE_{0}_LIMIT_15_0.in_index) as temp_opio17
WHERE LINEAGE_{0}_PIECEWISE_MERGE_JOIN_18_1.out_index=LINEAGE_{0}_ORDER_BY_20_0.in_index
  and LINEAGE_{0}_PIECEWISE_MERGE_JOIN_18_1.lhs_index=LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index
  and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_4_0.in_index=LINEAGE_{0}_HASH_JOIN_3_1.out_index
  and LINEAGE_{0}_HASH_JOIN_3_0.out_address=LINEAGE_{0}_HASH_JOIN_3_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_3_0.in_index=LINEAGE_{0}_SEQ_SCAN_2_0.out_index
  and LINEAGE_{0}_PIECEWISE_MERGE_JOIN_18_1.rhs_index=temp_opio17.out_index
), final_count as (
  select 44858879
)
select * from end_to_end
