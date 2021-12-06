with end_to_end as (
  select LINEAGE_0_HASH_JOIN_4_1.rhs_index as partsupp_rowid_opid_0,
      LINEAGE_0_HASH_JOIN_3_1.rhs_index as supplier_rowid_opid_1,
      LINEAGE_0_SEQ_SCAN_2_0.in_index as nation_rowid_opid_2,
      temp_opio17.*, LINEAGE_0_ORDER_BY_20_0.out_index
FROM  LINEAGE_0_ORDER_BY_20_0,
      LINEAGE_0_PIECEWISE_MERGE_JOIN_18_0,
      LINEAGE_0_HASH_GROUP_BY_6_0,
      LINEAGE_0_HASH_JOIN_4_1,
      LINEAGE_0_HASH_JOIN_4_0,
      LINEAGE_0_HASH_JOIN_3_1,
      LINEAGE_0_HASH_JOIN_3_0,
      LINEAGE_0_SEQ_SCAN_2_0,
      (SELECT temp_opio13.*, 0 as out_index
        from LINEAGE_0_LIMIT_15_0,
              (SELECT LINEAGE_0_HASH_JOIN_11_1.rhs_index as partsupp_rowid_opid_7,
                      LINEAGE_0_HASH_JOIN_10_1.rhs_index as supplier_rowid_opid_8,
                      LINEAGE_0_SEQ_SCAN_9_0.in_index as nation_rowid_opid_9,
                      0 as out_index 
                from LINEAGE_0_HASH_JOIN_11_1, LINEAGE_0_HASH_JOIN_11_0, LINEAGE_0_HASH_JOIN_10_1, LINEAGE_0_HASH_JOIN_10_0, LINEAGE_0_SEQ_SCAN_9_0
                where LINEAGE_0_HASH_JOIN_11_0.out_address=LINEAGE_0_HASH_JOIN_11_1.lhs_address
                    and LINEAGE_0_HASH_JOIN_11_0.in_index=LINEAGE_0_HASH_JOIN_10_1.out_index
                    and LINEAGE_0_HASH_JOIN_10_0.out_address=LINEAGE_0_HASH_JOIN_10_1.lhs_address
                    and LINEAGE_0_HASH_JOIN_10_0.in_index=LINEAGE_0_SEQ_SCAN_9_0.out_index
                ) as temp_opio13
          where temp_opio13.out_index=LINEAGE_0_LIMIT_15_0.in_index
        ) as temp_opio17
where LINEAGE_0_PIECEWISE_MERGE_JOIN_18_0.out_index=LINEAGE_0_ORDER_BY_20_0.in_index
  and LINEAGE_0_PIECEWISE_MERGE_JOIN_18_0.rhs_index=LINEAGE_0_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_0_HASH_JOIN_4_1.out_index=LINEAGE_0_HASH_GROUP_BY_6_0.in_index
  and LINEAGE_0_HASH_JOIN_4_0.out_address=LINEAGE_0_HASH_JOIN_4_1.lhs_address
  and LINEAGE_0_HASH_JOIN_4_0.in_index=LINEAGE_0_HASH_JOIN_3_1.out_index
  and LINEAGE_0_HASH_JOIN_3_0.out_address=LINEAGE_0_HASH_JOIN_3_1.lhs_address
  and LINEAGE_0_HASH_JOIN_3_0.in_index=LINEAGE_0_SEQ_SCAN_2_0.out_index
  and LINEAGE_0_PIECEWISE_MERGE_JOIN_18_0.lhs_index=temp_opio17.out_index
), final_count as (
select 42926399
)
select * from end_to_end
