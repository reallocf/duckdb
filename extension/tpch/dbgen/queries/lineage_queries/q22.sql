SELECT LINEAGE_{0}_HASH_JOIN_2_0.in_index as chunkscan_rowid_1,
        LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as customer_rowid_0,
        temp_opio14.*, LINEAGE_{0}_ORDER_BY_21_0.out_index
FROM LINEAGE_{0}_ORDER_BY_21_0, LINEAGE_{0}_HASH_GROUP_BY_20_1,
     LINEAGE_{0}_HASH_GROUP_BY_20_0, LINEAGE_{0}_HASH_JOIN_17_1,
     LINEAGE_{0}_PIECEWISE_MERGE_JOIN_15_1, LINEAGE_{0}_FILTER_3_0,
     LINEAGE_{0}_HASH_JOIN_2_1
     left join LINEAGE_{0}_HASH_JOIN_2_0
     on (LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address),
    (SELECT temp_opio11.*, 0 as opio14_out_index from LINEAGE_{0}_LIMIT_12_0,
            (SELECT group_concat(LINEAGE_{0}_HASH_JOIN_7_0.in_index) as chunkscan_rowid_6,
                    group_concat(LINEAGE_{0}_SEQ_SCAN_5_0.in_index) as customer_rowid_5, 0 as opio11_out_index
             from LINEAGE_{0}_FILTER_8_0, LINEAGE_{0}_HASH_JOIN_7_1
                  left join LINEAGE_{0}_HASH_JOIN_7_0
                  on (LINEAGE_{0}_HASH_JOIN_7_0.out_address=LINEAGE_{0}_HASH_JOIN_7_1.lhs_address),
                  LINEAGE_{0}_SEQ_SCAN_5_0
              where LINEAGE_{0}_HASH_JOIN_7_1.out_index=LINEAGE_{0}_FILTER_8_0.in_index
                and LINEAGE_{0}_HASH_JOIN_7_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_5_0.out_index
           ) as temp_opio11
      where temp_opio11.opio11_out_index=LINEAGE_{0}_LIMIT_12_0.in_index
    ) as temp_opio14
  WHERE LINEAGE_{0}_HASH_GROUP_BY_20_1.out_index=LINEAGE_{0}_ORDER_BY_21_0.in_index
    and LINEAGE_{0}_HASH_GROUP_BY_20_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_20_0.out_index
    and LINEAGE_{0}_HASH_JOIN_17_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_20_0.in_index
    and LINEAGE_{0}_HASH_JOIN_17_1.rhs_index=LINEAGE_{0}_PIECEWISE_MERGE_JOIN_15_1.out_index
    and LINEAGE_{0}_PIECEWISE_MERGE_JOIN_15_1.lhs_index=LINEAGE_{0}_FILTER_3_0.out_index
    and LINEAGE_{0}_HASH_JOIN_2_1.out_index=LINEAGE_{0}_FILTER_3_0.in_index
    and LINEAGE_{0}_PIECEWISE_MERGE_JOIN_15_1.rhs_index=temp_opio14.opio14_out_index
