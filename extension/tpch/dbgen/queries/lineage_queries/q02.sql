SELECT temp_26.*, LINEAGE_{0}_HASH_JOIN_18_1.rhs_index as partsupp_rowid_16,
      LINEAGE_{0}_HASH_JOIN_18_0.in_index as supplier_rowid_17,
      LINEAGE_{0}_HASH_JOIN_21_1.rhs_index as nation_rowid_19,
      LINEAGE_{0}_SEQ_SCAN_20_0.in_index as region_rowid_20,
      LINEAGE_{0}_SEQ_SCAN_23_0.in_index as part_rowid_23,
      LINEAGE_{0}_LIMIT_32_0.out_index
  FROM LINEAGE_{0}_LIMIT_32_0, LINEAGE_{0}_ORDER_BY_31_0, LINEAGE_{0}_FILTER_28_0,
       LINEAGE_{0}_HASH_JOIN_13_1 left join
       LINEAGE_{0}_HASH_JOIN_13_0 on (LINEAGE_{0}_HASH_JOIN_13_0.out_address=LINEAGE_{0}_HASH_JOIN_13_1.lhs_address)
       left join (select temp_15.*, LINEAGE_{0}_HASH_JOIN_11_0.in_index as delimscan_rowid_15,
                         LINEAGE_{0}_HASH_JOIN_11_1.out_index as temp26_out_index
                  from LINEAGE_{0}_HASH_JOIN_11_0 left join LINEAGE_{0}_HASH_JOIN_11_1 on (LINEAGE_{0}_HASH_JOIN_11_0.out_address=LINEAGE_{0}_HASH_JOIN_11_1.lhs_address)
                      left join (select LINEAGE_{0}_HASH_JOIN_3_1.rhs_index as partsupp_rowid_1,
                                        LINEAGE_{0}_HASH_JOIN_3_0.in_index as supplier_rowid_2,
                                        LINEAGE_{0}_HASH_JOIN_5_0.in_index as nation_rowid_4,
                                        LINEAGE_{0}_SEQ_SCAN_6_0.in_index as region_rowid_6,
                                        LINEAGE_{0}_HASH_GROUP_BY_9_1.out_index as temp15_out_index
                                  from LINEAGE_{0}_HASH_GROUP_BY_9_1, LINEAGE_{0}_HASH_GROUP_BY_9_0, LINEAGE_{0}_HASH_JOIN_7_1, LINEAGE_{0}_HASH_JOIN_7_0,
                                        LINEAGE_{0}_HASH_JOIN_5_1, LINEAGE_{0}_HASH_JOIN_5_0, LINEAGE_{0}_HASH_JOIN_3_1, LINEAGE_{0}_HASH_JOIN_3_0,
                                        LINEAGE_{0}_SEQ_SCAN_6_0
                                  where LINEAGE_{0}_HASH_GROUP_BY_9_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_9_0.out_index
                                    and LINEAGE_{0}_HASH_JOIN_7_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_9_0.in_index
                                    and LINEAGE_{0}_HASH_JOIN_7_0.out_address=LINEAGE_{0}_HASH_JOIN_7_1.lhs_address
                                    and LINEAGE_{0}_HASH_JOIN_7_1.rhs_index=LINEAGE_{0}_HASH_JOIN_5_1.out_index
                                    and LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address
                                    and LINEAGE_{0}_HASH_JOIN_5_1.rhs_index=LINEAGE_{0}_HASH_JOIN_3_1.out_index
                                    and LINEAGE_{0}_HASH_JOIN_3_0.out_address=LINEAGE_{0}_HASH_JOIN_3_1.lhs_address
                                    and LINEAGE_{0}_HASH_JOIN_7_0.in_index=LINEAGE_{0}_SEQ_SCAN_6_0.out_index) as temp_15
                            on (LINEAGE_{0}_HASH_JOIN_11_1.rhs_index=temp_15.temp15_out_index)) as temp_26
                            on (LINEAGE_{0}_HASH_JOIN_13_0.in_index=temp_26.temp26_out_index), LINEAGE_{0}_HASH_JOIN_26_1, LINEAGE_{0}_HASH_JOIN_26_0,
                            LINEAGE_{0}_HASH_JOIN_22_1, LINEAGE_{0}_HASH_JOIN_22_0, LINEAGE_{0}_HASH_JOIN_18_1, LINEAGE_{0}_HASH_JOIN_18_0,
                            LINEAGE_{0}_HASH_JOIN_21_1, LINEAGE_{0}_HASH_JOIN_21_0, LINEAGE_{0}_SEQ_SCAN_20_0, LINEAGE_{0}_FILTER_24_0, LINEAGE_{0}_SEQ_SCAN_23_0
    WHERE LINEAGE_{0}_ORDER_BY_31_0.out_index=LINEAGE_{0}_LIMIT_32_0.in_index and LINEAGE_{0}_FILTER_28_0.out_index=LINEAGE_{0}_ORDER_BY_31_0.in_index
      and LINEAGE_{0}_HASH_JOIN_13_1.out_index=LINEAGE_{0}_FILTER_28_0.in_index and LINEAGE_{0}_HASH_JOIN_13_1.rhs_index=LINEAGE_{0}_HASH_JOIN_26_1.out_index
      and LINEAGE_{0}_HASH_JOIN_26_0.out_address=LINEAGE_{0}_HASH_JOIN_26_1.lhs_address and LINEAGE_{0}_HASH_JOIN_26_1.rhs_index=LINEAGE_{0}_HASH_JOIN_22_1.out_index
      and LINEAGE_{0}_HASH_JOIN_22_0.out_address=LINEAGE_{0}_HASH_JOIN_22_1.lhs_address and LINEAGE_{0}_HASH_JOIN_22_1.rhs_index=LINEAGE_{0}_HASH_JOIN_18_1.out_index
      and LINEAGE_{0}_HASH_JOIN_18_0.out_address=LINEAGE_{0}_HASH_JOIN_18_1.lhs_address and LINEAGE_{0}_HASH_JOIN_22_0.in_index=LINEAGE_{0}_HASH_JOIN_21_1.out_index
      and LINEAGE_{0}_HASH_JOIN_21_0.out_address=LINEAGE_{0}_HASH_JOIN_21_1.lhs_address and LINEAGE_{0}_HASH_JOIN_21_0.in_index=LINEAGE_{0}_SEQ_SCAN_20_0.out_index
      and LINEAGE_{0}_HASH_JOIN_26_0.in_index=LINEAGE_{0}_FILTER_24_0.out_index and LINEAGE_{0}_SEQ_SCAN_23_0.out_index=LINEAGE_{0}_FILTER_24_0.in_index
