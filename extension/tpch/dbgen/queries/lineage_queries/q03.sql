SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0,
       LINEAGE_{0}_SEQ_SCAN_1_0.in_index as orders_rowid_1,
       LINEAGE_{0}_SEQ_SCAN_3_0.in_index as customer_rowid_3,
       LINEAGE_{0}_LIMIT_9_0.out_index
FROM LINEAGE_{0}_LIMIT_9_0, LINEAGE_{0}_ORDER_BY_8_0, LINEAGE_{0}_HASH_GROUP_BY_6_1,
     LINEAGE_{0}_HASH_GROUP_BY_6_0, LINEAGE_{0}_HASH_JOIN_4_1, LINEAGE_{0}_HASH_JOIN_4_0,
     LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0, LINEAGE_{0}_SEQ_SCAN_0_0,
     LINEAGE_{0}_SEQ_SCAN_1_0, LINEAGE_{0}_SEQ_SCAN_3_0
WHERE LINEAGE_{0}_ORDER_BY_8_0.out_index=LINEAGE_{0}_LIMIT_9_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index=LINEAGE_{0}_ORDER_BY_8_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index
  and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_HASH_JOIN_2_1.out_index
  and LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_2_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_0_0.out_index
  and LINEAGE_{0}_HASH_JOIN_2_0.in_index=LINEAGE_{0}_SEQ_SCAN_1_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_0.in_index=LINEAGE_{0}_SEQ_SCAN_3_0.out_index

