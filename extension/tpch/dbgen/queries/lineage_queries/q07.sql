SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0,
       LINEAGE_{0}_HASH_JOIN_5_1.rhs_index as orders_rowid_1,
       LINEAGE_{0}_HASH_JOIN_4_1.rhs_index as customer_rowid_2,
       LINEAGE_{0}_HASH_JOIN_4_0.in_index as nation_rowid_3,
       LINEAGE_{0}_HASH_JOIN_9_1.rhs_index as supplier_rowid_7,
       LINEAGE_{0}_HASH_JOIN_9_0.in_index as nation_rowid_8,
       LINEAGE_{0}_ORDER_BY_16_0.out_index
FROM LINEAGE_{0}_ORDER_BY_16_0, LINEAGE_{0}_HASH_GROUP_BY_15_1,
     LINEAGE_{0}_HASH_GROUP_BY_15_0, LINEAGE_{0}_FILTER_11_0,
     LINEAGE_{0}_HASH_JOIN_10_1, LINEAGE_{0}_HASH_JOIN_10_0,
     LINEAGE_{0}_HASH_JOIN_6_1, LINEAGE_{0}_HASH_JOIN_6_0,
     LINEAGE_{0}_SEQ_SCAN_0_0, LINEAGE_{0}_HASH_JOIN_5_1,
     LINEAGE_{0}_HASH_JOIN_5_0, LINEAGE_{0}_HASH_JOIN_4_1,
     LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_HASH_JOIN_9_1,
     LINEAGE_{0}_HASH_JOIN_9_0
WHERE LINEAGE_{0}_HASH_GROUP_BY_15_1.out_index=LINEAGE_{0}_ORDER_BY_16_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_15_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_15_0.out_index
  and LINEAGE_{0}_FILTER_11_0.out_index=LINEAGE_{0}_HASH_GROUP_BY_15_0.in_index
  and LINEAGE_{0}_HASH_JOIN_10_1.out_index=LINEAGE_{0}_FILTER_11_0.in_index
  and LINEAGE_{0}_HASH_JOIN_10_0.out_address=LINEAGE_{0}_HASH_JOIN_10_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_10_1.rhs_index=LINEAGE_{0}_HASH_JOIN_6_1.out_index
  and LINEAGE_{0}_HASH_JOIN_6_0.out_address=LINEAGE_{0}_HASH_JOIN_6_1.lhs_address 
  and LINEAGE_{0}_HASH_JOIN_6_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_0_0.out_index
  and LINEAGE_{0}_HASH_JOIN_6_0.in_index=LINEAGE_{0}_HASH_JOIN_5_1.out_index
  and LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_5_0.in_index=LINEAGE_{0}_HASH_JOIN_4_1.out_index
  and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_10_0.in_index=LINEAGE_{0}_HASH_JOIN_9_1.out_index
  and LINEAGE_{0}_HASH_JOIN_9_0.out_address=LINEAGE_{0}_HASH_JOIN_9_1.lhs_address
