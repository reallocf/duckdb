SELECT LINEAGE_{0}_HASH_JOIN_8_1.rhs_index as lineitem_rowid_0,
       LINEAGE_{0}_SEQ_SCAN_1_0.in_index as orders_rowid_1,
       LINEAGE_{0}_HASH_JOIN_6_1.rhs_index as customer_rowid_2,
       LINEAGE_{0}_HASH_JOIN_5_1.rhs_index as nation_rowid_3,
       LINEAGE_{0}_SEQ_SCAN_4_0.in_index as region_rowid_4,
       LINEAGE_{0}_HASH_JOIN_11_1.rhs_index as supplier_rowid_9,
       LINEAGE_{0}_HASH_JOIN_11_0.in_index as nation_rowid_10,
       LINEAGE_{0}_SEQ_SCAN_13_0.in_index as part_rowid_13,
       LINEAGE_{0}_ORDER_BY_19_0.out_index
FROM LINEAGE_{0}_ORDER_BY_19_0, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_1,
     LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_0, LINEAGE_{0}_HASH_JOIN_14_1,
     LINEAGE_{0}_HASH_JOIN_14_0, LINEAGE_{0}_HASH_JOIN_12_1, LINEAGE_{0}_HASH_JOIN_12_0,
     LINEAGE_{0}_HASH_JOIN_8_1, LINEAGE_{0}_HASH_JOIN_8_0, LINEAGE_{0}_HASH_JOIN_7_1,
     LINEAGE_{0}_HASH_JOIN_7_0, LINEAGE_{0}_SEQ_SCAN_1_0, LINEAGE_{0}_HASH_JOIN_6_1,
     LINEAGE_{0}_HASH_JOIN_6_0, LINEAGE_{0}_HASH_JOIN_5_1, LINEAGE_{0}_HASH_JOIN_5_0,
     LINEAGE_{0}_SEQ_SCAN_4_0, LINEAGE_{0}_HASH_JOIN_11_1, LINEAGE_{0}_HASH_JOIN_11_0,
     LINEAGE_{0}_SEQ_SCAN_13_0
WHERE LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_1.out_index=LINEAGE_{0}_ORDER_BY_19_0.in_index
  and LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_0.out_index
  and LINEAGE_{0}_HASH_JOIN_14_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_17_0.in_index
  and LINEAGE_{0}_HASH_JOIN_14_0.out_address=LINEAGE_{0}_HASH_JOIN_14_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_14_1.rhs_index=LINEAGE_{0}_HASH_JOIN_12_1.out_index
  and LINEAGE_{0}_HASH_JOIN_12_0.out_address=LINEAGE_{0}_HASH_JOIN_12_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_12_1.rhs_index=LINEAGE_{0}_HASH_JOIN_8_1.out_index
  and LINEAGE_{0}_HASH_JOIN_8_0.out_address=LINEAGE_{0}_HASH_JOIN_8_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_8_0.in_index=LINEAGE_{0}_HASH_JOIN_7_1.out_index
  and LINEAGE_{0}_HASH_JOIN_7_0.out_address=LINEAGE_{0}_HASH_JOIN_7_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_7_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_1_0.out_index
  and LINEAGE_{0}_HASH_JOIN_7_0.in_index=LINEAGE_{0}_HASH_JOIN_6_1.out_index
  and LINEAGE_{0}_HASH_JOIN_6_0.out_address=LINEAGE_{0}_HASH_JOIN_6_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_6_0.in_index=LINEAGE_{0}_HASH_JOIN_5_1.out_index
  and LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_5_0.in_index=LINEAGE_{0}_SEQ_SCAN_4_0.out_index
  and LINEAGE_{0}_HASH_JOIN_12_0.in_index=LINEAGE_{0}_HASH_JOIN_11_1.out_index
  and LINEAGE_{0}_HASH_JOIN_11_0.out_address=LINEAGE_{0}_HASH_JOIN_11_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_14_0.in_index=LINEAGE_{0}_SEQ_SCAN_13_0.out_index
