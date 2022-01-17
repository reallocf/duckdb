with end_to_end as (
SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0,
       LINEAGE_{0}_SEQ_SCAN_1_0.in_index as orders_rowid_1,
       LINEAGE_{0}_HASH_JOIN_5_1.rhs_index as customer_rowid_3,
       LINEAGE_{0}_HASH_JOIN_5_0.in_index as nation_rowid_4,
       LINEAGE_{0}_LIMIT_11_0.out_index
FROM LINEAGE_{0}_LIMIT_11_0, LINEAGE_{0}_ORDER_BY_10_0, LINEAGE_{0}_HASH_GROUP_BY_8_1,
     LINEAGE_{0}_HASH_GROUP_BY_8_0, LINEAGE_{0}_HASH_JOIN_6_1, LINEAGE_{0}_HASH_JOIN_6_0,
     LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0, LINEAGE_{0}_SEQ_SCAN_0_0, LINEAGE_{0}_SEQ_SCAN_1_0, LINEAGE_{0}_HASH_JOIN_5_1, LINEAGE_{0}_HASH_JOIN_5_0 WHERE LINEAGE_{0}_ORDER_BY_10_0.out_index=LINEAGE_{0}_LIMIT_11_0.in_index and LINEAGE_{0}_HASH_GROUP_BY_8_1.out_index=LINEAGE_{0}_ORDER_BY_10_0.in_index and LINEAGE_{0}_HASH_GROUP_BY_8_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_8_0.out_index and LINEAGE_{0}_HASH_JOIN_6_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_8_0.in_index and LINEAGE_{0}_HASH_JOIN_6_0.out_address=LINEAGE_{0}_HASH_JOIN_6_1.lhs_address and LINEAGE_{0}_HASH_JOIN_6_1.rhs_index=LINEAGE_{0}_HASH_JOIN_2_1.out_index and LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address and LINEAGE_{0}_HASH_JOIN_2_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_0_0.out_index and LINEAGE_{0}_HASH_JOIN_2_0.in_index=LINEAGE_{0}_SEQ_SCAN_1_0.out_index and LINEAGE_{0}_HASH_JOIN_6_0.in_index=LINEAGE_{0}_HASH_JOIN_5_1.out_index and LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address
), original as (
  SELECT l_orderkey, o_orderkey, c_nationkey, n_nationkey, o_custkey, c_custkey, c_name
  FROM end_to_end, lineitem, customer, orders, nation n1
  WHERE lineitem.rowid=lineitem_rowid_0
    and orders.rowid=orders_rowid_1
    and customer.rowid=customer_rowid_3
    and n1.rowid=nation_rowid_4
), final_count as (
  select 274
)
select * from original;
