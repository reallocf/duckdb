with end_to_end as (
SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0, LINEAGE_{0}_HASH_JOIN_4_0.in_index as orders_rowid_3,
       LINEAGE_{0}_ORDER_BY_7_0.out_index
FROM LINEAGE_{0}_ORDER_BY_7_0, LINEAGE_{0}_HASH_GROUP_BY_6_1, LINEAGE_{0}_HASH_GROUP_BY_6_0, LINEAGE_{0}_HASH_JOIN_4_1,
     LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_FILTER_1_0, LINEAGE_{0}_SEQ_SCAN_0_0
WHERE LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index=LINEAGE_{0}_ORDER_BY_7_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index
  and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_FILTER_1_0.out_index
  and LINEAGE_{0}_SEQ_SCAN_0_0.out_index=LINEAGE_{0}_FILTER_1_0.in_index
), original as (
    SELECT l_orderkey, o_orderkey, l_shipmode, out_index
    FROM end_to_end, lineitem, orders
    WHERE lineitem.rowid=lineitem_rowid_0
      and orders.rowid=orders_rowid_3
), final_count as (
select 30987
)

SELECT * FROM original;
