with end_to_end as (SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as orders_rowid_0,
      LINEAGE_{0}_FILTER_2_0.in_index as lineitem_rowid_1,
      LINEAGE_{0}_ORDER_BY_7_0.out_index
FROM LINEAGE_{0}_ORDER_BY_7_0, LINEAGE_{0}_HASH_GROUP_BY_6_1,
    LINEAGE_{0}_HASH_GROUP_BY_6_0, LINEAGE_{0}_HASH_JOIN_4_1,
    LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_SEQ_SCAN_0_0,
    LINEAGE_{0}_FILTER_2_0
WHERE LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index=LINEAGE_{0}_ORDER_BY_7_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index
  and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
  and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_SEQ_SCAN_0_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_0.in_index=LINEAGE_{0}_FILTER_2_0.out_index
), original as (
select o_orderkey, l_orderkey, l_commitdate, l_receiptdate
from end_to_end, lineitem, orders
where lineitem.rowid=lineitem_rowid_1 and orders.rowid=orders_rowid_0
)
select * from end_to_end
