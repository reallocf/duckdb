with end_to_end as (
  SELECT LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as lineitem_rowid_0,
         LINEAGE_{0}_HASH_JOIN_2_0.in_index as orders_rowid_1,
         LINEAGE_{0}_HASH_JOIN_4_0.in_index as customer_rowid_3,
         LINEAGE_{0}_HASH_GROUP_BY_7_0.in_index as lineitem_rowid_5,
         LINEAGE_{0}_LIMIT_14_0.out_index
  FROM LINEAGE_{0}_LIMIT_14_0, LINEAGE_{0}_ORDER_BY_13_0,
       LINEAGE_{0}_HASH_GROUP_BY_12_1, LINEAGE_{0}_HASH_GROUP_BY_12_0,
       LINEAGE_{0}_HASH_JOIN_10_1, LINEAGE_{0}_HASH_JOIN_10_0,
       LINEAGE_{0}_HASH_JOIN_4_1, LINEAGE_{0}_HASH_JOIN_4_0,
       LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0,
       LINEAGE_{0}_FILTER_8_0, LINEAGE_{0}_HASH_GROUP_BY_7_1,
      LINEAGE_{0}_HASH_GROUP_BY_7_0
  WHERE LINEAGE_{0}_ORDER_BY_13_0.out_index=LINEAGE_{0}_LIMIT_14_0.in_index
    and LINEAGE_{0}_HASH_GROUP_BY_12_1.out_index=LINEAGE_{0}_ORDER_BY_13_0.in_index
    and LINEAGE_{0}_HASH_GROUP_BY_12_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_12_0.out_index
    and LINEAGE_{0}_HASH_JOIN_10_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_12_0.in_index
    and LINEAGE_{0}_HASH_JOIN_10_0.out_address=LINEAGE_{0}_HASH_JOIN_10_1.lhs_address
    and LINEAGE_{0}_HASH_JOIN_10_1.rhs_index=LINEAGE_{0}_HASH_JOIN_4_1.out_index
    and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
    and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_HASH_JOIN_2_1.out_index
    and LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address
    and LINEAGE_{0}_HASH_JOIN_10_0.in_index=LINEAGE_{0}_FILTER_8_0.out_index
    and LINEAGE_{0}_HASH_GROUP_BY_7_1.out_index=LINEAGE_{0}_FILTER_8_0.in_index
    and LINEAGE_{0}_HASH_GROUP_BY_7_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_7_0.out_index
), final_count as (
select 8434
), original as (
select l0.l_orderkey, o_orderkey, o_custkey, c_custkey, l5.l_orderkey, c_name
from end_to_end, lineitem as l0, lineitem as l5, customer, orders
where
l0.rowid=lineitem_rowid_0 and
l5.rowid=lineitem_rowid_5 and
customer.rowid=customer_rowid_3 and
orders.rowid=orders_rowid_1
)
select * from end_to_end;
