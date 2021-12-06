with end_to_end as (
select  LINEAGE_0_SEQ_SCAN_0_0.in_index as lineitem_rowid_opid_0,
        LINEAGE_0_SEQ_SCAN_1_0.in_index as orders_rowid_opid_1,
        LINEAGE_0_HASH_JOIN_5_1.rhs_index as customer_rowid_opid_3,
        LINEAGE_0_HASH_JOIN_5_0.in_index as nation_rowid_opid_4,
        LINEAGE_0_LIMIT_11_0.out_index
FROM    LINEAGE_0_LIMIT_11_0,
        LINEAGE_0_ORDER_BY_10_0,
        LINEAGE_0_HASH_GROUP_BY_8_0,
        LINEAGE_0_HASH_JOIN_6_1,
        LINEAGE_0_HASH_JOIN_6_0,
        LINEAGE_0_HASH_JOIN_2_1,
        LINEAGE_0_HASH_JOIN_2_0,
        LINEAGE_0_SEQ_SCAN_0_0,
        LINEAGE_0_SEQ_SCAN_1_0,
        LINEAGE_0_HASH_JOIN_5_1,
        LINEAGE_0_HASH_JOIN_5_0
where   LINEAGE_0_ORDER_BY_10_0.out_index=LINEAGE_0_LIMIT_11_0.in_index
    and LINEAGE_0_HASH_GROUP_BY_8_0.out_index=LINEAGE_0_ORDER_BY_10_0.in_index
    and LINEAGE_0_HASH_JOIN_6_1.out_index=LINEAGE_0_HASH_GROUP_BY_8_0.in_index
    and LINEAGE_0_HASH_JOIN_6_0.out_address=LINEAGE_0_HASH_JOIN_6_1.lhs_address
    and LINEAGE_0_HASH_JOIN_6_1.rhs_index=LINEAGE_0_HASH_JOIN_2_1.out_index
    and LINEAGE_0_HASH_JOIN_2_0.out_address=LINEAGE_0_HASH_JOIN_2_1.lhs_address
    and LINEAGE_0_HASH_JOIN_2_1.rhs_index=LINEAGE_0_SEQ_SCAN_0_0.out_index
    and LINEAGE_0_HASH_JOIN_2_0.in_index=LINEAGE_0_SEQ_SCAN_1_0.out_index
    and LINEAGE_0_HASH_JOIN_6_0.in_index=LINEAGE_0_HASH_JOIN_5_1.out_index
    and LINEAGE_0_HASH_JOIN_5_0.out_address=LINEAGE_0_HASH_JOIN_5_1.lhs_address
), original as (
  SELECT l_orderkey, o_orderkey, c_nationkey, n_nationkey, o_custkey, c_custkey, c_name
  FROM end_to_end, lineitem, customer, orders, nation n1
  WHERE lineitem.rowid=lineitem_rowid_opid_0
    and orders.rowid=orders_rowid_opid_1
    and customer.rowid=customer_rowid_opid_3
    and n1.rowid=nation_rowid_opid_4
), final_count as (
select 167
)
select * from end_to_end;
