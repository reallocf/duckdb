with end_to_end as (SELECT LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as lineitem_rowid_0,
       LINEAGE_{0}_HASH_JOIN_2_0.in_index as partsupp_rowid_1,
       LINEAGE_{0}_HASH_JOIN_4_0.in_index as orders_rowid_3,
       LINEAGE_{0}_HASH_JOIN_7_1.rhs_index as supplier_rowid_5,
       LINEAGE_{0}_HASH_JOIN_7_0.in_index as nation_rowid_6,
       LINEAGE_{0}_FILTER_10_0.in_index as part_rowid_9,
       LINEAGE_{0}_ORDER_BY_16_0.out_index
FROM LINEAGE_{0}_ORDER_BY_16_0, LINEAGE_{0}_HASH_GROUP_BY_15_1,
     LINEAGE_{0}_HASH_GROUP_BY_15_0, LINEAGE_{0}_HASH_JOIN_12_1,
     LINEAGE_{0}_HASH_JOIN_12_0, LINEAGE_{0}_HASH_JOIN_8_1,
     LINEAGE_{0}_HASH_JOIN_8_0, LINEAGE_{0}_HASH_JOIN_4_1,
     LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_HASH_JOIN_2_1,
    LINEAGE_{0}_HASH_JOIN_2_0, LINEAGE_{0}_HASH_JOIN_7_1, LINEAGE_{0}_HASH_JOIN_7_0,
    LINEAGE_{0}_FILTER_10_0
WHERE LINEAGE_{0}_HASH_GROUP_BY_15_1.out_index=LINEAGE_{0}_ORDER_BY_16_0.in_index
and LINEAGE_{0}_HASH_GROUP_BY_15_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_15_0.out_index
and LINEAGE_{0}_HASH_JOIN_12_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_15_0.in_index
and LINEAGE_{0}_HASH_JOIN_12_0.out_address=LINEAGE_{0}_HASH_JOIN_12_1.lhs_address
and LINEAGE_{0}_HASH_JOIN_12_1.rhs_index=LINEAGE_{0}_HASH_JOIN_8_1.out_index
and LINEAGE_{0}_HASH_JOIN_8_0.out_address=LINEAGE_{0}_HASH_JOIN_8_1.lhs_address
and LINEAGE_{0}_HASH_JOIN_8_1.rhs_index=LINEAGE_{0}_HASH_JOIN_4_1.out_index
and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address
and LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=LINEAGE_{0}_HASH_JOIN_2_1.out_index
and LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address
and LINEAGE_{0}_HASH_JOIN_8_0.in_index=LINEAGE_{0}_HASH_JOIN_7_1.out_index
and LINEAGE_{0}_HASH_JOIN_7_0.out_address=LINEAGE_{0}_HASH_JOIN_7_1.lhs_address
and LINEAGE_{0}_HASH_JOIN_12_0.in_index=LINEAGE_{0}_FILTER_10_0.out_index
), original as (
select out_index, l_partkey, ps_partkey, l_suppkey, ps_suppkey, l_orderkey, o_orderkey
from end_to_end, lineitem, partsupp, orders
where lineitem_rowid_0=lineitem.rowid and partsupp.rowid=partsupp_rowid_1 and orders.rowid=orders_rowid_3
)

select * from original
