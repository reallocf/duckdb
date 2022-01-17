with end_to_end as (
SELECT temp_3.*, LINEAGE_{0}_HASH_JOIN_4_0.in_index as customer_rowid_3,
       LINEAGE_{0}_ORDER_BY_10_0.out_index
FROM LINEAGE_{0}_ORDER_BY_10_0, LINEAGE_{0}_HASH_GROUP_BY_9_1,
     LINEAGE_{0}_HASH_GROUP_BY_9_0, LINEAGE_{0}_HASH_GROUP_BY_6_1,
    LINEAGE_{0}_HASH_GROUP_BY_6_0, LINEAGE_{0}_HASH_JOIN_4_0
    left join LINEAGE_{0}_HASH_JOIN_4_1 on (LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address)
    left join (select LINEAGE_{0}_FILTER_1_0.in_index as orders_rowid_0, LINEAGE_{0}_FILTER_1_0.out_index as temp3_out_index
               from LINEAGE_{0}_FILTER_1_0) as temp_3
    on (LINEAGE_{0}_HASH_JOIN_4_1.rhs_index=temp_3.temp3_out_index)
WHERE LINEAGE_{0}_HASH_GROUP_BY_9_1.out_index=LINEAGE_{0}_ORDER_BY_10_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_9_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_9_0.out_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_9_0.in_index
  and LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index
  and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index
), original as (
  select o_custkey, c_custkey from end_to_end, orders, customer
  where orders.rowid=orders_rowid_0
    and customer.rowid=customer_rowid_3
), final_count as (
select 1533922
)

select * from end_to_end;
