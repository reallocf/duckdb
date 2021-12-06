with end_to_end as (
select temp_3.*,
    LINEAGE_0_HASH_JOIN_4_0.in_index as customer_rowid_opid_3,
    LINEAGE_0_ORDER_BY_10_0.out_index
FROM LINEAGE_0_ORDER_BY_10_0, LINEAGE_0_HASH_GROUP_BY_9_0,
    LINEAGE_0_HASH_GROUP_BY_6_0,
    LINEAGE_0_HASH_JOIN_4_0 left join LINEAGE_0_HASH_JOIN_4_1
    on (LINEAGE_0_HASH_JOIN_4_0.out_address=LINEAGE_0_HASH_JOIN_4_1.lhs_address)
    left join (select LINEAGE_0_FILTER_1_0.in_index as orders_rowid_opid_0,
                      LINEAGE_0_FILTER_1_0.out_index as out_index
                from LINEAGE_0_FILTER_1_0
              ) as temp_3
    on (LINEAGE_0_HASH_JOIN_4_1.rhs_index=temp_3.out_index)
where LINEAGE_0_HASH_GROUP_BY_9_0.out_index=LINEAGE_0_ORDER_BY_10_0.in_index
  and LINEAGE_0_HASH_GROUP_BY_6_0.out_index=LINEAGE_0_HASH_GROUP_BY_9_0.in_index
  and LINEAGE_0_HASH_JOIN_4_1.out_index=LINEAGE_0_HASH_GROUP_BY_6_0.in_index
  order by out_index
), original as (
  select o_custkey, c_custkey from end_to_end, orders, customer
  where orders.rowid=orders_rowid_opid_0
    and customer.rowid=customer_rowid_opid_3
), final_count as (
select 1533922
)

select * from end_to_end;
