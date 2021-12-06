with end_to_end as (
  select  LINEAGE_0_FILTER_8_0.in_index as supplier_rowid_opid_7,
          LINEAGE_0_HASH_JOIN_6_1.rhs_index as partsupp_rowid_opid_0,
          LINEAGE_0_HASH_JOIN_3_0.in_index as chunkscan_rowid_opid_2,
          LINEAGE_0_HASH_JOIN_3_1.rhs_index as part_rowid_opid_1,
          LINEAGE_0_ORDER_BY_15_0.out_index
  FROM LINEAGE_0_ORDER_BY_15_0, LINEAGE_0_HASH_GROUP_BY_14_0, LINEAGE_0_FILTER_11_0,
    LINEAGE_0_HASH_JOIN_10_1 left join LINEAGE_0_HASH_JOIN_10_0 on (LINEAGE_0_HASH_JOIN_10_0.out_address=LINEAGE_0_HASH_JOIN_10_1.lhs_address)
    left join LINEAGE_0_FILTER_8_0 on (LINEAGE_0_HASH_JOIN_10_0.in_index=LINEAGE_0_FILTER_8_0.out_index),
    LINEAGE_0_HASH_JOIN_6_1, LINEAGE_0_HASH_JOIN_6_0,
    LINEAGE_0_FILTER_4_0, LINEAGE_0_HASH_JOIN_3_1 left join LINEAGE_0_HASH_JOIN_3_0 on (LINEAGE_0_HASH_JOIN_3_0.out_address=LINEAGE_0_HASH_JOIN_3_1.lhs_address)
  where LINEAGE_0_HASH_GROUP_BY_14_0.out_index=LINEAGE_0_ORDER_BY_15_0.in_index
    and LINEAGE_0_FILTER_11_0.out_index=LINEAGE_0_HASH_GROUP_BY_14_0.in_index
    and LINEAGE_0_HASH_JOIN_10_1.out_index=LINEAGE_0_FILTER_11_0.in_index
    and LINEAGE_0_HASH_JOIN_10_1.rhs_index=LINEAGE_0_HASH_JOIN_6_1.out_index
    and LINEAGE_0_HASH_JOIN_6_0.out_address=LINEAGE_0_HASH_JOIN_6_1.lhs_address
    and LINEAGE_0_HASH_JOIN_6_0.in_index=LINEAGE_0_FILTER_4_0.out_index
    and LINEAGE_0_HASH_JOIN_3_1.out_index=LINEAGE_0_FILTER_4_0.in_index
    order by out_index
), original as (
  select p_size, ps_partkey, p_partkey, ps_suppkey, s_suppkey
  from end_to_end full outer join supplier on (supplier.rowid=supplier_rowid_opid_7), partsupp, part
  where partsupp.rowid=partsupp_rowid_opid_0
    and part.rowid=part_rowid_opid_1
), final_count as (
select 117749
)

select * from end_to_end;
