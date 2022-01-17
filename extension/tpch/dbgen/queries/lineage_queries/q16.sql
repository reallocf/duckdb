with end_to_end as (
  SELECT temp_6.*, LINEAGE_{0}_HASH_JOIN_6_1.rhs_index as partsupp_rowid_0,
        LINEAGE_{0}_HASH_JOIN_3_0.in_index as chunkscan_rowid_2, LINEAGE_{0}_HASH_JOIN_3_1.rhs_index as part_rowid_1,
        LINEAGE_{0}_ORDER_BY_15_0.out_index
  FROM LINEAGE_{0}_ORDER_BY_15_0, LINEAGE_{0}_HASH_GROUP_BY_14_1, LINEAGE_{0}_HASH_GROUP_BY_14_0, LINEAGE_{0}_FILTER_11_0,
       LINEAGE_{0}_HASH_JOIN_10_1
      left join LINEAGE_{0}_HASH_JOIN_10_0 on (LINEAGE_{0}_HASH_JOIN_10_0.out_address=LINEAGE_{0}_HASH_JOIN_10_1.lhs_address)
      left join (select LINEAGE_{0}_FILTER_8_0.in_index as supplier_rowid_7, LINEAGE_{0}_FILTER_8_0.out_index as temp6_out_index
                  from LINEAGE_{0}_FILTER_8_0) as temp_6
      on (LINEAGE_{0}_HASH_JOIN_10_0.in_index=temp_6.temp6_out_index), LINEAGE_{0}_HASH_JOIN_6_1, LINEAGE_{0}_HASH_JOIN_6_0,
      LINEAGE_{0}_FILTER_4_0, LINEAGE_{0}_HASH_JOIN_3_1
      left join LINEAGE_{0}_HASH_JOIN_3_0 on (LINEAGE_{0}_HASH_JOIN_3_0.out_address=LINEAGE_{0}_HASH_JOIN_3_1.lhs_address)
  WHERE LINEAGE_{0}_HASH_GROUP_BY_14_1.out_index=LINEAGE_{0}_ORDER_BY_15_0.in_index
    and LINEAGE_{0}_HASH_GROUP_BY_14_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_14_0.out_index
    and LINEAGE_{0}_FILTER_11_0.out_index=LINEAGE_{0}_HASH_GROUP_BY_14_0.in_index
    and LINEAGE_{0}_HASH_JOIN_10_1.out_index=LINEAGE_{0}_FILTER_11_0.in_index
    and LINEAGE_{0}_HASH_JOIN_10_1.rhs_index=LINEAGE_{0}_HASH_JOIN_6_1.out_index
    and LINEAGE_{0}_HASH_JOIN_6_0.out_address=LINEAGE_{0}_HASH_JOIN_6_1.lhs_address
    and LINEAGE_{0}_HASH_JOIN_6_0.in_index=LINEAGE_{0}_FILTER_4_0.out_index
    and LINEAGE_{0}_HASH_JOIN_3_1.out_index=LINEAGE_{0}_FILTER_4_0.in_index
), original as (
  select p_size, ps_partkey, p_partkey, ps_suppkey, s_suppkey
  from end_to_end full outer join supplier on (supplier.rowid=supplier_rowid_7), partsupp, part
  where partsupp.rowid=partsupp_rowid_0
    and part.rowid=part_rowid_1
), final_count as (
select 117749
)

select * from end_to_end;
