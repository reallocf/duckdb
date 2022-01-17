with end_to_end as (
  SELECT LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as supplier_rowid_0,
         LINEAGE_{0}_SEQ_SCAN_1_0.in_index as nation_rowid_1, temp_17.*,
         LINEAGE_{0}_HASH_JOIN_17_1.rhs_index as partsupp_rowid_13,
         LINEAGE_{0}_SEQ_SCAN_14_0.in_index as part_rowid_14, LINEAGE_{0}_ORDER_BY_23_0.out_index
  FROM LINEAGE_{0}_ORDER_BY_23_0, LINEAGE_{0}_HASH_JOIN_21_1, LINEAGE_{0}_HASH_JOIN_21_0, LINEAGE_{0}_HASH_JOIN_2_1,
       LINEAGE_{0}_HASH_JOIN_2_0, LINEAGE_{0}_SEQ_SCAN_1_0, LINEAGE_{0}_FILTER_19_0,
       LINEAGE_{0}_HASH_JOIN_10_1 left join LINEAGE_{0}_HASH_JOIN_10_0 on (LINEAGE_{0}_HASH_JOIN_10_0.out_address=LINEAGE_{0}_HASH_JOIN_10_1.lhs_address)
      left join (select temp_12.*, LINEAGE_{0}_HASH_JOIN_8_0.in_index as delimscan_rowid_12, LINEAGE_{0}_HASH_JOIN_8_1.out_index as temp17_out_index from LINEAGE_{0}_HASH_JOIN_8_0 left join LINEAGE_{0}_HASH_JOIN_8_1 on (LINEAGE_{0}_HASH_JOIN_8_0.out_address=LINEAGE_{0}_HASH_JOIN_8_1.lhs_address) left join (select LINEAGE_{0}_SEQ_SCAN_4_0.in_index as lineitem_rowid_4, LINEAGE_{0}_HASH_GROUP_BY_6_1.out_index as temp12_out_index from LINEAGE_{0}_HASH_GROUP_BY_6_1, LINEAGE_{0}_HASH_GROUP_BY_6_0, LINEAGE_{0}_SEQ_SCAN_4_0 where LINEAGE_{0}_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.out_index and LINEAGE_{0}_SEQ_SCAN_4_0.out_index=LINEAGE_{0}_HASH_GROUP_BY_6_0.in_index) as temp_12 on (LINEAGE_{0}_HASH_JOIN_8_1.rhs_index=temp_12.temp12_out_index)) as temp_17 on (LINEAGE_{0}_HASH_JOIN_10_0.in_index=temp_17.temp17_out_index), LINEAGE_{0}_HASH_JOIN_17_1, LINEAGE_{0}_HASH_JOIN_17_0, LINEAGE_{0}_FILTER_15_0, LINEAGE_{0}_SEQ_SCAN_14_0 WHERE LINEAGE_{0}_HASH_JOIN_21_1.out_index=LINEAGE_{0}_ORDER_BY_23_0.in_index and LINEAGE_{0}_HASH_JOIN_21_0.out_address=LINEAGE_{0}_HASH_JOIN_21_1.lhs_address and LINEAGE_{0}_HASH_JOIN_21_1.rhs_index=LINEAGE_{0}_HASH_JOIN_2_1.out_index and LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address and LINEAGE_{0}_HASH_JOIN_2_0.in_index=LINEAGE_{0}_SEQ_SCAN_1_0.out_index and LINEAGE_{0}_HASH_JOIN_21_0.in_index=LINEAGE_{0}_FILTER_19_0.out_index and LINEAGE_{0}_HASH_JOIN_10_1.out_index=LINEAGE_{0}_FILTER_19_0.in_index and LINEAGE_{0}_HASH_JOIN_10_1.rhs_index=LINEAGE_{0}_HASH_JOIN_17_1.out_index and LINEAGE_{0}_HASH_JOIN_17_0.out_address=LINEAGE_{0}_HASH_JOIN_17_1.lhs_address and LINEAGE_{0}_HASH_JOIN_17_0.in_index=LINEAGE_{0}_FILTER_15_0.out_index and LINEAGE_{0}_SEQ_SCAN_14_0.out_index=LINEAGE_{0}_FILTER_15_0.in_index
), original as (
  select s_name, s_nationkey, n_nationkey, p_partkey,  ps_partkey, 
        l_partkey, s_suppkey, l_suppkey, ps_suppkey
        from end_to_end left join lineitem on (lineitem_rowid_4=lineitem.rowid)
             left join  nation on (nation_rowid_1=nation.rowid)
             left join supplier on (supplier_rowid_0=supplier.rowid)
             left join  partsupp on (partsupp_rowid_13=partsupp.rowid)
             left join  part on (part_rowid_14=part.rowid)
), final_count as (
select 274
)
select * from end_to_end;
