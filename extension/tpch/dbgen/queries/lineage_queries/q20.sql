with end_to_end as (
select  LINEAGE_0_HASH_JOIN_2_1.rhs_index as supplier_rowid_opid_0,
        LINEAGE_0_SEQ_SCAN_1_0.in_index as nation_rowid_opid_1,
        temp_12.*,
        LINEAGE_0_HASH_JOIN_8_0.in_index as delimscan_rowid_opid_12,
        LINEAGE_0_HASH_JOIN_17_1.rhs_index as partsupp_rowid_opid_13,
        LINEAGE_0_SEQ_SCAN_14_0.in_index as part_rowid_opid_14,
        LINEAGE_0_ORDER_BY_23_0.out_index
FROM LINEAGE_0_ORDER_BY_23_0, LINEAGE_0_HASH_JOIN_21_1, LINEAGE_0_HASH_JOIN_21_0,
    LINEAGE_0_HASH_JOIN_2_1, LINEAGE_0_HASH_JOIN_2_0, LINEAGE_0_SEQ_SCAN_1_0,
    LINEAGE_0_FILTER_19_0,
    LINEAGE_0_HASH_JOIN_10_1
    left join LINEAGE_0_HASH_JOIN_10_0
    on (LINEAGE_0_HASH_JOIN_10_0.out_address=LINEAGE_0_HASH_JOIN_10_1.lhs_address)
    left join LINEAGE_0_HASH_JOIN_8_1
    on (LINEAGE_0_HASH_JOIN_10_0.in_index=LINEAGE_0_HASH_JOIN_8_1.out_index)
    left join LINEAGE_0_HASH_JOIN_8_0
    on (LINEAGE_0_HASH_JOIN_8_0.out_address=LINEAGE_0_HASH_JOIN_8_1.lhs_address)
    left join (select LINEAGE_0_SEQ_SCAN_4_0.in_index as lineitem_rowid_opid_4,
                      LINEAGE_0_HASH_GROUP_BY_6_0.out_index as out_index
                from LINEAGE_0_HASH_GROUP_BY_6_0, LINEAGE_0_SEQ_SCAN_4_0
                where LINEAGE_0_SEQ_SCAN_4_0.out_index=LINEAGE_0_HASH_GROUP_BY_6_0.in_index
              ) as temp_12
      on (LINEAGE_0_HASH_JOIN_8_1.rhs_index=temp_12.out_index),
      LINEAGE_0_HASH_JOIN_17_1, LINEAGE_0_HASH_JOIN_17_0,
      LINEAGE_0_FILTER_15_0, LINEAGE_0_SEQ_SCAN_14_0
where LINEAGE_0_HASH_JOIN_21_1.out_index=LINEAGE_0_ORDER_BY_23_0.in_index
  and LINEAGE_0_HASH_JOIN_21_0.out_address=LINEAGE_0_HASH_JOIN_21_1.lhs_address
  and LINEAGE_0_HASH_JOIN_21_1.rhs_index=LINEAGE_0_HASH_JOIN_2_1.out_index
  and LINEAGE_0_HASH_JOIN_2_0.out_address=LINEAGE_0_HASH_JOIN_2_1.lhs_address
  and LINEAGE_0_HASH_JOIN_2_0.in_index=LINEAGE_0_SEQ_SCAN_1_0.out_index
  and LINEAGE_0_HASH_JOIN_21_0.in_index=LINEAGE_0_FILTER_19_0.out_index
  and LINEAGE_0_HASH_JOIN_10_1.out_index=LINEAGE_0_FILTER_19_0.in_index and LINEAGE_0_HASH_JOIN_10_1.rhs_index=LINEAGE_0_HASH_JOIN_17_1.out_index and LINEAGE_0_HASH_JOIN_17_0.out_address=LINEAGE_0_HASH_JOIN_17_1.lhs_address and LINEAGE_0_HASH_JOIN_17_0.in_index=LINEAGE_0_FILTER_15_0.out_index and LINEAGE_0_SEQ_SCAN_14_0.out_index=LINEAGE_0_FILTER_15_0.in_index
  
), original as (
  select s_name, s_nationkey, n_nationkey, p_partkey,  ps_partkey, 
        l_partkey, s_suppkey, l_suppkey, ps_suppkey
        from end_to_end left join lineitem on (lineitem_rowid_opid_4=lineitem.rowid)
             left join  nation on (nation_rowid_opid_1=nation.rowid)
             left join supplier on (supplier_rowid_opid_0=supplier.rowid)
             left join  partsupp on (partsupp_rowid_opid_13=partsupp.rowid)
             left join  part on (part_rowid_opid_14=part.rowid)
), final_count as (
select 311
)
select * from original;
