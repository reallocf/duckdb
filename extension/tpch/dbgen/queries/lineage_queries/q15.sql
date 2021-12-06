with end_to_end as (
  select  LINEAGE_0_HASH_JOIN_4_1.rhs_index as supplier_rowid_opid_0,
          LINEAGE_0_SEQ_SCAN_1_0.in_index as lineitem_rowid_opid_1,
          temp_opio13.*, LINEAGE_0_ORDER_BY_16_0.out_index
  FROM  LINEAGE_0_ORDER_BY_16_0,
        LINEAGE_0_HASH_JOIN_14_1,
        LINEAGE_0_HASH_JOIN_14_0,
        LINEAGE_0_HASH_JOIN_4_1,
        LINEAGE_0_HASH_JOIN_4_0,
        LINEAGE_0_HASH_GROUP_BY_3_0,
        LINEAGE_0_SEQ_SCAN_1_0,
        (SELECT temp_opio10.*, 0 as out_index
          from LINEAGE_0_LIMIT_11_0,
                (SELECT LINEAGE_0_SEQ_SCAN_5_0.in_index as lineitem_rowid_opid_5, 0 as out_index
                  from LINEAGE_0_HASH_GROUP_BY_7_0, LINEAGE_0_SEQ_SCAN_5_0
                  where LINEAGE_0_SEQ_SCAN_5_0.out_index=LINEAGE_0_HASH_GROUP_BY_7_0.in_index
              ) as temp_opio10 where temp_opio10.out_index=LINEAGE_0_LIMIT_11_0.in_index
        ) as temp_opio13
  where LINEAGE_0_HASH_JOIN_14_1.out_index=LINEAGE_0_ORDER_BY_16_0.in_index
    and LINEAGE_0_HASH_JOIN_14_0.out_address=LINEAGE_0_HASH_JOIN_14_1.lhs_address
    and LINEAGE_0_HASH_JOIN_14_1.rhs_index=LINEAGE_0_HASH_JOIN_4_1.out_index
    and LINEAGE_0_HASH_JOIN_4_0.out_address=LINEAGE_0_HASH_JOIN_4_1.lhs_address
    and LINEAGE_0_HASH_JOIN_4_0.in_index=LINEAGE_0_HASH_GROUP_BY_3_0.out_index
    and LINEAGE_0_SEQ_SCAN_1_0.out_index=LINEAGE_0_HASH_GROUP_BY_3_0.in_index
    and LINEAGE_0_HASH_JOIN_14_0.in_index=temp_opio13.out_index
), original as (
  select l1.l_suppkey, s_suppkey, l2.l_suppkey
  from end_to_end, supplier, lineitem as l1, lineitem as l2
  where supplier.rowid=supplier_rowid_opid_0
  and l1.rowid=lineitem_rowid_opid_1
  and l2.rowid=lineitem_rowid_opid_5
), final_count as (
select 7456481
)

select * from end_to_end;
