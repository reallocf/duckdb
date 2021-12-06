with HJ_12 as (
  select probe.rhs_index as lineitem_rowid, seq_part.in_index as part_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_12_0 as sink,
       LINEAGE_0_HASH_JOIN_12_1 as probe,
       LINEAGE_0_SEQ_SCAN_11_0 as seq_part
  WHERE sink.out_address=probe.lhs_address
    and seq_part.out_index=sink.in_index
), HG_8 as (
  SELECT op_input.part_rowid, op_input.lineitem_rowid, gb_sink.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_8_0 as gb_sink,
       HJ_12 as op_input
  WHERE gb_sink.in_index=op_input.out_index
), HJ_5 as (
  select hgb_sink_lineitem.in_index as lineitem_rowid_2,  lineitem_rowid, part_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_5_0 as sink full outer join
       LINEAGE_0_HASH_JOIN_5_1 as probe on (sink.out_address=probe.lhs_address),
       LINEAGE_0_HASH_GROUP_BY_3_0 as hgb_sink_lineitem,
       LINEAGE_0_HASH_GROUP_BY_3_1 as hgb_scan_lineitem,
       HG_8
  WHERE  HG_8.out_index=sink.in_index
    and probe.rhs_index=hgb_scan_lineitem.out_index
    and hgb_sink_lineitem.out_index=hgb_scan_lineitem.in_index
), end_to_end as (
  select HJ_12.lineitem_rowid as lineitem_rowid_3, HJ_12.part_rowid as part_rowid_3,
         HJ_5.lineitem_rowid_2, HJ_5.lineitem_rowid, HJ_5.part_rowid, filter_top.out_index
  from LINEAGE_0_HASH_JOIN_7_0 as sink full outer join
       LINEAGE_0_HASH_JOIN_7_1 as probe on (sink.out_address=probe.lhs_address),
       LINEAGE_0_FILTER_14_0 as filter_top,
       HJ_12, HJ_5
  WHERE  sink.in_index=HJ_5.out_index
      and probe.rhs_index=HJ_12.out_index
      and filter_top.in_index=probe.out_index
), test as (
select temp_opio17.*, 0 as out_index
FROM (SELECT temp_9.*,
            LINEAGE_0_HASH_JOIN_5_0.in_index as delimscan_rowid_opid_9,
            LINEAGE_0_HASH_JOIN_12_1.rhs_index as lineitem_rowid_opid_10,
            LINEAGE_0_SEQ_SCAN_11_0.in_index as part_rowid_opid_11,
            0 as out_index
      from LINEAGE_0_FILTER_14_0,
          LINEAGE_0_HASH_JOIN_7_1 left join
          LINEAGE_0_HASH_JOIN_7_0 on (LINEAGE_0_HASH_JOIN_7_0.out_address=LINEAGE_0_HASH_JOIN_7_1.lhs_address)
          left join LINEAGE_0_HASH_JOIN_5_1
            on (LINEAGE_0_HASH_JOIN_7_0.in_index=LINEAGE_0_HASH_JOIN_5_1.out_index)
          left join LINEAGE_0_HASH_JOIN_5_0
           on (LINEAGE_0_HASH_JOIN_5_0.out_address=LINEAGE_0_HASH_JOIN_5_1.lhs_address)
          left join (select LINEAGE_0_HASH_GROUP_BY_3_0.in_index as lineitem_rowid_opid_1,
                            LINEAGE_0_HASH_GROUP_BY_3_0.out_index as out_index
                      from LINEAGE_0_HASH_GROUP_BY_3_0
                  ) as temp_9
            on (LINEAGE_0_HASH_JOIN_5_1.rhs_index=temp_9.out_index),
            LINEAGE_0_HASH_JOIN_12_1, LINEAGE_0_HASH_JOIN_12_0, LINEAGE_0_SEQ_SCAN_11_0
        where LINEAGE_0_HASH_JOIN_7_1.out_index=LINEAGE_0_FILTER_14_0.in_index
        and LINEAGE_0_HASH_JOIN_7_1.rhs_index=LINEAGE_0_HASH_JOIN_12_1.out_index
        and LINEAGE_0_HASH_JOIN_12_0.out_address=LINEAGE_0_HASH_JOIN_12_1.lhs_address
        and LINEAGE_0_HASH_JOIN_12_0.in_index=LINEAGE_0_SEQ_SCAN_11_0.out_index) as temp_opio17 
), original as (
    SELECT p_partkey, l1.l_partkey, l2.l_partkey, p_brand, p_container
    FROM test, lineitem as l1, part, lineitem as l2
    WHERE l1.rowid=lineitem_rowid_opid_1
          and l2.rowid=lineitem_rowid_opid_10
          and part.rowid=part_rowid_opid_11
), final_count as (
select 25317
)

select * from test
