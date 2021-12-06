with end_to_end as (
  select seq_lineitem.in_index as lineitem_rowid,
        sink.in_index as part_rowid,
        probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe,
       LINEAGE_0_SEQ_SCAN_0_0 as seq_lineitem,
       LINEAGE_0_FILTER_1_0 as filter_lineitem
  WHERE sink.out_address=probe.lhs_address
        and seq_lineitem.out_index=filter_lineitem.in_index
        and filter_lineitem.out_index=probe.rhs_index
), test as (
select temp_opio8.*,
      0 as out_index
FROM (SELECT LINEAGE_0_SEQ_SCAN_0_0.in_index as lineitem_rowid_opid_0,
            LINEAGE_0_HASH_JOIN_4_0.in_index as part_rowid_opid_3,
            0 as out_index
      from LINEAGE_0_FILTER_5_0, LINEAGE_0_HASH_JOIN_4_1,
          LINEAGE_0_HASH_JOIN_4_0, LINEAGE_0_FILTER_1_0,
          LINEAGE_0_SEQ_SCAN_0_0
      where LINEAGE_0_HASH_JOIN_4_1.out_index=LINEAGE_0_FILTER_5_0.in_index
        and LINEAGE_0_HASH_JOIN_4_0.out_address=LINEAGE_0_HASH_JOIN_4_1.lhs_address
        and LINEAGE_0_HASH_JOIN_4_1.rhs_index=LINEAGE_0_FILTER_1_0.out_index
        and LINEAGE_0_SEQ_SCAN_0_0.out_index=LINEAGE_0_FILTER_1_0.in_index
      ) as temp_opio8
), original as (
    SELECT l_partkey, p_partkey
    FROM test, lineitem, part
    WHERE lineitem.rowid=lineitem_rowid_opid_0
      and part.rowid=part_rowid_opid_3
), final_count as (
select 120
)

SELECT * FROM test;
