with HJ_18 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_18_0 as sink,
       LINEAGE_0_HASH_JOIN_18_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_21 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_21_0 as sink,
       LINEAGE_0_HASH_JOIN_21_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_22 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_22_0 as sink,
       LINEAGE_0_HASH_JOIN_22_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_26 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_26_0 as sink,
       LINEAGE_0_HASH_JOIN_26_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_1_end_to_end as (
  select HJ_18.rhs_index as partsupp_rowid,
         HJ_18.lhs_index as supplier_rowid,
         HJ_21.rhs_index as region_rowid,
         HJ_21.lhs_index as nation_rowid,
         HJ_26.rhs_index as part_rowid,
         HJ_26.out_index
  from HJ_26, HJ_22, HJ_21, HJ_18
  where HJ_18.out_index=HJ_22.lhs_index
    AND HJ_21.out_index=HJ_22.rhs_index
    AND HJ_22.out_index=HJ_26.lhs_index
), HJ_3 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_3_0 as sink,
       LINEAGE_0_HASH_JOIN_3_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_5 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_5_0 as sink,
       LINEAGE_0_HASH_JOIN_5_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_7 as (
  select rhs_index as lhs_index, sink.in_index as rhs_index, out_index
  from LINEAGE_0_HASH_JOIN_7_0 as sink,
       LINEAGE_0_HASH_JOIN_7_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_2_end_to_end as (
  select HJ_3.rhs_index as partsupp_rowid,
         HJ_3.lhs_index as supplier_rowid,
         HJ_5.rhs_index as nation_rowid,
         HJ_7.rhs_index as region_rowid,
         HJ_7.out_index
  from HJ_3, HJ_5, HJ_7
  where HJ_3.out_index=HJ_5.lhs_index
    AND HJ_5.out_index=HJ_7.lhs_index
  ), end_to_end as (
  select  LINEAGE_0_HASH_JOIN_3_1.rhs_index as partsupp_rowid_1,
          LINEAGE_0_HASH_JOIN_3_0.in_index as supplier_rowid_2,
          LINEAGE_0_HASH_JOIN_5_0.in_index as nation_rowid_4,
          LINEAGE_0_SEQ_SCAN_6_0.in_index as region_rowid_6,
          LINEAGE_0_HASH_JOIN_11_0.in_index as delimscan_rowid_15,
          LINEAGE_0_HASH_JOIN_18_1.rhs_index as partsupp_rowid_16,
          LINEAGE_0_HASH_JOIN_18_0.in_index as supplier_rowid_17,
          LINEAGE_0_HASH_JOIN_21_1.rhs_index as nation_rowid_19,
          LINEAGE_0_SEQ_SCAN_20_0.in_index as region_rowid_20,
          LINEAGE_0_SEQ_SCAN_23_0.in_index as part_rowid_23,
          LINEAGE_0_LIMIT_32_0.out_index
    FROM  LINEAGE_0_LIMIT_32_0, LINEAGE_0_ORDER_BY_31_0, LINEAGE_0_FILTER_28_0,
          LINEAGE_0_HASH_JOIN_11_1, LINEAGE_0_HASH_JOIN_11_0,
          LINEAGE_0_HASH_JOIN_13_1, LINEAGE_0_HASH_JOIN_13_0,
          LINEAGE_0_HASH_GROUP_BY_9_0,
          LINEAGE_0_HASH_JOIN_7_1, LINEAGE_0_HASH_JOIN_7_0, LINEAGE_0_HASH_JOIN_5_1,
          LINEAGE_0_HASH_JOIN_5_0, LINEAGE_0_HASH_JOIN_3_1, LINEAGE_0_HASH_JOIN_3_0,
          LINEAGE_0_SEQ_SCAN_6_0, LINEAGE_0_HASH_JOIN_26_1, LINEAGE_0_HASH_JOIN_26_0,
          LINEAGE_0_HASH_JOIN_22_1, LINEAGE_0_HASH_JOIN_22_0, LINEAGE_0_HASH_JOIN_18_1,
          LINEAGE_0_HASH_JOIN_18_0, LINEAGE_0_HASH_JOIN_21_1, LINEAGE_0_HASH_JOIN_21_0,
          LINEAGE_0_SEQ_SCAN_20_0, LINEAGE_0_FILTER_24_0, LINEAGE_0_SEQ_SCAN_23_0
      where LINEAGE_0_ORDER_BY_31_0.out_index=LINEAGE_0_LIMIT_32_0.in_index
        and LINEAGE_0_FILTER_28_0.out_index=LINEAGE_0_ORDER_BY_31_0.in_index
        and LINEAGE_0_HASH_JOIN_13_1.out_index=LINEAGE_0_FILTER_28_0.in_index
        and LINEAGE_0_HASH_JOIN_13_0.out_address=LINEAGE_0_HASH_JOIN_13_1.lhs_address
        and LINEAGE_0_HASH_JOIN_11_0.out_address=LINEAGE_0_HASH_JOIN_11_1.lhs_address
        and LINEAGE_0_HASH_JOIN_11_1.rhs_index=LINEAGE_0_HASH_GROUP_BY_9_0.out_index
        and LINEAGE_0_HASH_JOIN_7_1.out_index=LINEAGE_0_HASH_GROUP_BY_9_0.in_index
        and LINEAGE_0_HASH_JOIN_7_0.out_address=LINEAGE_0_HASH_JOIN_7_1.lhs_address
        and LINEAGE_0_HASH_JOIN_7_1.rhs_index=LINEAGE_0_HASH_JOIN_5_1.out_index
        and LINEAGE_0_HASH_JOIN_5_0.out_address=LINEAGE_0_HASH_JOIN_5_1.lhs_address
        and LINEAGE_0_HASH_JOIN_5_1.rhs_index=LINEAGE_0_HASH_JOIN_3_1.out_index
        and LINEAGE_0_HASH_JOIN_3_0.out_address=LINEAGE_0_HASH_JOIN_3_1.lhs_address
        and LINEAGE_0_HASH_JOIN_7_0.in_index=LINEAGE_0_SEQ_SCAN_6_0.out_index
        and LINEAGE_0_HASH_JOIN_26_0.out_address=LINEAGE_0_HASH_JOIN_26_1.lhs_address
        and LINEAGE_0_HASH_JOIN_26_1.rhs_index=LINEAGE_0_HASH_JOIN_22_1.out_index
        and LINEAGE_0_HASH_JOIN_22_0.out_address=LINEAGE_0_HASH_JOIN_22_1.lhs_address
        and LINEAGE_0_HASH_JOIN_22_1.rhs_index=LINEAGE_0_HASH_JOIN_18_1.out_index
        and LINEAGE_0_HASH_JOIN_18_0.out_address=LINEAGE_0_HASH_JOIN_18_1.lhs_address
        and LINEAGE_0_HASH_JOIN_22_0.in_index=LINEAGE_0_HASH_JOIN_21_1.out_index
        and LINEAGE_0_HASH_JOIN_21_0.out_address=LINEAGE_0_HASH_JOIN_21_1.lhs_address
        and LINEAGE_0_HASH_JOIN_21_0.in_index=LINEAGE_0_SEQ_SCAN_20_0.out_index
        and LINEAGE_0_HASH_JOIN_26_0.in_index=LINEAGE_0_FILTER_24_0.out_index
        and LINEAGE_0_SEQ_SCAN_23_0.out_index=LINEAGE_0_FILTER_24_0.in_index
        and LINEAGE_0_HASH_JOIN_13_1.rhs_index=LINEAGE_0_HASH_JOIN_26_1.out_index
        and LINEAGE_0_HASH_JOIN_13_0.in_index=LINEAGE_0_HASH_JOIN_11_1.out_index

order by out_index
), original as (
    SELECT  out_index, supplier.s_name, partsupp.ps_suppkey, supplier.s_suppkey,
            nation.n_regionkey, region.r_regionkey,
            part.p_partkey, partsupp.ps_partkey,
            partsupp.ps_supplycost, partsupp2.ps_supplycost,
            partsupp2.ps_suppkey, supplier2.s_suppkey,
            nation2.n_nationkey, supplier2.s_nationkey,
            region2.r_regionkey, nation2.n_regionkey
    FROM  end_to_end, partsupp, supplier, nation, region, part,
          partsupp as partsupp2, supplier as supplier2, nation as nation2,
          region as region2
    WHERE partsupp.rowid=partsupp_rowid_16
      and partsupp2.rowid=partsupp_rowid_1
      and supplier2.rowid=supplier_rowid_2
      and nation2.rowid=nation_rowid_4
      and region2.rowid=region_rowid_6
      and supplier.rowid=supplier_rowid_17
      and nation.rowid=nation_rowid_19
      and region.rowid=region_rowid_20
      and part.rowid=part_rowid_23
), final_count as (
select 124
)
select * from end_to_end;

