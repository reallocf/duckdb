with HJ_2 as (
  select probe.rhs_index as lineitem_rowid, sink.in_index as partsupp_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_2_0 as sink,
       LINEAGE_0_HASH_JOIN_2_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_4 as (
  select lineitem_rowid, partsupp_rowid, sink.in_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe,
       HJ_2
  WHERE sink.out_address=probe.lhs_address
      and HJ_2.out_index=probe.rhs_index
), HJ_7 as (
  select probe.rhs_index as supplier_rowid, sink.in_index as nation_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_7_0 as sink,
       LINEAGE_0_HASH_JOIN_7_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_8 as (
  select supplier_rowid, nation_rowid, lineitem_rowid, partsupp_rowid, 
         orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_8_0 as sink,
       LINEAGE_0_HASH_JOIN_8_1 as probe,
       HJ_7, HJ_4
  WHERE sink.out_address=probe.lhs_address
      and HJ_7.out_index=sink.in_index
      and HJ_4.out_index=probe.rhs_index
), HJ_12 as (
  select supplier_rowid, nation_rowid, lineitem_rowid, partsupp_rowid, 
         orders_rowid, filter_sink.in_index as part_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_12_0 as sink,
       LINEAGE_0_HASH_JOIN_12_1 as probe,
       HJ_8,
       LINEAGE_0_FILTER_10_0 as filter_sink
  WHERE sink.out_address=probe.lhs_address
    and filter_sink.out_index=sink.in_index
    and HJ_8.out_index=probe.rhs_index
),  end_to_end as (
  select supplier_rowid, nation_rowid, lineitem_rowid, partsupp_rowid, 
         orders_rowid, part_rowid, orderby.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_15_0 as gb_sink,
       HJ_12 as op_input,
       LINEAGE_0_ORDER_BY_16_0 as orderby
  WHERE gb_sink.in_index=op_input.out_index
    and orderby.in_index=gb_sink.out_index
), original as (
  SELECT ps_partkey, l_partkey, ps_suppkey, l_suppkey, o_orderkey, l_orderkey, out_index
  FROM HJ_4, lineitem, partsupp, orders
  WHERE lineitem.rowid=lineitem_rowid
    and partsupp.rowid=partsupp_rowid
    and orders.rowid=orders_rowid
), final_count as (
select 319364
)

SELECT * FROM end_to_end
