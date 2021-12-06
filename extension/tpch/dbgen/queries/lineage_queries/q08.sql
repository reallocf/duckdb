with HJ_5 as (
  select probe.rhs_index as nation_rowid, seq_region.in_index as region_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_5_0 as sink,
       LINEAGE_0_HASH_JOIN_5_1 as probe,
       LINEAGE_0_SEQ_SCAN_4_0 as seq_region
  WHERE sink.out_address=probe.lhs_address
    and seq_region.out_index=sink.in_index
), HJ_6 as (
  select nation_rowid, region_rowid, probe.rhs_index as customer_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_6_0 as sink,
       LINEAGE_0_HASH_JOIN_6_1 as probe,
       HJ_5
  WHERE sink.out_address=probe.lhs_address
      and HJ_5.out_index=sink.in_index
), HJ_7 as (
  select nation_rowid, customer_rowid, region_rowid, seq_orders.in_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_7_0 as sink,
       LINEAGE_0_HASH_JOIN_7_1 as probe,
       HJ_6,
       LINEAGE_0_SEQ_SCAN_1_0 as seq_orders
  WHERE sink.out_address=probe.lhs_address
      and HJ_6.out_index=sink.in_index
      and seq_orders.out_index=probe.rhs_index
), HJ_8 as (
  select nation_rowid, customer_rowid, region_rowid, orders_rowid,
         probe.rhs_index as lineitem_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_8_0 as sink,
       LINEAGE_0_HASH_JOIN_8_1 as probe,
       HJ_7
  WHERE sink.out_address=probe.lhs_address
      and HJ_7.out_index=sink.in_index
), HJ_11 as (
  select probe.rhs_index as supplier_rowid, sink.in_index as nation_rowid_2, probe.out_index
  from LINEAGE_0_HASH_JOIN_11_0 as sink,
       LINEAGE_0_HASH_JOIN_11_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_12 as (
  select supplier_rowid, nation_rowid_2, nation_rowid, customer_rowid, region_rowid, orders_rowid, lineitem_rowid,
         probe.out_index
  from LINEAGE_0_HASH_JOIN_12_0 as sink,
       LINEAGE_0_HASH_JOIN_12_1 as probe,
       HJ_11, HJ_8
  WHERE sink.out_address=probe.lhs_address
    and HJ_11.out_index=sink.in_index
    and HJ_8.out_index=probe.rhs_index
), HJ_14 as (
  select supplier_rowid, nation_rowid_2, nation_rowid, customer_rowid, region_rowid, orders_rowid, lineitem_rowid,
        seq_part.in_index as part_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_14_0 as sink,
       LINEAGE_0_HASH_JOIN_14_1 as probe,
       LINEAGE_0_SEQ_SCAN_13_0 as seq_part,
       HJ_12
  WHERE sink.out_address=probe.lhs_address
    and seq_part.out_index=sink.in_index
    and HJ_12.out_index=probe.rhs_index
),  end_to_end as (
  select supplier_rowid, nation_rowid_2, nation_rowid, customer_rowid, region_rowid, orders_rowid, lineitem_rowid,
         part_rowid, orderby.out_index
  FROM LINEAGE_0_PERFECT_HASH_GROUP_BY_17_0 as gb_sink,
       LINEAGE_0_PERFECT_HASH_GROUP_BY_17_1 as gb_scan,
       HJ_14 as op_input,
       LINEAGE_0_ORDER_BY_19_0 as orderby
  WHERE gb_scan.out_index=orderby.in_index
    and gb_sink.in_index=op_input.out_index
    AND gb_scan.in_index=gb_sink.out_index
), original as (
  SELECT l_orderkey, o_orderkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey,
         n1.n_nationkey, n2.n_nationkey, r_name, p_type
  FROM end_to_end, lineitem, customer, orders, supplier, nation n1, nation n2, part, region
  WHERE lineitem.rowid=lineitem_rowid
    and orders.rowid=orders_rowid
    and customer.rowid=customer_rowid
    and supplier.rowid=supplier_rowid
    and n1.rowid=nation_rowid
    and n2.rowid=nation_rowid_2
    and part.rowid=part_rowid
    and region.rowid=region_rowid
), final_count as (
select 2602
)

SELECT * FROM end_to_end
