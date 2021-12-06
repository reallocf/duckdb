with HJ_4 as (
  select probe.rhs_index as nation_rowid, seq_region.in_index as region_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe,
       LINEAGE_0_SEQ_SCAN_3_0 as seq_region
  WHERE sink.out_address=probe.lhs_address
        and seq_region.out_index=sink.in_index
), HJ_5 as (
  select nation_rowid, region_rowid, probe.rhs_index as supplier_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_5_0 as sink,
       LINEAGE_0_HASH_JOIN_5_1 as probe,
       HJ_4
  WHERE sink.out_address=probe.lhs_address
      and HJ_4.out_index=sink.in_index
), HJ_6 as (
  select nation_rowid, region_rowid, supplier_rowid, probe.rhs_index as lineitem_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_6_0 as sink,
       LINEAGE_0_HASH_JOIN_6_1 as probe,
       HJ_5
  WHERE sink.out_address=probe.lhs_address
      and HJ_5.out_index=sink.in_index
), HJ_8 as (
  select nation_rowid, region_rowid, supplier_rowid, lineitem_rowid,
         seq_orders.in_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_8_0 as sink,
       LINEAGE_0_HASH_JOIN_8_1 as probe,
       HJ_6, LINEAGE_0_SEQ_SCAN_7_0 as seq_orders
  WHERE sink.out_address=probe.lhs_address
      and seq_orders.out_index=sink.in_index
      and HJ_6.out_index=probe.rhs_index
), HJ_10 as (
  select nation_rowid, region_rowid, supplier_rowid, lineitem_rowid,
         orders_rowid, sink.in_index as customer_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_10_0 as sink,
       LINEAGE_0_HASH_JOIN_10_1 as probe,
       HJ_8
  WHERE sink.out_address=probe.lhs_address
      and HJ_8.out_index=probe.rhs_index
),  end_to_end as (
  select nation_rowid, region_rowid, supplier_rowid, lineitem_rowid,
         orders_rowid, customer_rowid, orderby.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_12_0 as gb_sink,
       HJ_10 as op_input,
       LINEAGE_0_ORDER_BY_13_0 as orderby
  WHERE gb_sink.in_index=op_input.out_index
    AND orderby.in_index=gb_sink.out_index
  ),
  original as (
    SELECT l_orderkey, o_orderkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey,
           n_regionkey, r_regionkey, r_name
    FROM end_to_end, lineitem, customer, orders, supplier, nation, region
    WHERE lineitem.rowid=lineitem_rowid
      and orders.rowid=orders_rowid
      and customer.rowid=customer_rowid
      and supplier.rowid=supplier_rowid
      and nation.rowid=nation_rowid
      and region.rowid=region_rowid
  ), final_count as (
  select 7242
)

  SELECT * FROM end_to_end
