with HJ_4 as (
  select probe.rhs_index as customer_rowid, sink.in_index as nation_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_5 as (
  select nation_rowid, customer_rowid, probe.rhs_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_5_0 as sink,
       LINEAGE_0_HASH_JOIN_5_1 as probe,
       HJ_4
  WHERE sink.out_address=probe.lhs_address
      and HJ_4.out_index=sink.in_index
), HJ_6 as (
  select nation_rowid, customer_rowid, orders_rowid, seq_lineitem.in_index as lineitem_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_6_0 as sink,
       LINEAGE_0_HASH_JOIN_6_1 as probe,
       HJ_5,
       LINEAGE_0_SEQ_SCAN_0_0 as seq_lineitem
  WHERE sink.out_address=probe.lhs_address
      and HJ_5.out_index=sink.in_index
      and seq_lineitem.out_index=probe.rhs_index
), HJ_9 as (
  select sink.in_index as nation_rowid_2, probe.rhs_index as supplier_rowid_2, probe.out_index
  from LINEAGE_0_HASH_JOIN_9_0 as sink,
       LINEAGE_0_HASH_JOIN_9_1 as probe
  WHERE sink.out_address=probe.lhs_address
), HJ_10 as (
  select nation_rowid, customer_rowid, orders_rowid, lineitem_rowid,
         nation_rowid_2, supplier_rowid_2, probe.out_index
  from LINEAGE_0_HASH_JOIN_10_0 as sink,
       LINEAGE_0_HASH_JOIN_10_1 as probe,
       HJ_9,
       HJ_6
  WHERE sink.out_address=probe.lhs_address
      and HJ_6.out_index=probe.rhs_index
      and HJ_9.out_index=sink.in_index
),  end_to_end as (
  select nation_rowid, customer_rowid, orders_rowid, lineitem_rowid,
         nation_rowid_2, supplier_rowid_2, orderby.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_15_0 as gb_sink,
       HJ_10 as op_input,
       LINEAGE_0_FILTER_11_0 as filter_hj,
       LINEAGE_0_ORDER_BY_16_0 as orderby
  WHERE gb_sink.in_index=filter_hj.out_index
    and op_input.out_index=filter_hj.in_index
    AND orderby.in_index=gb_sink.out_index
), original as (
  SELECT l_orderkey, o_orderkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey,
         n1.n_nationkey, n2.n_nationkey, n1.n_name, n2.n_name
  FROM end_to_end, lineitem, customer, orders, supplier, nation n1, nation n2
  WHERE lineitem.rowid=lineitem_rowid
    and orders.rowid=orders_rowid
    and customer.rowid=customer_rowid
    and supplier.rowid=supplier_rowid_2
    and n1.rowid=nation_rowid
    and n2.rowid=nation_rowid_2
), final_count as (
select 5923
)

SELECT * FROM original
