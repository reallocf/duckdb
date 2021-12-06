with HJ_4 as (
  select filter_lineitem.in_index as lineitem_rowid, seq_orders.in_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe,
       LINEAGE_0_SEQ_SCAN_0_0 as seq_orders,
       LINEAGE_0_FILTER_2_0 as filter_lineitem
  WHERE sink.out_address=probe.lhs_address
        and filter_lineitem.out_index=sink.in_index
        and seq_orders.out_index=probe.rhs_index
), end_to_end as (
  SELECT op_input.lineitem_rowid, op_input.orders_rowid,orderby.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_6_0 as gb_sink,
       HJ_4 as op_input,
       LINEAGE_0_ORDER_BY_7_0 as orderby
  WHERE gb_sink.in_index=op_input.out_index
    AND orderby.in_index=gb_sink.out_index
), original as (
    SELECT o_orderkey, o_orderkey
    FROM end_to_end, lineitem, orders
    WHERE lineitem.rowid=lineitem_rowid
      and orders.rowid=orders_rowid
  ), final_count as (
  select 52522
)

SELECT * FROM end_to_end;
