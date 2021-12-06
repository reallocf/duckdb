with HJ_2 as (
  select seq_lineitem.in_index as lineitem_rowid, seq_orders.in_index as orders_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_2_0 as sink,
       LINEAGE_0_HASH_JOIN_2_1 as probe,
       LINEAGE_0_SEQ_SCAN_1_0 as seq_orders,
       LINEAGE_0_SEQ_SCAN_0_0 as seq_lineitem
  WHERE sink.out_address=probe.lhs_address
        and seq_orders.out_index=sink.in_index
        and seq_lineitem.out_index=probe.rhs_index
), HJ_4 as (
  select lineitem_rowid, orders_rowid, seq_customer.in_index as customer_rowid, probe.out_index
  from LINEAGE_0_HASH_JOIN_4_0 as sink,
       LINEAGE_0_HASH_JOIN_4_1 as probe,
       HJ_2,
       LINEAGE_0_SEQ_SCAN_3_0 as seq_customer
  WHERE sink.out_address=probe.lhs_address
      and seq_customer.out_index=sink.in_index
      and HJ_2.out_index=probe.rhs_index
),  end_to_end as (
  SELECT op_input.lineitem_rowid, op_input.orders_rowid,
         op_input.customer_rowid, op_limit.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_6_0 as gb_sink,
       HJ_4 as op_input,
       LINEAGE_0_ORDER_BY_8_0 as orderby,
       LINEAGE_0_LIMIT_9_0 as op_limit
  WHERE gb_sink.in_index=op_input.out_index
    AND orderby.in_index=gb_sink.out_index
    AND op_limit.in_index=orderby.out_index
  ),
  original as (
    SELECT l_orderkey, o_orderkey, c_custkey, o_custkey, out_index
    FROM end_to_end, lineitem, customer, orders
    WHERE lineitem.rowid=lineitem_rowid
      and orders.rowid=orders_rowid
      and customer.rowid=customer_rowid
  ), final_count as (
  select 52
)

SELECT * FROM end_to_end;
