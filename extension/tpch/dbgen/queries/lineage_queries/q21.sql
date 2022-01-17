with end_to_end as (
  SELECT LINEAGE_{0}_FILTER_19_0.in_index as lineitem_rowid_18, LINEAGE_{0}_SEQ_SCAN_21_0.in_index as orders_rowid_21, LINEAGE_{0}_HASH_JOIN_25_1.rhs_index as supplier_rowid_23, LINEAGE_{0}_SEQ_SCAN_24_0.in_index as nation_rowid_24, LINEAGE_{0}_HASH_JOIN_13_1.rhs_index as delimscan_rowid_17, LINEAGE_{0}_HASH_JOIN_13_0.in_index as lineitem_rowid_12, LINEAGE_{0}_LIMIT_32_0.out_index FROM LINEAGE_{0}_LIMIT_32_0, LINEAGE_{0}_ORDER_BY_31_0, LINEAGE_{0}_HASH_GROUP_BY_30_1, LINEAGE_{0}_HASH_GROUP_BY_30_0, LINEAGE_{0}_HASH_JOIN_7_1, LINEAGE_{0}_HASH_JOIN_15_1, LINEAGE_{0}_HASH_JOIN_15_0, LINEAGE_{0}_HASH_JOIN_26_1, LINEAGE_{0}_HASH_JOIN_26_0, LINEAGE_{0}_HASH_JOIN_22_1, LINEAGE_{0}_HASH_JOIN_22_0, LINEAGE_{0}_FILTER_19_0, LINEAGE_{0}_SEQ_SCAN_21_0, LINEAGE_{0}_HASH_JOIN_25_1, LINEAGE_{0}_HASH_JOIN_25_0, LINEAGE_{0}_SEQ_SCAN_24_0, LINEAGE_{0}_HASH_JOIN_13_1, LINEAGE_{0}_HASH_JOIN_13_0 WHERE LINEAGE_{0}_ORDER_BY_31_0.out_index=LINEAGE_{0}_LIMIT_32_0.in_index and LINEAGE_{0}_HASH_GROUP_BY_30_1.out_index=LINEAGE_{0}_ORDER_BY_31_0.in_index and LINEAGE_{0}_HASH_GROUP_BY_30_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_30_0.out_index and LINEAGE_{0}_HASH_JOIN_7_1.out_index=LINEAGE_{0}_HASH_GROUP_BY_30_0.in_index and LINEAGE_{0}_HASH_JOIN_7_1.rhs_index=LINEAGE_{0}_HASH_JOIN_15_1.out_index and LINEAGE_{0}_HASH_JOIN_15_0.out_address=LINEAGE_{0}_HASH_JOIN_15_1.lhs_address and LINEAGE_{0}_HASH_JOIN_15_1.rhs_index=LINEAGE_{0}_HASH_JOIN_26_1.out_index and LINEAGE_{0}_HASH_JOIN_26_0.out_address=LINEAGE_{0}_HASH_JOIN_26_1.lhs_address and LINEAGE_{0}_HASH_JOIN_26_1.rhs_index=LINEAGE_{0}_HASH_JOIN_22_1.out_index and LINEAGE_{0}_HASH_JOIN_22_0.out_address=LINEAGE_{0}_HASH_JOIN_22_1.lhs_address and LINEAGE_{0}_HASH_JOIN_22_1.rhs_index=LINEAGE_{0}_FILTER_19_0.out_index and LINEAGE_{0}_HASH_JOIN_22_0.in_index=LINEAGE_{0}_SEQ_SCAN_21_0.out_index and LINEAGE_{0}_HASH_JOIN_26_0.in_index=LINEAGE_{0}_HASH_JOIN_25_1.out_index and LINEAGE_{0}_HASH_JOIN_25_0.out_address=LINEAGE_{0}_HASH_JOIN_25_1.lhs_address and LINEAGE_{0}_HASH_JOIN_25_0.in_index=LINEAGE_{0}_SEQ_SCAN_24_0.out_index and LINEAGE_{0}_HASH_JOIN_15_0.in_index=LINEAGE_{0}_HASH_JOIN_13_1.out_index and LINEAGE_{0}_HASH_JOIN_13_0.out_address=LINEAGE_{0}_HASH_JOIN_13_1.lhs_address
), original as (
  select 
  l12.l_orderkey, l18.l_orderkey, o_orderkey,
  l12.l_suppkey, l18.l_suppkey, s_suppkey,
  s_nationkey, n_nationkey
  from end_to_end, lineitem as l18, orders, supplier, nation, lineitem as l12
  where
  lineitem_rowid_18=l18.rowid and
  orders_rowid_21=orders.rowid and
  supplier_rowid_23=supplier.rowid and
  nation_rowid_24=nation.rowid and
  lineitem_rowid_12=l12.rowid
), out_count as (
select 1406
)
select * from end_to_end;
