with end_to_end as (
  SELECT scan_lineitem.in_index, orderby.out_index
  FROM LINEAGE_0_HASH_GROUP_BY_3_0 as gb_sink,
       LINEAGE_0_SEQ_SCAN_0_0 as scan_lineitem,
       LINEAGE_0_ORDER_BY_4_0 as orderby
  WHERE gb_sink.in_index=scan_lineitem.out_index
    AND orderby.in_index=gb_sink.out_index
  ),
  original as (
    SELECT l_returnflag, l_linestatus, out_index
    FROM end_to_end, lineitem
    WHERE lineitem.rowid=in_index
  ), final_count as (
  select 5916590
)

SELECT * FROM end_to_end;
