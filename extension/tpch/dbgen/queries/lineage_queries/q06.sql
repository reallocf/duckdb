SELECT temp_opio2.*, 0 as out_index
FROM (SELECT LINEAGE_{0}_SEQ_SCAN_0_0.in_index as lineitem_rowid_0, 0 as out_index
      from LINEAGE_{0}_SEQ_SCAN_0_0) as temp_opio2
