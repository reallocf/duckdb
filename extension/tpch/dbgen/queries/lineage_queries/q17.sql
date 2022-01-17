with end_to_end as (
  SELECT temp_opio17.*, 0 as out_index
  FROM (SELECT temp_12.*, LINEAGE_{0}_HASH_JOIN_12_1.rhs_index as lineitem_rowid_10,
              LINEAGE_{0}_SEQ_SCAN_11_0.in_index as part_rowid_11, 0 as out_index
        from LINEAGE_{0}_FILTER_14_0, LINEAGE_{0}_HASH_JOIN_7_1
            left join LINEAGE_{0}_HASH_JOIN_7_0 on (LINEAGE_{0}_HASH_JOIN_7_0.out_address=LINEAGE_{0}_HASH_JOIN_7_1.lhs_address)
            left join (select temp_9.*, LINEAGE_{0}_HASH_JOIN_5_0.in_index as delimscan_rowid_9,
                              LINEAGE_{0}_HASH_JOIN_5_1.out_index as temp12_out_index
                      from LINEAGE_{0}_HASH_JOIN_5_0
                          left join LINEAGE_{0}_HASH_JOIN_5_1 on (LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address)
                          left join (select LINEAGE_{0}_HASH_GROUP_BY_3_0.in_index as lineitem_rowid_1,
                                            LINEAGE_{0}_HASH_GROUP_BY_3_1.out_index as temp9_out_index
                                     from LINEAGE_{0}_HASH_GROUP_BY_3_1, LINEAGE_{0}_HASH_GROUP_BY_3_0
                                     where LINEAGE_{0}_HASH_GROUP_BY_3_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_3_0.out_index
                                    ) as temp_9
                            on (LINEAGE_{0}_HASH_JOIN_5_1.rhs_index=temp_9.temp9_out_index)
                        ) as temp_12 on (LINEAGE_{0}_HASH_JOIN_7_0.in_index=temp_12.temp12_out_index),
              LINEAGE_{0}_HASH_JOIN_12_1, LINEAGE_{0}_HASH_JOIN_12_0, LINEAGE_{0}_SEQ_SCAN_11_0
                where LINEAGE_{0}_HASH_JOIN_7_1.out_index=LINEAGE_{0}_FILTER_14_0.in_index
                  and LINEAGE_{0}_HASH_JOIN_7_1.rhs_index=LINEAGE_{0}_HASH_JOIN_12_1.out_index
                  and LINEAGE_{0}_HASH_JOIN_12_0.out_address=LINEAGE_{0}_HASH_JOIN_12_1.lhs_address
                  and LINEAGE_{0}_HASH_JOIN_12_0.in_index=LINEAGE_{0}_SEQ_SCAN_11_0.out_index) as temp_opio17
), original as (
    SELECT p_partkey, l2.l_partkey,l1.l_partkey, p_brand, p_container
    FROM end_to_end, lineitem as l1, part, lineitem as l2
    WHERE l1.rowid=lineitem_rowid_1
          and l2.rowid=lineitem_rowid_10
          and part.rowid=part_rowid_11
), final_count as (
select 18121
)

select * from end_to_end
