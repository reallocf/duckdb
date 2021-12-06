with agg as (
  select  temp_opio14.*
  FROM  
        (SELECT temp_opio11.*, 0 as out_index from LINEAGE_0_LIMIT_12_0,
            (SELECT LINEAGE_0_HASH_JOIN_7_0.in_index as chunkscan_rowid_opid_6,
                    LINEAGE_0_SEQ_SCAN_5_0.in_index as customer_rowid_opid_5, 0 as out_index
              from LINEAGE_0_FILTER_8_0, LINEAGE_0_HASH_JOIN_7_1
                  left join LINEAGE_0_HASH_JOIN_7_0
                  on (LINEAGE_0_HASH_JOIN_7_0.out_address=LINEAGE_0_HASH_JOIN_7_1.lhs_address),
                  LINEAGE_0_SEQ_SCAN_5_0
              where LINEAGE_0_HASH_JOIN_7_1.out_index=LINEAGE_0_FILTER_8_0.in_index
                and LINEAGE_0_HASH_JOIN_7_1.rhs_index=LINEAGE_0_SEQ_SCAN_5_0.out_index
            ) as temp_opio11
          where temp_opio11.out_index=LINEAGE_0_LIMIT_12_0.in_index
        ) as temp_opio14
), no_agg as (
  select  LINEAGE_0_HASH_JOIN_2_0.in_index as chunkscan_rowid_opid_1,
          LINEAGE_0_HASH_JOIN_2_1.rhs_index as customer_rowid_opid_0,
          LINEAGE_0_ORDER_BY_21_0.out_index
  FROM  LINEAGE_0_ORDER_BY_21_0, LINEAGE_0_HASH_GROUP_BY_20_0,
        LINEAGE_0_HASH_JOIN_17_1,
        LINEAGE_0_PIECEWISE_MERGE_JOIN_15_0,
        LINEAGE_0_FILTER_3_0,
        LINEAGE_0_HASH_JOIN_2_1 left join LINEAGE_0_HASH_JOIN_2_0
          on (LINEAGE_0_HASH_JOIN_2_0.out_address=LINEAGE_0_HASH_JOIN_2_1.lhs_address)
  where LINEAGE_0_HASH_GROUP_BY_20_0.out_index=LINEAGE_0_ORDER_BY_21_0.in_index
    and LINEAGE_0_HASH_JOIN_17_1.out_index=LINEAGE_0_HASH_GROUP_BY_20_0.in_index
    and LINEAGE_0_HASH_JOIN_17_1.rhs_index=LINEAGE_0_PIECEWISE_MERGE_JOIN_15_0.out_index
    and LINEAGE_0_PIECEWISE_MERGE_JOIN_15_0.rhs_index=LINEAGE_0_FILTER_3_0.out_index
    and LINEAGE_0_HASH_JOIN_2_1.out_index=LINEAGE_0_FILTER_3_0.in_index
), original as (
  select c_phone from no_agg, customer
  where customer_rowid_opid_0=customer.rowid
)
select * from no_agg;
