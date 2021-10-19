import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

q3 = "extension/tpch/dbgen/queries/q03.sql"
text_file = open(q3, "r")

#read whole file to a string
tpch = text_file.read()
tpch = " ".join(tpch.split())

#close file
text_file.close()

def execute(Q):
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    print("Time in sec: ", end - start) 
    return df

con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CALL dbgen(sf=1);")
con.execute("PRAGMA enable_profiling;")
con.execute("PRAGMA trace_lineage='ON'")

df = execute(tpch)
print("Query Result: ")
print(df)

con.execute("PRAGMA trace_lineage='OFF'")
con.execute("PRAGMA disable_profiling;")
print(con.execute("PRAGMA show_tables").fetchdf())

print("*********** 1st pipeline *************")
# sink mainly needed when there are null values in the key, then it is not a one to one mapping
# min_offset = con.execute("select min(rhs_address) from hash_join_5_7_sink").fetchdf().iloc[0][0]

# by processing this within a pipeline, we avoid joining on out_chunk_id
pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT rhs_address, in_index+(1024*in_chunk_id) as in_rowid, seq.rowid as out_rowid
FROM hash_join_5_7_sink as sink, seq_scan_7_7 as seq
WHERE sink.out_chunk_id=seq.out_chunk_id AND sink.rhs_value=seq.out_index
ORDER BY out_rowid)
"""
execute(pipeline1)

print(con.execute("select * from pipeline1").fetchdf())

print("*********** 2nd pipeline *************")
# min_offset = con.execute("select min(rhs_address) from hash_join_4_7_sink").fetchdf().iloc[0][0]
pipeline2 = """
CREATE TABLE pipeline2 AS (SELECT rhs_address, in_index+(1024*in_chunk_id) as in_rowid, seq.rowid as out_rowid
FROM hash_join_4_7_sink as sink, seq_scan_8_7 as seq
WHERE sink.out_chunk_id=seq.out_chunk_id AND sink.rhs_value=seq.out_index
ORDER BY out_rowid)
"""
execute(pipeline2)
print(con.execute("select * from pipeline2").fetchdf())

print("*********** 3rd pipeline *************")

# process this within a pipeline to avoid three way join
pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT seq.in_index+(1024*seq.in_chunk_id) as in_rowid_t1, p1.rhs_address as in_rowid_t2, p2.rhs_address as in_rowid_t3
, group_id as out_rowid
FROM hash_join_5_7_probe p1, hash_join_4_7_probe p2, hash_group_by_2_7_sink sink, seq_scan_6_7 as seq
WHERE p2.out_chunk_id=p1.out_chunk_id
AND p2.lhs_value=p1.out_index
AND sink.out_chunk_id=p2.out_chunk_id
AND sink.in_index=p2.out_index
AND p2.probe_idx=p1.out_chunk_id
AND p1.probe_idx=seq.out_chunk_id
AND p1.lhs_value=seq.out_index
and group_id<10
)
"""
execute(pipeline3)
print(con.execute("SELECT * FROM pipeline3").fetchdf())

print("*********** LINEAG  *************")

print("t3: ")
lineage_t3 = """
CREATE TABLE lineage3 AS (SELECT  p2.in_rowid as in_rowid_t3, p3.out_rowid
FROM  pipeline2 as p2, pipeline3 as p3
where p2.rhs_address=p3.in_rowid_t3
)
"""
execute(lineage_t3)
print(con.execute("select * from lineage3").fetchdf())

print("t2: ")
lineage_t2 = """
CREATE TABLE lineage2 AS (SELECT  p1.in_rowid as in_rowid_t2, p3.out_rowid
FROM  pipeline1 as p1, pipeline3 as p3
where p1.rhs_address=p3.in_rowid_t2
)
"""
execute(lineage_t2)
print(con.execute("select * from lineage2").fetchdf())

lineage_all = """
CREATE TABLE lineage AS (SELECT p3.in_rowid_t1, p1.in_rowid as in_rowid_t2, p2.in_rowid as in_rowid_t3, p3.out_rowid
FROM pipeline1 as p1, pipeline2 as p2, pipeline3 as p3
WHERE p1.rhs_address=p3.in_rowid_t2
AND p2.rhs_address=p3.in_rowid_t3
)
"""
execute(lineage_all)
print(con.execute("select * from lineage").fetchdf())

lineage_lineitem = """
SELECT l_orderkey, o_orderdate, o_shippriority, o_custkey, c_custkey FROM lineitem, orders, customer, lineage
WHERE lineitem.rowid=lineage.in_rowid_t1
AND orders.rowid=lineage.in_rowid_t2
AND customer.rowid=lineage.in_rowid_t3
"""
print(execute(lineage_lineitem))

