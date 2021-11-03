import duckdb
import pandas as pd
from timeit import default_timer as timer

q5 = "extension/tpch/dbgen/queries/q05.sql"
text_file = open(q5, "r")

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

con.execute("CALL dbgen(sf=0.01);")
con.execute("PRAGMA enable_profiling;")
con.execute("PRAGMA trace_lineage='ON'")

df = execute(tpch)
print("Query Result: ")
print(df)

con.execute("PRAGMA trace_lineage='OFF'")
con.execute("PRAGMA disable_profiling;")
print(con.execute("PRAGMA show_tables").fetchdf())

print("*********** 1st pipeline *************")
# by processing this within a pipeline, we avoid joining on out_chunk_id
print(con.execute("select * from hash_join_8_7_sink").fetchdf())
print(con.execute("select * from seq_scan_10_7").fetchdf()) # region
pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT rhs_address, in_index+(1024*in_chunk_id) as in_rowid, seq.rowid as out_rowid
FROM hash_join_8_7_sink as sink, seq_scan_10_7 as seq
WHERE sink.out_chunk_id=seq.out_chunk_id AND sink.rhs_value=seq.out_index
ORDER BY out_rowid)
"""
execute(pipeline1)

print(con.execute("select * from pipeline1").fetchdf())

print("*********** 2nd pipeline *************")
print(con.execute("select * from hash_join_6_7_sink").fetchdf())
print(con.execute("select * from hash_join_8_7_probe").fetchdf()) # supplier
pipeline2 = """
CREATE TABLE pipeline2 AS (SELECT sink.rhs_address, child.lhs_value as in_rowid_lhs, child.rhs_address as in_rowid_rhs, child.rowid as out_rowid, child.probe_idx
FROM hash_join_6_7_sink as sink, hash_join_8_7_probe as child
WHERE sink.out_chunk_id=child.out_chunk_id AND sink.rhs_value=child.out_index
ORDER BY out_rowid)
"""
execute(pipeline2)
print(con.execute("select * from pipeline2").fetchdf())

print("*********** 3rd pipeline *************")
print(con.execute("select * from hash_join_4_7_sink").fetchdf())
print(con.execute("select * from hash_join_6_7_probe").fetchdf())

pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT sink.rhs_address, child.lhs_value as in_rowid_lhs, child.rhs_address as in_rowid_rhs, child.rowid as out_rowid, child.probe_idx
FROM hash_join_4_7_sink as sink, hash_join_6_7_probe as child
WHERE sink.out_chunk_id=child.out_chunk_id AND sink.rhs_value=child.out_index
ORDER BY out_rowid)
"""
execute(pipeline3)
print(con.execute("select * from pipeline3").fetchdf())

print("*********** 4th pipeline *************")
print(con.execute("select * from hash_join_3_7_sink").fetchdf())
print(con.execute("select * from seq_scan_11_7").fetchdf()) # region
pipeline4 = """
CREATE TABLE pipeline4 AS (SELECT rhs_address, in_index+(1024*in_chunk_id) as in_rowid, seq.rowid as out_rowid
FROM hash_join_3_7_sink as sink, seq_scan_11_7 as seq
WHERE sink.out_chunk_id=seq.out_chunk_id AND sink.rhs_value=seq.out_index
ORDER BY out_rowid)
"""
execute(pipeline4)

print(con.execute("select * from pipeline4").fetchdf())

print("*********** 5th pipeline *************")
# why in_chunk_id reset?
print(con.execute("select * from hash_join_2_7_sink").fetchdf())
print(con.execute("select * from seq_scan_12_7").fetchdf()) # region
pipeline5 = """
CREATE TABLE pipeline5 AS (SELECT rhs_address, sink.rowid as in_rowid, sink.rowid as out_rowid
FROM hash_join_2_7_sink as sink
ORDER BY out_rowid)
"""
execute(pipeline5)

print(con.execute("select * from pipeline5").fetchdf())

print("*********** 6th pipeline *************")
print(con.execute("select * from hash_group_by_0_7_sink").fetchdf())
print(con.execute("select * from hash_join_2_7_probe").fetchdf())
print(con.execute("select * from hash_join_3_7_probe").fetchdf())
print(con.execute("select * from hash_join_4_7_probe").fetchdf())
print(con.execute("select * from hash_join_6_7_probe").fetchdf())
print(con.execute("select * from pipeline2").fetchdf())
print(con.execute("select * from seq_scan_9_7").fetchdf())

pipeline6 = """
CREATE TABLE pipeline6 AS (SELECT  group_id as out_rowid,  pip5.in_rowid as rowid_customer, pip4.in_rowid as rowid_orders, p4.lhs_value+(1024*seq5.in_chunk_id) as rowid_lineitem,
pip3.in_rowid_lhs+(1024*seq7.in_chunk_id) as rowid_supplier, pip2.in_rowid_lhs+(1024*seq9.in_chunk_id) as rowid_nation
FROM hash_join_2_7_probe p2, hash_group_by_0_7_sink gb, pipeline5 pip5,
hash_join_3_7_probe as p3, pipeline4 pip4, hash_join_4_7_probe as p4, seq_scan_5_7 as seq5, pipeline3 as pip3, seq_scan_7_7 as seq7, pipeline2 as pip2, seq_scan_9_7 as seq9
WHERE p2.out_index=gb.in_index
AND p2.out_chunk_id=gb.out_chunk_id
AND p2.rhs_address=pip5.rhs_address
AND p2.lhs_value=p3.out_index
AND p3.out_chunk_id=p2.probe_idx
AND p3.probe_idx=p4.out_chunk_id
AND p3.rhs_address=pip4.rhs_address
AND p3.lhs_value=p4.out_index
AND p4.probe_idx=seq5.out_chunk_id
AND p4.rhs_address=pip3.rhs_address
AND pip3.probe_idx=seq7.out_chunk_id
AND pip3.in_rowid_rhs=pip2.rhs_address
AND seq9.out_chunk_id=pip2.probe_idx
and group_id=0
)
"""
execute(pipeline6)
print(con.execute("SELECT * FROM pipeline6").fetchdf())
# why group id=x?
print(con.execute("""SELECT n_name, n_regionkey, s_nationkey, n_nationkey, l_suppkey, s_suppkey, l_orderkey, o_orderkey
FROM pipeline6, nation, orders,  supplier, lineitem
where rowid_orders=orders.rowid and rowid_supplier=supplier.rowid and rowid_lineitem=lineitem.rowid and rowid_nation=nation.rowid""").fetchdf())
# polynomial: 
