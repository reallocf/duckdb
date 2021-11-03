import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

# 13 right
# q16, q22 mark join segmentation fault!
# q17 delim join
# q04, q18 semi join
# q20, 21 delim + semi
q3 = "extension/tpch/dbgen/queries/q18.sql"
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
print(con.execute("select * from seq_scan_6_7").fetchdf())
print(con.execute("select * from hash_join_4_7_sink").fetchdf())

pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT rhs_address, sink.rowid as rowid_orders, sink.rowid as out_rowid
FROM hash_join_4_7_sink as sink
ORDER BY out_rowid)
"""
execute(pipeline1)
print(con.execute("select * from pipeline1").fetchdf())

print("*********** 2nd pipeline *************")
print(con.execute("select * from seq_scan_5_7").fetchdf())
print(con.execute("select * from hash_join_4_7_probe").fetchdf())

pipeline2 = """
CREATE TABLE pipeline2 AS (SELECT p.lhs_value, p.lhs_value+(1024*seq.in_chunk_id) as rowid_lineitem, rowid_orders, p.out_index as out_index, p.out_chunk_id
from hash_join_4_7_probe as p, pipeline1 as p1, seq_scan_5_7 as seq
where p.rhs_address=p1.rhs_address and p.probe_idx=seq.out_chunk_id)
"""
execute(pipeline2)
print(con.execute("select * from pipeline2").fetchdf())
#print(con.execute("select l_orderkey, o_orderkey from pipeline2, lineitem, orders where lineitem.rowid=rowid_lineitem and orders.rowid=rowid_orders").fetchdf())

print("*********** 3rd pipeline *************")
print(con.execute("select * from seq_scan_7_7").fetchdf())
print(con.execute("select * from hash_join_3_7_sink").fetchdf())

pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT rhs_address, sink.rowid as rowid_customer, sink.rowid as out_rowid
FROM hash_join_3_7_sink as sink
ORDER BY out_rowid)
"""
execute(pipeline3)
print(con.execute("select * from pipeline3").fetchdf())

print("*********** 4th pipeline *************")
print(con.execute("select * from hash_join_3_7_probe").fetchdf())

pipeline2 = """
CREATE TABLE pipeline4 AS (SELECT rowid_lineitem, rowid_orders, rowid_customer, p.out_index as out_index, p.out_chunk_id
from hash_join_3_7_probe as p, pipeline2 as p2, pipeline3 as p3
where p.rhs_address=p3.rhs_address and p2.lhs_value=p.lhs_value and p.probe_idx=p2.out_chunk_id)
"""
execute(pipeline2)
print(con.execute("select * from pipeline4").fetchdf())
#print(con.execute("select l_orderkey, o_orderkey, o_custkey, c_custkey from pipeline4, lineitem, orders, customer where customer.rowid=rowid_customer and lineitem.rowid=rowid_lineitem and orders.rowid=rowid_orders").fetchdf())

print("*********** 5th pipeline *************")
print(con.execute("select * from seq_scan_12_7").fetchdf())
print(con.execute("select * from hash_group_by_10_7_sink").fetchdf())
# 8190
pipeline5 = """
CREATE TABLE pipeline5 AS (SELECT group_id as out_index, sink.in_index+(1024*seq.in_chunk_id) as rowid_lineitem2
FROM hash_group_by_10_7_sink sink, seq_scan_12_7 as seq
where seq.out_chunk_id=sink.out_chunk_id)

"""
execute(pipeline5)
print(con.execute("SELECT * FROM pipeline5").fetchdf())
#print(con.execute("select l_orderkey  from pipeline5, lineitem where lineitem.rowid=rowid_lineitem2").fetchdf())

print("*********** 6th pipeline *************")
print(con.execute("select * from filter_9_7").fetchdf())
print(con.execute("select * from hash_group_by_10_7_probe").fetchdf())
pipeline6 = """
CREATE TABLE pipeline6 AS (SELECT  p.group_id, rowid_lineitem2, f.out_index, f.out_chunk_id
FROM filter_9_7 as f, pipeline5 as p5, hash_group_by_10_7_probe as p
where f.in_index=p.out_index and f.out_chunk_id=p.out_chunk_id
and p.group_id=p5.out_index
)
"""
execute(pipeline6)
print(con.execute("SELECT * FROM pipeline6").fetchdf())
#print(con.execute("select l_orderkey, pipeline6.group_id  from pipeline6, lineitem where lineitem.rowid=rowid_lineitem2").fetchdf())

print("*********** 7th pipeline *************")
print(con.execute("select * from hash_join_2_7_sink").fetchdf())

pipeline7 = """
CREATE TABLE pipeline7 AS (SELECT rhs_address, rowid_lineitem2
FROM hash_join_2_7_sink as sink, pipeline6 as p6 where sink.out_chunk_id=p6.out_chunk_id
and p6.out_index=sink.rhs_value)
"""
execute(pipeline7)
print(con.execute("select * from pipeline7").fetchdf())

print("*********** 8th pipeline *************")
print(con.execute("select * from hash_join_2_7_probe").fetchdf())
pipeline8 = """
CREATE TABLE pipeline8 AS (SELECT rowid_lineitem2, rowid_lineitem, rowid_orders, rowid_customer,  p.out_index as out_index, p.out_chunk_id
from hash_join_2_7_probe as p, pipeline7 as p7, pipeline4 as p4
where p.rhs_address=p7.rhs_address and p4.out_index=p.lhs_value and p4.out_chunk_id=p.probe_idx)
"""
execute(pipeline8)
print(con.execute("select * from pipeline8").fetchdf())
#print(con.execute("select c_name, l1.l_orderkey, l2.l_orderkey,  o_orderkey, o_custkey, c_custkey from pipeline8, lineitem as l1, lineitem as l2, orders, customer where customer.rowid=rowid_customer and l2.rowid=rowid_lineitem2 and l1.rowid=rowid_lineitem and orders.rowid=rowid_orders").fetchdf())

print("*********** 9th pipeline *************")
print(con.execute("select * from hash_group_by_10_7_sink").fetchdf())
# 8190
pipeline9 = """
CREATE TABLE pipeline9 AS (SELECT group_id as out_index, rowid_lineitem2, rowid_lineitem, rowid_orders, rowid_customer
FROM hash_group_by_0_7_sink sink, pipeline8 as p8
where p8.out_chunk_id=sink.out_chunk_id and p8.out_index=sink.in_index)

"""
execute(pipeline9)
print(con.execute("SELECT * FROM pipeline9").fetchdf())
print(con.execute("select c_name, l1.l_orderkey, l2.l_orderkey,  o_orderkey, o_custkey, c_custkey from pipeline9, lineitem as l1, lineitem as l2, orders, customer where customer.rowid=rowid_customer and l2.rowid=rowid_lineitem2 and l1.rowid=rowid_lineitem and orders.rowid=rowid_orders and out_index=0").fetchdf())
