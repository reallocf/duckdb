import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

q3 = "extension/tpch/dbgen/queries/q04.sql"
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
#print(con.execute("select * from seq_scan_6_7").fetchdf())
#print(con.execute("select * from filter_5_7").fetchdf())
#print(con.execute("select * from hash_join_2_7_sink").fetchdf())

pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT rhs_address, filter.in_index+(1024*seq.in_chunk_id) as rowid_lineitem, filter.rowid as out_rowid
FROM hash_join_2_7_sink as sink, seq_scan_6_7 as seq, filter_5_7 as filter
WHERE sink.out_chunk_id=filter.out_chunk_id AND seq.out_chunk_id=filter.out_chunk_id
and sink.rhs_value=filter.out_index
ORDER BY out_rowid)
"""
execute(pipeline1)

print("*********** 2nd pipeline *************")

pipeline2 = """
CREATE TABLE pipeline2 AS (SELECT seq.in_index+(1024*seq.in_chunk_id) as rowid_orders, rowid_lineitem, p.out_index as out_index
from hash_join_2_7_probe as p, pipeline1 as p1, seq_scan_3_7 as seq
where p.rhs_address=p1.rhs_address and p.probe_idx=seq.out_chunk_id and p.lhs_value=seq.out_index)
"""
execute(pipeline2)

print("*********** 3rd pipeline *************")
pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT  rowid_orders, rowid_lineitem, sink.group_id as out_index
from pipeline2 as p2, hash_group_by_0_7_sink as sink
where p2.out_index=sink.in_index)
"""
execute(pipeline3)

print(con.execute("select o_orderkey, l_orderkey, o_orderpriority from orders, lineitem, pipeline3 where out_index=4 and orders.rowid=rowid_orders and lineitem.rowid=rowid_lineitem").fetchdf())
