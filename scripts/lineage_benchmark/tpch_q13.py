import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer
# q04, q18 semi join done

# 13 right join
q3 = "extension/tpch/dbgen/queries/q13.sql"
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
print(con.execute("select * from seq_scan_9_7").fetchdf())
print(con.execute("select * from hash_join_5_7_sink").fetchdf())

pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT rhs_address, sink.rowid as rowid_customer, sink.rowid as out_rowid
FROM hash_join_5_7_sink as sink
ORDER BY out_rowid)
"""
execute(pipeline1)
print(con.execute("select * from pipeline1").fetchdf())

print("*********** 2nd pipeline *************")
print(con.execute("select * from seq_scan_8_7").fetchdf())
print(con.execute("select * from filter_7_7").fetchdf())
print(con.execute("select * from hash_join_5_7_probe").fetchdf())

pipeline2 = """
CREATE TABLE pipeline2 AS (SELECT rowid_customer,  filter.in_index+(1024*filter.out_chunk_id) as rowid_orders, p.out_index as out_index, p.out_chunk_id
from pipeline1 as p1, hash_join_5_7_probe as p left join filter_7_7 as filter on (p.probe_idx=filter.out_chunk_id and  p.lhs_value=filter.out_index)
where p.rhs_address=p1.rhs_address)
"""
execute(pipeline2)
print(con.execute("select * from pipeline2").fetchdf())
print(con.execute("select o_custkey, c_custkey from pipeline2, customer, orders where customer.rowid=rowid_customer and orders.rowid=rowid_orders").fetchdf())

print("*********** 3rd pipeline *************")
print(con.execute("select * from perfect_hash_group_by_3_7_sink").fetchdf())
# 8190
pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT group_id as out_index, rowid_customer, rowid_orders
FROM perfect_hash_group_by_3_7_sink sink join pipeline2 as p2 on (p2.out_chunk_id=sink.out_chunk_id and p2.out_index=sink.in_index)
where sink.out_chunk_id=15)
"""
execute(pipeline3)
print(con.execute("SELECT * FROM pipeline3").fetchdf())
print(con.execute("select c_custkey from pipeline3, customer where customer.rowid=rowid_customer").fetchdf())

print("*********** 4th pipeline *************")
print(con.execute("select * from perfect_hash_group_by_3_7_probe").fetchdf())
print(con.execute("select * from hash_group_by_0_7_sink").fetchdf())
pipeline4 = """
CREATE TABLE pipeline4 AS (SELECT sink.group_id as out_index, rowid_customer, rowid_orders
FROM hash_group_by_0_7_sink sink, perfect_hash_group_by_3_7_probe as p, pipeline3 as p3
where p.out_chunk_id=sink.out_chunk_id and p.out_index=sink.in_index
and p3.out_index=p.group_id)
"""
execute(pipeline4)
print(con.execute("SELECT * FROM pipeline4").fetchdf())

print("*********** 5th pipeline *************")
print(con.execute("select * from hash_group_by_0_7_probe").fetchdf())
print(con.execute("select  c_custkey, out_index from pipeline4, customer where customer.rowid=rowid_customer").fetchdf())

#print(con.execute("select o_custkey, c_custkey from pipeline3, customer, orders where customer.rowid=rowid_customer and orders.rowid=rowid_orders and out_index=4").fetchdf())
