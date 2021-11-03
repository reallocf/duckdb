import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

q3 = "extension/tpch/dbgen/queries/q01.sql"
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
print(con.execute("select * from seq_scan_3_7").fetchdf())
print(con.execute("select * from hash_group_by_0_7_sink").fetchdf())
pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT group_id as out_index, scan.in_index+(1024*scan.in_chunk_id) as rowid_lineitem
FROM hash_group_by_0_7_sink sink, seq_scan_3_7 scan
WHERE scan.out_index=sink.in_index
AND scan.out_chunk_id=sink.out_chunk_id)
"""
execute(pipeline1)
print(con.execute("SELECT * FROM pipeline1").fetchdf())
lineage = """
SELECT sum(l_quantity) as sub_base_price from lineitem, pipeline1
WHERE lineitem.rowid=pipeline1.rowid_lineitem
and pipeline1.out_index=3
"""
print(con.execute(lineage).fetchdf())
