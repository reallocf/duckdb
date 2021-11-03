import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer
# q04, q18 semi join done
# 13 right
# 17 delim join + simple agg
# q07, q08, q09 supported, need testing
# q02 delim join
# q20, 21 delim + semi
# q06, q15 (bad cast), q14, q19 simple aggregate

# q16 mark

# q22 mark join, anti, piecewise segmentation fault!
# q11 simple aggregate + piecewise merge join
q3 = "extension/tpch/dbgen/queries/q02.sql"
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
print(con.execute("select * from seq_scan_10_7").fetchdf())
print(con.execute("select * from hash_join_9_7_sink").fetchdf())
print(con.execute("select * from hash_join_9_7_probe").fetchdf())

pipeline1 = """
CREATE TABLE pipeline1 AS (SELECT lhs_value+(1024*seq.in_chunk_id) as rowid_part, rhs_value, probe.out_index, probe.out_chunk_id
FROM hash_join_9_7_sink as sink, seq_scan_10_7 as seq,hash_join_9_7_probe as probe
WHERE sink.rhs_address=probe.rhs_address and seq.out_chunk_id=probe.probe_idx
)
"""
execute(pipeline1)
print(con.execute("select * from pipeline1").fetchdf())
print(con.execute("select p_brand, p_type, p_size from pipeline1, part where part.rowid=rowid_part").fetchdf())

print("*********** 2nd pipeline *************")
print(con.execute("select * from hash_join_4_7_sink").fetchdf())
print(con.execute("select * from hash_join_4_7_probe").fetchdf())
print(con.execute("select * from filter_8_7").fetchdf())
pipeline2 = """
CREATE TABLE pipeline2 AS (
SELECT probe.lhs_value+(1024*probe.probe_idx) as rowid_lineitem, seq.in_index+(1024*seq.in_chunk_id) as rowid_part, probe.rowid as out_rowid, probe.out_chunk_id
FROM hash_join_16_7_sink as sink, seq_scan_18_7 as seq, hash_join_16_7_probe as probe
where sink.out_chunk_id=seq.out_chunk_id and sink.rhs_value=seq.out_index
and probe.rhs_address=sink.rhs_address
ORDER BY out_rowid
)
"""
execute(pipeline2)
print(con.execute("select * from pipeline2").fetchdf())
print(con.execute("select l_partkey, p_partkey, l_quantity from pipeline2, lineitem, part where lineitem.rowid=rowid_lineitem and part.rowid=rowid_part").fetchdf())

print("*********** 3nd pipeline *************")
#print("--->", con.execute("select * from hash_group_by_14_7_sink").fetchdf())
#print("--->", con.execute("select * from hash_group_by_14_7_probe").fetchdf())

pipeline3 = """
CREATE TABLE pipeline3 AS (SELECT group_id as out_index, rowid_lineitem, rowid_part, sink.out_chunk_id
FROM hash_group_by_14_7_sink as sink, pipeline2 as p2
where sink.out_chunk_id=p2.out_chunk_id and sink.in_index=p2.out_rowid order by group_id)
"""
execute(pipeline3)
print(con.execute("select * from pipeline3").fetchdf())
print(con.execute("select l_partkey, p_partkey, l_quantity from pipeline3, lineitem, part where lineitem.rowid=rowid_lineitem and part.rowid=rowid_part and out_index=0").fetchdf())

print("*********** 4nd pipeline *************")
print(con.execute("select * from hash_join_9_7_sink").fetchdf())
print("--->", con.execute("select * from hash_join_9_7_probe").fetchdf())

pipeline4 = """
CREATE TABLE pipeline4 AS (
SELECT distinct rowid_lineitem,  rowid_lineitem2, rowid_part, probe.rowid as out_rowid, probe.out_chunk_id
FROM hash_join_9_7_sink as sink, pipeline1 as p1, hash_join_9_7_probe as probe, pipeline3 as p3
where sink.out_chunk_id=p3.out_chunk_id and sink.rhs_value=p3.out_index
and probe.rhs_address=sink.rhs_address
and probe.lhs_value=p1.out_index
and probe.probe_idx=p1.out_chunk_id
ORDER BY out_rowid
)
"""
execute(pipeline4)
print(con.execute("select * from pipeline4").fetchdf())
#print(con.execute("select l2.l_partkey, lineitem.l_partkey, p_partkey from pipeline4, lineitem, lineitem as l2, part where l2.rowid=rowid_lineitem2 and lineitem.rowid=rowid_lineitem and part.rowid=rowid_part").fetchdf())

print("*********** 5nd pipeline *************")
print(con.execute("select * from hash_join_6_7_sink").fetchdf())
print(con.execute("select * from hash_join_6_7_probe").fetchdf())
print(con.execute("select * from pipeline2").fetchdf())

pipeline5 = """
CREATE TABLE pipeline5 AS (
SELECT p4.rowid_lineitem,  p4.rowid_lineitem2, p4.rowid_part, p2.rowid_lineitem as rowid_lineitem3, p2.rowid_part as rowid_part2, probe.rowid as out_index, probe.out_chunk_id as out_chunk_id
FROM hash_join_6_7_sink as sink, pipeline4 as p4, hash_join_6_7_probe as probe, pipeline2 as p2
where sink.out_chunk_id=p4.out_chunk_id and sink.rhs_value=p4.out_rowid
and sink.rhs_address=probe.rhs_address
and probe.probe_idx=p2.out_chunk_id and probe.lhs_value=p2.out_rowid
)
"""
execute(pipeline5)
print(con.execute("select * from pipeline5").fetchdf())

print("*********** 6nd pipeline *************")
print(con.execute("select * from filter_4_7").fetchdf())
pipeline6 = """
CREATE TABLE pipeline6 AS (SELECT f.rowid as out_rowid, f.out_index as out_index, f.out_chunk_id as out_chunk_id, rowid_lineitem, rowid_lineitem2, rowid_part, rowid_lineitem3, rowid_part2
FROM filter_4_7 as f, pipeline5 as p5
where f.out_chunk_id=p5.out_chunk_id and f.in_index=p5.out_index
)
"""
execute(pipeline6)
print(con.execute("select * from pipeline6 order by out_rowid").fetchdf())
