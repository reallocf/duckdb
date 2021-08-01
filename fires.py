import pandas as pd
import duckdb

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('/tmp/fires_with_dropped_cols.csv')")

con.execute("SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR").fetchall()
con.execute("SELECT STATE, COUNT(*) FROM fires GROUP BY STATE").fetchall()
con.execute("SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR").fetchall()

##### Query # 1
con.execute("PRAGMA trace_lineage='ON'")
arr = con.execute("SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR").fetchdf()
print(arr)
con.execute("PRAGMA trace_lineage='OFF'")
queries_ids = con.execute("SELECT * FROM queries_list").fetchdf()
print("\n")
print(queries_ids)
print("\n")
con.execute("CREATE OR REPLACE VIEW lineage_5 AS SELECT sink.index AS input, out.index AS output, sink.chunk_id FROM HASH_GROUP_BY_5_0_OUT AS out, HASH_GROUP_BY_5_0_SINK AS sink WHERE out.value=sink.value")
bw = con.execute("SELECT input+range_start as input, output FROM lineage_5, seq_scan_5_0_range AS r WHERE r.chunk_id=lineage_5.chunk_id AND output=1").fetchdf()
print(bw)
for index, row in bw.iterrows():
    # get the actual record
    fw = con.execute("SELECT * FROM fires WHERE rowid="+str(row["input"])).fetchdf()
    print(fw)
    break

##### Query # 2
con.execute("PRAGMA trace_lineage='ON'")
arr = con.execute("SELECT STATE, COUNT(*) FROM fires GROUP BY STATE").fetchdf()
print(arr)
con.execute("PRAGMA trace_lineage='OFF'")
queries_ids = con.execute("SELECT * FROM queries_list").fetchdf()
print("\n")
print(queries_ids)
print("\n")
queries_ids = con.execute("PRAGMA show_tables").fetchdf()
print("\n")
print(queries_ids)
print("\n")
con.execute("CREATE OR REPLACE VIEW lineage_19 AS SELECT sink.index AS input, out.index AS output, sink.chunk_id FROM HASH_GROUP_BY_19_0_OUT AS out, HASH_GROUP_BY_19_0_SINK AS sink WHERE out.value=sink.value")
bw = con.execute("SELECT input+range_start as input, output FROM lineage_19, seq_scan_19_0_range AS r WHERE r.chunk_id=lineage_19.chunk_id AND output=1").fetchdf()
print(bw)
for index, row in bw.iterrows():
    # get the actual record
    fw = con.execute("SELECT * FROM fires WHERE rowid="+str(row["input"])).fetchdf()
    print(fw)
    break
