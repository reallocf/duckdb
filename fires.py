import time
import pandas as pd
import duckdb

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('/tmp/fires_with_dropped_cols.csv')")

# enable lineage
#con.execute("PRAGMA enable_profiling")
con.execute("PRAGMA trace_lineage='ON'")
Q1 = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR"
Q2 = "SELECT STATE, COUNT(*) FROM fires GROUP BY STATE"
Q3 = "SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR"

# run queries
Q1_out = con.execute(Q1).fetchdf()
Q2_out = con.execute(Q2).fetchdf()
Q3_out = con.execute(Q3).fetchdf()

# disable lineage
con.execute("PRAGMA trace_lineage='OFF'")

# get queries' ids
q1_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q1)).fetchdf().iloc[0,0]
q2_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q2)).fetchdf().iloc[0,0]
q3_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q3)).fetchdf().iloc[0,0]

tables_list = con.execute("PRAGMA show_tables").fetchdf()
print(tables_list)

def backward_lineage_groupby(qid, predicate):
    '''
    Test lineage for group by
    '''
    con.execute("CREATE OR REPLACE VIEW lineage_{} AS SELECT index AS input, value AS output, chunk_id FROM HASH_GROUP_BY_{}_0_SINK".format(qid, qid))
    start = time.time()
    bw = con.execute("SELECT input+range_start as input, output FROM lineage_{}, seq_scan_{}_0_range AS r WHERE r.chunk_id=lineage_{}.chunk_id AND {}".format(qid, qid, qid, predicate)).fetchdf()
    end = time.time()
    print("Time: {} ms".format( (end - start) * 1000 ))
    return bw

def get_record(rowid):
    rec = con.execute("SELECT * FROM fires WHERE rowid={}".format(rowid)).fetchdf()
    return rec

def backward_lineage_perfect_groupby(qid, predicate):
    '''
    Test lineage for perfect group by
    '''
    con.execute("CREATE OR REPLACE VIEW lineage_{} AS SELECT sink.index AS input, out.index AS output, sink.chunk_id FROM PERFECT_HASH_GROUP_BY_{}_0_OUT AS out, PERFECT_HASH_GROUP_BY_{}_0_SINK AS sink WHERE sink.value=out.value".format(qid, qid, qid))
    start = time.time()
    bw = con.execute("SELECT input+range_start as input, output FROM lineage_{}, seq_scan_{}_0_range AS r WHERE r.chunk_id=lineage_{}.chunk_id AND {}".format(qid, qid, qid, predicate)).fetchdf()
    end = time.time()
    print("Time: {} ms".format( (end - start) * 1000))
    return bw


print("++++++++++", Q1, "+++++++++++")
print(Q1_out)
q1_bw = backward_lineage_groupby(q1_id, "output=9")
q1_rec = get_record(q1_bw.iloc[0]["input"])
print(q1_rec)

print("++++++++++", Q2, "+++++++++++")
print(Q2_out)
q2_bw = backward_lineage_groupby(q2_id, "output=38")
q2_rec = get_record(q2_bw.iloc[0]["input"])
print(q2_rec)
print("++++++++++", Q3, "+++++++++++")
print(Q3_out)
q3_bw = backward_lineage_perfect_groupby(q3_id, "output=23")
q3_rec = get_record(q3_bw.iloc[0]["input"])
print(q3_rec)
