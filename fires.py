import time
import pandas as pd
import duckdb

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('/tmp/fires_with_dropped_cols.csv')")

# enable lineage
con.execute("PRAGMA trace_lineage='ON'")
con.execute("PRAGMA enable_profiling")
Q1 = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR"
Q2 = "SELECT STATE, COUNT(*) FROM fires GROUP BY STATE"
#Q3 = "SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR"

# run queries
Q1_out = con.execute(Q1).fetchdf()
Q2_out = con.execute(Q2).fetchdf()
#Q3_out = con.execute(Q3).fetchdf()

# disable lineage
con.execute("PRAGMA trace_lineage='OFF'")

# get queries' ids
q1_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q1)).fetchdf().iloc[0,0]
q2_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q2)).fetchdf().iloc[0,0]
#q3_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q3)).fetchdf().iloc[0,0]

tables_list = con.execute("PRAGMA show_tables").fetchdf()
print(tables_list)

def test_lineage_for_q(qid):
    '''
    This only supports selet group by query
    '''
    con.execute("CREATE OR REPLACE VIEW lineage_{} AS SELECT index AS input, value AS output, chunk_id FROM HASH_GROUP_BY_{}_0_SINK".format(qid, qid, qid))
    start = time.time()
    bw = con.execute("SELECT input+range_start as input, output FROM lineage_{}, seq_scan_{}_0_range AS r WHERE r.chunk_id=lineage_{}.chunk_id AND output=1".format(qid, qid, qid)).fetchdf()
    end = time.time()
    print("Time: ", end - start)
    for index, row in bw.iterrows():
        # get the actual record
        fw = con.execute("SELECT * FROM fires WHERE rowid="+str(row["input"])).fetchdf()
        print(fw)
        break

test_lineage_for_q(q1_id)
test_lineage_for_q(q2_id)
