import time
import pandas as pd
import duckdb

def init_bw_lineage_groupby(qid):
    con.execute("CREATE OR REPLACE VIEW lineage_{} AS SELECT index AS input, value AS output, chunk_id FROM HASH_GROUP_BY_{}_0_SINK".format(qid, qid))

def init_bw_lineage_perfect_groupby(qid):
    con.execute("CREATE OR REPLACE VIEW lineage_{} AS SELECT sink.index AS input, out.index AS output, sink.chunk_id FROM PERFECT_HASH_GROUP_BY_{}_0_OUT AS out, PERFECT_HASH_GROUP_BY_{}_0_SINK AS sink WHERE sink.value=out.value".format(qid, qid, qid))

def get_record(cols, rowids):
    rec = con.execute("SELECT {} FROM fires WHERE rowid IN ({})".format(cols, rowids)).fetchdf()
    return rec

def get_record_query(cols, rowids):
    return "SELECT {} FROM fires WHERE rowid IN ({})".format(cols, rowids)

def execute(q):
    start = time.time()
    res = con.execute(q).fetchdf()
    end = time.time()
    print("Time: {} ms".format( (end - start) * 1000 ))
    return res
    
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

init_bw_lineage_groupby(q1_id)
init_bw_lineage_groupby(q2_id)
init_bw_lineage_perfect_groupby(q3_id)

def backward_lineage_groupby(qid, predicate):
    '''
    Test lineage for group by
    '''
    return execute("SELECT input+range_start as input, output FROM lineage_{}, seq_scan_{}_0_range AS r WHERE r.chunk_id=lineage_{}.chunk_id AND {}".format(qid, qid, qid, predicate))

def get_backward_lineage_query(qid, predicate):
    return "SELECT input+range_start as input, output FROM lineage_{}, seq_scan_{}_0_range AS r WHERE r.chunk_id=lineage_{}.chunk_id AND {}".format(qid, qid, qid, predicate)


years = ['1999', '2000']
print("+++ Testing filtering on {} with values {} +++".format(Q3, years))
print(Q3_out)

selected = Q3_out[Q3_out['FIRE_YEAR'].isin(years)]
selected_index = selected.index.astype(str).tolist()

q3_bw_query = get_backward_lineage_query(q3_id, "output IN ({})".format(','.join(selected_index)))
con.execute("CREATE VIEW bw_lineage AS "+q3_bw_query)

q3_rec = get_record_query('STATE, STAT_CAUSE_DESCR', "SELECT input FROM bw_lineage")
con.execute("CREATE VIEW bw_records AS "+q3_rec)

Q2_cf = "SELECT STATE, COUNT(*) FROM {} GROUP BY STATE".format("bw_records")
print(Q2_cf)
Q2_cf_out = execute(Q2_cf)
print(Q2_cf_out)

Q2_filter = "SELECT STATE, COUNT(*) FROM fires where FIRE_YEAR IN ({}) GROUP BY STATE".format(','.join(years))
Q2_filter_out = execute(Q2_filter)
print(Q2_filter_out)


Q1_cf = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM {} GROUP BY STAT_CAUSE_DESCR".format("bw_records")
print(Q1_cf)
Q1_cf_out = execute(Q1_cf)
print(Q1_cf_out)

Q1_filter = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires where FIRE_YEAR IN ({}) GROUP BY STAT_CAUSE_DESCR".format(','.join(years))
Q1_filter_out = execute(Q1_filter)
print(Q1_filter_out)
print("++++++++++", Q1, "+++++++++++")
print(Q1_out)
q1_bw = backward_lineage_groupby(q1_id, "output=9")
q1_rec = get_record("*", q1_bw.iloc[0]["input"])
print(q1_rec)

print("++++++++++", Q2, "+++++++++++")
print(Q2_out)
q2_bw = backward_lineage_groupby(q2_id, "output=38")
q2_rec = get_record("*", q2_bw.iloc[0]["input"])
print(q2_rec)

print("++++++++++", Q3, "+++++++++++")
print(Q3_out)
q3_bw = backward_lineage_groupby(q3_id, "output=23")
q3_rec = get_record("*", q3_bw.iloc[0]["input"])
print(q3_rec)
