import time
import pandas as pd
import duckdb

def init_bw_lineage_groupby(qid):
    con.execute("""CREATE TABLE lineage_{} AS SELECT (s.index+r.range_start) AS input, s.value AS output
                    FROM HASH_GROUP_BY_{}_0_SINK AS s, SEQ_SCAN_{}_0_RANGE AS r
                    WHERE r.chunk_id=s.chunk_id""".format(qid, qid, qid))
    # con.execute("CREATE INDEX i_index_{} ON lineage_{} using art(output)".format(qid, qid))


def init_bw_lineage_perfect_groupby(qid):
    con.execute("""CREATE TABLE lineage_{} AS SELECT (sink.index+r.range_start) AS input, out.index AS output
                    FROM PERFECT_HASH_GROUP_BY_{}_0_OUT AS out, PERFECT_HASH_GROUP_BY_{}_0_SINK AS sink, SEQ_SCAN_{}_0_RANGE AS r
                    WHERE sink.value=out.value AND sink.chunk_id=r.chunk_id""".format(qid, qid, qid, qid))

def get_record(cols, rowids):
    rec = con.execute("SELECT {} FROM fires WHERE rowid IN ({})".format(cols, rowids)).fetchdf()
    return rec

def get_lineage_query(qid, select, predicate):
    return "SELECT {} FROM lineage_{} WHERE true AND {}".format(select, qid, predicate)

def get_record_query(cols, rowids):
    return "SELECT {} FROM fires WHERE rowid IN ({})".format(cols, rowids)

def backward_lineage_groupby(qid, select, predicate):
    '''
    Test lineage for group by
    '''
    q = get_lineage_query(qid, select, predicate)
    return execute(q)

def execute_no(q):
    print("Q: ", q)
    start = time.time()
    res = con.execute(q)
    end = time.time()
    print("Time: {} ms".format( (end - start) * 1000 ))
    return res
def execute(q):
    print("Q: ", q)
    start = time.time()
    res = con.execute(q).fetchdf()
    end = time.time()
    print("Time: {} ms".format( (end - start) * 1000 ))
    return res
    
con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('/tmp/fires_with_dropped_cols.csv')")

# enable lineage
con.execute("PRAGMA trace_lineage='ON'")
Q1 = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR"
Q2 = "SELECT STATE, COUNT(*) FROM fires GROUP BY STATE"
Q3 = "SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR"

# run queries
Q1_out = con.execute(Q1).fetchdf()
Q2_out = con.execute(Q2).fetchdf()
Q3_out = con.execute(Q3).fetchdf()

print("++++++++++", Q1, "+++++++++++")
print(Q1_out)
print("++++++++++", Q2, "+++++++++++")
print(Q2_out)
print("++++++++++", Q3, "+++++++++++")
print(Q3_out)

# disable lineage
con.execute("PRAGMA trace_lineage='OFF'")

# get queries' ids
q1_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q1)).fetchdf().iloc[0,0]
q2_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q2)).fetchdf().iloc[0,0]
q3_id = con.execute("SELECT query_id FROM queries_list WHERE query='{}'".format(Q3)).fetchdf().iloc[0,0]

init_bw_lineage_groupby(q1_id)
init_bw_lineage_groupby(q2_id)
init_bw_lineage_perfect_groupby(q3_id)
#fires.STATE, fires.STAT_CAUSE_DESCR, fires.FIRE_YEAR,
out = execute("""create table lineage as SELECT 
            fires.rowid as input, lineage_{}.output as output_{}, lineage_{}.output as output_{}, lineage_{}.output as output_{}
            FROM fires, lineage_{}, lineage_{}, lineage_{}
            WHERE fires.rowid=lineage_{}.input
                    and fires.rowid=lineage_{}.input
                    and fires.rowid=lineage_{}.input""".format(q1_id, q1_id, q2_id, q2_id, q3_id, q3_id,q1_id, q2_id, q3_id, q1_id, q2_id, q3_id))
print(out)
tables_list = con.execute("PRAGMA show_tables").fetchdf()
print(tables_list)


years = ['1999', '2000']
print("+++ Testing filtering on {} with values {} +++".format(Q3, years))

selected = Q3_out[Q3_out['FIRE_YEAR'].isin(years)]
selected_index = selected.index.astype(str).tolist()

q1_fw = execute("SELECT * from lineage where output_{} IN ({})".format(q3_id, ','.join(selected_index)))
print(q1_fw)
"""
q1_fw = execute("create table  bw_lineage as select input from lineage where output_{} IN ({})".format(q3_id, ','.join(selected_index)))
print(q1_fw)

q3_fw_query_records = get_record_query("STAT_CAUSE_DESCR, STATE", "SELECT input FROM bw_lineage")
q3_fw_query_records_out = execute(q3_fw_query_records)
print(q3_fw_query_records_out)

con.execute("PRAGMA enable_profiling")

#################
select = "STAT_CAUSE_DESCR, STATE, FIRE_YEAR"
print("++++++++++", Q1, "+++++++++++")
q1_bw = backward_lineage_groupby(q1_id, "input", "output=9")
q1_rec = get_record(select, q1_bw.iloc[0]["input"])
print(q1_rec)

print("++++++++++", Q2, "+++++++++++")
q2_bw = backward_lineage_groupby(q2_id, "input", "output=38")
q2_rec = get_record(select, q2_bw.iloc[0]["input"])
print(q2_rec)

print("++++++++++", Q3, "+++++++++++")
q3_bw = backward_lineage_groupby(q3_id, "input", "output=23")
q3_rec = get_record(select, q3_bw.iloc[0]["input"])
print(q3_rec)

#############################

# 1. backward lineage for selected groups -> list of input tuples that contributed to the selected groups (80363) (11ms)
q3_bw_query = get_lineage_query(q3_id, "input", "output IN ({})".format(','.join(selected_index)))
q3_bw = execute_no("CREATE TABLE bw_lineage AS " + q3_bw_query)

# 2. forward lineage to the output group each row belongs to (40ms)
q1_fw_query = get_lineage_query(q1_id, "output", "input IN (SELECT input FROM bw_lineage)")
q1_fw = execute(q1_fw_query)
print(q1_fw)

q2_fw_query = get_lineage_query(q2_id, "output, input", "input IN (SELECT input FROM bw_lineage)")
q2_fw = execute("CREATE TABLE fw_lineage AS " + q2_fw_query)
print(q2_fw)

# 3. get the actual tuples (123ms)
q3_fw_query_records = get_record_query("STAT_CAUSE_DESCR", "SELECT input FROM fw_lineage")
q3_fw_query_records_out = execute(q3_fw_query_records)
print(q3_fw_query_records_out)


execute_no("CREATE TABLE bw_lineage AS "+q3_bw_query)

q3_rec = get_record_query('STATE, STAT_CAUSE_DESCR', "SELECT input FROM bw_lineage")
execute_no("CREATE TABLE fw_records AS "+q3_rec)
out = execute("SELECT * FROM fw_records")
print(out)

Q2_cf = "SELECT STATE, COUNT(*) FROM {} GROUP BY STATE".format("fw_records")
Q2_cf_out = execute(Q2_cf)
#print(Q2_cf_out)

Q2_filter = "SELECT STATE, COUNT(*) FROM fires where FIRE_YEAR IN ({}) GROUP BY STATE".format(','.join(years))
Q2_filter_out = execute(Q2_filter)
#print(Q2_filter_out)


Q1_cf = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM {} GROUP BY STAT_CAUSE_DESCR".format("fw_records")
print(Q1_cf)
Q1_cf_out = execute(Q1_cf)
#print(Q1_cf_out)

Q1_filter = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires where FIRE_YEAR IN ({}) GROUP BY STAT_CAUSE_DESCR".format(','.join(years))
Q1_filter_out = execute(Q1_filter)
#print(Q1_filter_out)

###########################3
"""
