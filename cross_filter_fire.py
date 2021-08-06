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

# precompute denormalized lineage table
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

fw_lineage  = execute("SELECT output_{}, output_{} from lineage where output_{} IN ({})".format(q1_id, q2_id, q3_id, ','.join(selected_index)))
print(fw_lineage)
