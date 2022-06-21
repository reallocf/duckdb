import argparse
import duckdb
from timeit import default_timer as timer
import sys


parser = argparse.ArgumentParser(description='Charlie\'s TPCH benchmarking script to test individual lineage operators')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
args = parser.parse_args()

print("queryId,sf,duration")

con = duckdb.connect(database=':memory:', read_only=False)
con.execute(f"CALL dbgen(sf={args.sf});")

def drop_lineage_tables():
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            con.execute(f'DROP TABLE {row["name"]}')

def execute(q, enable_lineage, enable_profiling, profile_output_file):
    q = " ".join(q.split())
    if enable_profiling:
        con.execute("PRAGMA enable_profiling=json;")
        con.execute(f"PRAGMA profiling_output='{profile_output_file}';")
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='ON'")
    start = timer()
    df = con.execute(q).fetchdf()
    end = timer()
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='OFF'")
    if enable_profiling:
        con.execute("PRAGMA disable_profiling;")
    return df, end - start

def get_tpch_query(prefix, qid):
    q = prefix + str(qid).zfill(2) + ".sql"
    text_file = open(q, "r")
    tpch_q = text_file.read()
    text_file.close()
    return tpch_q

def setup_experiment(tpch_query, qid):
    tpch_q = get_tpch_query(f'extension/tpch/dbgen/queries/q', qid)
    execute(tpch_q, True, True, f"profile_output2/profile_output_{qid}_base.json")
    query_id = con.execute("SELECT MAX(query_id) AS qid FROM queries_list").fetchdf().loc[0, 'qid'] - 1
    tpch_query = tpch_query.format(query_id) # Reformat lineage queries with the query id
    # Warmup by running a single query execution, don't record
    execute(tpch_query, False, False, None)
    return tpch_query

def run_experiment(q, qid, repeat_count):
    for ex_count in range(repeat_count):
        df, duration = execute(q, False, True, f"profile_output2/profile_output_{qid}_{ex_count}.json")
        print(f'{qid},{args.sf},{duration}')

def teardown_experiment():
    drop_lineage_tables()

rng = range(1, 23)
prefix = "extension/tpch/dbgen/queries/lineage_queries/q"

for qid in rng:
    tpch_q = get_tpch_query(prefix, qid)
    tpch_q = setup_experiment(tpch_q, qid)
    run_experiment(tpch_q, qid, args.repeat)
    teardown_experiment()

# Perm
perm_prefix = "extension/tpch/dbgen/queries/perm/q"

for qid in range(1,22): # skip query 22 for perm since it can't do it
    tpch_q = get_tpch_query(perm_prefix, qid)
    df, duration = execute(tpch_q.format(qid), False, True, f"profile_output2/profile_output_{qid}_perm.json")
    print(f'{qid},{args.sf},{duration}')
