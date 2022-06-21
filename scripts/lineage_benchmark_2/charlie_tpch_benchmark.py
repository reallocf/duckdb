import argparse
import duckdb
from timeit import default_timer as timer


parser = argparse.ArgumentParser(description='Charlie\'s TPCH benchmarking script')
parser.add_argument('--mode', type=str, choices=['BASE', 'PERM', 'LINEAGE'], help='Test mode', default='LINEAGE')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
args = parser.parse_args()

print("queryId,sf,mode,duration,rowCount")

con = duckdb.connect(database=':memory:', read_only=False)
con.execute(f"CALL dbgen(sf={args.sf});")

def drop_lineage_tables():
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            con.execute(f'DROP TABLE {row["name"]}')

def execute(q, enable_lineage):
    q = " ".join(q.split())
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='ON'")
    start = timer()
    df = con.execute(q).fetchdf()
    end = timer()
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='OFF'")
    return df, end - start

def get_tpch_query(prefix, qid):
    q = prefix + str(qid).zfill(2) + ".sql"
    text_file = open(q, "r")
    tpch_q = text_file.read()
    text_file.close()
    return tpch_q

def setup_experiment(tpch_query, qid, mode):
    if mode == 'LINEAGE':
        tpch_q = get_tpch_query(f'extension/tpch/dbgen/queries/q', qid)
        execute(tpch_q, True)
        query_id = con.execute("SELECT MAX(query_id) AS qid FROM queries_list").fetchdf().loc[0, 'qid'] - 1
        tpch_query = tpch_query.format(query_id) # Reformat lineage queries with the query id
    # Warmup by running a single query execution, don't record
    execute(tpch_query, False)
    return tpch_query

def run_experiment(q, qid, repeat_count):
    for _ in range(repeat_count):
        df, duration = execute(q, False)
        print(f'{qid},{args.sf},{args.mode},{duration},{len(df)}')

def teardown_experiment(mode):
    if mode == 'LINEAGE':
        drop_lineage_tables()

prefix = "extension/tpch/dbgen/queries/q"
rng = range(1, 23)
if args.mode == 'PERM':
    rng = range(1, 22) # PERM can't do query 22
    prefix = "extension/tpch/dbgen/queries/perm/q"
elif args.mode == 'LINEAGE':
    prefix = "extension/tpch/dbgen/queries/lineage_queries/q"

for qid in rng:
    tpch_q = get_tpch_query(prefix, qid)
    tpch_q = setup_experiment(tpch_q, qid, args.mode)
    run_experiment(tpch_q, qid, args.repeat)
    teardown_experiment(args.mode)
