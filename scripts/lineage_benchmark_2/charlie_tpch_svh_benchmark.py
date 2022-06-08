import argparse
import duckdb
from timeit import default_timer as timer
import time
import sys


parser = argparse.ArgumentParser(description='Charlie\'s TPCH benchmarking script for selection vector hopping')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
args = parser.parse_args()

mode = "LIN"
print("queryId,sf,mode,avg_duration,rowCount", file=sys.stderr) # Row count is now count of total returned lineage values

con = duckdb.connect(database=':memory:', read_only=False)
con.execute(f"CALL dbgen(sf={args.sf});")

def execute(q, enable_lineage):
    if enable_lineage:
        con.execute('PRAGMA trace_lineage = "ON"')
    start = timer()
    try:
        size_lst = con.execute(q).fetchall()
    except:
        print("Runtime error!")
        size_lst = [[0]]
    end = timer()
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='OFF'")
    return size_lst, end - start

def get_tpch_query(prefix, qid):
    q = prefix + str(qid).zfill(2) + ".sql"
    text_file = open(q, "r")
    tpch_q = text_file.read()
    text_file.close()
    return tpch_q

def setup_experiment(qid):
    tpch_q = get_tpch_query(f'extension/tpch/dbgen/queries/q', qid)
    clean_tpch_q = " ".join(tpch_q.split())
    execute(clean_tpch_q, True)
    tpch_query = f'PRAGMA lineage_query("{clean_tpch_q}", 0, "LIN", 1)' # TODO: randomize 0
    # Warmup by running a single query execution, don't record
    execute(tpch_query, False)
    return tpch_query

def run_experiment(q, qid, repeat_count):
    avg_duration = 0
    size_lst = []
    for _ in range(repeat_count):
        size_lst, duration = execute(q, False)
        avg_duration += duration
    avg_duration /= repeat_count
    print(f'{qid},{args.sf},{mode},{avg_duration},{size_lst[0][0]}', file=sys.stderr)

def teardown_experiment():
    pass

rng = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
if args.sf == 10:
    # Can't do 11 at sf=10
    rng = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

for qid in rng:
    tpch_q = setup_experiment(qid)
    run_experiment(tpch_q, qid, args.repeat)
    teardown_experiment()
