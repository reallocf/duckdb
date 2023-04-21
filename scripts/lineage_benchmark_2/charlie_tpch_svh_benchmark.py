import argparse
import duckdb
import random
import sys
from timeit import default_timer as timer
import time


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
        #con.execute('pragma enable_profiling')
        con.execute('PRAGMA trace_lineage = "ON"')
    start = timer()
    try:
        res = con.execute(q).fetchall()
    except Exception as e:
        print(e)
        print("Runtime error!")
        res = [[0]]
    end = timer()
    if enable_lineage:
        con.execute("PRAGMA trace_lineage='OFF'")
        #con.execute('pragma disable_profiling')
    return res, end - start

def get_tpch_query(prefix, qid):
    q = prefix + str(qid).zfill(2) + ".sql"
    text_file = open(q, "r")
    tpch_q = text_file.read()
    text_file.close()
    if 'sum(ps_supplycost * ps_availqty) * 0.0001000000' in tpch_q and args.sf == 10:
        # Handling query 11 at sf=10
        tpch_q = tpch_q.replace('sum(ps_supplycost * ps_availqty) * 0.0001000000', 'sum(ps_supplycost * ps_availqty) * 0.0000100000')
    return tpch_q

def gen_pragma(clean_tpch_q, res_count):
    r = random.randrange(res_count)
    #print(f'Querying index {r}')
    return f'PRAGMA lineage_query("{clean_tpch_q}", "{r}", "LIN", 1)'

def setup_experiment(qid):
    tpch_q = get_tpch_query(f'extension/tpch/dbgen/queries/q', qid)
    clean_tpch_q = " ".join(tpch_q.split())
    res, _ = execute(clean_tpch_q, True)
    # Warmup by running a single query execution, don't record
    execute(gen_pragma(clean_tpch_q, len(res)), False)
    return clean_tpch_q, len(res)

def run_experiment(q, qid, res_count, repeat_count):
    avg_duration = 0
    res = []
    for i in range(repeat_count):
        res, duration = execute(gen_pragma(q, res_count), False)
        avg_duration += duration
    avg_duration /= repeat_count
    print(f'{qid},{args.sf},{mode},{avg_duration},{res[0][0]}', file=sys.stderr)

def teardown_experiment():
    con.execute('pragma clear_lineage')

random.seed(1024)
rng = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
#rng = [17]
#if args.sf == 10:
#    # Can't do 11 at sf=10
#    rng = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22]

for qid in rng:
    tpch_q, res_count = setup_experiment(qid)
    print(f"Running {qid}")
    run_experiment(tpch_q, qid, res_count, args.repeat)
    teardown_experiment()