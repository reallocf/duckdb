import argparse
import duckdb
import numpy as np
import pandas as pd
from timeit import default_timer as timer
import time
import scipy.stats as stats
import sys


parser = argparse.ArgumentParser(description="Charlie's microbenchmark script for lineage querying")
parser.add_argument('--op', type=str, help="Which op to test")
parser.add_argument('--a', type=int, help="Zipfian distribution value")
parser.add_argument('--num', type=int, help="Number of records in test table", default=10000000)
parser.add_argument('--range', type=int, help="Range of values being tested", default=5000)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
args = parser.parse_args()

print("op,avg_duration,rowCount", file=sys.stderr)

con = duckdb.connect(database=':memory:', read_only=False)

def build_table(a, num, rng):
    # https://stackoverflow.com/questions/33331087/sampling-from-a-bounded-domain-zipf-distribution
    x = np.arange(0, rng)
    weights = x ** -(1 + a)
    weights /= weights.sum()
    bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))
    z = bounded_zipf.rvs(size=num)
    v = np.random.randint(low=0, high=10, size=len(z))
    df = pd.DataFrame({'z': z, 'v': v})
    con.execute('create table zipf as select * from df')

def build_base_query(op):
    if op == 'groupby':
        return "select z, count(*), sum(v), sum(v * v), sum(sqrt(v)), min(v), max(v) from zipf group by z"

def execute_base_query(q):
    con.execute('pragma trace_lineage = "ON"')
    con.execute(q)
    con.execute('pragma trace_lineage = "OFF"')

def build_lineage_query():
    pass

def setup_experiment():
    build_table(args.a, args.n)
    q = build_base_query(args.op)
    execute_base_query(q)
    return build_lineage_query()

def execute_lineage_query(lineage_q):
    start = timer()
    res = con.execute(lineage_q).fetchall()
    end = timer()
    return (res[0][0], end - start)

def teardown_experiment():
    pass

if __name__ == '__main__':
    lineage_q = setup_experiment()
    print(con.execute('select * from df limit 10').fetchall())
    # avg_res = 0
    # avg_t = 0
    # for _ in range(args.repeat):
    #     (num_res, t) = execute_lineage_query(lineage_q)
    #     avg_res += num_res
    #     avg_t += t
    # avg_res /= args.repeat
    # avg_t /= args.repeat
    # teardown_experiment()


