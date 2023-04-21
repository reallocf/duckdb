import argparse
import duckdb
import numpy as np
import os
import pandas as pd
import random
import scipy.stats as stats
import sys
from timeit import default_timer as timer
import time


parser = argparse.ArgumentParser(description="Charlie's microbenchmark script for lineage querying")
parser.add_argument('--op', type=str, help="Which op to test")
parser.add_argument('--a', type=float, help="Zipfian distribution value", default=0.1)
parser.add_argument('--num', type=int, help="Number of records in test table", default=10000000)
parser.add_argument('--range', type=int, help="Range of values being tested", default=5000)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--full', type=int, help="Run all the tests", default=0)
parser.add_argument('--fixed', type=int, help="Fixed output for each val", default=0)
parser.add_argument('--num_oids', type=int, help="Number of output ids", default=1)
parser.add_argument('--skew', type=int, help="Add skew to fixed", default=0)
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)

def build_table(a, num, rng, fixed):
    if fixed:
        z = range(0, num)
    else:
        # https://stackoverflow.com/questions/33331087/sampling-from-a-bounded-domain-zipf-distribution
        x = np.arange(1, rng + 1)
        weights = x ** -(1 + a)
        weights /= weights.sum()
        bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))
        z = bounded_zipf.rvs(size=num)
    v = np.random.randint(low=0, high=10, size=len(z))
    df = pd.DataFrame({'z': z, 'v': v})
    df.to_csv('tmp.csv')
    con.execute("create table zipf as select * from 'tmp.csv'")
    os.remove('tmp.csv')

def build_base_query(op):
    if op == 'groupby' or op == 'perfgroupby':
        return "select z, count(*), sum(v), sum(v * v), sum(sqrt(v)), min(v), max(v) from zipf group by z"
    elif op == 'filter':
        return "select z from zipf where v < 9"
    elif op == 'orderby':
        return "select z from zipf order by z"
    elif op == 'simpleagg':
        return "select sum(z) from zipf"
    elif op == 'hashjoin':
        return "select t1.z, t2.z from zipf as t1 join zipf as t2 on t1.z = t2.z + 1"
    elif op == 'mergejoin' or op == 'nljoin':
        return "select t1.z, t2.z from zipf as t1 join zipf as t2 on t1.z >= t2.z + 1 and t1.z <= t2.z + 1"
    else:
        raise Exception("op must be set")

def execute_base_query(q):
    con.execute('pragma enable_profiling')
    con.execute('pragma trace_lineage = "ON"')
    base_res = con.execute(q).df()
    print(base_res)
    con.execute('pragma trace_lineage = "OFF"')
    con.execute('pragma disable_profiling')
    return base_res

def build_lineage_query(bq):
    return f"pragma lineage_query('{bq}', '%s', 'LIN', 1)"

def build_query_range(op, rng, base_res, mx):
    if op == 'groupby' or op == 'perfgroupby':
        # We want to return the list of values ordered by z
        res = [None for _ in range(rng)]
        base_res_z = base_res['z']
        for i in range(len(base_res)):
            res[base_res_z[i] - 1] = i
        res = [i for i in res if i is not None] # Remove Nones in the case of an element in the range never being selected
        return res
    elif op == 'filter' or op == 'orderby' or op == 'hashjoin' or op == 'mergejoin' or op == 'nljoin':
        return range(mx)
    elif op == 'simpleagg':
        return range(1)
    else:
        raise Exception("think through these...")

def setup_experiment(op, a, num, rng, fixed):
    if op == 'groupby':
        con.execute("pragma set_agg('reg')")
    elif op == 'perfgroupby':
        con.execute("pragma set_agg('perfect')")
    elif op == 'filter':
        con.execute("pragma set_filter('filter')")
    elif op == 'hashjoin':
        con.execute("pragma set_join('hash')")
    elif op == 'mergejoin':
        con.execute("pragma set_join('merge')")
    build_table(a, num, rng, fixed)
    q = build_base_query(op)
    base_res = execute_base_query(q)
    return (base_res, build_lineage_query(q))

def execute_lineage_query(lineage_q, lids):
    final_q = lineage_q % ','.join(lids)
    start = timer()
    res = con.execute(final_q).fetchall()
    end = timer()
    return (res[0][0], end - start)

def teardown_experiment():
    con.execute('pragma clear_lineage')
    con.execute('drop table zipf')

if __name__ == '__main__':
    if args.full == 0:
        aas = [args.a]
        nums = [args.num]
        rngs = [args.range]
        f = sys.stderr
        mx = 50000
    else:
        aas = [0.0, 0.4, 0.8, 1.6]
        rngs = [100, 1000, 5000]
        nums = [
            100000,
            1000000,
            10000000
        ]
        mx = 5000
        f = open(f'{args.op}_full.csv', 'w')

    random.seed(1024)
    print("op,avg_duration,row_count,zipf_a,num_records,groups,oids", file=f)

    for a in aas:
        for rng in rngs:
            for num in nums:
                (base_res, lineage_q) = setup_experiment(args.op, a, num, rng, args.fixed)
                if args.fixed:
                    avg_res = 0
                    avg_t = 0
                    if args.skew != 0:
                        x = np.arange(1, len(base_res) + 1)
                        weights = x ** -(1 + args.a)
                        weights /= weights.sum()
                        bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))
                        lids = [str(y) for y in bounded_zipf.rvs(size=args.num_oids)]
                    else:
                        lids = [str(x) for x in np.random.randint(low=0, high=len(base_res), size=args.num_oids)]
                    for _ in range(args.repeat):
                        (num_res, t) = execute_lineage_query(lineage_q, lids)
                        avg_res += num_res
                        avg_t += t
                    avg_res /= args.repeat
                    avg_t /= args.repeat
                    print(f"{args.op},{avg_t},{avg_res},{a},{num},{rng},{args.num_oids}", file=f)
                else:
                    for lid in build_query_range(args.op, args.range, base_res, mx):
                        avg_res = 0
                        avg_t = 0
                        lids = ['0'] * args.num_oids
                        for _ in range(args.repeat):
                            (num_res, t) = execute_lineage_query(lineage_q, lids)
                            avg_res += num_res
                            avg_t += t
                        avg_res /= args.repeat
                        avg_t /= args.repeat
                        print(f"{args.op},{avg_t},{avg_res},{a},{num},{rng},{args.num_oids}", file=f)
                teardown_experiment()