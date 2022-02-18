### for Q11, use for queries/q11.sql and perm_bw/q11.sql
### sf10: sum(ps_supplycost * ps_availqty) * 0.0000100000
### sf1: sum(ps_supplycost * ps_availqty) * 0.0001000000
import duckdb
import datetime
import pandas as pd
import argparse
import csv
import random

from utils import Run

parser = argparse.ArgumentParser(description='TPCH single script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--threads', type=int, help="number of threads", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="enable profiling")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf="+str(args.sf)+");")
if args.threads > 1:
    con.execute("PRAGMA threads="+str(args.threads))
    con.execute("PRAGMA force_parallelism")
results = []
print(args)


base = "extension/tpch/dbgen/queries"
for qid in range(1, 23):
    print("=======" + str(qid) + "========")
    prefix = base+"/q"
    # run base query
    qkeys = base+"/perm_keys/q"+str(qid).zfill(2)+".sql"
    qkeys_text_file = open(qkeys, "r")
    qkeys = qkeys_text_file.read()
    qkeys = " ".join(qkeys.split())
    qkeys = qkeys.split(',')
    q = prefix+str(qid).zfill(2)+".sql"
    text_file = open(q, "r")
    base_q = text_file.read()
    base_q = " ".join(base_q.split())
    text_file.close()
    df = con.execute(base_q).fetchdf()
    prefix = base + "/perm_bw/q"
    t = 0
    tmin = 1000000
    tmax = 0
    sample_size = min(len(df), 10)
    print(sample_size)
    test_out = random.sample(range(0, len(df)), sample_size)
    print(test_out)
    if sample_size == 0:
        continue
    for i in test_out:
        predicate=''
        for k in qkeys:
            if len(k) == 0:
                continue
            if len(predicate)>0:
                predicate+=" AND "
            val = df.loc[i, k]
            if isinstance(val, int) or isinstance(val, float):
                predicate+=k+"="+str(val)
            elif isinstance(val, str):
                predicate+=k+ "='"+str(val)+"'"
            elif isinstance(val,datetime.date):
                predicate+=k+"='"+str(val)+"'"
            else:
                predicate+=k+"="+str(val)
        if len(predicate) > 0:
            predicate = " where " + predicate
        q = prefix+str(qid).zfill(2)+".sql"
        text_file = open(q, "r")
        tpch = text_file.read()
        tpch = " ".join(tpch.split()) + " " + predicate
        text_file.close()
        avg, _ = Run(tpch, args, con)
        output_size = con.execute(tpch).fetchdf().loc[0, 'c']
        if avg < tmin: tmin = avg
        if avg > tmax: tmax = avg
        t += avg
    results.append([qid, t/sample_size, tmin, tmax, args.sf, args.repeat, "Logical-RID", args.threads, output_size, len(df)])

if args.save_csv:
    filename="tpch_bw_notes_"+args.notes+"_lineage_type_Logical-RID.csv"
    print(filename)
    header = ["query", "runtime", "min", "max", "sf", "repeat", "lineage_type", "n_threads", "lineage_size", "base_size"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

