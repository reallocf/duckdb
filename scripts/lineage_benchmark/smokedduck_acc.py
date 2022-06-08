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
for qid in range(1, 22):
#for qid in [14]:
    print("=======" + str(qid) + "========")
    prefix = base+"/q"
    # run base query
    q = prefix+str(qid).zfill(2)+".sql"
    text_file = open(q, "r")
    base_q = text_file.read()
    base_q = " ".join(base_q.split())
    text_file.close()
    df = con.execute(base_q).fetchdf()
    args.enable_lineage = True
    avg, base_size = Run(base_q, args, con)
    args.enable_lineage = False
    results.append([qid, avg, args.sf, args.repeat, "SmokedDuck", args.threads, base_size, base_size, "preprocess"])
    t = 0
    sample_size = min(len(df), 10)
    print(sample_size)
    test_out = random.sample(range(0, len(df)), sample_size)
    if qid == 13:
        test_out = [2, 40, 30, 7, 6, 12, 34, 20, 18, 37]
    print(test_out)
    if sample_size == 0:
        continue
    for i in test_out:
        bw_q = 'PRAGMA backward_lineage("'+base_q+'"' + ', "COUNT", '+str(i)+')'
        avg, _ = Run(bw_q, args, con)
        bw_df = con.execute(bw_q).fetchdf()
        output_size = bw_df.iloc[0,0]
        results.append([qid, avg, args.sf, args.repeat, "SmokedDuck", args.threads, output_size, base_size, "bw"])

if args.save_csv:
    filename="tpch_acc_notes_"+args.notes+"_lineage_type_SmokedDuck.csv"
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type", "n_threads", "lineage_size", "base_size", "stage_type"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

