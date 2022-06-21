import duckdb
import pandas as pd
import argparse
import csv

from utils import Run, DropLineageTables

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--query_lineage', action='store_true',  help="query lineage")
parser.add_argument('--persist', action='store_true',  help="Persist lineage captured")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf="+str(args.sf)+");")
    
prefix = "extension/tpch/dbgen/queries/q"
if args.perm:
    prefix = "extension/tpch/dbgen/queries/perm/q"
    args.lineage_query = False
    lineage_type = "Logical-RID"
elif not args.enable_lineage:
    lineage_type = "Baseline"
elif args.persist:
    lineage_type = "SD_Persist"
else:
    lineage_type = "SD_Capture"

results = []
for i in range(1, 2):
    q = prefix+str(i).zfill(2)+".sql"
    text_file = open(q, "r")
    tpch = text_file.read()
    text_file.close()
    print("%%%%%%%%%%%%%%%% Running Query # ", i)
    avg, output_size = Run(tpch, args, con)
    results.append([i, avg, args.sf, args.repeat, lineage_type])
    if args.show_tables:
        print(con.execute("PRAGMA show_tables").fetchdf())
    print(args.persist, args.query_lineage)
    if args.persist and args.query_lineage:
        args.enable_lineage=False
        print("%%%%%% Running Lineage Query # ", i)
        lineage_prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
        lineage_q = lineage_prefix+str(i).zfill(2)+".sql"
        text_file = open(lineage_q, "r")
        lineage_q = text_file.read()
        text_file.close()
        query_id = con.execute("select max(query_id) as qid from queries_list").fetchdf().loc[0, 'qid'] - 1
        avg, output_size = Run(lineage_q.format(query_id), args, con)
        results.append([i, avg, args.sf, args.repeat, "SD_Query"])
        DropLineageTables(con)
        args.enable_lineage=True

if args.save_csv:
    filename="tpch_benchmark_sf"+str(args.sf)+"_notes_"+args.notes+"_lineage_type_"+lineage_type+".csv"
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type"]
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
