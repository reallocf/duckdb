import duckdb
import pandas as pd
from timeit import default_timer as timer
import argparse
import csv

def execute(Q):
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    return df, end-start

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--query_lineage', action='store_true',  help="query lineage")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
args = parser.parse_args()

print("Arguments: "+args.notes+" enable_lineage=", args.enable_lineage, ", sf=", args.sf, " repeat=",
        args.repeat, " profile=", args.profile, ", show_tables=", args.show_tables)
con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CALL dbgen(sf="+str(args.sf)+");")
if args.profile:
    con.execute("PRAGMA enable_profiling;")
    
prefix = "extension/tpch/dbgen/queries/q"
if args.perm:
    prefix = "extension/tpch/dbgen/queries/perm/q"
results = []
for i in range(1, 23):
    q = prefix+str(i).zfill(2)+".sql"
    text_file = open(q, "r")

    #read whole file to a string
    tpch = text_file.read()
    tpch = " ".join(tpch.split())

    #close file
    text_file.close()
    print("Running query: ", i, " ", tpch)
    dur_acc = 0.0
    lineage_query_dur_acc = 0.0
    tables = []
    for j in range(args.repeat):
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")
        df, duration = execute(tpch)
        if args.show_output:
            print(df)
        print("Time in sec: ", duration) 
        dur_acc += duration
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")

        if args.query_lineage:
            lineage_prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
            lineage_q = lineage_prefix+str(i).zfill(2)+".sql"
            text_file = open(lineage_q, "r")
            lineage_q = text_file.read()
            lineage_q = " ".join(lineage_q.split())
            text_file.close()
            df, duration = execute(lineage_q)
            lineage_query_dur_acc += duration
            print("Lineage Query Time in sec: ", duration) 
        tables = con.execute("PRAGMA show_tables").fetchdf()
        for index, row in tables.iterrows():
            if row["name"][:7] == "LINEAGE":
                con.execute("DROP TABLE "+row["name"])
    avg = dur_acc/args.repeat
    lineage_query_dur_acc_avg = lineage_query_dur_acc/args.repeat
    print("Avg Time in sec: ", avg, " Avg Time in sec lineage query: ", lineage_query_dur_acc_avg) 
    results.append([i, avg, lineage_query_dur_acc_avg, args.sf, args.repeat, args.enable_lineage])
    if args.show_tables:
        print(tables)
        tables = con.execute("PRAGMA show_tables").fetchdf()
        print(tables)

if args.profile:
    con.execute("PRAGMA disable_profiling;")

elif args.query_lineage:
    prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
if args.save_csv:
    filename="tpch_benchmark_sf"+str(args.sf)+"_notes_"+args.notes+".csv"
    print(filename)
    header = ["query", "runtime", "lineage_query_runtime", "sf", "repeat", "lineage"]
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
