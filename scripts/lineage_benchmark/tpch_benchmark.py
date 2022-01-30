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
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
parser.add_argument('--threads', type=int, help="number of threads", default=1)
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
prefix = "extension/tpch/dbgen/queries/q"
table_name=None
if args.perm:
    prefix = "extension/tpch/dbgen/queries/perm/q"
    args.lineage_query = False
    lineage_type = "Logical-RID"
    table_name='lineage'
elif not args.enable_lineage:
    lineage_type = "Baseline"
elif args.persist:
    lineage_type = "SD_Persist"
else:
    lineage_type = "SD_Capture"

# sf: 1, 5, 10, 20
# threads: 1, 4, 8, 12, 16
sf_list = [10]
threads_list = [1, 4, 8, 12, 16]
results = []
for sf in sf_list:
    con.execute("CALL dbgen(sf="+str(sf)+");")
    for th_id in threads_list:
        con.execute("PRAGMA threads="+str(th_id))
        con.execute("PRAGMA force_parallelism")
    
        for i in range(1, 23):
            q = prefix+str(i).zfill(2)+".sql"
            text_file = open(q, "r")
            tpch = text_file.read()
            text_file.close()
            print("%%%%%%%%%%%%%%%% Running Query # ", i, " threads: ", th_id)
            avg, output_size = Run(tpch, args, con, table_name)
            if table_name:
                con.execute("DROP TABLE "+table_name)
            Q = " ".join(tpch.split())
            Q = Q.replace("'", "''")
            if args.show_tables:
                print(con.execute("PRAGMA show_tables").fetchdf())
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(Q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                print("Query ID: ", query_id, " Lineage Size: ", lineage_size/(1024.0*1024), " MB")
                results.append([i, avg, sf, args.repeat, lineage_type, th_id, lineage_size])
            else:
                results.append([i, avg, sf, args.repeat, lineage_type, th_id, 0])
            if args.persist and args.query_lineage:
                args.enable_lineage=False
                print("%%%%%% Running Lineage Query # ", i)
                lineage_prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
                lineage_q = lineage_prefix+str(i).zfill(2)+".sql"
                text_file = open(lineage_q, "r")
                lineage_q = text_file.read()
                text_file.close()
                avg, output_size = Run(lineage_q.format(query_id), args, con)
                results.append([i, avg, sf, args.repeat, "SD_Query", th_id, output_size])
                DropLineageTables(con)
                args.enable_lineage=True

if args.save_csv:
    filename="tpch_benchmark_notes_"+args.notes+"_lineage_type_"+lineage_type+".csv"
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type", "n_threads", "size"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
