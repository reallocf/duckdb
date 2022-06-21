import duckdb
import pandas as pd
import argparse
import csv

from utils import Run

parser = argparse.ArgumentParser(description='TPCH single script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('query_id', type=int,  help="query id")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="enable profiling")
parser.add_argument('--query_lineage', action='store_true',  help="run lineage query")
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf="+str(args.sf)+");")

prefix = "extension/tpch/dbgen/queries/q"
if args.perm:
    prefix = "extension/tpch/dbgen/queries/perm/q"
    args.query_lineage = False

q = prefix+str(args.query_id).zfill(2)+".sql"
text_file = open(q, "r")
tpch = text_file.read()
text_file.close()
avg, output_size = Run(tpch, args, con)

if args.show_tables:
    print(con.execute("PRAGMA show_tables").fetchdf())

if args.query_lineage:
    args.profile=False
    lineage_prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
    lineage_q = lineage_prefix+str(args.query_id).zfill(2)+".sql"
    text_file = open(lineage_q, "r")
    lineage_q = text_file.read()
    query_id = con.execute("select max(query_id) as qid from queries_list").fetchdf().loc[0, 'qid'] - 1
    avg, output_size = Run(lineage_q.format(query_id), args, con)
