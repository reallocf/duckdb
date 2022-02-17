import duckdb
import datetime
import pandas as pd
import argparse
import csv

from utils import Run

parser = argparse.ArgumentParser(description='TPCH single script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('query_id', type=int,  help="query id")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--perm_bw', action='store_true',  help="use perm bw queries")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--threads', type=int, help="number of threads", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--profile', action='store_true',  help="enable profiling")
parser.add_argument('--query_lineage', action='store_true',  help="run lineage query")
args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf="+str(args.sf)+");")
if args.threads > 1:
    con.execute("PRAGMA threads="+str(args.threads))
    con.execute("PRAGMA force_parallelism")

print(args)

base = "extension/tpch/dbgen/queries"
prefix = base+"/q"
table_name = None
if args.perm:
    prefix = base+"/perm/q"
    table_name = 'lineage'
    args.perm_bw = False

if args.perm_bw:
    # run base query
    qkeys = base+"/perm_keys/q"+str(args.query_id).zfill(2)+".sql"
    qkeys_text_file = open(qkeys, "r")
    qkeys = qkeys_text_file.read()
    qkeys = " ".join(qkeys.split())
    qkeys = qkeys.split(',')
    print(qkeys)

    q = prefix+str(args.query_id).zfill(2)+".sql"
    text_file = open(q, "r")
    base_q = text_file.read()
    base_q = " ".join(base_q.split())
    text_file.close()
    df = con.execute(base_q).fetchdf()
    print(df)
    prefix = base + "/perm_bw/q"
    for index, row in df.iterrows():
        predicate=''
        for k in qkeys:
            if len(k) == 0:
                continue
            if len(predicate)>0:
                predicate+=" AND "
            val = row[k]
            if isinstance(row[k], int) or isinstance(row[k], float):
                predicate+=k+"="+str(val)
            elif isinstance(row[k], str):
                predicate+=k+ "='"+str(val)+"'"
            elif isinstance(row[k],datetime.date):
                predicate+=k+"='"+val.strftime('%d-%m-%y')+"'"
            else:
                predicate+=k+"="+str(val)
        if len(predicate) > 0:
            print(predicate)
            predicate = " where " + predicate
        q = prefix+str(args.query_id).zfill(2)+".sql"
        text_file = open(q, "r")
        tpch = text_file.read()
        tpch = " ".join(tpch.split())
        text_file.close()
        avg, output_size = Run(tpch + " " + predicate, args, con, table_name)


else:
    q = prefix+str(args.query_id).zfill(2)+".sql"
    text_file = open(q, "r")
    tpch = text_file.read()
    tpch = " ".join(tpch.split())
    text_file.close()
    avg, output_size = Run(tpch, args, con, table_name)

if args.show_tables:
    print(con.execute("PRAGMA show_tables").fetchdf())

if args.query_lineage:
    if args.enable_lineage:
        args.profile=False
        query_id = con.execute("select max(query_id) as qid from queries_list").fetchdf().loc[0, 'qid'] 
        query_info = con.execute("select * from queries_list where query_id='{}'".format(query_id)).fetchdf()
        lineage_size = query_info.loc[0, 'lineage_size']

        lineage_prefix = "extension/tpch/dbgen/queries/lineage_queries/q"
        lineage_q = lineage_prefix+str(args.query_id).zfill(2)+".sql"
        text_file = open(lineage_q, "r")
        lineage_q = text_file.read()
        avg, output_size = Run(lineage_q.format(query_id), args, con)
        print("Query ID: ", query_id, " output size: ", output_size, " Lineage Size: ", lineage_size/(1024.0*1024), " MB")
    elif args.perm:
        print(con.execute("select * from lineage").fetchdf())
        df = con.execute("select count(*) as c from lineage").fetchdf()
        output_size = df.loc[0,'c']
        print("Lineage output size: ", output_size)
