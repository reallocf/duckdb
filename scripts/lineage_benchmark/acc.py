### for Q11, use for queries/q11.sql and perm_bw/q11.sql
### sf10: sum(ps_supplycost * ps_availqty) * 0.0000100000
### sf1: sum(ps_supplycost * ps_availqty) * 0.0001000000
import duckdb
import datetime
import pandas as pd
import argparse
import csv
import random

from utils import Run, InternalRun

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
parser.add_argument('--end2end', action='store_true',  help="end to end logical-rid")
parser.add_argument('--end2end_index', action='store_true',  help="end to end logical-rid + index")

args = parser.parse_args()

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf="+str(args.sf)+");")
if args.threads > 1:
    con.execute("PRAGMA threads="+str(args.threads))
    con.execute("PRAGMA force_parallelism")
results = []
print(args)


lineage_type = "Logical-RID"
if args.end2end:
    lineage_type = "Logical-RID-e2e"
if args.end2end_index:
    lineage_type = "Logical-RID-e2eindex"

## if end_to_end + filter, create table lineaage, then scan and apply predicate for bw query
## if end_to_end + index, create table, create index + timeit, then scan + predicate
base = "extension/tpch/dbgen/queries"
table_name = None
for qid in range(1, 23):
    index_avg = 0
    preprocess_avg = 0
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
    preprocess_avg, lendf, df = InternalRun(base_q, args, con)
    if not args.end2end and not args.end2end_index:
        results.append([qid, preprocess_avg, args.sf, args.repeat, lineage_type, args.threads, lendf, "preprocess"])
    if args.end2end or args.end2end_index:
        # load the select attributes
        qselect = base+"/perm_select/q"+str(qid).zfill(2)+".sql"
        qselect_text_file = open(qselect, "r")
        qselect = qselect_text_file.read()
        qselect = " ".join(qselect.split())
        qselect = qselect.split(',')

        prefix = base + "/perm/q"
        table_name = "lineage"
        q = prefix+str(qid).zfill(2)+".sql"
        text_file = open(q, "r")
        tpch = text_file.read()
        text_file.close()
        # time it
        preprocess_avg, _ = Run(tpch, args, con, table_name)
        query_info = con.execute("select count(*) as c from lineage").fetchdf()
        lineage_size = query_info.loc[0, 'c']
        results.append([qid, preprocess_avg, args.sf, args.repeat, lineage_type, args.threads, lineage_size, "preprocess"])

        if args.end2end_index and len(qkeys) > 0 and len(qkeys[0]) > 0:
            print(qkeys)
            index_keys = ''
            for att in qkeys:
                if len(index_keys): index_keys += ","
                index_keys += att
            for att in qselect:
                if len(index_keys): index_keys += ","
                index_keys += att
            indexsql = "create index i_index ON lineage("+index_keys+");"
            repeat = args.repeat
            args.repeat = 1
            index_avg, _ = Run(indexsql, args, con)
            args.repeat = repeat
            results.append([qid, index_avg, args.sf, args.repeat, lineage_type, args.threads, -1, "postprocess"])
    else:
        prefix = base + "/perm_bw/q"
    t = 0
    tmin = 1000000
    tmax = 0
    sample_size = min(len(df), 10)
    print(sample_size)
    test_out = random.sample(range(0, len(df)), sample_size)
    if qid == 13:
        test_out = [2, 40, 30, 7, 6, 12, 34, 20, 18, 37]
    print(test_out)
    if sample_size == 0:
        continue

    print("start")
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
        if args.end2end or args.end2end_index:
            select = 'count(*) as c'
            for att in qselect:
                select += ", max("+att+")"
            tpch = "select "+select+" from lineage" + predicate
            # todo:
            #   1) to have fair comparison. have similar select args as the
            #       one below
            #   2) if  index, then create index
            avg, output_size = Run(tpch, args, con)
        else:
            q = prefix+str(qid).zfill(2)+".sql"
            text_file = open(q, "r")
            tpch = text_file.read()
            tpch = " ".join(tpch.split()) + " " + predicate
            text_file.close()
            avg, _ = Run(tpch, args, con)
            output_size = con.execute(tpch).fetchdf().loc[0, 'c']
        results.append([qid, avg, args.sf, args.repeat, lineage_type, args.threads, output_size, "bw"])

    if args.end2end or args.end2end_index:
        if args.end2end_index and len(qkeys) > 0 and len(qkeys[0]) > 0:
            con.execute("DROP INDEX i_index")
        con.execute("DROP TABLE lineage")

if args.save_csv:
    filename="tpch_acc_notes_"+args.notes+"_lineage_type_Logical-RID.csv"
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type", "n_threads", "lineage_size", "base_size", "stage_type"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

