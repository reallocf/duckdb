import duckdb
import pandas as pd
from timeit import default_timer as timer
import argparse
import csv

def execute(Q):
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    return df, end - start

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_output', action='store_true',  help="query output")
args = parser.parse_args()
results = []

con = duckdb.connect(database=':memory:', read_only=False)

############## order by ##########
print("------------ Test Order By zipfan 1")
groups = [100]
cardinality = [1000000, 5000000, 10000000]
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")
        q = "SELECT z  FROM zipf1 Order By z"
        dur_acc = 0.0
        for j in range(args.repeat):
            df, duration = execute(q)
            if args.show_output:
                print(df)
            print("Time: ", duration)
            dur_acc += duration
        avg = dur_acc/args.repeat
        print("Avg Time in sec: ", avg) 
        results.append(["orderby", avg, card, g, 1, args.enable_lineage])
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")

############## filter ##########
# 208205/1000000, 1040334/5000000, 2079114/10000000
print("------------ Test Filter zipfan 1")
groups = [100]
cardinality = [1000000, 5000000, 10000000]
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")
        q = "SELECT z,v  FROM zipf1 where z=0"
        dur_acc = 0.0
        size = 0
        for j in range(args.repeat):
            df, duration = execute(q)
            if args.show_output:
                print(df)
            size = len(df)
            print("Time: ", duration)
            dur_acc += duration
        selectivity = float(size)/card
        avg = dur_acc/args.repeat
        print("Avg Time in sec: ", avg) 
        results.append(["filter", avg, card, g, selectivity, args.enable_lineage])
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")

############## Group By ##########
print("------------ Test Group By zipfan 1")
groups = [100]
cardinality = [1000000, 5000000, 10000000]
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")
        q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
        dur_acc = 0.0
        for j in range(args.repeat):
            df, duration = execute(q)
            if args.show_output:
                print(df)
            print("Time: ", duration)
            dur_acc += duration
        avg = dur_acc/args.repeat
        print("Avg Time in sec: ", avg) 
        results.append(["groupby", avg, card, g, 1, args.enable_lineage])
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")

############## PK-FK ##########
print("------------ Test Pk-Fk zipfan 1")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
for g in groups:
    idx = list(range(0, g))
    gid = pd.DataFrame({'id':idx})
    con.register('gids_view', gid)
    con.execute("create table gids as select * from gids_view")
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        
        q = "SELECT * FROM gids, zipf1 WHERE gids.id=zipf1.z"
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")

        dur_acc = 0.0
        for j in range(args.repeat):
            df, duration = execute(q)
            if args.show_output:
                print(df)
            print("Time: ", duration)
            dur_acc += duration
        avg = dur_acc/args.repeat
        print("Avg Time in sec: ", avg) 

        results.append(["pkfk", avg, card, g, 1, args.enable_lineage])
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")
    con.execute("drop table gids")

############## many-to-many join ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
print("------------ Test Many to Many Join zipfan 1")
groups = [10, 100]
cardinality = [1000, 10000, 100000]
filename = "zipfan_g100_card1000_a1.0.csv"
zipf2 = pd.read_csv(filename)
con.register('zipf2_view', zipf2)
con.execute("create table zipf2 as select * from zipf2_view")
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        
        q = "SELECT * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='ON'")

        dur_acc = 0.0
        for j in range(args.repeat):
            df, duration = execute(q)
            if args.show_output:
                print(df)
            print("Time: ", duration)
            dur_acc += duration
        avg = dur_acc/args.repeat
        print("Avg Time in sec: ", avg) 

        results.append(["m:n", avg, card, g, 1, args.enable_lineage])
        if args.enable_lineage:
            con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")
con.execute("drop table zipf2")


if args.save_csv:
    filename="micro_benchmark_notes_"+args.notes+".csv"
    print(filename)
    header = ["query", "runtime", "cardinality", "groups", "z", "lineage"]
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
