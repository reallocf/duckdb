# Benchmark lineage systems with increasingly complex queries

import duckdb
import pandas as pd
import numpy as np
import argparse
import csv
import os.path

from utils import execute, ZipfanGenerator, DropLineageTables, Run

parser = argparse.ArgumentParser(description='Micro Benchmark: Physical Join Operators')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--persist', action='store_true',  help="Lineage persist")
parser.add_argument('--perm', action='store_true',  help="run perm lineage queries")
args = parser.parse_args()
if args.enable_lineage and args.perm:
    args.perm = False

old_profile_val = args.profile
results = []

if args.enable_lineage and args.persist:
    lineage_type = "SD_Persist"
elif args.enable_lineage:
    lineage_type = "SD_Capture"
elif args.perm:
    lineage_type = "Logical_RID"
else:
    lineage_type = "Baseline"

con = duckdb.connect(database=':memory:', read_only=False)

def SmokedDuck(q, q_lineage, level):
    args.enable_lineage = True
    args.profile=old_profile_val
    avg, output_size = Run(q, args, con)
    if args.persist:
        args.enable_lineage = False
        args.profile=False
        results.append(["SD_Persist", "level2", avg, output_size, card, N[0], N[1], N[2], N[3]])
        query_id = con.execute("select max(query_id) as qid from queries_list").fetchdf().loc[0, 'qid'] - 1
        avg, output_size = Run(q_lineage.format(query_id), args, con, "lineage")
        output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
        results.append(["SD_Query", level, avg, output_size, card, N[0], N[1], N[2], N[3]])
        DropLineageTables(con)
        con.execute("DROP TABLE lineage");
    else:
        results.append(["SD_Capture", level, avg, -1, card, N[0], N[1], N[2], N[3]])
################### Check data exists if not, then generate data
folder = "benchmark_data/"
cardinality = [1000]
N_list = [(10, 10, 10, 10)]
a = 1

for N in N_list:
    for card in cardinality:
        filename = folder+"zipfan_card"+str(card)+"_N"+str(N)+".csv"
        print(filename)
        if not os.path.exists(filename):
            print("geenrate ", filename)
            n1, n2, n3, n4 = N[0], N[1], N[2], N[3]
            A = ZipfanGenerator(n1, a, card)
            ## n1 elements in B=0 and the rest are random
            B = ZipfanGenerator(n2, a, card)
            ## n2 elements in v1 less than n1
            C = ZipfanGenerator(n3, a, card)
            ## n3 elements in v2 less than n2
            D = ZipfanGenerator(n4, a, card)

            idx = list(range(0, card))
            dataset = pd.DataFrame({'idx':idx, 'A': A.getAll(), 'B': B.getAll(), 'C': C.getAll(), 'D': D.getAll()})
            dataset.to_csv(filename, index=False)
        else:
            dataset = pd.read_csv(filename)

        # initialize table
        con.register('t1_view', dataset)
        con.execute("create table t1 as select * from t1_view")
        output = con.execute("select rowid, * from t1").fetchdf()
        print(output)
        # queries
        level1 = "select A, B, C, D from t1 group by A, B, C, D"
        level2 = "select A, B, C from t1 join ({}) using (A,B,C, D) group by A, B, C".format(level1)
        level3 = "select A, B from t1 join ({}) using (A, B, C) group by A, B".format(level2)
        level4 = """select A from t1 join ({}) using (A, B) group by A""".format(level3)
        out_index = "ROW_NUMBER() OVER (ORDER BY (SELECT 0))"

        if args.enable_lineage:
            # capture lineage using SmokedDUck
            ################## level 1 #####################
            level1_lineage = """create table lineage as (
            SELECT LINEAGE_{0}_HASH_GROUP_BY_2_0.in_index as t1_rowid_0,
                   LINEAGE_{0}_HASH_GROUP_BY_2_1.out_index
            FROM LINEAGE_{0}_HASH_GROUP_BY_2_1, LINEAGE_{0}_HASH_GROUP_BY_2_0
            WHERE LINEAGE_{0}_HASH_GROUP_BY_2_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_2_0.out_index
            )
            """
            SmokedDuck(level1, level1_lineage, "level1")
            ################## level 2 #####################
            level2_lineage = """create table lineage as (
            SELECT LINEAGE_{0}_HASH_JOIN_4_1.rhs_index as t1_rowid_0, LINEAGE_{0}_HASH_GROUP_BY_3_0.in_index as t1_rowid_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_1.out_index FROM LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_0, LINEAGE_{0}_HASH_JOIN_4_1, LINEAGE_{0}_HASH_JOIN_4_0, LINEAGE_{0}_HASH_GROUP_BY_3_1, LINEAGE_{0}_HASH_GROUP_BY_3_0 WHERE LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_0.out_index and LINEAGE_{0}_HASH_JOIN_4_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_6_0.in_index and LINEAGE_{0}_HASH_JOIN_4_0.out_address=LINEAGE_{0}_HASH_JOIN_4_1.lhs_address and LINEAGE_{0}_HASH_JOIN_4_0.in_index=LINEAGE_{0}_HASH_GROUP_BY_3_1.out_index and LINEAGE_{0}_HASH_GROUP_BY_3_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_3_0.out_index
            )"""
            SmokedDuck(level2, level2_lineage, "level2")
            ################## level 3 #####################
            level3_lineage = """create table lineage as (
            SELECT LINEAGE_{0}_HASH_JOIN_8_1.rhs_index as t1_rowid_0, LINEAGE_{0}_HASH_JOIN_5_1.rhs_index as t1_rowid_1, LINEAGE_{0}_HASH_GROUP_BY_4_0.in_index as t1_rowid_2, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_1.out_index FROM LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_0, LINEAGE_{0}_HASH_JOIN_8_1, LINEAGE_{0}_HASH_JOIN_8_0, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_0, LINEAGE_{0}_HASH_JOIN_5_1, LINEAGE_{0}_HASH_JOIN_5_0, LINEAGE_{0}_HASH_GROUP_BY_4_1, LINEAGE_{0}_HASH_GROUP_BY_4_0 WHERE LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_0.out_index and LINEAGE_{0}_HASH_JOIN_8_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_10_0.in_index and LINEAGE_{0}_HASH_JOIN_8_0.out_address=LINEAGE_{0}_HASH_JOIN_8_1.lhs_address and LINEAGE_{0}_HASH_JOIN_8_0.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_1.out_index and LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_0.out_index and LINEAGE_{0}_HASH_JOIN_5_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_7_0.in_index and LINEAGE_{0}_HASH_JOIN_5_0.out_address=LINEAGE_{0}_HASH_JOIN_5_1.lhs_address and LINEAGE_{0}_HASH_JOIN_5_0.in_index=LINEAGE_{0}_HASH_GROUP_BY_4_1.out_index and LINEAGE_{0}_HASH_GROUP_BY_4_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_4_0.out_index
              )"""
            SmokedDuck(level3, level3_lineage, "level3")
            ################## level 4 #####################
            level4_lineage = """create table lineage as (
            SELECT LINEAGE_{0}_HASH_JOIN_12_1.rhs_index as t1_rowid_0, LINEAGE_{0}_HASH_JOIN_9_1.rhs_index as t1_rowid_1,
            LINEAGE_{0}_HASH_JOIN_6_1.rhs_index as t1_rowid_2, LINEAGE_{0}_HASH_GROUP_BY_5_0.in_index as t1_rowid_3,
            LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_1.out_index
            FROM LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_0, LINEAGE_{0}_HASH_JOIN_12_1, LINEAGE_{0}_HASH_JOIN_12_0, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_0, LINEAGE_{0}_HASH_JOIN_9_1, LINEAGE_{0}_HASH_JOIN_9_0, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_0, LINEAGE_{0}_HASH_JOIN_6_1, LINEAGE_{0}_HASH_JOIN_6_0, LINEAGE_{0}_HASH_GROUP_BY_5_1, LINEAGE_{0}_HASH_GROUP_BY_5_0 WHERE LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_0.out_index and LINEAGE_{0}_HASH_JOIN_12_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_14_0.in_index and LINEAGE_{0}_HASH_JOIN_12_0.out_address=LINEAGE_{0}_HASH_JOIN_12_1.lhs_address and LINEAGE_{0}_HASH_JOIN_12_0.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_1.out_index and LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_0.out_index and LINEAGE_{0}_HASH_JOIN_9_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_11_0.in_index and LINEAGE_{0}_HASH_JOIN_9_0.out_address=LINEAGE_{0}_HASH_JOIN_9_1.lhs_address and LINEAGE_{0}_HASH_JOIN_9_0.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_1.out_index and LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_0.out_index and LINEAGE_{0}_HASH_JOIN_6_1.out_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_8_0.in_index and LINEAGE_{0}_HASH_JOIN_6_0.out_address=LINEAGE_{0}_HASH_JOIN_6_1.lhs_address and LINEAGE_{0}_HASH_JOIN_6_0.in_index=LINEAGE_{0}_HASH_GROUP_BY_5_1.out_index and LINEAGE_{0}_HASH_GROUP_BY_5_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_5_0.out_index
              )"""
            SmokedDuck(level4, level4_lineage, "level4")
        elif args.perm:
            ################## level 1 #####################
            level1_lineage = """create table lineage as (
                select A, B, C, D, t1.rowid as t1_rowid_0 from t1 join (
                select A, B, C, D from t1 group by A, B, C, D) using (A, B, C, D)
            )""".format(out_index)
            avg, output_size = Run(level1_lineage, args, con, "lineage")
            output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
            results.append(["Logical_RID", "level1", avg, output_size, card, N[0], N[1], N[2], N[3]])
            con.execute("DROP TABLE lineage");
            ################## level 2 #####################
            level2_join = """
            create table lineage as (
                select t1.A, t1.B, t1.C, t1.rowid as t1_rowid_0,  t1_rowid_1 from t1 join (
                    select t1.rowid as t1_rowid_1, A, B, C, D
                    from t1 join (select A, B, C, D from t1 group by A, B, C, D) using (A, B, C, D)
                ) as level1_lineage using (A, B, C, D) join
                (select A, B, C from t1 join
                    (select A, B, C, D from t1 group by A, B, C, D) using (A,B,C, D) group by A, B, C) as level2
                using (A, B, C)
            )
            """
            avg, output_size = Run(level2_join, args, con, "lineage")
            output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
            results.append(["Logical_RID", "level2", avg, output_size, card, N[0], N[1], N[2], N[3]])
            con.execute("DROP TABLE lineage");
            ################## level 3 #####################
            avg_level2, output_size = Run("create table level2 as ({})".format(level2), args, con, "level2")
            level3_join = """
            create table lineage as (
                select t0.A, t0.B, t0.rowid as t1_rowid_0, level2_lineage.t1_rowid_1, level2_lineage.t1_rowid_2
                 from t1 as t0 join (
                    select t1.A, t1.B, t1.C, t1.rowid as t1_rowid_1,  t1_rowid_2 from t1 join (
                        select t1.rowid as t1_rowid_2, A, B, C, D from t1 join (select A, B, C, D from t1 group by A, B, C, D) using (A, B, C, D)
                    ) as level1_lineage using (A, B, C, D) join level2 using (A, B, C)
                 ) as level2_lineage using (A, B, C) join (select A, B from t1 join level2 using (A, B, C) group by A, B
                 ) as level3 using (A, B)
            )
            """.format(out_index)
            avg, output_size = Run(level3_join, args, con, "lineage")
            output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
            total = avg + avg_level2
            print("level4 total: ", total)
            results.append(["Logical_RID", "level3", total, output_size, card, N[0], N[1], N[2], N[3]])
            con.execute("DROP TABLE lineage");
            con.execute("DROP TABLE level2");
            ################## level 4 #####################
            avg_level1, output_size = Run("create table level1 as ({})".format(level1), args, con, "level1")
            avg_level2, output_size = Run("create table level2 as ({})".format(level2), args, con, "level2")
            avg_level3, output_size = Run("create table level3 as ({})".format(level3), args, con, "level3")
            level4_join = """
            create table lineage as (
            select t1.A, t1.rowid as t1_rowid_0, t1_rowid_1, t1_rowid_2, t1_rowid_3 from t1 join (
                    select t1.A, t1.B, t1.rowid as t1_rowid_1, level2_lineage.t1_rowid_2, level2_lineage.t1_rowid_3
                     from t1 join (
                        select t1.A, t1.B, t1.C, t1.rowid as t1_rowid_2,  t1_rowid_3 from t1 join (
                            select t1.rowid as t1_rowid_3, A, B, C, D from t1 join level1  using (A, B, C, D)
                        ) as level1_lineage using (A, B, C, D) join level2 using (A, B, C)
                     ) as level2_lineage using (A, B, C) join (select A, B from t1 join level2 using (A, B, C) group by A, B
                     ) as level3 using (A, B)
                ) as level3_lineage using (A, B) join (select A from t1 join level3 using (A, B) group by A) as level4 using (A) 
            )
            """.format(out_index)
            avg, output_size = Run(level4_join, args, con, "lineage")
            output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
            total = avg + avg_level1 + avg_level2 + avg_level3
            print("level4 total: ", total)
            results.append(["Logical_RID", "level4", total, output_size, card, N[0], N[1], N[2], N[3]])
            con.execute("DROP TABLE lineage");
            con.execute("DROP TABLE level3");
            con.execute("DROP TABLE level2");
            con.execute("DROP TABLE level1");
        else:
            baseline_avg, output_size = Run(level1, args, con)
            results.append(["Baseline", "level1", baseline_avg, output_size, card, N[0], N[1], N[2], N[3]])

            baseline_avg, output_size = Run(level2, args, con)
            results.append(["Baseline", "level2", baseline_avg, output_size, card, N[0], N[1], N[2], N[3]])

            baseline_avg, output_size = Run(level3, args, con)
            results.append(["Baseline", "level3", baseline_avg, output_size, card, N[0], N[1], N[2], N[3]])

            baseline_avg, output_size = Run(level4, args, con)
            results.append(["Baseline", "level4", baseline_avg, output_size, card, N[0], N[1], N[2], N[3]])

        con.execute("drop table t1")


########### Write results to CSV
if args.save_csv:
    filename="nested_agg_notes_"+args.notes+"_lineage_type_"+lineage_type+".csv"
    print(filename)
    header = ["lineage_type", "level", "runtime", "output_size", "cardinality", "n1", "n2", "n3", "n4"]
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
