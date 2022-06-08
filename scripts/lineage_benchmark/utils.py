from timeit import default_timer as timer
import random 
import sys
import pandas as pd
import numpy as np

def execute(Q, con, args):
    Q = " ".join(Q.split())
    if args.profile:
        con.execute("PRAGMA enable_profiling;")
    if args.enable_lineage:
        con.execute("PRAGMA trace_lineage='ON'")
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    if args.enable_lineage:
        con.execute("PRAGMA trace_lineage='OFF'")
    if args.profile:
        con.execute("PRAGMA disable_profiling;")
    return df, end - start

def DropLineageTables(con):
    con.execute("DELETE FROM queries_list")
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            con.execute("DROP TABLE "+row["name"])

def InternalRun(q, args, con, table_name=None):
    dur_acc = 0.0
    print("Run: ", table_name, q)
    for j in range(args.repeat-1):
        df, duration = execute(q, con, args)
        dur_acc += duration
        if args.enable_lineage:
            DropLineageTables(con)
        if table_name:
            con.execute("drop table {}".format(table_name))
    
    df, duration = execute(q, con, args)
    dur_acc += duration
    if args.show_output:
        print(df)
    avg = dur_acc/args.repeat
    print("Avg Time in sec: ", avg, " output size: ", len(df)) 
    return avg, len(df), df

def Run(q, args, con, table_name=None):
    avg, lendf, _ = InternalRun(q, args, con, table_name)
    return avg, lendf
"""
z is an integer that follows a zipfian distribution
and v is a double that follows a uniform distribution
in [0, 100]. θ controls the zipfian skew, n is the table
size, and g specifies the number of distinct z values
"""

class ZipfanGenerator(object):
    def __init__(self, n_groups, zipf_constant, card):
        self.n_groups = n_groups
        self.zipf_constant = zipf_constant
        self.card = card
        self.initZipfan()

    def zeta(self, n, theta):
        ssum = 0.0
        for i in range(0, n):
            ssum += 1. / pow(i+1, theta)
        return ssum

    def initZipfan(self):
        zetan = 1./self.zeta(self.n_groups, self.zipf_constant)
        proba = [.0] * self.n_groups
        proba[0] = zetan
        for i in range(0, self.n_groups):
            proba[i] = proba[i-1] + zetan / pow(i+1., self.zipf_constant)
        self.proba = proba

    def nextVal(self):
        uni = random.uniform(0.1, 1.01)
        lower = 0
        for i, v in enumerate(self.proba):
            if v >= uni:
                break
            lower = i
        return lower

    def getAll(self):
        result = []
        for i in range(0, self.card):
            result.append(self.nextVal())
        return result

import random

def SelectivityGenerator(selectivity, card):
    card = card
    sel = selectivity
    n = int(float(card) * sel)
    print(card, sel, n)
    result = [0] * n
    for i in range(card-n):
        result.append(random.randint(1, card))

    random.shuffle(result)
    test_sel = [a for a in result if a == 0]
    print("execpted selectivity : ", sel, " actual: ", len(test_sel)/float(card))
    return result
