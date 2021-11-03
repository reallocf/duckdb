import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

def execute(Q):
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    print("Time in sec: ", end - start) 
    return df, end - start

con = duckdb.connect(database=':memory:', read_only=False)

groups = [100]
cardinality = [1000000, 5000000, 10000000]
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
        perm_q = """SELECT q.*, zipf1_p.rowid from zipf1 as zipf1_p,
        (SELECT z, count(*) FROM zipf1 GROUP BY z) as q
        where zipf1_p.z=q.z
        """
        out, t1 = execute(q)
        out, t2 = execute(perm_q)
        overhead = ((t2-t1)/t1) * 100.0
        print("overhead: ", overhead)
        con.execute("drop table zipf1")
############## PK-FK ##########

print("Pk-Fk")
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
        perm_q = """SELECT gids.rowid, zipf1.rowid, * FROM gids, zipf1 WHERE gids.id=zipf1.z
        """
        out, t1 = execute(q)
        out, t2 = execute(perm_q)
        overhead = ((t2-t1)/t1) * 100.0
        print("overhead: ", overhead)
        con.execute("drop table zipf1")
    con.execute("drop table gids")

############## many-to-many join ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
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
        perm_q = "SELECT zipf1.rowid, zipf2.rowid, * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
        out, t1 = execute(q)
        out, t2 = execute(perm_q)
        overhead = ((t2-t1)/t1) * 100.0
        print("overhead: ", overhead)
        con.execute("drop table zipf1")
con.execute("drop table zipf2")
