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
t2s = [0.006638531000135117, 0.027228143000684213, 0.05309844800103747]
i = 0
for g in groups:
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        con.execute("PRAGMA trace_lineage='ON'")
        q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
        out, t1 = execute(q)
        overhead = ((t1-t2s[i])/t2s[i])
        i += 1
        print("overhead: ", overhead)

        con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")
############## PK-FK ##########

print("Pk-Fk *******")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
t2_g = {10: [0.05568498699904012,0.32895101999929466, 0.6486240760004875],
        100: [0.053311012001358904, 0.45648362999963865,  0.6572541589994216],
        1000: [0.07304794999981823, 0.3445980509986839, 0.6731980709992058]}
for g in groups:
    idx = list(range(0, g))
    gid = pd.DataFrame({'id':idx})
    con.register('gids_view', gid)
    con.execute("create table gids as select * from gids_view")
    t2 = t2_g[g]
    i = 0
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        
        q = "SELECT * FROM gids, zipf1 WHERE gids.id=zipf1.z"
        con.execute("PRAGMA trace_lineage='ON'")
        out, t1 = execute(q)
        overhead = ((t1-t2[i])/t2[i])
        print("overhead: ", overhead)
        i += 1
        con.execute("PRAGMA trace_lineage='OFF'")
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
t2_g = {10: [0.006285223000304541, 0.08540539599925978,  0.6879868140003964],
        100: [0.003966889000366791, 0.042753309000545414, 0.41224532399974123]}
print("M:N join *********")
for g in groups:
    t2 = t2_g[g]
    i = 0
    for card in cardinality:
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.0.csv"
        print(filename, g, card)
        zipf1 = pd.read_csv(filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        
        q = "SELECT * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
        con.execute("PRAGMA trace_lineage='ON'")
        out, t1 = execute(q)
        overhead = ((t1-t2[i])/t2[i])
        print("overhead: ", overhead)
        i += 1
        con.execute("PRAGMA trace_lineage='OFF'")
        con.execute("drop table zipf1")
con.execute("drop table zipf2")
