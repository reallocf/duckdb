import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

if len(sys.argv) < 2:
    sys.exit()

text_file = open(sys.argv[1], "r")

#read whole file to a string
tpch = text_file.read()
tpch = " ".join(tpch.split())

text_file.close()

con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CALL dbgen(sf=1);")
con.execute("PRAGMA enable_profiling;")

start = timer()
q = con.execute(tpch).fetchdf()
end = timer()
print("Time in sec: ", end - start) 
con.execute("PRAGMA disable_profiling;")
print(q)
q.to_csv("perm_q3.csv")
