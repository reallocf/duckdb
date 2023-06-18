import duckdb
from flask import Flask, request
app = Flask(__name__)

con = duckdb.connect(database=':memory:', read_only=False)
sf = 0.1
print(f"Loading TPC-H data with sf {sf}")
con.execute(f"CALL dbgen(sf={sf});")
sf2 = 1
print(f"Also loading TPC-H data with sf {sf2}")
con.execute(f"CALL dbgen(sf={sf2}, suffix='_sf{sf2}')")

@app.route('/')
def hello_world():
    return 'Hello, Docker!'

@app.post("/sql")
def execute_sql():
    body = request.json
    query = body['query']
    should_capture = body['capture']
    if should_capture:
        con.execute("PRAGMA trace_lineage='ON'")
    res = con.execute(query).fetchall()
    if should_capture:
        con.execute("PRAGMA trace_lineage='OFF'")
    return {"res": res}
