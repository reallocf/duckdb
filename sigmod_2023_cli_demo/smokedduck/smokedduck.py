import duckdb
from flask import Flask, request
app = Flask(__name__)

con = duckdb.connect(database=':memory:', read_only=False)
con.execute("CALL dbgen(sf=1);")

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
