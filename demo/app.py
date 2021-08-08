import duckdb
from flask import Flask, request
from flask_cors import CORS
from threading import Lock

app = Flask(__name__)
CORS(app)

# Keep track of expected values for each so we can make sure that zeros are set when a brushing would otherwise remove a bar
expectedCauses = []
expectedStates = []
expectedYears = []

con = duckdb.connect(database=":memory:", read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('data/fires_with_dropped_cols.csv')")
dbLock = Lock()

def hello_world():
  return "<p>Hello, World!</p>"

def jsonifyDuckData(data):
  return [{"x": d[0], "y": d[1]} for d in data]

@app.route("/sql", methods=["POST"])
def executeSQL():
  # Expects sql to be a list of sql statements of the form "SELECT foo, COUNT(*) FROM fires" or "SELECT bar, MAX(foo) FROM fires"
  # or some lineage query that results in a map from x to y
  req = request.json
  sqls = req["sqls"]
  print(sqls)
  # Execute one set of queries at a time so we can handle PRAGMA trace_lineage='ON'/'OFF'
  dbLock.acquire()
  try:
    res = {"res": dict()}
    for sql in sqls:
      thisRes = ''
      if sql['nores'] is True:
        con.execute(sql['value'])
      else:
        thisRes = jsonifyDuckData(con.execute(sql["value"]).fetchall())
      res['res'][sql['name']] = thisRes
  finally:
    dbLock.release()
  return res

if __name__ == "__main__":
  app.run(host='0.0.0.0', debug=False, threaded=True)
