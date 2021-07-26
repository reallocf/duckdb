import duckdb
from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Keep track of expected values for each so we can make sure that zeros are set when a brushing would otherwise remove a bar
expectedCauses = []
expectedStates = []
expectedYears = []

con = duckdb.connect(database=":memory:", read_only=False)
con.execute("CREATE TABLE fires AS SELECT * FROM read_csv_auto('data/fires_with_dropped_cols.csv', SAMPLE_SIZE=-1)")

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

def jsonifyDuckData(data):
  return [{"x": d[0], "y": d[1]} for d in data]

@app.route("/initial", methods=["POST"])
def getInitialData():
  global expectedCauses, expectedStates, expectedYears
  causeData = jsonifyDuckData(con.execute("SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR").fetchall())
  expectedCauses = [d["x"] for d in causeData]
  stateData = jsonifyDuckData(con.execute("SELECT STATE, COUNT(*) FROM fires GROUP BY STATE").fetchall())
  expectedStates = [d["x"] for d in stateData]
  yearData = jsonifyDuckData(con.execute("SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR").fetchall())
  expectedYears = [d["x"] for d in yearData]
  return {"cause": causeData, "state": stateData, "year": yearData}

@app.route("/update", methods=["POST"])
def getDataByBrushings():
  global expectedCauses, expectedStates, expectedYears
  if len(expectedCauses) == 0 or len(expectedStates) == 0 or len(expectedYears) == 0:
    # hackiness...
    raise RuntimeError("Must call /initial before /update")
  req = request.json
  whatChanged = req["changed"]
  causeBrush = req["cause"]
  stateBrush = req["state"]
  yearBrush = req["year"]
  yearBrush = [str(y) for y in yearBrush]
  if whatChanged == "cause":
    causeData = ""
    stateData = updateState(causeBrush, yearBrush)
    yearData = updateYear(causeBrush, stateBrush)
  elif whatChanged == "state":
    causeData = updateCause(stateBrush, yearBrush)
    stateData = ""
    yearData = updateYear(causeBrush, stateBrush)
  elif whatChanged == "year":
    causeData = updateCause(stateBrush, yearBrush)
    stateData = updateState(causeBrush, yearBrush)
    yearData = ""
  else:
    raise ValueError(f"Unexpected whatChanged {whatChanged}")
  causeData = {cause[0]: cause[1] for cause in causeData}
  causeData = [[cause, 0] if cause not in causeData else [cause, causeData[cause]] for cause in expectedCauses]
  stateData = {state[0]: state[1] for state in stateData}
  stateData = [[state, 0] if state not in stateData else [state, stateData[state]] for state in expectedStates]
  yearData = {year[0]: year[1] for year in yearData}
  yearData = [[year, 0] if year not in yearData else [year, yearData[year]] for year in expectedYears]
  res = {"cause": jsonifyDuckData(causeData), "state": jsonifyDuckData(stateData), "year": jsonifyDuckData(yearData)}
  return res

# TODO replace all of the below with actual lineage queries
def updateCause(stateBrush, yearBrush):
  if len(stateBrush) != 0 and len(yearBrush) != 0:
    q = f"""
SELECT STAT_CAUSE_DESCR, COUNT(*)
FROM fires
WHERE STATE IN ('{"','".join(stateBrush)}') AND FIRE_YEAR IN ('{"','".join(yearBrush)}')
GROUP BY STAT_CAUSE_DESCR
    """
  elif len(stateBrush) != 0:
    q = f"""
SELECT STAT_CAUSE_DESCR, COUNT(*)
FROM fires
WHERE STATE IN ('{"','".join(stateBrush)}')
GROUP BY STAT_CAUSE_DESCR
    """
  elif len(yearBrush) != 0:
    q = f"""
SELECT STAT_CAUSE_DESCR, COUNT(*)
FROM fires
WHERE FIRE_YEAR IN ('{"','".join(yearBrush)}')
GROUP BY STAT_CAUSE_DESCR
    """
  else:
    q = "SELECT STAT_CAUSE_DESCR, COUNT(*) FROM fires GROUP BY STAT_CAUSE_DESCR"
  return con.execute(q).fetchall()

def updateState(causeBrush, yearBrush):
  if len(causeBrush) != 0 and len(yearBrush) != 0:
    q = f"""
SELECT STATE, COUNT(*)
FROM fires
WHERE STAT_CAUSE_DESCR IN ('{"','".join(causeBrush)}') AND FIRE_YEAR IN ('{"','".join(yearBrush)}')
GROUP BY STATE
    """
  elif len(causeBrush) != 0:
    q = f"""
SELECT STATE, COUNT(*)
FROM fires
WHERE STAT_CAUSE_DESCR IN ('{"','".join(causeBrush)}')
GROUP BY STATE
    """
  elif len(yearBrush) != 0:
    q = f"""
SELECT STATE, COUNT(*)
FROM fires
WHERE FIRE_YEAR IN ('{"','".join(yearBrush)}')
GROUP BY STATE
    """
  else:
    q = "SELECT STATE, COUNT(*) FROM fires GROUP BY STATE"
  return con.execute(q).fetchall()

def updateYear(causeBrush, stateBrush):
  if len(causeBrush) != 0 and len(stateBrush) != 0:
    q = f"""
SELECT FIRE_YEAR, COUNT(*)
FROM fires
WHERE STAT_CAUSE_DESCR IN ('{"','".join(causeBrush)}') AND STATE IN ('{"','".join(stateBrush)}')
GROUP BY FIRE_YEAR
    """
  elif len(causeBrush) != 0:
    q = f"""
SELECT FIRE_YEAR, COUNT(*)
FROM fires
WHERE STAT_CAUSE_DESCR IN ('{"','".join(causeBrush)}')
GROUP BY FIRE_YEAR
    """
  elif len(stateBrush) != 0:
    q = f"""
SELECT FIRE_YEAR, COUNT(*)
FROM fires
WHERE STATE IN ('{"','".join(stateBrush)}')
GROUP BY FIRE_YEAR
    """
  else:
    q = "SELECT FIRE_YEAR, COUNT(*) FROM fires GROUP BY FIRE_YEAR"
  return con.execute(q).fetchall()

