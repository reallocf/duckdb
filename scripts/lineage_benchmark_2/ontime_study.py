from concurrent import futures
import duckdb
import pandas as pd
import sys
from timeit import default_timer as timer

con = duckdb.connect(database=':memory:', read_only=False)
'''
print('Loading in data')
con.execute('create table ontime(FlightDate DATE, Origin VARCHAR, DepDelay INTEGER, Reporting_Airline VARCHAR)')
years = [y for y in range(2006, 2009)]
months = [m for m in range(1, 13)]

for y in years:
    for m in months:
        if y == 2005 and m < 4:
            # No data yet
            continue
        if y == 2008 and m > 4:
            # Stopping at Aril
            continue
        print(f'Inserting {m}/{y}')
        #con.execute(f"insert into ontime select FlightDate, Origin, DepDelay, Reporting_Airline FROM read_csv_auto('ontime/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{y}_{m}.csv', all_varchar=True, column_name=)")
        df = pd.read_csv(f'ontime/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{y}_{m}.csv', encoding = "ISO-8859-1", usecols = ['FlightDate', 'Origin', 'DepDelay', 'Reporting_Airline'])
        df = df[['FlightDate', 'Origin', 'DepDelay', 'Reporting_Airline']]
        df.to_csv('/tmp/flights.csv.gz', encoding = 'utf-8', index = False, compression = 'gzip')
        con.execute("insert into ontime select * FROM read_csv_auto('/tmp/flights.csv.gz')")
con.execute("copy ontime to 'ontime/airlines_final.csv'")
'''
'''
print(con.execute('select * from ontime limit 10').df())
print(con.execute('select count(*) from ontime').df())
#df = pd.read_csv('ontime/airline.csv.shuffle', encoding = 'ISO-8859-1', usecols = ['UniqueCarrier', 'DepDelay', 'Origin', 'Year', 'Month', 'DayofMonth'])
#df.to_csv('ontime/full_new.csv', index = False)
#con.execute("copy ontime to 'ontime/airlines.csv'")
'''
#exit()

# DayofMonth  DepDelay Month Origin UniqueCarrier  Year
print('Loading in data')
#con.execute('''create table ontime as select
#            DayofMonth::string || '-' || Month::string || '-' || Year::string as FlightDate,
#            Origin,
#            DepDelay,
#            UniqueCarrier as ReportingAirline
#        from read_csv_auto('ontime/full_new.csv')''')
con.execute("create table ontime as select * from read_csv_auto('ontime/airlines_final.csv')")
#print(con.execute(
#    '''
#with
#cte as (
#    select distinct
#        FlightDate,
#        Origin,
#        DepDelay,
#        ReportingAirline
#    from ontime
#)
#
#select
#    count(*)
#from cte
#    '''
#).df())
#print(con.execute('with cte as (select distinct FlightDate from ontime) select count(*) from cte').df())
#print(con.execute('with cte as (select distinct Origin from ontime) select count(*) from cte').df())
#print(con.execute('with cte as (select distinct DepDelay from ontime) select count(*) from cte').df())
#print(con.execute('with cte as (select distinct ReportingAirline from ontime) select count(*) from cte').df())

# Execute study
flight_date_cnt = con.execute('select count(distinct FlightDate) as cnt from ontime').df()['cnt'][0]
origin_cnt = con.execute('select count(distinct Origin) as cnt from ontime').df()['cnt'][0]
dep_delay_cnt = con.execute('select count(distinct DepDelay) as cnt from ontime').df()['cnt'][0]
reporting_airline_cnt = con.execute('select count(distinct ReportingAirline) as cnt from ontime').df()['cnt'][0]
cols = {'FlightDate': flight_date_cnt, 'Origin': origin_cnt, 'DepDelay': dep_delay_cnt, 'ReportingAirline': reporting_airline_cnt}
queries = {
    'FlightDate': 'select FlightDate, count(*) from ontime group by FlightDate',
    'Origin': 'select Origin, count(*) from ontime group by Origin',
    'DepDelay': 'select DepDelay, count(*) from ontime group by DepDelay',
    'ReportingAirline': 'select ReportingAirline, count(*) from ontime group by ReportingAirline',
}
print(cols)
print(queries)
print('type,duration', file=sys.stderr)
#selected_buckets = all_buckets.sample(n=10000, random_state=1024)
#print(selected_buckets)
pool = futures.ThreadPoolExecutor(max_workers=3)
limit = 3

def exec_db(q):
    print(q)
    return con.execute(q).fetchall()

for _ in range(5):
    print('Starting trial')
    print('Executing base queries with lineage capture on')
    con.execute('pragma trace_lineage = "ON"')
    base_start = timer()
    [x for x in map(exec_db, queries.values())]
    base_end = timer()
    print(f'base,{base_end - base_start}', file=sys.stderr)
    con.execute('pragma trace_lineage = "OFF"')
    print('Executing lineage queries')
    for col, cnt in cols.items():
        print(f'Executing {col}')
        xfilter_timer_agg = 0
        for idx in range(cnt):
            xfilter_start = timer()
            res = con.execute(f"pragma lineage_query('{queries[col]}', '{idx}', 'LIN', 0)").fetchall()
            res = ','.join([str(r[0]) for r in res])
            fw_queries = [f"select {inner_col}, count(*) from ontime where rowid in ({res}) group by {inner_col}" for inner_col in cols.keys() if inner_col != col]
            [x for x in map(exec_db, fw_queries)]
            xfilter_end = timer()
            print(f'xfilter_{col},{xfilter_end - xfilter_start}', file=sys.stderr)
            if idx > 100:
                break