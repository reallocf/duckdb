from cmd import Cmd
import pandas as pd
import psycopg2
from pygg import *
import requests
import time

perm_url = 'http://localhost:8000/sql'
smokedduck_url = 'http://localhost:8001/sql'

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="test",
    user="test",
    password="test",
    options="-c search_path=provsql_test,provsql"
)
provsql_cursor = conn.cursor()

def lineage_capture_smokedduck(q):
    payload = {
        'query': q,
        'capture': True,
    }
    start = time.time()
    resp = requests.post(smokedduck_url, json=payload)
    end = time.time()
    res = resp.json()['res']
    print('SmokedDuck Response:')
    print_relation(res)
    t = end - start
    print(f'SmokedDuck Capture Time: {t}')
    return res, t

def lineage_capture_perm(q):
    payload = {
        'query': q,
    }
    start = time.time()
    resp = requests.post(perm_url, json=payload)
    end = time.time()
    perm_res = resp.json()['perm_res']
    if perm_res:
        res = resp.json()['res']
        print('Perm Response:')
        print_relation(res)
        t = end - start
        print(f'Perm Capture Time: {t}')
        return res, t
    else:
        print('No Perm query found')
        return None, 0

def lineage_capture_provsql(q):
    start = time.time()
    provsql_cursor.execute(q)
    res = provsql_cursor.fetchall()
    end = time.time()
    print('ProvSQL Response:')
    print_relation(res)
    t = end - start
    print(f'ProvSQL Capture Time: {t}')
    return res, t

def lineage_query_smokedduck(q):
    payload = {
        'query': q,
        'capture': False,
    }
    resp = requests.post(smokedduck_url, json=payload)
    print_relation(resp.json()['res'])

def build_figure(smokedduck_t, perm_t, provsql_t):
    df = pd.DataFrame({
        'System': ['SmokedDuck', 'Perm', 'ProvSQL'],
        'Duration': [smokedduck_t, perm_t, provsql_t],
    })
    print(df)
    p = ggplot(df, aes(x='System', y='Duration')) \
        + geom_bar(stat=esc('identity'), width=0.8)
    ggsave("demo.png", p, width=5, height=3)

def metadata_query_smokedduck(q):
    payload = {
        'query': q,
        'capture': False,
    }
    resp = requests.post(smokedduck_url, json=payload)
    print_relation(resp.json()['res'])

def print_relation(rel):
    for row in rel:
        for col in row:
            print(col, '\t', end='')
        print()

class Prompt(Cmd):
    prompt = 'smokedduck> '
    intro = '''
Welcome to the SmokedDuck shell!

Type in a TPC-H query to execute and capture lineage across SmokedDuck, Perm, and ProvSQL.
A figure is generated to compare runtime execution performance.
If a non TPC-H query is issued, only SmokedDuck and ProvSQL are queried.
Lineage queries using SmokedDuck's relational syntax are executed only against SmokedDuck.

Type ? to view other commands.
'''

    def do_exit(self, inp):
        '''exit the application'''
        return True

    def do_tpch(self, inp):
        '''execute a tpch query'''
        args = inp.split()
        if len(args) == 0:
            print("Must provide a query id")
        elif len(args) == 1:
            query = tpch_queries[int(args[0])]
            print("Executing the following query:")
            print(query)
            smokedduck_res, smokedduck_t = lineage_capture_smokedduck(q)
            perm_res, perm_t = lineage_capture_perm(q)
            provsql_res, provsql_t = lineage_capture_provsql(q)
            build_figure(smokedduck_t, perm_t, provsql_t)
        elif args[1] == 'sf1':
            query = tpch_queries[int(args[0])]
            print("Executing the following query at sf1:")
            lineage_capture_smokedduck(query)

    def default(self, inp):
        if 'lineage' in inp.lower():
            lineage_query_smokedduck(inp)
        elif 'pragma' in inp.lower() or 'queries_list' in inp.lower():
            metadata_query_smokedduck(inp)
        return False

Prompt().cmdloop()