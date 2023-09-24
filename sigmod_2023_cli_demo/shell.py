from cmd import Cmd
import pandas as pd
import psycopg2
from pygg import *
import requests
from termgraph import termgraph
import time
import tpch_queries

perm_url = 'http://localhost:8000/sql'
smokedduck_url = 'http://localhost:8001/sql'
timeout = 60 # seconds

termgraph_file = "tmp"
termgraph_args = {
    "filename": termgraph_file,
    "verbose": False,
    "title": None,
    "color": ["blue"],
    "vertical": False,
    "stacked": False,
    "histogram": False,
    "width": 100,
    "no_labels": False,
    "no_values": False,
    "format": "{:.3f}",
    "suffix": " ms",
}

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="test",
    user="test",
    password="test",
    options="-c search_path=provsql_test,provsql"
)
provsql_cursor = conn.cursor()
provsql_cursor.execute("SET statement_timeout TO %s", (timeout * 1000,))  # Convert timeout to milliseconds

def red_print(t):
    print(f'\033[91m{t}\033[0m')

def lineage_capture_smokedduck(q):
    payload = {
        'query': q,
        'capture': True,
    }
    print('Starting SmokedDuck query')
    start = time.time()
    try:
        resp = requests.post(smokedduck_url, json=payload, timeout=timeout)
        end = time.time()
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        red_print(f'Timeout after {timeout} seconds')
        return None, 0
    res = resp.json()['res']
    t = end - start
    print(f'SmokedDuck Capture Time: {t}')
    return res, t

def lineage_capture_perm(q):
    payload = {
        'query': q,
    }
    print('Starting Perm query')
    start = time.time()
    try:
        resp = requests.post(perm_url, json=payload, timeout=timeout)
        end = time.time()
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        red_print(f'Timeout after {timeout} seconds')
        return None, 0
    perm_res = resp.json()['perm_res']
    if perm_res:
        res = resp.json()['res']
        t = end - start
        print(f'Perm Capture Time: {t}')
        return res, t
    else:
        print('No Perm query found')
        return None, 0

def lineage_capture_provsql(q):
    start = time.time()
    print('Starting ProvSQL query')
    res = None
    t = 0
    try:
        provsql_cursor.execute(q)
        res = provsql_cursor.fetchall()
    except psycopg2.Error:
        end = time.time()
        if end - start > timeout:
            red_print(f'Timeout after {timeout} seconds')
        else:
            red_print('Unsupported ProvSQL semantics')
    else:
        end = time.time()
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

def no_lineage_capture_smokedduck(q):
    start = time.time()
    payload = {
        'query': q,
        'capture': False,
    }
    resp = requests.post(smokedduck_url, json=payload)
    print_relation(resp.json()['res'])
    end = time.time()
    print(f'SmokedDuck w/o Lineage Capture Time: {end - start}')

def build_figure(smokedduck_t, perm_t, provsql_t):
    df_map = {'System': [], 'Duration': []}
    if smokedduck_t != 0:
        df_map['System'].append('SmokedDuck')
        df_map['Duration'].append(smokedduck_t * 1000)
    if perm_t != 0:
        df_map['System'].append('Perm')
        df_map['Duration'].append(perm_t * 1000)
    if provsql_t != 0:
        df_map['System'].append('ProvSQL')
        df_map['Duration'].append(provsql_t * 1000)
    df = pd.DataFrame(df_map)
    df.to_csv(termgraph_file, sep=" ", index=False, header=False)
    _, labels, data, colors = termgraph.read_data(termgraph_args)
    termgraph.chart(colors, data, termgraph_args, labels)
    # p = ggplot(df, aes(x='System', y='Duration')) \
    #     + geom_bar(stat=esc('identity'), width=0.8)
    # ggsave("demo.png", p, width=5, height=3)

def metadata_query_smokedduck(q):
    payload = {
        'query': q,
        'capture': False,
    }
    resp = requests.post(smokedduck_url, json=payload)
    print_relation(resp.json()['res'])

def print_relation(rel):
    cnt = 0
    for row in rel:
        cnt += 1
        for col in row:
            print(col, '\t', end='')
        print()
        if cnt == 10:
            break
    if len(rel) > 10:
        print('...')
        print(f'Total row count: {len(rel)}')

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
        elif len(args) == 1 or (len(args) == 2 and args[1] == 'nolin'):
            query = tpch_queries.sf01[int(args[0]) - 1]
            if len(args) == 2 and args[1] == 'nolin':
                print("Executing the following query without lineage capture:")
                print(query)
                no_lineage_capture_smokedduck(query)
            else:
                print("Executing the following query:")
                print(query)
                smokedduck_res, smokedduck_t = lineage_capture_smokedduck(query)
                _, perm_t = lineage_capture_perm(query)
                _, provsql_t = lineage_capture_provsql(query)
                print("Query results:")
                print_relation(smokedduck_res)
                build_figure(smokedduck_t, perm_t, provsql_t)
        elif args[1] == 'sf1':
            query = tpch_queries.sf1[int(args[0]) - 1]
            if len(args) == 3 and args[2] == 'nolin':
                print("Executing the following query without lineage capture at sf1:")
                print(query)
                no_lineage_capture_smokedduck(query)
            else:
                print("Executing the following query at sf1:")
                print(query)
                smokedduck_res, _ = lineage_capture_smokedduck(query)
                print_relation(smokedduck_res)

    def default(self, inp):
        if 'lineage' in inp.lower():
            lineage_query_smokedduck(inp)
        elif 'pragma' in inp.lower() or 'queries_list' in inp.lower():
            metadata_query_smokedduck(inp)
        return False

Prompt().cmdloop()