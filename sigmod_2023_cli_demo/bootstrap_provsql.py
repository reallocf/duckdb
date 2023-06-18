import duckdb
import psycopg2

duckdb_conn = duckdb.connect(database=':memory:', read_only=False)
sf = 0.1
print(f"Loading TPC-H data with sf {sf}")
duckdb_conn.execute(f"CALL dbgen(sf={sf});")

tables = [
    'part',
    'supplier',
    'partsupp',
    'customer',
    'orders',
    'lineitem',
    'nation',
    'region',
]

print(f"Outputting TPC-H sf={sf} from DuckDB into CSVs")
for table in tables:
    print(f"Outputting {table}.csv")
    duckdb_conn.execute(f"COPY {table} TO '{table}.csv' (HEADER, DELIMITER ',');")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="test",
    user="test",
    password="test",
    options="-c search_path=provsql_test,provsql"
)

cursor = conn.cursor()

# Ref: https://github.com/Data-Science-Platform/tpch-pgsql/blob/master/query_root/prep_query/create_tbl.sql

tables_to_create = [
"""
CREATE TABLE IF NOT EXISTS PART (
    P_PARTKEY        SERIAL,
    P_NAME            VARCHAR(55),
    P_MFGR            CHAR(25),
    P_BRAND            CHAR(10),
    P_TYPE            VARCHAR(25),
    P_SIZE            INTEGER,
    P_CONTAINER        CHAR(10),
    P_RETAILPRICE    DECIMAL,
    P_COMMENT        VARCHAR(23)
);
""",
"""
CREATE TABLE IF NOT EXISTS SUPPLIER (
    S_SUPPKEY        SERIAL,
    S_NAME            CHAR(25),
    S_ADDRESS        VARCHAR(40),
    S_NATIONKEY        INTEGER NOT NULL, -- references N_NATIONKEY
    S_PHONE            CHAR(15),
    S_ACCTBAL        DECIMAL,
    S_COMMENT        VARCHAR(101)
);
""",
"""
CREATE TABLE IF NOT EXISTS PARTSUPP (
    PS_PARTKEY        INTEGER NOT NULL, -- references P_PARTKEY
    PS_SUPPKEY        INTEGER NOT NULL, -- references S_SUPPKEY
    PS_AVAILQTY        INTEGER,
    PS_SUPPLYCOST    DECIMAL,
    PS_COMMENT        VARCHAR(199)
);
""",
"""
CREATE TABLE IF NOT EXISTS CUSTOMER (
    C_CUSTKEY        SERIAL,
    C_NAME            VARCHAR(25),
    C_ADDRESS        VARCHAR(40),
    C_NATIONKEY        INTEGER NOT NULL, -- references N_NATIONKEY
    C_PHONE            CHAR(15),
    C_ACCTBAL        DECIMAL,
    C_MKTSEGMENT    CHAR(10),
    C_COMMENT        VARCHAR(117)
);
""",
"""
CREATE TABLE IF NOT EXISTS ORDERS (
    O_ORDERKEY        SERIAL,
    O_CUSTKEY        INTEGER NOT NULL, -- references C_CUSTKEY
    O_ORDERSTATUS    CHAR(1),
    O_TOTALPRICE    DECIMAL,
    O_ORDERDATE        DATE,
    O_ORDERPRIORITY    CHAR(15),
    O_CLERK            CHAR(15),
    O_SHIPPRIORITY    INTEGER,
    O_COMMENT        VARCHAR(79)
);
""",
"""
CREATE TABLE IF NOT EXISTS LINEITEM (
    L_ORDERKEY        INTEGER NOT NULL, -- references O_ORDERKEY
    L_PARTKEY        INTEGER NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
    L_SUPPKEY        INTEGER NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
    L_LINENUMBER    INTEGER,
    L_QUANTITY        DECIMAL,
    L_EXTENDEDPRICE    DECIMAL,
    L_DISCOUNT        DECIMAL,
    L_TAX            DECIMAL,
    L_RETURNFLAG    CHAR(1),
    L_LINESTATUS    CHAR(1),
    L_SHIPDATE        DATE,
    L_COMMITDATE    DATE,
    L_RECEIPTDATE    DATE,
    L_SHIPINSTRUCT    CHAR(25),
    L_SHIPMODE        CHAR(10),
    L_COMMENT        VARCHAR(44)
);
""",
"""
CREATE TABLE IF NOT EXISTS NATION (
    N_NATIONKEY        SERIAL,
    N_NAME            CHAR(25),
    N_REGIONKEY        INTEGER NOT NULL,  -- references R_REGIONKEY
    N_COMMENT        VARCHAR(152)
);
""",
"""
CREATE TABLE IF NOT EXISTS REGION (
    R_REGIONKEY    SERIAL,
    R_NAME        CHAR(25),
    R_COMMENT    VARCHAR(152)
);
""",
]

print("Creating tables in ProvSQL")
for table_sql in tables_to_create:
    cursor.execute(table_sql)

print(f"Loading TPC-H sf={sf} data from CSVs into ProvSQL")
for table in tables:
    print(f"Loading table {table}")
    csv_file = f"{table}.csv"
    copy_sql = f"COPY {table} FROM STDIN DELIMITER ',' CSV HEADER"
    with open(csv_file, 'r') as f:
        cursor.copy_expert(copy_sql, f)

for table in tables:
    print(f"Adding provenance to table {table}")
    cursor.execute(f"select add_provenance('{table}')")

conn.commit()

cursor.close()
conn.close()
