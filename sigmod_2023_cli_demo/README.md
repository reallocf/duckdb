SIGMOD 2023 Demo
====

Run all commands from the `sigmod_2023_cli_demo` directory.

First, set up the local environment with
```shell
python3 -m venv venvDemo
source venvDemo/bin/activate
pip install -r requirements.txt
```

Then, we start up the servers.

1. The ProvSQL Postgres Server
   1. `docker run -v postgresql.conf:/etc/postgresql/postgresql.conf -e POSTGRES_CONFIG_FILE=/etc/postgresql/postgresql.conf --memory=8g --cpus=5 -e POSTGRES_PASSWORD=abc123 -p 5432:5432 -p 8080:80 inriavalda/provsql`
   2. `python bootstrap_provsql.py`
2. The DuckDB Server for executing Perm queries
   1. `docker build --tag perm-server perm`
   2. `docker run --memory=8g --cpus=5 --publish 8000:5000 perm-server`
3. The SmokedDuck Server
   1. `docker build --tag smokedduck-server smokedduck`
   2. `docker run --memory=8g --cpus=5 --publish 8001:5000 smokedduck-server`

Finally, enter the SmokedDuck shell:
```shell
python shell.py
```
