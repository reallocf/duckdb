SIGMOD 2023 Demo
====

Run all commands from the `sigmod_2023_cli_demo` directory.

First, we start up the servers.

1. The ProvSQL Postgres Server
   1. `docker run inriavalda/provsql`
2. The DuckDB Server for executing Perm queries
   1. `docker build --tag perm-server perm`
   2. `docker run --publish 8000:5000 perm-server`
3. The SmokedDuck Server
   1. `docker build --tag smokedduck-server smokedduck`
   2. `docker run --publish 8001:5000 perm-server`

Next, set up the local environment with
```shell
python3 -m venv venvDemo
source venvDemo/bin/activate
pip install -r requirements.txt
```
