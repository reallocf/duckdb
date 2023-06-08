FROM python:3.8-buster

# Use v0.2.9 since that's what SmokedDuck is built off of
RUN git clone https://github.com/duckdb/duckdb.git --branch v0.2.9

RUN pip install pandas scipy numpy Flask

WORKDIR "duckdb/tools/pythonpkg"

RUN python setup.py install

WORKDIR "../.."

COPY ./perm.py ./app.py

CMD ["python3", "-m" , "flask", "run", "--host=0.0.0.0"]
