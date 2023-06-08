FROM python:3.8-buster

RUN git clone https://github.com/reallocf/duckdb.git --branch charlie-demo

RUN pip install pandas scipy numpy Flask

WORKDIR "duckdb/tools/pythonpkg"

RUN python setup.py install

WORKDIR "../.."

COPY ./smokedduck.py ./app.py

CMD ["python3", "-m" , "flask", "run", "--host=0.0.0.0"]
