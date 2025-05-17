FROM apache/airflow:2.7.1
USER root
RUN apt-get update && apt-get install -y python3-dev
USER airflow
RUN pip install pandas snowflake-connector-python hdfs yfinance cassandra-driver==3.28.0 fredapi pyarrow


