FROM apache/airflow:2.7.1-python3.9
USER root
RUN apt-get update && apt-get install -y python3-dev
USER airflow
RUN pip install snowflake-connector-python hdfs yfinance cassandra-driver==3.28.0 fredapi pyarrow mlflow==2.17.2 numpy==2.0.2 joblib pandas==2.2.2


