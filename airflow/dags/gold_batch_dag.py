import sys
import os

# Ajouter le chemin des scripts au sys.path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_macro_to_hdfs import fetch_macro_data
from fetch_etf_to_hdfs import fetch_etf_data
from transform_load_to_snowflake import transform_load_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "gold_batch_pipeline",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # Tous les jours à 8h
    catchup=False,
) as dag:
    fetch_macro_task = PythonOperator(
        task_id="fetch_macro_to_hdfs",
        python_callable=fetch_macro_data,
    )
    
    fetch_etf_task = PythonOperator(
        task_id="fetch_etf_to_hdfs",
        python_callable=fetch_etf_data,
    )
    
    transform_load_task = PythonOperator(
        task_id="transform_load_to_snowflake",
        python_callable=transform_load_data,
    )
    
    [fetch_macro_task, fetch_etf_task] >> transform_load_task