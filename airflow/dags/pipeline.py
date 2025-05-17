from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_gold import extract_gold_data
from transform import transform_gold_data
from load import load_gold_data

with DAG(
    dag_id="gold_pipeline",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 0 1 * *",  # Tous les 1er du mois à 00:00
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_gold_data",
        python_callable=extract_gold_data
    )

    transform_task = PythonOperator(
        task_id="transform_gold_data",
        python_callable=transform_gold_data
    )

    load_task = PythonOperator(
        task_id="load_gold_data",
        python_callable=load_gold_data
    )

    extract_task >> transform_task >> load_task