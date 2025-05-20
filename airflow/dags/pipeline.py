from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_gold import extract_gold_data
from transform import transform_gold_data
from load import load_gold_data
from verification import check_fred_api,check_hdfs_connection,check_snowflake_connection,check_yfinance_api


with DAG(
    dag_id="gold_pipelines",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 0 1 * *",  # Tous les 1er du mois à 00:00
    catchup=False
) as dag:

    verify_fred = PythonOperator(
        task_id="verify_fred_api",
        python_callable=check_fred_api
    )

    verify_hdfs = PythonOperator(
        task_id="verify_hdfs_connection",
        python_callable=check_hdfs_connection
    )

    verify_snowflake = PythonOperator(
        task_id="verify_snowflake_connection",
        python_callable=check_snowflake_connection
    )

    # Tâches existantes
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

    # Définir les dépendances
    [verify_fred, verify_hdfs, verify_snowflake] >> extract_task
    extract_task >> transform_task >> load_task