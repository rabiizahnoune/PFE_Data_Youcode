from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

# Récupérer la date actuelle pour la vérification HDFS
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "start_kafka_spark",
    default_args=default_args,
    description="Start Kafka and Spark containers, run scripts, and verify HDFS data",
    schedule_interval=None,  # 06:00 +01 (UTC+01)
    start_date=datetime(2025, 5, 20),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Tâche pour démarrer les conteneurs
    # start_containers = BashOperator(
    #     task_id="start_containers",
    #     bash_command="""
    #         docker start gold_price_project-kafka-1 &&
    #         docker start gold_price_project-spark-1
    #     """,
    #     dag=dag,
    # )

    # Attendre 2 minutes après le démarrage
    # wait_2_minutes = TimeDeltaSensor(
    #     task_id="wait_2_minutes",
    #     delta=timedelta(minutes=2),
    #     mode="reschedule",
    #     dag=dag,
    # )

    # Lancer les scripts dans les conteneurs en parallèle
    run_kafka_script = BashOperator(
      task_id="run_kafka_script",
      bash_command="""
docker exec -d gold_price_project-kafka-1 bash -c 'python3 /scripts/gold_price.py > /scripts/logs/kafka.log 2>&1'
"""
,
      dag=dag,
)
    

    run_spark_script = BashOperator(
    task_id="run_spark_script",
    bash_command="""
     docker exec -d gold_price_project-spark-1 bash -c 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /scripts/gold_price_streaming.py > /scripts/logs/spark.log 2>&1'
     """,
    dag=dag,
)


    wait_4_minutes = TimeDeltaSensor(
        task_id="wait_4_minutes",
        delta=timedelta(minutes=4),
        mode="reschedule",
        poke_interval=30,
        timeout=240,
        dag=dag,
    )
    # Vérification des données HDFS pour le jour actuel
    verify_hdfs_data = BashOperator(
        task_id="verify_hdfs_data",
        bash_command=f"""
            # Vérifier si des données existent dans HDFS pour aujourd'hui
            hdfs_output=$(docker exec -i namenode bash -c "hdfs dfs -ls /gold_price/aggregates/year={current_year}/month={current_month}/day={current_day:02d}" 2>/dev/null)

            if [ -n "$hdfs_output" ]; then
                echo "Données HDFS présentes pour aujourd'hui ({current_year}-{current_month:02d}-{current_day:02d})"
                curl -X POST -H "Content-Type: application/json" -d '{{"content":"✅ Données HDFS présentes pour {current_year}-{current_month:02d}-{current_day:02d} ! Pipeline fonctionne."}}' https://discord.com/api/webhooks/1374382502981931008/BRaqnl_BHNxEep-dItvMinlhhcd9Tk5-sj3RbKdG3qDzLjwfYGRze8IZTxRoaiQj0DE2
            else
                echo "Aucune donnée HDFS trouvée pour aujourd'hui ({current_year}-{current_month:02d}-{current_day:02d})"
                curl -X POST -H "Content-Type: application/json" -d '{{"content":"❌ Aucune donnée HDFS trouvée pour {current_year}-{current_month:02d}-{current_day:02d}. Vérifiez le pipeline."}}' https://discord.com/api/webhooks/1374382502981931008/BRaqnl_BHNxEep-dItvMinlhhcd9Tk5-sj3RbKdG3qDzLjwfYGRze8IZTxRoaiQj0DE2
            fi
        """,
        dag=dag,
    )

    # Dépendances
    [run_kafka_script, run_spark_script] >> wait_4_minutes >> verify_hdfs_data