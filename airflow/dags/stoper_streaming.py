from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1374382502981931008/BRaqnl_BHNxEep-dItvMinlhhcd9Tk5-sj3RbKdG3qDzLjwfYGRze8IZTxRoaiQj0DE2"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='kill_running_scripts',
    default_args=default_args,
    description='Tuer les scripts Python Spark/Kafka chaque nuit avec vérification de logs',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Chaque jour à minuit
    catchup=False,
) as dag:

    check_log_errors = BashOperator(
        task_id='check_spark_log_errors',
        bash_command="""
docker exec gold_price_project-spark-1 bash -c '
    LOG_FILE="/scripts/logs/spark.log"
    if grep -i "error" "$LOG_FILE"; then
        echo "❌ Erreur détectée, envoi à Discord..."
        grep -i -B 20 -A 20 "error" "$LOG_FILE" > /tmp/spark_error_excerpt.log

        ESCAPED=$(sed "s/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g" /tmp/spark_error_excerpt.log | head -c 1900)

        curl -X POST -H "Content-Type: application/json" \
             -d "{\\"content\\": \\"🚨 Erreur détectée dans spark.log à 00:00 :\\\\n\\\\n$ESCAPED\\"}" \
             {DISCORD_WEBHOOK}
    else
        echo "✅ Aucun problème détecté dans spark.log"
    fi

    echo "" > "$LOG_FILE"
'
"""
    )


    kill_scripts = BashOperator(
        task_id='kill_spark_kafka_scripts',
        bash_command="""
            echo "🔍 Killing script in Kafka container..."
            docker exec gold_price_project-kafka-1 pkill -f gold_price.py || echo "Aucun script gold_price.py trouvé."

            echo "🔍 Killing script in Spark container..."
            docker exec gold_price_project-spark-1 pkill -f gold_price_streaming.py || echo "Aucun script gold_price_streaming.py trouvé."

            echo "✅ Scripts killed in both containers (if they were running)."
        """,
    )

    notify_discord = BashOperator(
        task_id='notify_kill',
        bash_command=f"""
            curl -X POST -H "Content-Type: application/json" \\
            -d '{{"content":"💤 Les scripts Spark/Kafka ont été arrêtés automatiquement à 00:00."}}' \\
            {DISCORD_WEBHOOK}
        """,
    )

    check_log_errors >> kill_scripts >> notify_discord
