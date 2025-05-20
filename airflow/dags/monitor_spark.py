from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuration du webhook Discord (remplace-le par le tien)
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1374382502981931008/BRaqnl_BHNxEep-dItvMinlhhcd9Tk5-sj3RbKdG3qDzLjwfYGRze8IZTxRoaiQj0DE2"

# Définir les arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Créer le DAG
with DAG(
    dag_id='monitor_spark_logs',
    default_args=default_args,
    description='Surveillance du fichier spark.log pour détecter les erreurs',
    schedule_interval=None,  # Toutes les 10 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'spark', 'logs'],
) as dag:

    check_spark_logs = BashOperator(
        task_id="check_spark_logs",
        bash_command=f"""
        docker exec gold_price_project-spark-1 bash -c '
        if grep -Ei "error|exception|traceback" /scripts/logs/spark.log; then
            echo "❌ Erreur détectée dans spark.log"
            curl -X POST -H "Content-Type: application/json" \
            -d "{{\\"content\\":\\"❌ Erreur détectée dans spark.log ! Vérifiez le pipeline à {{$(date +"%Y-%m-%d %H:%M:%S")}}.\\"}}" \
            {DISCORD_WEBHOOK_URL}
        else
            echo "✅ Aucun problème détecté dans spark.log"
        fi
        '
        """,
    )

    check_spark_logs
