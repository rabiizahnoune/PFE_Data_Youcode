# from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator
# from datetime import datetime, timedelta

# # Configuration du DAG
# default_args = {
#     'owner': 'rabii',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 4, 15),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Définir le DAG
# with DAG(
#     'retrain_lstm_gold_4h',
#     default_args=default_args,
#     description='DAG pour réentraîner le modèle LSTM sur les données de l\'or (4H)',
#     schedule_interval=timedelta(days=1),  # Réentraînement quotidien
#     catchup=False,
# ) as dag:

#     # Tâche pour entraîner le modèle dans le conteneur ai-trainer
#     train_task = DockerOperator(
#         task_id='train_lstm_model',
#         image='ai-trainer',  # Nom du service dans docker-compose
#         container_name='ai-trainer-task',
#         api_version='auto',
#         auto_remove=True,
#         command='python train_lstm.py',
#         docker_url='unix://var/run/docker.sock',
#         network_mode='airflow_network',
#         volumes=[
#             '/var/run/docker.sock:/var/run/docker.sock',
#             '/path/to/ai_scripts:/ai_scripts'  # Remplace par le chemin absolu de ./ai_scripts sur l'hôte
#         ],
#         environment={
#             'SNOWFLAKE_ACCOUNT': 'ZTRXFTF-PN46984',
#             'SNOWFLAKE_USER': 'RABIIZAHNOUNE',
#             'SNOWFLAKE_PASSWORD': 'YiqgYAiqWm9J9a6',
#             'MLFLOW_TRACKING_URI': 'http://mlflow-server:5000'
#         },
#     )

#     train_task