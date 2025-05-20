from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import mlflow
import logging
import os
from hdfs import InsecureClient
import joblib
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fonction pour déterminer le mois et l'année actuels
def get_current_month_year():
    today = datetime.now()
    return today.year, today.month

# Tâche 1 : Préparer les paramètres pour le fine-tuning
def run_fine_tune_command(**kwargs):
    logger.info("Début de la tâche prepare_fine_tune")
    try:
        year, month = get_current_month_year()
        logger.info(f"Année et mois actuels : {year}, {month}")
        mlflow.set_tracking_uri("sqlite:////app/mlflow.db")
        logger.info("Tracking URI défini avec succès")
        runs = mlflow.search_runs(experiment_ids=['0'])
        logger.info(f"Nombre de runs trouvés : {len(runs)}")
        if not runs.empty:
            initial_run_id = runs.loc[runs['metrics.rmse'].idxmin()]['run_id']
            logger.info(f"Run ID initial sélectionné : {initial_run_id}")
        else:
            initial_run_id = "default"
            logger.info("Aucun modèle existant trouvé, démarrage avec un nouveau modèle")
        kwargs['ti'].xcom_push(key='year', value=year)
        kwargs['ti'].xcom_push(key='month', value=month)
        kwargs['ti'].xcom_push(key='initial_run_id', value=initial_run_id)
        logger.info("Paramètres poussés dans XCom avec succès")
    except Exception as e:
        logger.error(f"Erreur dans run_fine_tune_command : {str(e)}", exc_info=True)
        raise

# Tâche 2 : Sélectionner la meilleure version
def select_best_model(**kwargs):
    try:
        mlflow.set_tracking_uri("sqlite:////app/mlflow.db")
        runs = mlflow.search_runs(experiment_ids=['0'])
        logger.info(f"Nombre de runs trouvés pour sélection : {len(runs)}")
        if not runs.empty:
            best_run_id = runs.loc[runs['metrics.rmse'].idxmin()]['run_id']
            logger.info(f"Meilleure version sélectionnée avec run_id : {best_run_id}")
            kwargs['ti'].xcom_push(key='best_run_id', value=best_run_id)
        else:
            logger.error("Aucune exécution trouvée pour sélectionner la meilleure version.")
            raise Exception("Aucune exécution trouvée")
    except Exception as e:
        logger.error(f"Erreur dans select_best_model : {str(e)}", exc_info=True)
        raise

# Tâche 3 : Déployer le modèle dans HDFS
def deploy_to_hdfs(**kwargs):
    try:
        best_run_id = kwargs['ti'].xcom_pull(key='best_run_id')
        model_uri = f"runs:/{best_run_id}/gold_price_model"
        mlflow.set_tracking_uri("sqlite:////app/mlflow.db")
        model = mlflow.sklearn.load_model(model_uri)
        HDFS_URL = "http://namenode:9870"
        HDFS_USER = "hadoop"
        HDFS_MODEL_PATH = f"/gold_price/models/best_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"

        # Modifier les permissions du répertoire /gold_price dans HDFS
        chmod_command = [
            "docker", "exec", "namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/gold_price"
        ]
        logger.info(f"Exécution de la commande : {' '.join(chmod_command)}")
        subprocess.run(chmod_command, check=True, text=True, capture_output=True)
        logger.info("Permissions modifiées avec succès dans HDFS")

        client = InsecureClient(HDFS_URL, user=HDFS_USER, root='/', timeout=30)
        logger.info("Connexion à HDFS pour déploiement")
        local_temp_model = "/tmp/temp_model.joblib"
        joblib.dump(model, local_temp_model)
        with open(local_temp_model, 'rb') as reader:
            client.write(HDFS_MODEL_PATH, reader, overwrite=True)
        os.remove(local_temp_model)
        logger.info(f"Modèle déployé avec succès à {HDFS_MODEL_PATH}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de la modification des permissions HDFS : {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Erreur dans deploy_to_hdfs : {str(e)}", exc_info=True)
        raise

# Définition du DAG
with DAG(
    dag_id="gold_model_finetuning_bash_container",
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:
    prepare_task = PythonOperator(
        task_id="prepare_fine_tune",
        python_callable=run_fine_tune_command
    )

    fine_tune_task = BashOperator(
        task_id="execute_fine_tune_in_container",
        bash_command='docker exec mlflow-container python3 /app/fine_tune_model.py --year {{ ti.xcom_pull(task_ids="prepare_fine_tune", key="year") }} --month {{ ti.xcom_pull(task_ids="prepare_fine_tune", key="month") }} --run_id {{ ti.xcom_pull(task_ids="prepare_fine_tune", key="initial_run_id") }}',
        env={"PATH": "/usr/local/bin:/usr/bin:/bin"}
    )

    select_best_task = PythonOperator(
        task_id="select_best_model",
        python_callable=select_best_model
    )

    deploy_task = PythonOperator(
        task_id="deploy_to_hdfs",
        python_callable=deploy_to_hdfs
    )

    prepare_task >> fine_tune_task >> select_best_task >> deploy_task