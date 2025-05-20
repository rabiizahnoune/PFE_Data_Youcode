import mlflow
import mlflow.sklearn
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import logging
import glob
import os
import sklearn
import argparse
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fonction pour déterminer le mois et l'année précédents
def get_previous_month_year():
    today = datetime.now()
    # On soustrait un jour pour s'assurer qu'on obtient le mois précédent si on est le 1er du mois
    last_day_of_prev_month = today.replace(day=1) - timedelta(days=1)
    return last_day_of_prev_month.year, last_day_of_prev_month.month

def get_current_month_year():
    today = datetime.now()
    return today.year, today.month

# Parse command-line arguments for year, month, and run_id
parser = argparse.ArgumentParser(description="Fine-tune gold price prediction model for a specific month.")
parser.add_argument('--year', type=int, help='Année des données à utiliser (par défaut : année précédente)')
parser.add_argument('--month', type=int, help='Mois des données à utiliser (par défaut : mois précédent)')
parser.add_argument('--run_id', type=str, required=True, help='ID de l\'exécution du modèle initial à fine-tuner')
args = parser.parse_args()

# Définir l'année et le mois au mois précédent si non fourni
if args.year is None or args.month is None:
    current_year, current_month = get_current_month_year()
    args.year = args.year if args.year is not None else current_year
    args.month = args.month if args.month is not None else current_month
    logger.info(f"Utilisation du mois actuel pour le fine-tuning : {args.year}-{args.month:02d}")
else:
    logger.info(f"Utilisation du mois spécifié pour le fine-tuning : {args.year}-{args.month:02d}")
# Configuration HDFS
HDFS_URL = "http://namenode:9870"
HDFS_USER = "hadoop"
HDFS_MONTH_PATH = f"/gold_price/aggregates/year={args.year}/month={args.month}"

# Chemins locaux temporaires
LOCAL_TEMP_DIR = "/app/temp_month_data"
COMBINED_PARQUET_PATH = "/app/combined_month_data.parquet"

# Connecter à HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER, root='/', timeout=30)
logger.info("Connexion à HDFS réussie")

# Télécharger tous les fichiers Parquet pour le mois (à travers tous les jours)
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
files = []
for root, _, filenames in client.walk(HDFS_MONTH_PATH):
    for fname in filenames:
        if fname.endswith('.parquet'):
            hdfs_path = f"{root}/{fname}"
            local_path = f"{LOCAL_TEMP_DIR}/{fname}"
            logger.info(f"Téléchargement de {hdfs_path} vers {local_path}")
            with client.read(hdfs_path) as reader:
                with open(local_path, 'wb') as writer:
                    writer.write(reader.read())
            files.append(local_path)
logger.info(f"{len(files)} fichiers téléchargés pour le fine-tuning")

# Vérifier si des fichiers ont été trouvés
if not files:
    logger.error("Aucun fichier Parquet trouvé pour le mois spécifié. Sortie.")
    exit(1)

# Combiner tous les fichiers Parquet en un seul fichier
logger.info("Combinaison des fichiers Parquet en un seul fichier...")
combined_df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
combined_df['Datetime'] = pd.to_datetime(combined_df['Datetime'])
combined_df = combined_df.sort_values("Datetime")

# Sauvegarder le DataFrame combiné en un seul fichier Parquet
combined_df.to_parquet(COMBINED_PARQUET_PATH, index=False)
logger.info(f"Fichier combiné sauvegardé à {COMBINED_PARQUET_PATH}. Total de lignes : {len(combined_df)}")

# Nettoyer les fichiers temporaires téléchargés
for f in files:
    os.remove(f)
os.rmdir(LOCAL_TEMP_DIR)
logger.info("Fichiers temporaires nettoyés")

# Charger le fichier Parquet combiné pour le fine-tuning
df = pd.read_parquet(COMBINED_PARQUET_PATH)
logger.info(f"Données de fine-tuning chargées avec succès. Total de lignes : {len(df)}")

# Valider la taille des données
window_size = 30
target_shift = 30
min_rows_required = window_size + target_shift + 1  # 30 lags + 30 target shift + 1 ligne valide
if len(df) < min_rows_required:
    logger.error(f"Données insuffisantes pour le fine-tuning. Il faut au moins {min_rows_required} lignes, mais seulement {len(df)} trouvées.")
    exit(1)

# Préparer les features
for i in range(1, window_size + 1):
    df[f"lag_{i}"] = df["Price"].shift(i)

df["target"] = df["Price"].shift(-target_shift)
df = df.dropna()

# Vérifier s'il y a assez de lignes après suppression des NaNs
if len(df) == 0:
    logger.error("Aucune ligne valide après préparation des features (à cause des NaNs). Sortie.")
    exit(1)

X = df[[f"lag_{i}" for i in range(1, window_size + 1)]]
y = df["target"]
logger.info(f"Features préparées pour le fine-tuning. Lignes après ingénierie des features : {len(df)}")

# Charger le modèle existant
mlflow.set_tracking_uri("sqlite:///mlflow.db")
run_id = args.run_id
model_uri = f"runs:/{run_id}/gold_price_model"
try:
    model = mlflow.sklearn.load_model(model_uri)
    logger.info("Modèle existant chargé avec succès")
except Exception as e:
    logger.error(f"Échec du chargement du modèle avec run_id {run_id} : {str(e)}")
    exit(1)

# Fine-tuner le modèle
with mlflow.start_run():
    model.fit(X, y)  # Fine-tuning en ré-entraînant sur les nouvelles données

    # Log des métriques (gérer la compatibilité des versions de scikit-learn)
    predictions = model.predict(X)
    if int(sklearn.__version__.split('.')[0]) >= 0 and int(sklearn.__version__.split('.')[1]) >= 22:
        rmse = mean_squared_error(y, predictions, squared=False)
    else:
        mse = mean_squared_error(y, predictions)
        rmse = mse ** 0.5
    mlflow.log_metric("rmse", rmse)

    # Log des paramètres
    mlflow.log_param("window_size", window_size)
    mlflow.log_param("target_shift", target_shift)
    mlflow.log_param("fine_tune_month", f"{args.year}-{args.month:02d}")

    # Log du modèle mis à jour avec un exemple d'entrée
    input_example = X.iloc[:1].to_dict('records')[0]  # Prendre la première ligne comme exemple
    mlflow.sklearn.log_model(model, "gold_price_model", input_example=input_example)
    new_run_id = mlflow.active_run().info.run_id
    logger.info(f"Modèle fine-tuné et loggé avec MLflow. Nouveau Run ID : {new_run_id}")