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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# HDFS Configuration
HDFS_URL = "http://namenode:9870"
HDFS_USER = "hadoop"
HDFS_MONTH_PATH = "/gold_price/aggregates/year=2025/month=6"

# Local temporary path for downloading Parquet files
LOCAL_TEMP_DIR = "/app/temp_month_data"

# Connect to HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER, root='/', timeout=30)
logger.info("Connected to HDFS successfully")

# Download all Parquet files for the month
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
files = []
for root, _, filenames in client.walk(HDFS_MONTH_PATH):
    for fname in filenames:
        if fname.endswith('.parquet'):
            hdfs_path = f"{root}/{fname}"
            local_path = f"{LOCAL_TEMP_DIR}/{fname}"
            with client.read(hdfs_path) as reader:
                with open(local_path, 'wb') as writer:
                    writer.write(reader.read())
            files.append(local_path)
logger.info(f"Downloaded {len(files)} files for fine-tuning")

# Read all Parquet files
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
df['Datetime'] = pd.to_datetime(df['Datetime'])
df = df.sort_values("Datetime")
logger.info("Fine-tuning data loaded successfully")

# Prepare features
window_size = 30
target_shift = 30

for i in range(1, window_size + 1):
    df[f"lag_{i}"] = df["Price"].shift(i)

df["target"] = df["Price"].shift(-target_shift)
df = df.dropna()

X = df[[f"lag_{i}" for i in range(1, window_size + 1)]]
y = df["target"]

# Load the existing model
mlflow.set_tracking_uri("sqlite:///mlflow.db")
run_id = "<run_id>"  # Replace with the run_id from the initial training
model_uri = f"runs:/{run_id}/gold_price_model"
model = mlflow.sklearn.load_model(model_uri)
logger.info("Existing model loaded successfully")

# Fine-tune the model
with mlflow.start_run():
    model.fit(X, y)  # Fine-tuning by retraining on new data

    # Log metrics (handle scikit-learn version compatibility)
    predictions = model.predict(X)
    if int(sklearn.__version__.split('.')[0]) >= 0 and int(sklearn.__version__.split('.')[1]) >= 22:
        rmse = mean_squared_error(y, predictions, squared=False)
    else:
        mse = mean_squared_error(y, predictions)
        rmse = mse ** 0.5
    mlflow.log_metric("rmse", rmse)

    # Log parameters
    mlflow.log_param("window_size", window_size)
    mlflow.log_param("target_shift", target_shift)
    mlflow.log_param("fine_tune_month", "2025-06")

    # Log the updated model
    mlflow.sklearn.log_model(model, "gold_price_model")
    new_run_id = mlflow.active_run().info.run_id
    logger.info(f"Model fine-tuned and logged with MLflow. New Run ID: {new_run_id}")