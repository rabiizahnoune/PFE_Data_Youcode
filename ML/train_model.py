import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import logging
import sklearn

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to the CSV file
CSV_PATH = "/app/gold_price_historical.csv"

# Load the CSV file
logger.info(f"Loading data from {CSV_PATH}")
df = pd.read_csv(CSV_PATH)
df['Datetime'] = pd.to_datetime(df['Datetime'])
df = df.sort_values("Datetime")
logger.info("Data loaded successfully")

# Prepare features: Use the last 30 minutes to predict the price at t+30
window_size = 30
target_shift = 30

# Create lag features (Price at t-1, t-2, ..., t-30)
for i in range(1, window_size + 1):
    df[f"lag_{i}"] = df["Price"].shift(i)

# Define the target (Price at t+30)
df["target"] = df["Price"].shift(-target_shift)

# Drop rows with missing values
df = df.dropna()

# Define features and target
X = df[[f"lag_{i}" for i in range(1, window_size + 1)]]
y = df["target"]

# Configure MLflow
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("gold_price_prediction")

# Train the model
with mlflow.start_run():
    model = LinearRegression()
    model.fit(X, y)

    # Log metrics (handle scikit-learn version compatibility)
    predictions = model.predict(X)
    if int(sklearn.__version__.split('.')[0]) >= 0 and int(sklearn.__version__.split('.')[1]) >= 22:
        rmse = mean_squared_error(y, predictions, squared=False)  # RMSE directly
    else:
        mse = mean_squared_error(y, predictions)  # MSE
        rmse = mse ** 0.5  # Calculate RMSE manually
    mlflow.log_metric("rmse", rmse)

    # Log parameters
    mlflow.log_param("window_size", window_size)
    mlflow.log_param("target_shift", target_shift)
    mlflow.log_param("data_source", "csv")

    # Log the model with an input example to infer signature
    input_example = X.iloc[:1].to_dict('records')[0]  # Take the first row as an example
    mlflow.sklearn.log_model(model, "gold_price_model", input_example=input_example)
    run_id = mlflow.active_run().info.run_id
    logger.info(f"Model trained and logged with MLflow. Run ID: {run_id}")