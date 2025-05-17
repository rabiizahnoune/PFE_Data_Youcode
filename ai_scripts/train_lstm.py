import os
import sys
import time
import requests
from requests.exceptions import ConnectionError
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Configurer un répertoire temporaire personnalisé pour MLflow
os.environ["MLFLOW_TMP_DIR"] = "/ai_scripts/tmp"
os.makedirs("/ai_scripts/tmp", exist_ok=True)

# Désactiver complètement le GPU
os.environ["CUDA_VISIBLE_DEVICES"] = ""
# Supprimer les avertissements et messages d'info
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

# Rediriger les logs TensorFlow (stderr) vers /dev/null pour supprimer les messages restants
stderr_fd = os.dup(2)  # Sauvegarder stderr
os.dup2(os.open(os.devnull, os.O_RDWR), 2)  # Rediriger stderr vers /dev/null

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import accuracy_score
from sklearn.utils.class_weight import compute_class_weight
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import mlflow
import mlflow.keras
from mlflow.models import ModelSignature
from mlflow.types import Schema, TensorSpec
from datetime import datetime, timedelta

# Configuration MLflow
MLFLOW_TRACKING_URI = "http://localhost:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Attendre que le serveur MLflow soit prêt
def wait_for_mlflow_server(url, timeout=60, interval=5):
    """Attend que le serveur MLflow soit disponible."""
    start_time = time.time()
    while True:
        try:
            response = requests.get(url + "/health")
            if response.status_code == 200:
                print("Serveur MLflow prêt !")
                break
        except ConnectionError:
            print("Attente du démarrage du serveur MLflow...")
            time.sleep(interval)
            if time.time() - start_time > timeout:
                raise Exception("Timeout : le serveur MLflow n'est pas prêt après {} secondes".format(timeout))

# Attendre que le serveur MLflow soit prêt avant de continuer
wait_for_mlflow_server(MLFLOW_TRACKING_URI)

# Définir l'expérience MLflow
mlflow.set_experiment("gold_price_trend_prediction")

# Restaurer stderr après l'initialisation de MLflow et TensorFlow
os.dup2(stderr_fd, 2)
os.close(stderr_fd)

# Configuration Snowflake avec SQLAlchemy
SNOWFLAKE_USER = "RABIIZAHNOUNE05"
SNOWFLAKE_PASSWORD = "CdFbMNyjc87vueV"
SNOWFLAKE_ACCOUNT = "RRWRUAS-VI48008"
SNOWFLAKE_DATABASE = "GOLD"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# Créer une URL de connexion pour SQLAlchemy
snowflake_url = f"snowflake://{SNOWFLAKE_USER}:{quote_plus(SNOWFLAKE_PASSWORD)}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}"
engine = create_engine(snowflake_url)

# Paramètres du modèle
SEQUENCE_LENGTH = 6  # Réduit à 6 mois pour avoir plus de séquences
BATCH_SIZE = 16      # Réduit pour des mises à jour plus fréquentes
EPOCHS = 50
LSTM_UNITS = 50      # Réduit pour moins de complexité

def fetch_data():
    """Récupère les données historiques de l'or depuis la table gold_historique sur Snowflake."""
    query = """
        SELECT 
            date_id,
            date,
            hour,
            gold_price,
            open_price,
            close_price,
            volume,
            datetime
        FROM gold_historique
        ORDER BY datetime
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        df.columns = df.columns.str.lower()
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df

def prepare_monthly_data(df):
    """Regroupe les données par mois et calcule les prix moyens et des indicateurs."""
    df['year_month'] = df['datetime'].dt.to_period('M')
    monthly_data = df.groupby('year_month').agg({
        'open_price': 'mean',
        'close_price': 'mean',
        'volume': 'mean'
    }).reset_index()
    monthly_data['year_month'] = monthly_data['year_month'].dt.to_timestamp()

    # Ajouter des moyennes mobiles et la volatilité
    monthly_data['close_price_ma3'] = monthly_data['close_price'].rolling(window=3).mean()
    monthly_data['volume_ma3'] = monthly_data['volume'].rolling(window=3).mean()
    monthly_data['close_price_volatility'] = monthly_data['close_price'].rolling(window=3).std()

    # Remplacer les NaN par la moyenne des colonnes
    monthly_data = monthly_data.fillna(monthly_data.mean(numeric_only=True))
    return monthly_data

def create_trend_labels(monthly_data, target_col='close_price'):
    """Crée des étiquettes binaires pour la tendance (1: hausse, 0: baisse)."""
    labels = []
    for i in range(len(monthly_data) - 1):
        current_price = monthly_data[target_col].iloc[i]
        next_price = monthly_data[target_col].iloc[i + 1]
        label = 1 if next_price > current_price else 0
        labels.append(label)
    labels.append(None)
    monthly_data['trend'] = labels
    monthly_data = monthly_data.dropna(subset=['trend'])
    print("Distribution des tendances (1: Hausse, 0: Baisse):")
    print(monthly_data['trend'].value_counts(normalize=True))
    return monthly_data

def prepare_sequences(monthly_data, sequence_length=SEQUENCE_LENGTH):
    """Prépare les séquences pour l'entraînement du modèle LSTM."""
    features = ['open_price', 'close_price', 'volume', 'close_price_ma3', 'volume_ma3', 'close_price_volatility']
    data = monthly_data[features].values

    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)

    X, y = [], []
    for i in range(len(data_scaled) - sequence_length):
        X.append(data_scaled[i:i + sequence_length])
        y.append(monthly_data['trend'].iloc[i + sequence_length])

    X = np.array(X)
    y = np.array(y)

    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    return X_train, X_test, y_train, y_test, scaler

def build_lstm_model(input_shape, lstm_units=LSTM_UNITS):
    """Construit le modèle LSTM pour la classification binaire."""
    model = Sequential([
        LSTM(units=lstm_units, return_sequences=True, input_shape=input_shape, kernel_regularizer=tf.keras.regularizers.l2(0.02)),
        Dropout(0.4),
        LSTM(units=lstm_units, kernel_regularizer=tf.keras.regularizers.l2(0.02)),
        Dropout(0.4),
        Dense(units=1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

def predict_next_trend(model, monthly_data, scaler, sequence_length=SEQUENCE_LENGTH):
    """Prédit la tendance pour le mois suivant."""
    features = ['open_price', 'close_price', 'volume', 'close_price_ma3', 'volume_ma3', 'close_price_volatility']
    recent_data = monthly_data[features].values[-sequence_length:]
    data_scaled = scaler.transform(recent_data)
    X = np.array([data_scaled])

    trend_prob = model.predict(X, verbose=0)[0][0]
    trend = 1 if trend_prob >= 0.5 else 0
    trend_label = "Hausse" if trend == 1 else "Baisse"
    return trend, trend_label, trend_prob

def store_prediction(trend_label, trend_prob):
    """Stocke la prédiction de tendance dans Snowflake."""
    current_date = datetime.now()
    next_month = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1).date()

    prediction_data = pd.DataFrame({
        'prediction_date': [next_month],
        'predicted_trend': [trend_label],
        'trend_probability': [trend_prob]
    })

    with engine.connect() as conn:
        prediction_data.to_sql('gold_trend_predictions', conn, if_exists='append', index=False)
        print(f"Prédiction stockée dans Snowflake : {trend_label} pour {next_month} (Probabilité: {trend_prob:.2f})")

def train_model():
    """Entraîne le modèle LSTM pour prédire la tendance et stocke la prédiction."""
    df = fetch_data()
    if df.empty:
        print("Aucune donnée récupérée de Snowflake.")
        return

    monthly_data = prepare_monthly_data(df)
    print(f"Nombre de mois de données : {len(monthly_data)}")
    if len(monthly_data) < SEQUENCE_LENGTH + 1:
        print(f"Pas assez de données mensuelles pour entraîner le modèle. Nécessite au moins {SEQUENCE_LENGTH + 1} mois.")
        return

    monthly_data = create_trend_labels(monthly_data)
    X_train, X_test, y_train, y_test, scaler = prepare_sequences(monthly_data)

    model = build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))

    with mlflow.start_run(run_name=f"lstm_trend_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("sequence_length", SEQUENCE_LENGTH)
        mlflow.log_param("batch_size", BATCH_SIZE)
        mlflow.log_param("epochs", EPOCHS)
        mlflow.log_param("lstm_units", LSTM_UNITS)
        mlflow.log_param("source", "snowflake")

        # Calculer les poids des classes pour gérer le déséquilibre
        class_weights = compute_class_weight('balanced', classes=np.unique(y_train), y=y_train)
        class_weight_dict = dict(enumerate(class_weights))

        history = model.fit(
            X_train, y_train,
            epochs=EPOCHS,
            batch_size=BATCH_SIZE,
            validation_split=0.1,
            verbose=1,
            class_weight=class_weight_dict
        )

        y_pred_prob = model.predict(X_test)
        y_pred = (y_pred_prob >= 0.5).astype(int)
        accuracy = accuracy_score(y_test, y_pred)

        mlflow.log_metric("accuracy", accuracy)

        # Définir une signature avec TensorSpec
        input_schema = Schema([
            TensorSpec(np.dtype(np.float64), (-1, SEQUENCE_LENGTH, 6), "input")
        ])
        output_schema = Schema([
            TensorSpec(np.dtype(np.float64), (-1, 1), "output")
        ])
        signature = ModelSignature(inputs=input_schema, outputs=output_schema)

        # Enregistrer le modèle avec la signature
        mlflow.keras.log_model(
            model,
            "lstm_trend_model",
            signature=signature
        )

        for epoch, loss in enumerate(history.history['loss']):
            mlflow.log_metric("train_loss", loss, step=epoch)
        for epoch, val_loss in enumerate(history.history['val_loss']):
            mlflow.log_metric("val_loss", val_loss, step=epoch)

        print(f"Modèle entraîné. Accuracy: {accuracy}")

        # Prédire la tendance pour le mois suivant
        trend, trend_label, trend_prob = predict_next_trend(model, monthly_data, scaler)
        print(f"Tendance prédite pour le mois suivant : {trend_label} (Probabilité: {trend_prob:.2f})")

        # Stocker la prédiction dans Snowflake
        store_prediction(trend_label, trend_prob)

if __name__ == "__main__":
    train_model()