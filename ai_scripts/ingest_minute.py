import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Clé API Alpha Vantage
API_KEY = "5VM4EJK4P29MRWNJ"

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

def fetch_minute_data():
    """Récupère les données minute par minute de GLD (30 derniers jours) avec Alpha Vantage."""
    ts = TimeSeries(key=API_KEY, output_format="pandas")
    try:
        data, meta_data = ts.get_intraday(
            symbol="GLD",
            interval="1min",
            outputsize="full"
        )
    except Exception as e:
        print(f"Erreur lors de la récupération des données minute par minute pour GLD : {e}")
        return pd.DataFrame()

    if data.empty:
        print("Aucune donnée minute par minute disponible pour GLD.")
        return pd.DataFrame()

    print(f"Données minute par minute récupérées pour GLD : {len(data)} minutes")
    print(f"Période couverte : {data.index.min()} à {data.index.max()}")

    data = data.reset_index()
    data = data.rename(columns={
        "date": "datetime",
        "1. open": "open_price",
        "2. high": "high_price",
        "3. low": "low_price",
        "4. close": "close_price",
        "5. volume": "volume"
    })
    data['datetime'] = pd.to_datetime(data['datetime'])
    
    # Convertir les prix de GLD en XAUUSD (approximation : XAUUSD ≈ GLD * 10)
    data['open_price'] = data['open_price'] * 10
    data['high_price'] = data['high_price'] * 10
    data['low_price'] = data['low_price'] * 10
    data['close_price'] = data['close_price'] * 10

    return data

def fetch_daily_data(minute_data_start_date):
    """Récupère des données quotidiennes historiques de GLD (1 an) et les convertit en données minute par minute."""
    ts = TimeSeries(key=API_KEY, output_format="pandas")
    try:
        data, meta_data = ts.get_daily(
            symbol="GLD",
            outputsize="full"
        )
    except Exception as e:
        print(f"Erreur lors de la récupération des données quotidiennes pour GLD : {e}")
        return pd.DataFrame()

    if data.empty:
        print("Aucune donnée quotidienne disponible pour GLD.")
        return pd.DataFrame()

    print(f"Données quotidiennes récupérées pour GLD : {len(data)} jours")
    print(f"Période couverte : {data.index.min()} à {data.index.max()}")

    # Filtrer pour ne garder que les données d'il y a 1 an jusqu'à la veille de minute_data_start_date
    end_date = pd.to_datetime(minute_data_start_date) - timedelta(days=1)
    start_date = end_date - timedelta(days=365)
    data = data[(data.index >= start_date) & (data.index <= end_date)]

    print(f"Données quotidiennes après filtrage (1 an jusqu'à la veille de minute_data) : {len(data)} jours")

    data = data.reset_index()
    data = data.rename(columns={
        "date": "datetime",
        "1. open": "open_price",
        "2. high": "high_price",
        "3. low": "low_price",
        "4. close": "close_price",
        "5. volume": "volume"
    })
    data['datetime'] = pd.to_datetime(data['datetime'])

    # Convertir les données quotidiennes en données minute par minute
    all_minutes = []
    for _, row in data.iterrows():
        day = row['datetime']
        start_time = day.replace(hour=0, minute=0, second=0)
        end_time = start_time + timedelta(days=1) - timedelta(minutes=1)
        minutes = pd.date_range(start=start_time, end=end_time, freq='1min')
        day_data = pd.DataFrame({
            'datetime': minutes,
            'open_price': row['open_price'] * 10,  # Convertir en XAUUSD
            'high_price': row['high_price'] * 10,
            'low_price': row['low_price'] * 10,
            'close_price': row['close_price'] * 10,
            'volume': row['volume'] / 1440  # Répartir le volume sur les 1440 minutes
        })
        all_minutes.append(day_data)

    minute_data = pd.concat(all_minutes, ignore_index=True)
    print(f"Données quotidiennes converties en minutes pour GLD (converties en XAUUSD) : {len(minute_data)} minutes")
    return minute_data

def combine_data(minute_data, daily_data):
    """Combine les données minute par minute et les données quotidiennes converties."""
    # Concaténer les données
    combined_data = pd.concat([daily_data, minute_data], ignore_index=True)
    combined_data['datetime'] = pd.to_datetime(combined_data['datetime'])

    # Trier par datetime
    combined_data = combined_data.sort_values('datetime')

    # Ajouter les colonnes manquantes
    combined_data['date'] = combined_data['datetime'].dt.date
    combined_data['hour'] = combined_data['datetime'].dt.hour

    # Vérifier les doublons
    duplicates = combined_data.duplicated(subset=['datetime']).sum()
    print(f"Nombre de doublons dans les données combinées : {duplicates}")

    return combined_data[['datetime', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'date', 'hour']]

def ingest_data_to_snowflake(df):
    """Insère les données dans la table gold_minute_data de Snowflake par lots."""
    if df.empty:
        print("Aucune donnée à ingérer.")
        return

    with engine.connect() as conn:
       

        # Insérer les données par lots pour éviter la limite de Snowflake
        chunk_size = 50000  # Insérer par lots de 50 000 lignes
        df.to_sql('gold_minute_data', conn, if_exists='append', index=False, chunksize=chunk_size, method='multi')
        print(f"{len(df)} minutes de données ingérées dans Snowflake.")

def main():
    """Ingest le maximum de données minute par minute disponibles pour l'entraînement."""
    print("Ingesting maximum available minute data for GLD (converted to XAUUSD)...")

    # Étape 1 : Récupérer les données minute par minute (30 derniers jours)
    minute_data = fetch_minute_data()
    
    # Déterminer la date de début des données minute par minute
    minute_data_start_date = minute_data['datetime'].min().date() if not minute_data.empty else datetime.now().date()

    # Étape 2 : Récupérer les données quotidiennes (1 an avant la date de début de minute_data)
    daily_data = fetch_daily_data(minute_data_start_date)

    # Étape 3 : Combiner les données
    combined_data = combine_data(minute_data, daily_data)

    # Étape 4 : Ingérer les données dans Snowflake
    ingest_data_to_snowflake(combined_data)

if __name__ == "__main__":
    main()