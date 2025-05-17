import pandas as pd
import yfinance as yf
import snowflake.connector
from datetime import datetime

# Configuration Snowflake
SNOWFLAKE_CONN = {
    "account": "RRWRUAS-VI48008",
    "user": "RABIIZAHNOUNE05",
    "password": "CdFbMNyjc87vueV",
    "database": "GOLD",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH"
}

# Paramètres
TICKER = "GC=F"  # Ticker pour l'or (Gold Futures)
START_DATE = "2002-01-01"

def fetch_gold_data(start_date=START_DATE):
    """Récupère les données historiques de l'or depuis yfinance avec un timeframe de 1D."""
    # Récupérer les données journalières (1d) depuis 2006 via yfinance
    ticker = yf.Ticker(TICKER)
    df = ticker.history(start=start_date, interval="1d", auto_adjust=True)
    
    if df.empty:
        print("Aucune donnée récupérée de yfinance.")
        return pd.DataFrame()

    # Renommer les colonnes pour correspondre à la structure attendue
    df = df.reset_index()
    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open_price",
        "Close": "close_price",
        "Volume": "volume"
    })
    
    # S'assurer que la colonne datetime est au bon format (ignorer l'heure)
    df['datetime'] = pd.to_datetime(df['datetime']).dt.normalize()  # Fixe l'heure à 00:00:00
    
    # Ajouter les colonnes date et hour
    df['date'] = df['datetime'].dt.date
    df['hour'] = 0  # Fixé à 0 pour un timeframe journalier
    
    # Ajouter une colonne date_id (YYYYMMDD)
    df['date_id'] = (df['datetime'].dt.year * 10000 + 
                     df['datetime'].dt.month * 100 + 
                     df['datetime'].dt.day)
    
    # Ajouter gold_price (même que close_price)
    df['gold_price'] = df['close_price']
    
    # Réorganiser les colonnes pour correspondre à la table gold_historique
    df = df[['date_id', 'date', 'hour', 'gold_price', 'open_price', 'close_price', 'volume', 'datetime']]
    
    return df

def store_in_snowflake(df):
    """Stocke les données dans la table gold_historique sur Snowflake."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    try:
        cursor = conn.cursor()
        
        # Créer la table si elle n'existe pas
        create_table_query = """
        CREATE TABLE IF NOT EXISTS gold_historique (
            date_id BIGINT PRIMARY KEY,
            date DATE,
            hour INT,
            gold_price FLOAT,
            open_price FLOAT,
            close_price FLOAT,
            volume BIGINT,
            datetime TIMESTAMP
        );
        """
        cursor.execute(create_table_query)

        # Préparer les données pour l'insertion
        df['date'] = df['date'].astype(str)
        df['datetime'] = df['datetime'].astype(str)
        
        # Insérer les données
        insert_query = """
        INSERT INTO gold_historique (date_id, date, hour, gold_price, open_price, close_price, volume, datetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(insert_query, rows)
        
        # Valider la transaction
        conn.commit()
        print(f"{len(rows)} lignes insérées dans la table gold_historique.")
        
    except Exception as e:
        print(f"Erreur lors de l'insertion dans Snowflake : {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def main():
    """Pipeline principal pour ingérer et stocker les données."""
    print("Récupération des données depuis yfinance (timeframe 1D)...")
    df = fetch_gold_data()
    
    if df.empty:
        print("Aucune donnée à traiter. Fin du script.")
        return
    
    print(f"Nombre de lignes récupérées : {len(df)}")
    
    print("Stockage des données dans Snowflake...")
    store_in_snowflake(df)
    
    print("Pipeline d'ingestion terminé.")

if __name__ == "__main__":
    main()