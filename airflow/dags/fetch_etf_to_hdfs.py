import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from hdfs import InsecureClient
import subprocess

# Configuration du client HDFS
HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")

# Ticker pour XAUUSD (or en USD)
TICKER = "GC=F"

def setup_hdfs_directory():
    # Chemin HDFS cible
    hdfs_dir = "/gold_data/raw"
    
    # Créer le répertoire dans HDFS
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-mkdir", "-p", hdfs_dir
        ], check=True)
        print(f"Répertoire HDFS {hdfs_dir} créé avec succès")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la création du répertoire HDFS : {e}")
        raise
    
    # Appliquer les permissions chmod 777
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
        ], check=True)
        print(f"Permissions 777 appliquées sur {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du changement des permissions : {e}")
        raise

def fetch_etf_data():
    # Configurer le répertoire HDFS avant l'écriture
    setup_hdfs_directory()
    
    # Récupérer les données de XAUUSD
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    data_list = []
    
    # Récupérer les données via yfinance
    try:
        etf = yf.Ticker(TICKER)
        hist = etf.history(start=start_date, end=end_date, interval="4h")  # Granularité quotidienne
        if hist.empty:
            print(f"Aucune donnée récupérée pour {TICKER} entre {start_date} et {end_date}")
            return
        
        for date, row in hist.iterrows():
            data_list.append({
                "raw_date": date.strftime("%Y-%m-%d"),
                "source_name": "yfinance",
                "ticker_symbol": TICKER,
                "open_price": row["Open"],
                "close_price": row["Close"],
                "volume": int(row["Volume"]) if "Volume" in row and pd.notnull(row["Volume"]) else 0,
                "ingestion_timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {TICKER} : {e}")
        raise
    
    # Créer un DataFrame et écrire dans HDFS
    df = pd.DataFrame(data_list)
    if df.empty:
        print("Aucune donnée à écrire dans HDFS")
        return
    
    print("Aperçu des données récupérées :")
    print(df.head())
    
    hdfs_path = f"/gold_data/raw/etf_{datetime.now().strftime('%Y%m%d')}.csv"
    with HDFS_CLIENT.write(hdfs_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)
    print(f"Données XAUUSD écrites dans HDFS : {hdfs_path}")

if __name__ == "__main__":
    fetch_etf_data()