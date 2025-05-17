import requests
import pandas as pd
from datetime import datetime, timedelta
from hdfs import InsecureClient
import subprocess

FRED_API_KEY = "471b860a20dbaf85b05471dd25d51a33"
HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
FRED_SERIES = {
    "gold_price": "GOLDAMGBD228NLBM",
    "fed_funds_rate": "FEDFUNDS",
    "inflation_us": "CPIAUCSL",
    "dxy_index": "DTWEXBGS"
}

def setup_hdfs_directory():
    hdfs_dir = "/gold_data/raw"
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-mkdir", "-p", hdfs_dir
        ], check=True)
        print(f"Répertoire HDFS {hdfs_dir} créé avec succès")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la création du répertoire HDFS : {e}")
        raise
    
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
        ], check=True)
        print(f"Permissions 777 appliquées sur {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du changement des permissions : {e}")
        raise

def fetch_macro_data():
    setup_hdfs_directory()
    
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")  # Ajout d'une date de fin
    data_list = []
    
    for name, series in FRED_SERIES.items():
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={FRED_API_KEY}&file_type=json&observation_start={start_date}&observation_end={end_date}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Lever une exception pour les codes HTTP d'erreur
            response_json = response.json()
            
            # Log de la réponse pour déboguer
            print(f"Réponse FRED pour {series} : {response_json}")
            
            # Vérifier si "observations" existe
            if "observations" not in response_json:
                print(f"Erreur : 'observations' non trouvé dans la réponse pour {series}. Réponse complète : {response_json}")
                continue
            
            for obs in response_json["observations"]:
                if obs["value"] != ".":
                    data_list.append({
                        "raw_date": obs["date"],
                        "source_name": "FRED",
                        "indicator_name": name,
                        "value": float(obs["value"]),
                        "ingestion_timestamp": datetime.now().isoformat()
                    })
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la requête FRED pour {series} : {e}")
            continue
    
    if not data_list:
        raise ValueError("Aucune donnée FRED récupérée pour aucune série.")
    
    df = pd.DataFrame(data_list)
    print(df)
    hdfs_path = f"/gold_data/raw/macro_{datetime.now().strftime('%Y%m%d')}.csv"
    with HDFS_CLIENT.write(hdfs_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)
    print(f"Données macro écrites dans HDFS : {hdfs_path}")

if __name__ == "__main__":
    fetch_macro_data()