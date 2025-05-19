# import yfinance as yf

# data = yf.download("GC=F", interval="1m", period="1d")  # GC=F = Gold Futures
# print(data.tail())



# streamlit run gold_trading_dashboard.py --server.port=8050 --server.address=0.0.0.0
import logging
import os
import subprocess
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration HDFS
HDFS_URL = "http://172.18.0.2:9870"  # Remplacez par l'IP réelle de votre NameNode
HDFS_USER = "hadoop"  # Utilisateur HDFS
HDFS_PATH = "/gold_price/aggregates/year=2025/month=5/day=19/part-00000-7233b45a-f771-4fa6-ba0b-29d0487d32bf.c000.snappy.parquet"  # Fichier spécifique
HDFS_DIR = "/gold_price/aggregates"  # Répertoire HDFS à modifier pour les permissions
LOCAL_DOWNLOAD_DIR = "C:/Users/Youcode/Documents/gold_price_project/hdfs_download"  # Répertoire local Windows

def connect_to_hdfs():
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER, root='/', timeout=30)
        logger.info("Connexion à HDFS établie avec succès")
        client.status('/')
        return client
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à HDFS : {e}")
        raise

def set_hdfs_permissions(client, hdfs_dir):
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
        ], check=True)
        logger.info(f"Permissions définies avec succès sur {hdfs_dir} (777)")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de la définition des permissions sur {hdfs_dir} : {e}")
        raise

def download_file(client, hdfs_path, local_dir):
    try:
        # Créer le répertoire local s'il n'existe pas
        os.makedirs(local_dir, exist_ok=True)
        # Extraire le nom de fichier depuis le chemin HDFS
        file_name = os.path.basename(hdfs_path)
        local_path = os.path.join(local_dir, file_name)
        # Télécharger le fichier
        logger.info(f"Tentative de téléchargement : {hdfs_path}")
        with client.read(hdfs_path) as reader:
            content = reader.read()
            with open(local_path, 'wb') as writer:
                writer.write(content)
        logger.info(f"Fichier téléchargé avec succès : {hdfs_path} -> {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement du fichier {hdfs_path} : {e}")
        return None

def read_and_preview_file(file_path):
    try:
        if file_path.endswith('.parquet'):
            # Lire le fichier Parquet avec pyarrow
            table = pq.read_table(file_path)
            logger.info(f"Fichier Parquet lu avec succès : {file_path}")
            logger.info("Structure du fichier (Schéma) :")
            logger.info(table.schema)

            # Convertir en DataFrame pandas pour un aperçu
            df = table.to_pandas()
            logger.info(f"Aperçu des 5 premières lignes :\n{df.head().to_string()}")
            logger.info(f"Forme du DataFrame : {df.shape}")
            return df
        else:
            logger.warning(f"Format de fichier non pris en charge : {file_path}")
            return None
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {file_path} : {e}")
        return None

def main():
    logger.info(f"Répertoire local pour les téléchargements : {LOCAL_DOWNLOAD_DIR}")
    client = connect_to_hdfs()
    # Définir les permissions sur le répertoire HDFS
    set_hdfs_permissions(client, HDFS_DIR)
    # Télécharger et inspecter le fichier
    local_path = download_file(client, HDFS_PATH, LOCAL_DOWNLOAD_DIR)
    if local_path:
        read_and_preview_file(local_path)

if __name__ == "__main__":
    main()