import pandas as pd
from hdfs import InsecureClient
import pyarrow.parquet as pq
import os
import subprocess
import logging
from pathlib import Path

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration HDFS
HDFS_URL = "http://namenode:9870"
HDFS_USER = "hadoop"
RAW_DIR = "/gold_datalake/raw"
PROCESSED_DIR = "/gold_datalake/processed"
LOCAL_DOWNLOAD_DIR = "/app/hdfs_download"

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

def list_files(client, hdfs_dir):
    try:
        files = []
        for root, _, filenames in client.walk(hdfs_dir):
            for fname in filenames:
                file_path = f"{root}/{fname}"
                status = client.status(file_path)
                logger.info(f"  - {file_path} (taille: {status['length']} octets, type: fichier)")
                files.append(file_path)
        if not files:
            logger.warning(f"Aucun fichier trouvé dans {hdfs_dir}")
        return files
    except Exception as e:
        logger.error(f"Erreur lors de la liste des fichiers dans {hdfs_dir} : {e}")
        return []

def download_file(client, hdfs_path, local_dir):
    try:
        os.makedirs(local_dir, exist_ok=True)
        relative_path = hdfs_path.replace('/gold_datalake/', '').replace('/', os.sep)
        local_path = os.path.join(local_dir, relative_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
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
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            logger.info(f"Fichier CSV lu avec succès : {file_path}")
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            logger.info(f"Fichier Parquet lu avec succès : {file_path}")
        else:
            logger.warning(f"Format de fichier non pris en charge : {file_path}")
            return None
        logger.info(f"Aperçu des 5 premières lignes :\n{df.head().to_string()}")
        logger.info(f"Forme du DataFrame : {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {file_path} : {e}")
        return None

def main():
    logger.info(f"Répertoire local pour les téléchargements : {LOCAL_DOWNLOAD_DIR}")
    client = connect_to_hdfs()
    for hdfs_dir in [RAW_DIR, PROCESSED_DIR]:
        set_hdfs_permissions(client, hdfs_dir)
    logger.info("\n=== Traitement des fichiers dans /gold_datalake/raw ===")
    raw_files = list_files(client, RAW_DIR)
    for file_path in raw_files:
        if file_path.endswith('.csv'):
            local_path = download_file(client, file_path, LOCAL_DOWNLOAD_DIR)
            if local_path:
                read_and_preview_file(local_path)
        else:
            logger.warning(f"Fichier ignoré (format non CSV) : {file_path}")
    logger.info("\n=== Traitement des fichiers dans /gold_datalake/processed ===")
    processed_files = list_files(client, PROCESSED_DIR)
    for file_path in processed_files:
        if file_path.endswith('.parquet'):
            local_path = download_file(client, file_path, LOCAL_DOWNLOAD_DIR)
            if local_path:
                read_and_preview_file(local_path)
        else:
            logger.warning(f"Fichier ignoré (format non Parquet) : {file_path}")

if __name__ == "__main__":
    main()