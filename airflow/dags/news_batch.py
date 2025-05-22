from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import json
import logging
import subprocess
import os
from hdfs import InsecureClient
from cassandra.cluster import Cluster
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fonction pour déterminer le mois et l'année actuels
def get_current_month_year():
    today = datetime.now()
    return today.year, today.month

# Tâche 1 : Ingestion des news via NewsAPI (exécutée dans Airflow)
def ingest_news(**kwargs):
    API_KEY = "a461cd91622142459cf65995e3a403ed"  # Remplacez par une clé valide si nécessaire
    KEYWORDS = "gold market OR gold price"
    URL = "https://newsapi.org/v2/everything"

    try:
        all_news_data = []
        page = 1
        total_pages = 10  # Limité à 10 pages pour éviter l'erreur 426

        while page <= total_pages:
            params = {
                "q": KEYWORDS,
                "apiKey": API_KEY,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 10,
                "page": page
            }
            logger.info(f"Requête API NewsAPI, page {page}...")
            response = requests.get(URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "ok":
                raise ValueError(f"Erreur API NewsAPI : {data.get('message', 'Erreur inconnue')}")
            
            articles = data.get("articles", [])
            total_results = data.get("totalResults", 0)
            total_pages = min(total_pages, (total_results + 9) // 10)

            for article in articles:
                all_news_data.append({
                    "title": article["title"],
                    "description": article["description"] or "",
                    "content": article["content"] or "",
                    "published_at": article["publishedAt"],
                    "source": article["source"]["name"],
                    "url": article["url"]
                })

            page += 1

        if not all_news_data:
            raise ValueError("Aucune news n'a été récupérée depuis l'API")

        output_file = f"/app/news_gold_market_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        logger.info(f"Écriture des données dans {output_file}...")
        
        if not os.path.exists("/app"):
            raise FileNotFoundError("Le répertoire /app/ n'existe pas ou n'est pas accessible")
            
        with open(output_file, "w") as f:
            json.dump(all_news_data, f, indent=4)
            
        kwargs['ti'].xcom_push(key='news_file_path', value=output_file)
        logger.info(f"Total de {len(all_news_data)} news ingérées et sauvegardées dans {output_file}")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la requête API NewsAPI : {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Erreur dans ingest_news : {str(e)}")
        raise

# Tâche 2 : Préparer les paramètres pour le traitement
def prepare_params(**kwargs):
    logger.info("Préparation des paramètres")
    try:
        year, month = get_current_month_year()
        day = datetime.now().day
        hdfs_raw_path = f"/gold_price/news/raw/year={year}/month={month}/day={day}"  # Chemin sans le préfixe hdfs://namenode:9000
        kwargs['ti'].xcom_push(key='year', value=year)
        kwargs['ti'].xcom_push(key='month', value=month)
        kwargs['ti'].xcom_push(key='day', value=day)
        kwargs['ti'].xcom_push(key='hdfs_raw_path', value=hdfs_raw_path)
        logger.info("Paramètres poussés dans XCom avec succès")
    except Exception as e:
        logger.error(f"Erreur dans prepare_params : {str(e)}", exc_info=True)
        raise
    return True

# Tâche 3 : Créer le répertoire HDFS et ajuster les permissions
def create_hdfs_directory(**kwargs):
    logger.info("Création du répertoire HDFS et ajustement des permissions")
    try:
        hdfs_raw_path = kwargs['ti'].xcom_pull(key='hdfs_raw_path', task_ids='prepare_params')
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-mkdir", "-p", f"hdfs://namenode:9000{hdfs_raw_path}"
        ], check=True, text=True, capture_output=True)
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-chmod", "-R", "777", f"hdfs://namenode:9000{hdfs_raw_path}"
        ], check=True, text=True, capture_output=True)
        logger.info(f"Répertoire {hdfs_raw_path} créé et permissions ajustées à 777")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors de la gestion des permissions HDFS : {e.stderr}")
        raise
    return True

# Tâche 4 : Charger les données dans HDFS avec Python (sans Spark)
def store_news_to_hdfs(**kwargs):
    try:
        # Récupérer les chemins via XCom
        input_file = kwargs['ti'].xcom_pull(key='news_file_path', task_ids='ingest_news')
        hdfs_raw_path = kwargs['ti'].xcom_pull(key='hdfs_raw_path', task_ids='prepare_params')

        logger.info(f"Chargement du fichier {input_file} dans HDFS à {hdfs_raw_path}...")

        # Initialiser un client HDFS
        client = InsecureClient('http://namenode:9870', user='root')

        # Vérifier que le fichier local existe
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Le fichier {input_file} n'existe pas")

        # Construire le chemin complet dans HDFS
        hdfs_file_path = f"{hdfs_raw_path}/news_data.json"

        # Uploader le fichier JSON dans HDFS
        client.upload(hdfs_file_path, input_file, overwrite=True)

        logger.info(f"Fichier JSON stocké dans HDFS : {hdfs_file_path}")
        return True

    except Exception as e:
        logger.error(f"Erreur dans store_news_to_hdfs : {str(e)}")
        raise

# Tâche 5 : Analyser les actualités et stocker dans Cassandra (sans Spark)
def analyze_news(**kwargs):
    try:
        # Récupérer le chemin HDFS via XCom
        hdfs_raw_path = kwargs['ti'].xcom_pull(key='hdfs_raw_path', task_ids='prepare_params')
        hdfs_file_path = f"{hdfs_raw_path}/news_data.json"

        logger.info(f"Lecture du fichier JSON depuis HDFS : {hdfs_file_path}...")

        # Initialiser un client HDFS
        client = InsecureClient('http://namenode:9870', user='root')

        # Lire le fichier JSON depuis HDFS
        with client.read(hdfs_file_path) as reader:
            news_data = json.load(reader)

        # Connexion à Cassandra
        cassandra_cluster = Cluster(['cassandra'], port=9042)
        session = cassandra_cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS gold_market WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.set_keyspace('gold_market')
        session.execute("""
            CREATE TABLE IF NOT EXISTS news_impact (
                title text, description text, content text, published_at text, source text, url text, impact text,
                PRIMARY KEY (published_at, title)
            )
        """)

        # Analyse avec Google Generative AI (Gemini)
        GEMINI_API_KEY = "AIzaSyBqnMwcPJZqySSrdrcqnG828KmW_f7KkfE"  # Remplacez par votre clé API Google
        GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

        def analyze_news_with_gemini(news_content):
            if not news_content:
                logger.warning("Contenu de la news vide ou None. Retour par défaut.")
                return "Contenu manquant, impossible d'analyser."
            news_content = str(news_content)
            logger.info(f"Analyse de la news : {news_content[:50]}...")
            prompt = f"Voici une actualité sur le marché de l'or : {news_content}. Quel est l'impact potentiel de cette actualité sur le marché de l'or ? Répondez en une phrase concise."
            url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
            headers = {"Content-Type": "application/json"}
            payload = {
                "contents": [
                    {
                        "parts": [
                            {"text": prompt}
                        ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 50,
                    "temperature": 0.7
                }
            }
            try:
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                result = response.json()
                return result["candidates"][0]["content"]["parts"][0]["text"].strip()
            except Exception as e:
                logger.error(f"Erreur lors de l'appel à l'API Gemini : {str(e)}")
                return f"Erreur : {str(e)}"

        # Traiter chaque entrée avec une pause de 1 minute entre les appels
        if not isinstance(news_data, list):
            news_data = [news_data]  # Si le fichier contient un seul objet JSON

        logger.info(f"Nombre total de news à analyser : {len(news_data)}")
        for entry in news_data:
            title = entry.get("title", "")
            description = entry.get("description", "")
            content = entry.get("content", "")
            published_at = entry.get("published_at", "")
            source = entry.get("source", "")
            url = entry.get("url", "")
            
            # Utiliser le premier champ non-vide ou concaténer si nécessaire
            news_content = content or description or title or "Aucun contenu disponible"
            impact = analyze_news_with_gemini(news_content)
            time.sleep(5)  # Pause de 1 minute pour respecter la limite de l'API Gemini

            # Insérer dans Cassandra
            session.execute("""
                INSERT INTO news_impact (title, description, content, published_at, source, url, impact)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (title, description, content, published_at, source, url, impact))

        logger.info("Analyse terminée et résultats stockés dans Cassandra")
        cassandra_cluster.shutdown()

    except Exception as e:
        logger.error(f"Erreur dans analyze_news : {str(e)}")
        raise

# Définition du DAG
with DAG(
    dag_id="gold_news_pipeline",
    start_date=datetime(2024, 5, 21, 10, 0),  # 10:00 AM +01 aujourd'hui
    schedule_interval=None,  # Exécution quotidienne
    catchup=False,
    max_active_runs=1
) as dag:
    # Tâche 1 : Ingestion des news
    ingest_task = PythonOperator(
        task_id="ingest_news",
        python_callable=ingest_news
    )

    # Tâche 2 : Préparer les paramètres
    prepare_task = PythonOperator(
        task_id="prepare_params",
        python_callable=prepare_params
    )

    # Tâche 3 : Créer le répertoire HDFS et ajuster les permissions
    create_hdfs_task = PythonOperator(
        task_id="create_hdfs_directory",
        python_callable=create_hdfs_directory
    )

    # Tâche 4 : Stocker dans HDFS avec Python (sans Spark)
    store_hdfs_task = PythonOperator(
        task_id="store_news_to_hdfs",
        python_callable=store_news_to_hdfs
    )

    # Tâche 5 : Analyser avec Gemini AI et stocker dans Cassandra (sans Spark)
    analyze_task = PythonOperator(
        task_id="analyze_news",
        python_callable=analyze_news,
        on_failure_callback=lambda context: logger.error(f"Tâche échouée : {context['task_instance'].task_id}, logs : {context['task_instance'].log_url}")
    )

    # Dépendances
    ingest_task >> prepare_task >> create_hdfs_task >> store_hdfs_task >> analyze_task