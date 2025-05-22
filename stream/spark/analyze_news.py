from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from datetime import datetime
import requests
from cassandra.cluster import Cluster
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Définir le schéma attendu pour le fichier JSON
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("content", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("url", StringType(), True),
    StructField("_corrupt_record", StringType(), True)  # Inclure la colonne des enregistrements corrompus
])

# Fonction pour déterminer l'année, le mois et le jour actuels
def get_current_date_path():
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day
    return f"/gold_price/news/raw/year={year}/month={month}/day={day}/news_data.json"

# Initialiser Spark
spark = SparkSession.builder \
    .appName("GoldMarketNewsAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Calculer le chemin HDFS basé sur la date actuelle
hdfs_file_path = get_current_date_path()

# Lire le fichier JSON depuis HDFS avec un schéma explicite et gérer les enregistrements corrompus
logger.info(f"Lecture du fichier JSON depuis : {hdfs_file_path}")
news_df = spark.read \
    .schema(schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json(f"hdfs://namenode:9000{hdfs_file_path}")

# Afficher un échantillon pour débogage
logger.info("Aperçu des données brutes :")
news_df.show(10, truncate=False)

# Filtrer les enregistrements valides (au moins un champ non-null, incluant chaînes vides)
news_df = news_df.filter(news_df._corrupt_record.isNull()) \
    .filter((news_df.title.isNotNull()) | (news_df.description.isNotNull()) | (news_df.content.isNotNull()))

# Vérifier le nombre total de lignes valides
valid_count = news_df.count()
if valid_count == 0:
    logger.error("Aucune donnée valide trouvée après filtrage. Arrêt du traitement.")
    spark.stop()
    raise ValueError("Aucune donnée valide à analyser après filtrage.")
logger.info(f"{valid_count} lignes valides à analyser.")

# Analyse avec Gemini AI
GEMINI_API_KEY = "AIzaSyBqnMwcPJZqySSrdrcqnG828KmW_f7KkfE"
GEMINI_API_URL = "https://api.gemini.ai/v1/analyze"

def analyze_news_with_gemini(news_content):
    # Vérifier si news_content est None ou vide
    if not news_content:
        logger.warning("Contenu de la news vide ou None. Retour par défaut.")
        return "Contenu manquant, impossible d'analyser."
    
    # S'assurer que news_content est une chaîne et non None avant slicing
    news_content = str(news_content)
    logger.info(f"Analyse de la news : {news_content[:50]}...")
    
    prompt = f"Voici une actualité sur le marché de l'or : {news_content}. Quel est l'impact potentiel de cette actualité sur le marché de l'or ? Répondez en une phrase concise."
    headers = {"Authorization": f"Bearer {GEMINI_API_KEY}", "Content-Type": "application/json"}
    payload = {"prompt": prompt, "model": "gemini-1.5", "max_tokens": 50}
    try:
        response = requests.post(GEMINI_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("impact", "Erreur lors de l'analyse")
    except Exception as e:
        return f"Erreur : {str(e)}"

# Appliquer la transformation
news_rdd = news_df.rdd.map(lambda row: (
    row.title, row.description, row.content, row.published_at, row.source, row.url,
    analyze_news_with_gemini(row.content or row.description or row.title)
))

columns = ["title", "description", "content", "published_at", "source", "url", "impact"]
analyzed_df = news_rdd.toDF(columns)

# Stocker dans Cassandra
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

def write_to_cassandra(row):
    session.execute("""
        INSERT INTO news_impact (title, description, content, published_at, source, url, impact)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row.title, row.description, row.content, row.published_at, row.source, row.url, row.impact))

analyzed_df.foreach(write_to_cassandra)

cassandra_cluster.shutdown()
spark.stop()
logger.info("Analyse terminée et résultats stockés dans Cassandra")