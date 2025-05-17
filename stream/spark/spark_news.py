from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import requests
import time
import random
from datetime import datetime
import sys

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("GoldNewsStreaming") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.output.consistency.level", "ONE") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

# Schéma des données de news
schema = StructType([
    StructField("id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("source", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ingestion_time", StringType(), True)
])

# Lire les messages Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "gold-news") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 100) \
    .load()

# Parser les données JSON
news_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Fonction pour analyser les descriptions avec l'API Gemini
def analyze_batch_with_gemini(descriptions, max_retries=3, initial_delay=1.0):
    GEMINI_API_KEY = 'AIzaSyBqnMwcPJZqySSrdrcqnG828KmW_f7KkfE'
    if not GEMINI_API_KEY:
        return {"error": "GEMINI_API_KEY not set"}
    
    if not descriptions:
        return {"error": "No descriptions to analyze"}
    
    # Combiner toutes les descriptions en un seul texte
    combined_text = "\n".join([f"- {desc}" for desc in descriptions])
    
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
    prompt = f"""
    Vous êtes responsable de mes trades sur le marché de l'or (XAUUSD). Analysez les news suivantes et déterminez si elles influencent fortement le prix de l'or :
    
    {combined_text}
    
    Répondez en JSON avec :
    - "impact": 1 si les news influencent fortement le prix de l'or, 0 sinon.
    - "confidence": Un score de confiance entre 0 et 1 pour votre évaluation.
    - "explanation": Une courte explication de votre raisonnement.
    
    Exemple :
    {{
        "impact": 1,
        "confidence": 0.9,
        "explanation": "Les news mentionnent une crise géopolitique, ce qui augmente la demande pour l'or."
    }}
    """
    
    for attempt in range(max_retries):
        try:
            time.sleep(1.0 + random.uniform(0, 0.2))
            response = requests.post(
                f"{url}?key={GEMINI_API_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"response_mime_type": "application/json"}
                }
            )
            response.raise_for_status()
            result = response.json()
            return json.loads(result["candidates"][0]["content"]["parts"][0]["text"])
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt) * (1 + random.uniform(0, 0.2))
                    print(f"Limite de débit atteinte, réessai après {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                else:
                    return {"error": f"Nombre maximum de réessais atteint : 429 Too Many Requests"}
            return {"error": f"Erreur HTTP : {str(e)}"}
        except requests.RequestException as e:
            return {"error": f"Échec de la requête : {str(e)}"}
        except KeyError as e:
            return {"error": f"Format de réponse inattendu : {str(e)}"}
        except json.JSONDecodeError as e:
            return {"error": f"Échec du parsing JSON : {str(e)}"}
    return {"error": "Erreur inconnue après réessais"}

# Fonction pour traiter chaque batch
def process_batch(batch_df, batch_id):
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Processing batch {batch_id}")
    
    if not batch_df.isEmpty():
        # Collecter les données pour l'analyse et le stockage
        articles = [
            {
                "id": row["id"],
                "title": row["title"],
                "source": row["source"],
                "published_at": row["published_at"],
                "description": row["description"],
                "url": row["url"],
                "ingestion_time": row["ingestion_time"]
            }
            for row in batch_df.collect()
        ]
        
        # Stocker les news brutes dans Cassandra
        if articles:
            raw_news_df = spark.createDataFrame(articles, schema)
            raw_news_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", "gold_news") \
                .option("table", "raw_news") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} : {len(articles)} news brutes écrites dans gold_news.raw_news")
        
        # Filtrer les descriptions valides pour l'analyse
        descriptions = [article["description"] for article in articles if article["description"]]
        print(f"Batch {batch_id} : {len(descriptions)} descriptions valides pour analyse")
        
        if descriptions:
            try:
                # Analyser avec Gemini
                analysis_result = analyze_batch_with_gemini(descriptions)
                print(f"Analyse Gemini pour batch {batch_id} : {json.dumps(analysis_result, indent=2)}")
                
                if "error" not in analysis_result:
                    # Créer un DataFrame pour le résultat d'impact
                    window_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    impact_row = [{
                        "window_timestamp": window_timestamp,
                        "impact": analysis_result["impact"],
                        "confidence": analysis_result["confidence"],
                        "explanation": analysis_result["explanation"]
                    }]
                    impact_df = spark.createDataFrame(impact_row)
                    
                    # Écrire dans la table impact_analysis
                    impact_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .option("keyspace", "gold_news") \
                        .option("table", "impact_analysis") \
                        .mode("append") \
                        .save()
                    print(f"Batch {batch_id} : Résultat d'impact écrit dans gold_news.impact_analysis")
                else:
                    print(f"Échec de l'analyse Gemini pour batch {batch_id} : {analysis_result['error']}")
            except Exception as e:
                print(f"Erreur lors de l'analyse Gemini pour batch {batch_id} : {str(e)}")
        else:
            print(f"Batch {batch_id} : Aucune description valide à analyser")
    else:
        print(f"Batch {batch_id} : DataFrame vide")
    
    print(f"Batch {batch_id} processing completed")

# Traiter les batches
analysis_query = news_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", "/tmp/spark-checkpoint/impact_analysis") \
    .start()

# Attendre la fin de la requête
try:
    analysis_query.awaitTermination()
    print("Streaming query completed successfully")
except Exception as e:
    print(f"Streaming query failed: {str(e)}")
    raise e
finally:
    spark.stop()
    print("Spark session stopped")