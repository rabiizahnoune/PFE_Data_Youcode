from pyspark.sql import SparkSession
import sys
import subprocess
# Récupérer les arguments passés par Airflow
input_file = sys.argv[1]  # Chemin du fichier JSON local
hdfs_raw_path = sys.argv[2]  # Chemin HDFS pour stocker les données brutes


    
# Initialiser Spark
spark = SparkSession.builder \
    .appName("GoldMarketNewsProcessing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Lire le fichier JSON
news_df = spark.read.json(input_file)

# Sauvegarder les données brutes dans HDFS au format Parquet
news_df.write.mode("overwrite").parquet(hdfs_raw_path)

print(f"Données brutes stockées dans HDFS : {hdfs_raw_path}")

# Arrêter la session Spark
spark.stop()