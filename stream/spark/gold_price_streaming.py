from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("GoldPriceStreaming") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# Définir le schéma des données entrantes (basé sur le producer modifié)
schema = StructType([
    StructField("Datetime", StringType(), False),
    StructField("Price", DoubleType(), False),
    StructField("Close", DoubleType(), False),
    StructField("High", DoubleType(), False),
    StructField("Low", DoubleType(), False),
    StructField("Open", DoubleType(), False)
])

# Lire le flux Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "gold-price") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parser les données JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Ajouter des colonnes pour l'année, le mois et le jour basées sur Datetime
parsed_df = parsed_df.withColumn("year", col("Datetime").substr(1, 4).cast("int")) \
    .withColumn("month", col("Datetime").substr(6, 2).cast("int")) \
    .withColumn("day", col("Datetime").substr(9, 2).cast("int"))

# Configurer l'écriture vers HDFS en format Parquet
output_path = "hdfs://namenode:9000/gold_price/aggregates"

query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "hdfs://namenode:9000/gold_price/checkpoints") \
    .partitionBy("year", "month", "day") \
    .trigger(processingTime="1 minute") \
    .start()

# Attendre la terminaison
query.awaitTermination()