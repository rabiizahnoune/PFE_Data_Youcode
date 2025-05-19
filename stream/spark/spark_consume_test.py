from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType

# 1. Créer la session Spark
spark = (SparkSession.builder
         .appName("GoldPriceConsoleTest")
         .getOrCreate())

# 2. Définir le schéma JSON attendu
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("price", DoubleType())
])

# 3. Lire le flux Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "gold-price")
      .option("startingOffsets", "latest")
      .load())

# 4. Parser le JSON et sélectionner les colonnes
parsed = (df.select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*"))

# 5. Écrire dans la console pour test
query = (parsed.writeStream
               .format("console")
               .outputMode("append")
               .option("truncate", False)
               .start())

query.awaitTermination()
