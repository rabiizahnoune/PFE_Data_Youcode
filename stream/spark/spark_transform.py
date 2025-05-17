from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import socket
import time
import sys
import subprocess

def wait_for_service(host, port, service_name, timeout=180):  # Augmenté à 180s
    """Wait for a service to be available."""
    start_time = time.time()
    
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"Connected to {service_name} at {host}:{port}")
                return
        except (socket.timeout, ConnectionRefusedError) as e:
            print(f"Waiting for {service_name} at {host}:{port}... (Error: {str(e)})")
            time.sleep(5)
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise Exception(f"Timeout waiting for {service_name} at {host}:{port} after {timeout} seconds")

# Initialize Spark session with Cassandra connector
spark = SparkSession.builder \
    .appName("FinnhubKafkaStream") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.output.consistency.level", "ONE") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

# Wait for Kafka and Cassandra to be ready
try:
    wait_for_service("kafka", 9092, "Kafka")
    wait_for_service("cassandra", 9042, "Cassandra")
except Exception as e:
    print(f"Failed to connect to services: {str(e)}")
    sys.exit(1)

# Define schema for incoming Kafka data
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("timestamp", LongType(), False)
])

# Read from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "finnhub-raw") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write stream to console for debugging
console_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# Write stream to Cassandra
cassandra_query = parsed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "fintech") \
    .option("table", "trades") \
    .option("checkpointLocation", "/tmp/spark-checkpoint/cassandra") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

console_query.awaitTermination()

# Wait for both streams to terminate
spark.streams.awaitAnyTermination()