#!/bin/bash

# Nom des conteneurs
KAFKA_CONTAINER="gold_price_project-kafka-1"
SPARK_CONTAINER="gold_price_project-spark-1"

echo "🔍 Killing script in Kafka container..."
docker exec "$KAFKA_CONTAINER" pkill -f gold_price.py

echo "🔍 Killing script in Spark container..."
docker exec "$SPARK_CONTAINER" pkill -f gold_price_streaming.py

echo "✅ Scripts killed in both containers (if running)."
