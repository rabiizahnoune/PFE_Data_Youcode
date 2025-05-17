#!/bin/bash
# Wait for Kafka to be ready with a timeout
TIMEOUT=180
COUNT=0
echo "Checking Kafka connectivity..."
until nc -zv kafka 9092 2>/dev/null; do
    echo "Kafka not reachable at kafka-0.kafka-service.default.svc.cluster.local:9092, waiting..."
    sleep 5
    COUNT=$((COUNT + 5))
    if [ $COUNT -ge $TIMEOUT ]; then
        echo "Timeout waiting for Kafka"
        exit 1
    fi
done

echo "Kafka is reachable, proceeding with topic creation..."
# Create Kafka topics
kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic gold-news --if-not-exists
kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic gold-price --if-not-exists