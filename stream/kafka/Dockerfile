FROM confluentinc/cp-kafka:7.4.0
USER root
# Copy Kafka configuration
COPY stream/kafka/kafka.properties /etc/kafka/server.properties
# Copy setup script to /usr/bin/
COPY stream/kafka/kafka-setup-k8s.sh /usr/bin/kafka-setup-k8s.sh
RUN chmod +x /usr/bin/kafka-setup-k8s.sh

COPY stream/kafka/requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir -r ./requirements.txt

# Expose Kafka port
EXPOSE 9092
CMD ["/etc/confluent/docker/run"]