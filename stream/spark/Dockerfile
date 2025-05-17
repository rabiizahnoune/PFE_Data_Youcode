FROM apache/spark:3.5.1
# Set working directory
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY stream/spark/requirements.txt /opt/spark-apps/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark-apps/requirements.txt


# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH



# Set working directory
WORKDIR /opt/spark-apps

# Run Spark application
# Run Spark application
CMD ["tail", "-f", "/dev/null"]